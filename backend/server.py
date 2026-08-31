"""X 下载器后端插件。

以 Flask 蓝图形态运行在框架进程内，复用旧 run.py 的「子进程 + stdin/stdout 协议」：
- 通过 stdin 向 run.py 注入 params 与 context（物化 cookie、回调地址）；
- 后台线程逐行解析 run.py 的 stdout 上报（progress/log/error/await_input/result）；
- 入库回调 /input 长轮询由本蓝图提供，最终通过 host.ingest 登记资源。

run.py 的爬虫逻辑保持不变（仅依赖标准库 + ffmpeg），降低迁移风险。
"""

import os
import sys
import io
import re
import json
import time
import hashlib
import mimetypes
import sqlite3
import uuid
import threading
import concurrent.futures as _cf
import subprocess
import urllib.request
import urllib.error
import urllib.parse
from collections import OrderedDict

from flask import Blueprint, request, g, jsonify, Response, stream_with_context
import importlib.util as _ilu

# X 反爬令牌（x-client-transaction-id）生成。与本页面同包，用相对导入——
# 插件内裸名导入在宿主进程会 ModuleNotFoundError，进而导致整个蓝图 404。
from .x_client_tx import get_transaction_id

# run.py 仅依赖标准库（无重型副作用），可直接 import 复用其 X API 能力。
# 注意：不能用裸 `import run`，否则会和 pixiv 的 run.py 抢占全局 sys.modules['run']，
# 导致先加载的一方被后加载方覆盖（典型症状：module 'run' has no attribute 'get_tweet_thread'）。
# 这里按绝对路径加载到独立模块名，彻底规避模块名冲突。
_run_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'run.py')
_spec = _ilu.spec_from_file_location('x_run', _run_path)
xrun = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(xrun)


class _ProgressStore:
    """按资源身份持久化下载进度（SQLite）。

    下载进度是「资源」的属性而非进程内临时 job：退出/重启后任意平台都能从磁盘
    读回进度，前端不再静默丢失；再点下载时按 resource_key 查重——completed 则
    去重不重复拉取，running 则复用已下文件续传。
    """

    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        with self._lock, sqlite3.connect(db_path) as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS download_progress (
                    resource_key TEXT PRIMARY KEY,
                    platform TEXT,
                    resource_id TEXT,
                    status TEXT,
                    percent INTEGER DEFAULT 0,
                    message TEXT DEFAULT '',
                    files_done INTEGER DEFAULT 0,
                    files_total INTEGER DEFAULT 0,
                    working_dir TEXT,
                    job_id TEXT,
                    updated_at REAL
                )"""
            )
            c.commit()

    def get(self, resource_key):
        with self._lock, sqlite3.connect(self.db_path) as c:
            row = c.execute(
                "SELECT resource_key, platform, resource_id, status, percent, "
                "message, files_done, files_total, working_dir, job_id, updated_at "
                "FROM download_progress WHERE resource_key=?",
                (resource_key,),
            ).fetchone()
        if not row:
            return None
        keys = ('resource_key', 'platform', 'resource_id', 'status', 'percent',
                'message', 'files_done', 'files_total', 'working_dir', 'job_id', 'updated_at')
        return dict(zip(keys, row))

    def _set(self, **kw):
        with self._lock, sqlite3.connect(self.db_path) as c:
            c.execute(
                """INSERT INTO download_progress
                   (resource_key, platform, resource_id, status, percent, message,
                    files_done, files_total, working_dir, job_id, updated_at)
                   VALUES (:resource_key, :platform, :resource_id, :status, :percent,
                           :message, :files_done, :files_total, :working_dir, :job_id, :updated_at)
                   ON CONFLICT(resource_key) DO UPDATE SET
                       status=excluded.status, percent=excluded.percent,
                       message=excluded.message, files_done=excluded.files_done,
                       files_total=excluded.files_total, working_dir=excluded.working_dir,
                       job_id=excluded.job_id, updated_at=excluded.updated_at""",
                kw,
            )
            c.commit()

    def upsert(self, resource_key, platform, resource_id, working_dir=None, job_id=None,
               status='pending', percent=0, message='', files_done=0, files_total=0):
        self._set(resource_key=resource_key, platform=platform, resource_id=resource_id,
                  status=status, percent=percent, message=message,
                  files_done=files_done, files_total=files_total,
                  working_dir=working_dir or '', job_id=job_id or '',
                  updated_at=time.time())

    def update(self, resource_key, percent=None, message=None, status=None,
               files_done=None, files_total=None):
        cur = self.get(resource_key) or {}
        self._set(
            resource_key=resource_key, platform=cur.get('platform'),
            resource_id=cur.get('resource_id'),
            status=status if status is not None else cur.get('status', 'running'),
            percent=percent if percent is not None else cur.get('percent', 0),
            message=message if message is not None else cur.get('message', ''),
            files_done=files_done if files_done is not None else cur.get('files_done', 0),
            files_total=files_total if files_total is not None else cur.get('files_total', 0),
            working_dir=cur.get('working_dir', ''), job_id=cur.get('job_id', ''),
            updated_at=time.time(),
        )

    def mark_completed(self, resource_key, message='下载完成'):
        self.update(resource_key, status='completed', percent=100, message=message)

    def mark_failed(self, resource_key, message='下载失败'):
        self.update(resource_key, status='failed', message=message)

    def mark_cancelled(self, resource_key, message='已取消'):
        self.update(resource_key, status='cancelled', percent=0, message=message)



def create_blueprint(host):
    bp = Blueprint('x', __name__, url_prefix=host.url_prefix)

    # 进程级任务状态（存于 host.app_state，框架不干预内容）
    jobs = host.app_state.setdefault('jobs', {})
    jobs_lock = host.app_state.setdefault('jobs_lock', threading.Lock())
    input_events = host.app_state.setdefault('input_events', {})

    plugin_dir = os.path.dirname(os.path.abspath(__file__))   # .../x/backend
    plugin_root = os.path.dirname(plugin_dir)                 # .../x

    # 按资源身份持久化的下载进度（跨进程、退出不丢）
    _progress_store = _ProgressStore(
        os.path.join(host.data_dir, 'download_progress.db'))

    def _tweet_id_from_url(url):
        if not url:
            return None
        m = re.search(r'(?:x\.com|twitter\.com)/\w+/status/(\d+)', url)
        return m.group(1) if m else None

    def _resource_key(tid):
        return 'x:' + str(tid)

    def _resource_working_dir(tid):
        d = os.path.join(host.data_dir, 'downloads', 'x', str(tid))
        os.makedirs(d, exist_ok=True)
        return d

    def _job_dir(job_id):
        d = os.path.join(host.data_dir, 'jobs', job_id)
        os.makedirs(d, exist_ok=True)
        return d

    def _append_log(job_id, message):
        job = jobs.get(job_id)
        if job is not None:
            job['logs'].append(message)
            if len(job['logs']) > 500:
                job['logs'] = job['logs'][-500:]

    def _bearer():
        auth = request.headers.get('Authorization', '')
        return auth[7:] if auth.startswith('Bearer ') else auth

    def _x_cookie_header():
        """从保险库读取 x.com 原始 cookie，拼成 HTTP Cookie 头。"""
        try:
            rec = host.vault._vault.get_by_domain('x.com', kind='cookie')
        except Exception:
            return ''
        if not rec:
            return ''
        cookies = rec.get('cookies') or []
        return '; '.join(
            f"{c.get('name')}={c.get('value')}" for c in cookies
            if c.get('name') and c.get('value') is not None
        )

    # ---------- 媒体预览缓存（LRU 磁盘缓存，对标 ehentai 下载器） ----------
    # 用户点开的图片/视频预览直接代理下载并落盘缓存，回看命中本地字节，
    # 不再重复访问 twimg；也便于后续"缓存即下载"。
    _CACHE_DIR = os.path.join(host.data_dir, 'media_cache')
    _CACHE_LRU_DIR = os.path.join(_CACHE_DIR, 'lru')        # 字节：<md5(url)><ext>
    _CACHE_INDEX_FILE = os.path.join(_CACHE_DIR, 'lru_index.json')
    _CACHE_MAX_BYTES = 512 * 1024 * 1024                    # 512MB 上限
    os.makedirs(_CACHE_LRU_DIR, exist_ok=True)

    _LRU_LOCK = threading.Lock()
    _lru_meta = OrderedDict()   # key -> {"ext": str, "size": int, "type": str}
    _lru_total = 0

    def _cache_key(url):
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def _cache_load_index():
        nonlocal _lru_meta, _lru_total
        _lru_meta = OrderedDict()
        _lru_total = 0
        try:
            with open(_CACHE_INDEX_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            items = saved.get('items', {})
            for k in saved.get('keys', []):
                if k in items:
                    _lru_meta[k] = items[k]
                    _lru_total += items[k].get('size', 0)
        except Exception:
            # 索引缺失/损坏：按 mtime 重建
            try:
                for fn in os.listdir(_CACHE_LRU_DIR):
                    base, ext = os.path.splitext(fn)
                    if not base:
                        continue
                    sz = os.path.getsize(os.path.join(_CACHE_LRU_DIR, fn))
                    _lru_meta[base] = {'ext': ext, 'size': sz, 'type': 'image'}
                    _lru_total += sz
            except Exception:
                pass

    def _cache_save_index():
        try:
            with open(_CACHE_INDEX_FILE, 'w', encoding='utf-8') as f:
                json.dump({'keys': list(_lru_meta.keys()),
                           'items': _lru_meta}, f)
        except Exception:
            pass

    def _cache_evict():
        nonlocal _lru_total
        while _lru_total > _CACHE_MAX_BYTES and len(_lru_meta) > 1:
            old_key, old_val = _lru_meta.popitem(last=False)
            _lru_total -= old_val.get('size', 0)
            try:
                os.remove(os.path.join(_CACHE_LRU_DIR, old_key + old_val.get('ext', '')))
            except Exception:
                pass
        _cache_save_index()

    def _cache_get(url):
        """命中返回 (path, ext)；并刷新访问顺序。未命中返回 None。"""
        nonlocal _lru_total
        key = _cache_key(url)
        with _LRU_LOCK:
            if key not in _lru_meta:
                return None
            val = _lru_meta.pop(key)
            _lru_meta[key] = val
            path = os.path.join(_CACHE_LRU_DIR, key + val.get('ext', ''))
            if not os.path.exists(path):
                _lru_total -= val.get('size', 0)
                _cache_save_index()
                return None
            return path, val.get('ext', '')

    def _cache_put_file(url, tmp_path, ext):
        """把已落盘的临时文件登记进 LRU 缓存（流式代理用）。

        与 _cache_put 的区别：不要求整份字节已在内存里。视频动辄几十上百 MB，先
        read() 到内存再返回，前端就要等整个文件下载完才出画面（表现为「一直加载」），
        且整份字节驻留内存容易把进程撑爆。流式代理先写 .part，完整读完后原子改名
        登记；中途失败只留临时文件，不会被当成有效缓存。
        """
        nonlocal _lru_total
        key = _cache_key(url)
        path = os.path.join(_CACHE_LRU_DIR, key + ext)
        with _LRU_LOCK:
            if key in _lru_meta:
                _lru_total -= _lru_meta[key].get('size', 0)
                del _lru_meta[key]
            try:
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                return None
            sz = os.path.getsize(path)
            _lru_meta[key] = {'ext': ext, 'size': sz}
            _lru_total += sz
            _cache_evict()
            return path

    # ---------- 媒体边下边播（未缓存完也能拖动进度条） ----------
    # 同一 url 只起一个后台下载线程写 .part；响应从 .part 渐进读取并支持 Range，
    # 这样视频在下载途中也能拖动进度条（拖动即发 Range 请求，按当前已下大小截取）；
    # 完整下载完登记 LRU 缓存，之后走上面的 send_file 分支（同样支持 Range）。
    _media_dl_lock = threading.Lock()
    _media_dl_active = set()
    _media_dl_err = set()

    def _media_ext_ct(url, mtype):
        """按 URL 后缀 / 显式 type 推断扩展名与 mimetype（用于边下边播的响应类型）。"""
        path_ext = os.path.splitext(urlparse(url).path)[1].lower()
        if path_ext == '.m3u8':
            ext = '.m3u8'
        elif mtype == 'video' or path_ext in ('.mp4', '.m4v', '.webm', '.mov'):
            ext = path_ext if path_ext else '.mp4'
        elif mtype == 'image' or path_ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp'):
            ext = path_ext if path_ext else '.jpg'
        elif path_ext:
            ext = path_ext
        else:
            ext = '.mp4' if mtype == 'video' else '.jpg'
        ct = ('application/vnd.apple.mpegurl' if ext == '.m3u8'
              else 'video/mp4' if ext == '.mp4' else mimetypes.guess_type(url)[0] or 'image/jpeg')
        return ext, ct

    def _start_media_download(url, tmp_path, ext):
        """后台把上游 twimg 读到 .part，完成后登记 LRU 缓存；同一 url 并发只跑一个线程。"""
        with _media_dl_lock:
            if url in _media_dl_active:
                return
            _media_dl_active.add(url)
            _media_dl_err.discard(url)
        def run():
            try:
                cookie = _x_cookie_header()
                headers = xrun.build_headers(cookie, with_bearer=True)
                opener = xrun.make_opener(None)
                # SOCKS 只作用于建立连接这一步：连接一旦建立就还原全局 socket
                xrun._apply_socks()
                try:
                    up = opener.open(urllib.request.Request(url, headers=headers), timeout=90)
                finally:
                    xrun._restore_socks()
                with open(tmp_path, 'wb') as f:
                    while True:
                        chunk = up.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                try:
                    up.close()
                except Exception:
                    pass
                if os.path.getsize(tmp_path) > 0:
                    _cache_put_file(url, tmp_path, ext)
            except Exception:
                # 下载失败：清理半成品，别让 .part 污染缓存
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
                with _media_dl_lock:
                    _media_dl_err.add(url)
            finally:
                with _media_dl_lock:
                    _media_dl_active.discard(url)
        threading.Thread(target=run, daemon=True).start()

    def _serve_media_partial(tmp_path, ct, req, url):
        """从正在增长的 .part 渐进读取并响应：支持 Range（进度条可拖动）；
        未带 Range 的首次请求走分块流（连接保持，边下边播）。"""
        range_header = (req.headers.get('Range') or '').strip()
        rm = re.match(r'bytes=(\d+)-(\d*)$', range_header)
        if rm:
            start = int(rm.group(1))
            end = int(rm.group(2)) if rm.group(2) else None
            waited = 0.0
            while not os.path.exists(tmp_path) and waited < 30:
                if url in _media_dl_err:
                    return jsonify({'success': False, 'message': '代理失败'}), 502
                time.sleep(0.1); waited += 0.1
            if url in _media_dl_err and not os.path.exists(tmp_path):
                return jsonify({'success': False, 'message': '代理失败'}), 502
            cur = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            if cur == 0 or start >= cur:
                resp = Response(status=416)
                resp.headers['Content-Range'] = 'bytes */%d' % cur
                resp.headers['Accept-Ranges'] = 'bytes'
                return resp
            actual_end = end if end is not None else cur - 1
            if actual_end >= cur:
                actual_end = cur - 1
            total = actual_end - start + 1

            def gen():
                try:
                    f = open(tmp_path, 'rb')
                except Exception:
                    return
                try:
                    sent = 0
                    while sent < total:
                        if not os.path.exists(tmp_path):
                            return
                        avail = os.path.getsize(tmp_path)
                        readable = avail - (start + sent)
                        if readable <= 0:
                            time.sleep(0.05)
                            continue
                        to_read = min(total - sent, readable)
                        f.seek(start + sent)
                        chunk = f.read(to_read)
                        if not chunk:
                            time.sleep(0.05)
                            continue
                        sent += len(chunk)
                        yield chunk
                finally:
                    try:
                        f.close()
                    except Exception:
                        pass
            resp = Response(stream_with_context(gen()), status=206, mimetype=ct)
            resp.headers['Accept-Ranges'] = 'bytes'
            resp.headers['Content-Range'] = 'bytes %d-%d/%d' % (start, start + total - 1, cur)
            resp.headers['Content-Length'] = str(total)
            resp.headers['Cache-Control'] = 'private, max-age=86400'
            resp.headers['Content-Disposition'] = 'inline'
            return resp

        # 未带 Range：分块流，连接保持到下载完成（或客户端断开 / 下载失败）
        def gen():
            waited = 0.0
            while not os.path.exists(tmp_path) and waited < 30:
                if url in _media_dl_err:
                    return
                time.sleep(0.1); waited += 0.1
            try:
                f = open(tmp_path, 'rb')
            except Exception:
                return
            try:
                pos = 0
                while True:
                    if not os.path.exists(tmp_path):
                        break  # 下载完成（.part 已改名）或失败被清理
                    cur = os.path.getsize(tmp_path)
                    if pos < cur:
                        f.seek(pos)
                        chunk = f.read(cur - pos)
                        if chunk:
                            pos += len(chunk)
                            yield chunk
                            continue
                    time.sleep(0.1)
            finally:
                try:
                    f.close()
                except Exception:
                    pass
        resp = Response(stream_with_context(gen()), mimetype=ct)
        resp.headers['Accept-Ranges'] = 'bytes'
        resp.headers['Cache-Control'] = 'private, max-age=86400'
        resp.headers['Content-Disposition'] = 'inline'
        return resp

    # ---------- 本地 X 收藏夹（SQLite，独立于 X 账号，持久化快照） ----------
    _folder_db_path = os.path.join(host.data_dir, 'x_bookmarks.db')
    _folder_lock = threading.Lock()

    # 启动时恢复媒体缓存索引（访问顺序 + 总量），避免重启后重复下载已缓存资源
    _cache_load_index()

    def _folder_conn():
        conn = sqlite3.connect(_folder_db_path, timeout=10)
        conn.execute(
            '''CREATE TABLE IF NOT EXISTS bookmarks(
                tweet_id TEXT PRIMARY KEY,
                screen_name TEXT,
                author_name TEXT,
                avatar TEXT,
                text TEXT,
                created_at TEXT,
                media TEXT,
                url TEXT,
                added_at TEXT,
                collection TEXT
            )''')
        # 兼容旧库：若缺 collection 列则补齐
        try:
            cols = [r[1] for r in conn.execute('PRAGMA table_info(bookmarks)').fetchall()]
            if 'collection' not in cols:
                conn.execute('ALTER TABLE bookmarks ADD COLUMN collection TEXT')
                conn.commit()
        except Exception:
            pass
        return conn

    def _folder_list(collection=None):
        conn = _folder_conn()
        try:
            if collection:
                rows = conn.execute(
                    'SELECT tweet_id, screen_name, author_name, avatar, text, '
                    'created_at, media, url, added_at, collection FROM bookmarks '
                    'WHERE collection=? ORDER BY added_at DESC', (collection,)).fetchall()
            else:
                rows = conn.execute(
                    'SELECT tweet_id, screen_name, author_name, avatar, text, '
                    'created_at, media, url, added_at, collection FROM bookmarks '
                    'ORDER BY added_at DESC').fetchall()
        finally:
            conn.close()
        return [{
            'tweet_id': r[0], 'text': r[4], 'created_at': r[5],
            'media': json.loads(r[6]) if r[6] else [], 'url': r[7],
            'added_at': r[8], 'collection': r[9],
            'author': {
                'screen_name': r[1], 'name': r[2], 'avatar': r[3],
            },
        } for r in rows]

    def _folder_collections():
        """返回所有已使用的集合名。"""
        conn = _folder_conn()
        try:
            rows = conn.execute(
                "SELECT DISTINCT collection FROM bookmarks "
                "WHERE collection IS NOT NULL AND collection != '' "
                "ORDER BY collection").fetchall()
        finally:
            conn.close()
        return [r[0] for r in rows]

    # ---------- 本地浏览历史（P1-7） ----------
    def _history_conn():
        conn = sqlite3.connect(_folder_db_path, timeout=10)
        conn.execute(
            '''CREATE TABLE IF NOT EXISTS browse_history(
                tweet_id TEXT PRIMARY KEY,
                screen_name TEXT,
                author_name TEXT,
                avatar TEXT,
                text TEXT,
                created_at TEXT,
                media TEXT,
                url TEXT,
                viewed_at TEXT
            )''')
        return conn

    def _history_list(limit=100):
        conn = _history_conn()
        try:
            rows = conn.execute(
                'SELECT tweet_id, screen_name, author_name, avatar, text, '
                'created_at, media, url, viewed_at FROM browse_history '
                'ORDER BY viewed_at DESC LIMIT ?', (limit,)).fetchall()
        finally:
            conn.close()
        return [{
            'tweet_id': r[0], 'text': r[4], 'created_at': r[5],
            'media': json.loads(r[6]) if r[6] else [], 'url': r[7],
            'viewed_at': r[8],
            'author': {
                'screen_name': r[1], 'name': r[2], 'avatar': r[3],
            },
        } for r in rows]

    def _history_add(item):
        author = (item.get('author') or {})
        screen_name = author.get('screen_name') or ''
        author_name = author.get('name') or screen_name
        avatar = author.get('avatar') or ''
        media = item.get('media') or []
        tid = item.get('tweet_id')
        if not tid:
            return
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        with _folder_lock:
            conn = _history_conn()
            try:
                conn.execute(
                    'INSERT OR REPLACE INTO browse_history'
                    '(tweet_id, screen_name, author_name, avatar, text, '
                    'created_at, media, url, viewed_at) VALUES (?,?,?,?,?,?,?,?,?)',
                    (tid, screen_name, author_name, avatar, item.get('text'),
                     item.get('created_at'), json.dumps(media, ensure_ascii=False),
                     item.get('url'), now))
                conn.commit()
            finally:
                conn.close()

    def _history_delete(tweet_id):
        with _folder_lock:
            conn = _history_conn()
            try:
                conn.execute('DELETE FROM browse_history WHERE tweet_id=?', (tweet_id,))
                conn.commit()
            finally:
                conn.close()

    def _history_clear():
        with _folder_lock:
            conn = _history_conn()
            try:
                conn.execute('DELETE FROM browse_history')
                conn.commit()
            finally:
                conn.close()

    def _ingest_files(job_id, files):
        job = jobs.get(job_id)
        if not job:
            return
        # 同一条帖子（同一 group）下的资源索引收集，用于入库后统一生成帖子
        post_groups = {}

        def _group_meta(f):
            return {
                'group': f.get('group'),
                'title': f.get('post_title'),
                'content': f.get('content'),
                'source_url': f.get('source_url'),
                'author_name': f.get('author_name'),
                'author_url': f.get('author_url'),
            }

        for f in (files or []):
            path = f.get('path')
            if not path or not (os.path.isfile(path) or os.path.isdir(path)):
                # 演示占位文件可能不存在，跳过
                continue
            kind = f.get('type')  # video / gallery / document
            modes = f.get('target_modes') or (
                ['video', 'post'] if kind == 'video' else ['gallery', 'post'])
            hid = f.get('hidden', job.get('hidden', True))
            meta = {k: f.get(k) for k in (
                'content', 'post_title', 'source_url',
                'author_name', 'author_url', 'caption', 'group'
            ) if f.get(k) is not None}
            try:
                res = host.ingest(
                    job['library_id'], path,
                    kind=kind, modes=tuple(modes),
                    hidden=hid, meta=meta, owner_id=job['owner_id'],
                )
                if isinstance(res, dict) and not res.get('success', True):
                    _append_log(job_id, '入库失败: ' + str(res.get('message', res)))
                    continue
                _append_log(job_id, '已入库: ' + os.path.basename(path))
                ri_id = res.get('resource_index_id') if isinstance(res, dict) else None
                if ri_id and f.get('group'):
                    g = post_groups.setdefault(f['group'], _group_meta(f))
                    g.setdefault('resource_index_ids', []).append(ri_id)
            except Exception as e:
                _append_log(job_id, '入库失败: ' + str(e))

        # 入库完成后，按 group 聚合资源生成帖子（帖子的资源默认不进其他库，
        # 与帖子绑定紧密；通过 library_id 限定仅在所属库可见）
        for group, g in post_groups.items():
            try:
                r = host.upsert_post_by_group(
                    group_key=group,
                    title=g.get('title'),
                    content=g.get('content') or '',
                    resource_index_ids=g.get('resource_index_ids', []),
                    user_id=job['owner_id'],
                    author_name=g.get('author_name'),
                    author_url=g.get('author_url'),
                    source_url=g.get('source_url'),
                    library_id=job['library_id'],
                )
                if isinstance(r, dict) and r.get('success'):
                    _append_log(job_id, '已生成帖子(group=%s)' % group)
                else:
                    _append_log(job_id, '生成帖子失败: ' + str(r))
            except Exception as e:
                _append_log(job_id, '生成帖子失败: ' + str(e))

    @bp.route('/run', methods=['POST'])
    @host.login_required
    def run():
        data = request.get_json(force=True, silent=True) or {}
        params = data.get('params', {}) or {}
        library_id = data.get('library_id') or params.get('library_id')
        print(f'[x_downloader] /run library_id={library_id!r} data_keys={list(data.keys())}', flush=True)
        owner_id = data.get('owner_id', getattr(g, 'user_id', None))
        token = _bearer()
        job_id = uuid.uuid4().hex
        _append_log(job_id, f'[diag] received library_id={library_id!r}')

        # 资源身份：下载进度归属键（退出/重启后仍可按此查回）
        resource_id = _tweet_id_from_url(params.get('url') or '')
        resource_key = _resource_key(resource_id) if resource_id else None

        # 查重：已完成则去重不重复拉取；running/failed 可续传（复用已下文件重新拉起）
        if resource_key:
            existing = _progress_store.get(resource_key)
            if existing and existing['status'] == 'completed':
                return jsonify({'success': True, 'already_done': True,
                                'resource_key': resource_key,
                                'message': '该推文已下载完成'})
            wd = _resource_working_dir(resource_id)   # 固定目录：续传复用已下文件
            _progress_store.upsert(resource_key, 'x', resource_id,
                                   working_dir=wd, job_id=job_id,
                                   status='running', percent=0, message='已启动')
        else:
            wd = _job_dir(job_id)

        # 物化 cookie（插件按域名从保险库取，run.py 读取文件）
        # 直接通过 host.vault._vault.get_by_domain 拿原始 cookie 列表自行构建 Netscape 文本，
        # 绕过 _VaultProxy.get 的 token 优先级 / 过期 _cache 逻辑（实测会丢失 auth_token/ct0）。
        cookies_ctx = {}
        cookie_str = ''
        try:
            _rec = host.vault._vault.get_by_domain('x.com', kind='cookie')
            if _rec:
                _cookies = _rec.get('cookies') or []
                _lines = []
                for _c in _cookies:
                    _n = _c.get('name', '')
                    _val = _c.get('value', '')
                    if _n and _val:
                        _lines.append(f"{_c.get('domain','')}\tTRUE\t/\t{_c.get('path','/')}\t"
                                      f"{_c.get('secure','FALSE')}\t0\t{_n}\t{_val}")
                cookie_str = '\n'.join(_lines)
        except Exception as _e:
            import sys as _sys
            print(f'[cookie load err] {_e}', file=_sys.stderr, flush=True)
        # [调试] 打印从保险库实际读到的 x.com Cookie 关键字段，便于与浏览器复制的对比
        if cookie_str:
            has_auth = 'auth_token=' in cookie_str
            has_ct0 = 'ct0=' in cookie_str
            _append_log(job_id, f'[Cookie 诊断] vault.get 长度={len(cookie_str)} '
                                f'含auth_token={has_auth} 含ct0={has_ct0} '
                                f'首段={cookie_str[:40]!r} 尾段={cookie_str[-40:]!r}')
            import sys as _sys
            print(f'[Cookie 诊断-STDERR] len={len(cookie_str)} auth={has_auth} ct0={has_ct0}', file=_sys.stderr, flush=True)
            try:
                from shared.credential_vault import data_dir_for
                print(f'[VAULT-DIR] data_dir_for={data_dir_for()}', file=_sys.stderr, flush=True)
                _tk = host.vault._vault.get_token(domain='x.com')
                print(f'[VAULT-TOKEN] get_token(x.com)={repr(_tk)[:60]}', file=_sys.stderr, flush=True)
            except Exception as e:
                print(f'[VAULT-DIR] err {e}', file=_sys.stderr, flush=True)
            # 打印 vault 里 x.com 记录的原始结构（解密后）
            try:
                rec0 = host.vault._vault.get_by_domain('x.com', kind='cookie')
                if rec0:
                    ck = rec0.get('cookies') or []
                    raw = rec0.get('_raw')
                    print(f'[VAULT-RAW] cookies_count={len(ck)} has_raw={bool(raw)} raw_len={len(raw) if raw else 0}', file=_sys.stderr, flush=True)
                    if ck:
                        names = [c.get('name') for c in ck]
                        print(f'[VAULT-NAMES] {names}', file=_sys.stderr, flush=True)
                        for c in ck:
                            if c.get('name') in ('auth_token', 'ct0'):
                                vv = c.get('value')
                                print(f'[VAULT-VAL] {c.get("name")} vlen={len(vv) if vv else 0} pre={str(vv)[:20]!r}', file=_sys.stderr, flush=True)
                else:
                    print('[VAULT-RAW] rec0 is None', file=_sys.stderr, flush=True)
            except Exception as e:
                print(f'[VAULT-RAW] err {e}', file=_sys.stderr, flush=True)
            cookie_path = os.path.join(wd, 'x.com.cookie.txt')
            with open(cookie_path, 'w', encoding='utf-8') as f:
                f.write(cookie_str)
            cookies_ctx['x.com'] = {'path': cookie_path}
        else:
            _append_log(job_id, '[Cookie 诊断] vault.get 返回空（未配置或解密失败）')

        context = {
            'working_dir': wd,
            'notify': {
                'url': request.url_root.rstrip('/') + bp.url_prefix
                       + '/notify?job_id=' + job_id,
                'token': token,
            },
            'cookies': cookies_ctx,
            # 媒体缓存目录：下载时若 twimg URL 已缓存（用户预览时已下载），
            # 直接复用本地字节，不再重复访问 twimg（P0-2 缓存即下载）
            'media_cache_dir': _CACHE_LRU_DIR,
        }
        payload = {'params': params, 'context': context}

        try:
            proc = subprocess.Popen(
                [sys.executable, 'run.py'],
                cwd=plugin_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', bufsize=1,
            )
        except Exception as e:
            return jsonify({'success': False, 'message': '启动子进程失败: ' + str(e)}), 500

        proc.stdin.write(json.dumps(payload, ensure_ascii=False) + '\n')
        proc.stdin.close()

        with jobs_lock:
            jobs[job_id] = {
                'percent': 0, 'message': '已启动', 'logs': [],
                'done': False, 'error': None,
                'pending_input': None, 'input_response': None,
                'proc': proc, 'wd': wd, 'resource_key': resource_key,
                'library_id': library_id, 'owner_id': owner_id,
                'hidden': bool(params.get('hidden', True)),
            }
            input_events[job_id] = threading.Event()

        def reader():
            try:
                for line in proc.stdout:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        _append_log(job_id, line)
                        continue
                    t = obj.get('type')
                    if t == 'progress':
                        jobs[job_id]['percent'] = int(obj.get('percent', 0))
                        jobs[job_id]['message'] = obj.get('message', '')
                        if resource_key:
                            _progress_store.update(resource_key,
                                percent=int(obj.get('percent', 0)),
                                message=obj.get('message', ''))
                    elif t == 'log':
                        _append_log(job_id, obj.get('message', ''))
                    elif t == 'error':
                        jobs[job_id]['error'] = obj.get('message')
                        _append_log(job_id, 'ERROR: ' + obj.get('message', ''))
                        if resource_key:
                            _progress_store.mark_failed(resource_key, obj.get('message', '下载失败'))
                    elif t == 'await_input':
                        # 暂停读取，等待前端通过 /input 回写选择（与 run.py 的长轮询对齐）。
                        # 预览模式（input.type="preview"）等待用户浏览确认，不受 30s 超时限制；
                        # 其余选择型交互沿用原有 30s 超时。
                        inp = obj.get('input') or {}
                        jobs[job_id]['pending_input'] = inp
                        # 解析阶段已完成（进入预览/选择交互），进度应标满
                        jobs[job_id]['percent'] = 100
                        jobs[job_id]['message'] = inp.get('title') or '解析完成'
                        wait = None if inp.get('type') == 'preview' else 30
                        input_events[job_id].wait(timeout=wait)
                        jobs[job_id]['pending_input'] = None
                    elif t == 'result':
                        # 降级路径：run.py 直接带 files（未走 /notify）
                        _ingest_files(job_id, obj.get('files', []))
                        if resource_key:
                            _progress_store.mark_completed(resource_key, '下载完成')
            except Exception as e:
                jobs[job_id]['error'] = str(e)
            finally:
                try:
                    proc.stdout.close()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=60)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                jobs[job_id]['done'] = True

        threading.Thread(target=reader, daemon=True).start()
        return jsonify({'success': True, 'job_id': job_id})

    @bp.route('/notify', methods=['POST'])
    @host.login_required
    def notify():
        data = request.get_json(force=True, silent=True) or {}
        job_id = request.args.get('job_id')
        _ingest_files(job_id, data.get('files', []))
        return jsonify({'success': True})

    @bp.route('/input', methods=['GET', 'POST'])
    @host.login_required
    def input_endpoint():
        job_id = request.args.get('job_id') or (
            (request.get_json(silent=True) or {}).get('job_id'))
        with jobs_lock:
            job = jobs.get(job_id)
            if not job:
                return ('', 204)
            if request.method == 'POST':
                val = (request.get_json(force=True, silent=True) or {}).get('value')
                job['input_response'] = val
                input_events[job_id].set()
                return jsonify({'success': True})
            # GET：run.py 长轮询等待用户选择
            if job.get('input_response') is not None:
                resp = job['input_response']
                job['input_response'] = None
                return jsonify(resp)
            return ('', 204)

    @bp.route('/status', methods=['GET'])
    @host.login_required
    def status():
        job_id = request.args.get('job_id')
        resource_key = request.args.get('resource_key')
        # 优先按 resource_key 从持久进度读（退出后仍可取回）
        if not job_id and resource_key:
            rec = _progress_store.get(resource_key)
            if not rec:
                return jsonify({'success': False, 'message': '任务不存在'}), 404
            return jsonify({
                'success': True,
                'resource_key': resource_key,
                'percent': rec.get('percent', 0),
                'message': rec.get('message', ''),
                'done': rec.get('status') in ('completed', 'failed', 'cancelled'),
                'status': rec.get('status'),
                'error': rec.get('message') if rec.get('status') == 'failed' else None,
            })
        job = jobs.get(job_id)
        if not job:
            return jsonify({'success': False, 'message': '任务不存在'}), 404
        return jsonify({
            'success': True,
            'job_id': job_id,
            'percent': job['percent'],
            'message': job['message'],
            'logs': job['logs'][-200:],
            'done': job['done'],
            'error': job['error'],
            'pending_input': job.get('pending_input'),
        })

    @bp.route('/progress', methods=['GET'])
    @host.login_required
    def progress():
        """前端点下载前查询：返回该推文的持久化下载状态（去重/续传判断依据）。"""
        tid = request.args.get('tweet_id') or _tweet_id_from_url(request.args.get('url', ''))
        if not tid:
            return jsonify({'success': False, 'message': '缺少 tweet_id'}), 400
        resource_key = _resource_key(tid)
        rec = _progress_store.get(resource_key)
        if not rec:
            return jsonify({'success': True, 'resource_key': resource_key, 'status': 'none'})
        return jsonify({
            'success': True,
            'resource_key': resource_key,
            'status': rec.get('status'),
            'percent': rec.get('percent', 0),
            'message': rec.get('message', ''),
            'working_dir': rec.get('working_dir'),
        })

    @bp.route('/timeline', methods=['GET'])
    @host.login_required
    def timeline():
        """拉取 X 关注中（Following）时间线——只含用户真实关注的人。

        （之前误用 For You / HomeTimeline，会混入大量推荐与广告，已改用
        HomeLatestTimeline 关注流。）
        """
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        try:
            count = min(int(request.args.get('count', 20)), 50)
            cursor = request.args.get('cursor') or None
            items, next_cursor = xrun.list_following_timeline(cookie, count, cursor)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取 X 关注流失败: ' + str(e)}), 502

        # 服务端为唯一真相源：拉取结果先并入服务端缓存（union_by_id 去重、封顶 400），
        # 首次加载（无 cursor）时返回「服务端合并后的完整列表」，使任何设备打开
        # 刷新看到的都是同一份，而不是各自 localStorage 里分叉的那份。
        # 翻页（带 cursor）时只回本次新拉取的一页，避免每次回传 400 条。
        # union_by_id 只认规范记录 { id, order, ...载荷 }（通用状态层字段名无关），
        # 故在入口把 X 领域字段映射成 id / order；领域字段原样保留，渲染照常读取。
        # 键位与前端 DBoxState.feed('main') 对齐（feed:main:items / :cursor），
        # 前后端共用同一份服务端真相，不另存一份 cache。
        norm = []
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            rec = dict(it)
            tid = it.get('tweet_id')
            rec['id'] = str(tid if tid is not None else (it.get('id') or ''))
            rec['order'] = it.get('created_at')
            norm.append(rec)
        canonical = None
        try:
            merged = host.state.put('feed:main:items', norm, strategy='union_by_id', cap=400)
            if isinstance(merged, dict) and isinstance(merged.get('value'), list):
                canonical = merged['value']
            if next_cursor:
                host.state.put('feed:main:cursor', next_cursor, strategy='max')
        except Exception:
            canonical = None   # 状态服务不可用时退化为只返回本次结果

        return jsonify({'success': True,
                        'items': canonical if (canonical is not None and not cursor) else items,
                        'next_cursor': next_cursor,
                        'canonical': canonical is not None and not cursor})

    @bp.route('/check', methods=['GET'])
    def check():
        return jsonify({'success': True, **_check_x_session()})

    def _check_x_session():
        """实时校验 X 登录态：缺/残缺 Cookie 直接判否；字段齐全再拉 1 条关注流确认真实有效。

        返回 dict：{ok, reason, message, has_auth_token, has_ct0, newest_time?}
          - no_cookie    : 凭证库无 x.com Cookie
          - incomplete   : 有 Cookie 但缺 auth_token 或 ct0（X 必 401）
          - expired      : 字段齐全但实时请求被判为鉴权失败（会话过期）
          - error        : 实时请求抛其他异常
          - ok_empty     : 字段齐全且接口可达，但本次未返回内容（可能无新动态）
          - ok           : 登录态有效，附最新一条推文时间
        """
        rec = host.vault._vault.get_by_domain('x.com', kind='cookie')
        if not rec:
            return {'ok': False, 'reason': 'no_cookie',
                    'message': '未配置 x.com 登录 Cookie，请在凭证库注册后再用',
                    'has_auth_token': False, 'has_ct0': False}
        cookies = rec.get('cookies') or []
        header = '; '.join(
            f"{c.get('name')}={c.get('value')}" for c in cookies
            if c.get('name') and c.get('value') is not None)
        if not header:
            return {'ok': False, 'reason': 'no_cookie',
                    'message': 'x.com Cookie 为空，请在凭证库重新注册',
                    'has_auth_token': False, 'has_ct0': False}
        has_at = bool(xrun._has_auth_token(header))
        has_ct0 = bool(xrun._extract_ct0(header))
        if not (has_at and has_ct0):
            missing = []
            if not has_at:
                missing.append('auth_token')
            if not has_ct0:
                missing.append('ct0')
            return {'ok': False, 'reason': 'incomplete',
                    'message': 'X Cookie 残缺（缺少 ' + '/'.join(missing)
                               + '），登录态无效，请从浏览器复制完整 Cookie 重新覆盖到凭证库',
                    'has_auth_token': has_at, 'has_ct0': has_ct0}
        # 字段齐全 → 真实探测一次关注流
        try:
            items, _ = xrun.list_following_timeline(header, 1)
        except Exception as e:
            s = str(e)
            if '401' in s or '403' in s or 'Authorization' in s or 'auth' in s.lower():
                return {'ok': False, 'reason': 'expired',
                        'message': 'X 登录态已过期（Cookie 字段齐全但会话失效），请重新登录 x.com 并覆盖凭证库',
                        'has_auth_token': True, 'has_ct0': True}
            return {'ok': False, 'reason': 'error', 'message': '探测 X 失败: ' + s,
                    'has_auth_token': True, 'has_ct0': True}
        if not items:
            return {'ok': True, 'reason': 'ok_empty',
                    'message': 'X Cookie 字段齐全且接口可达，本次未返回内容（可能无新动态）',
                    'has_auth_token': True, 'has_ct0': True}
        newest = items[0].get('created_at')
        return {'ok': True, 'reason': 'ok', 'message': 'X 登录态有效',
                'has_auth_token': True, 'has_ct0': True, 'newest_time': newest}


    @bp.route('/search', methods=['GET'])
    @host.login_required
    def search():
        """按关键词/用户句柄搜索 X 推文（SearchTimeline）。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        if not (request.args.get('q') or '').strip():
            return jsonify({'success': False,
                            'message': '缺少搜索关键词'}), 400
        try:
            count = min(int(request.args.get('count', 20)), 50)
            cursor = request.args.get('cursor') or None
            product = (request.args.get('product') or 'Top').strip()
            # 搜索接口必须带 X 的反爬令牌，否则一律 404；令牌按最终请求路径现算。
            # 抓首页取令牌材料要用「网页浏览」那套头（不带 Bearer，否则 /home 会 401）
            _home_headers = xrun.build_headers(cookie, with_bearer=False)

            def _txid(method, path):
                return get_transaction_id(_home_headers, method, path, ua=xrun.UA)

            items, next_cursor = xrun.search_tweets(
                cookie, request.args.get('q').strip(), count, cursor, product,
                txid_func=_txid)
        except Exception as e:
            # 404 基本都指向 X 的反爬校验（拿不到令牌或令牌不被接受），
            # 给个能定位方向的提示，避免用户只看到一个干巴巴的状态码
            if '404' in str(e):
                return jsonify({'success': False, 'message': (
                    '搜索失败: X 拒绝了请求（404）。通常是反爬令牌失效或 X 前端改版，'
                    '可稍后重试；若持续出现，请检查凭证库里的 x.com Cookie 是否仍有效。')}), 502
            return jsonify({'success': False,
                            'message': '搜索失败: ' + str(e)}), 502
        return jsonify({'success': True, 'items': items,
                        'next_cursor': next_cursor})

    @bp.route('/tweet/<tweet_id>', methods=['GET'])
    @host.login_required
    def tweet_detail(tweet_id):
        """拉取单条推文详情 + 评论区（对话线程）。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        try:
            cursor = request.args.get('cursor') or None
            res = xrun.get_tweet_thread(tweet_id, cookie, cursor)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取推文详情失败: ' + str(e)}), 502
        return jsonify({'success': True, **res})

    @bp.route('/history', methods=['GET', 'POST', 'DELETE'])
    @host.login_required
    def history():
        """本地浏览历史（P1-7）：记录/查询/删除浏览过的推文。"""
        if request.method == 'POST':
            item = request.get_json(force=True, silent=True) or {}
            _history_add(item)
            return jsonify({'success': True})
        if request.method == 'DELETE':
            tid = (request.args.get('tweet_id') or '').strip()
            if request.args.get('clear'):
                _history_clear()
            elif tid:
                _history_delete(tid)
            return jsonify({'success': True})
        # GET 列表
        limit = min(int(request.args.get('limit', 100)), 300)
        return jsonify({'success': True, 'items': _history_list(limit)})

    @bp.route('/bookmarks', methods=['GET'])
    @host.login_required
    def bookmarks():
        """实时从 X 账号收藏夹拉取推文列表。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        try:
            count = min(int(request.args.get('count', 30)), 100)
            cursor = request.args.get('cursor') or None
            items, next_cursor = xrun.list_bookmarks(cookie, count, cursor)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取 X 收藏失败: ' + str(e)}), 502
        return jsonify({'success': True, 'items': items,
                        'next_cursor': next_cursor})

    @bp.route('/bookmarks/folder', methods=['GET'])
    @host.login_required
    def folder_list():
        """列出本地 X 收藏夹（dbox 持久化的收藏快照）。支持 ?collection= 过滤。"""
        collection = (request.args.get('collection') or '').strip() or None
        return jsonify({'success': True, 'items': _folder_list(collection)})

    @bp.route('/bookmarks/collections', methods=['GET'])
    @host.login_required
    def folder_collections():
        """返回所有收藏集合名。"""
        return jsonify({'success': True, 'items': _folder_collections()})

    @bp.route('/bookmarks/folder', methods=['POST'])
    @host.login_required
    def folder_add():
        """将一条 X 收藏加入本地收藏夹。"""
        data = request.get_json(force=True, silent=True) or {}
        tid = data.get('tweet_id')
        if not tid:
            return jsonify({'success': False, 'message': '缺少 tweet_id'}), 400
        media = data.get('media') or []
        # 兼容嵌套 author（来自 list_bookmarks）与扁平字段两种入参
        author = data.get('author') or {}
        screen_name = (data.get('screen_name')
                       or author.get('screen_name') or '')
        author_name = (data.get('author_name')
                       or author.get('name') or screen_name)
        avatar = (data.get('avatar')
                  or author.get('avatar') or '')
        url = (data.get('url')
               or (f'https://x.com/{screen_name}/status/{tid}'
                   if screen_name else ''))
        added = time.strftime('%Y-%m-%d %H:%M:%S')
        collection = (data.get('collection') or '').strip() or None
        with _folder_lock:
            conn = _folder_conn()
            try:
                conn.execute(
                    'INSERT OR REPLACE INTO bookmarks'
                    '(tweet_id, screen_name, author_name, avatar, text, '
                    'created_at, media, url, added_at, collection) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (tid, screen_name, author_name, avatar, data.get('text'),
                     data.get('created_at'), json.dumps(media, ensure_ascii=False),
                     url, added, collection))
                conn.commit()
            finally:
                conn.close()
        return jsonify({'success': True, 'items': _folder_list()})

    @bp.route('/bookmarks/folder/<tweet_id>', methods=['DELETE'])
    @host.login_required
    def folder_del(tweet_id):
        """从本地收藏夹删除一条收藏。"""
        with _folder_lock:
            conn = _folder_conn()
            try:
                conn.execute('DELETE FROM bookmarks WHERE tweet_id=?', (tweet_id,))
                conn.commit()
            finally:
                conn.close()
        return jsonify({'success': True, 'items': _folder_list()})

    @bp.route('/media', methods=['GET'])
    def media():  # 不要求 login_required：浏览器 <img>/<video> 原生加载不能带 Authorization 头
        """代理下载 X 媒体（twimg 图片 / mp4 视频），并写入本地 LRU 缓存。

        query: u=原始URL, type=image|video（缺省自动判断）
        命中缓存 → 返回本地字节（支持 Range，视频可拖动进度条）。
        未命中 → 带 X cookie 请求上游 twimg，写缓存后返回。
        这样用户"点开的图片/视频"即落到本地缓存，回看不再访问 twimg。
        """
        from flask import send_file
        url = (request.args.get('u') or '').strip()
        if not url or not url.startswith('http'):
            return jsonify({'success': False, 'message': '缺少合法的 u 参数'}), 400
        # 只允许 twimg 域名，防任意 URL 代理（SSRF）
        from urllib.parse import urlparse
        hostname = urlparse(url).hostname or ''
        if not (hostname.endswith('twimg.com') or hostname in ('x.com', 'twitter.com')):
            return jsonify({'success': False, 'message': '仅允许代理 twimg 媒体'}), 403

        mtype = (request.args.get('type') or '').lower()
        # 命中缓存：直接用本地文件响应（send_file 自动支持 Range）
        hit = _cache_get(url)
        if hit is not None:
            path, ext = hit
            if os.path.isfile(path):
                # m3u8 不在 mimetypes 默认表里，必须显式兜底：否则缓存命中时会被
                # 当成 image/jpeg 返回，hls.js 拿不到播放列表。
                ct = mimetypes.guess_type(path)[0] or (
                    'application/vnd.apple.mpegurl' if ext == '.m3u8'
                    else 'video/mp4' if ext == '.mp4' else 'image/jpeg')
                resp = send_file(path, mimetype=ct, conditional=True,
                                 max_age=86400)
                resp.headers['Cache-Control'] = 'private, max-age=86400'
                return resp

        # 未命中：后台把上游读到 .part，响应从 .part 渐进读取并支持 Range，
        # 实现「边下边播 + 进度条可拖动」。完整下载完登记 LRU 缓存，之后走上面的
        # send_file 分支（同样支持 Range）。缓存命中（已下完）走上面的 send_file 分支。
        ext, ct = _media_ext_ct(url, mtype)
        tmp_path = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext + '.part')
        # 同一 url 已失败过：直接报错，避免再次无谓等待
        with _media_dl_lock:
            if url in _media_dl_err:
                return jsonify({'success': False, 'message': '代理失败（已记录）'}), 502
        _start_media_download(url, tmp_path, ext)
        return _serve_media_partial(tmp_path, ct, request, url)

    @bp.route('/media/cache', methods=['GET', 'DELETE'])
    @host.login_required
    def media_cache():
        """媒体缓存管理（P2-9）：GET 列表 / DELETE 清空。"""
        if request.method == 'DELETE':
            nonlocal _lru_total
            with _LRU_LOCK:
                for fn in os.listdir(_CACHE_LRU_DIR):
                    try:
                        os.remove(os.path.join(_CACHE_LRU_DIR, fn))
                    except Exception:
                        pass
                _lru_meta.clear()
                _lru_total = 0
            _cache_save_index()
            return jsonify({'success': True})
        # GET 列表
        items = []
        try:
            for fn in os.listdir(_CACHE_LRU_DIR):
                p = os.path.join(_CACHE_LRU_DIR, fn)
                if os.path.isfile(p):
                    ext = os.path.splitext(fn)[1].lower()
                    items.append({
                        'file': fn,
                        'ext': ext,
                        'size': os.path.getsize(p),
                        'mtime': os.path.getmtime(p),
                        'type': 'video' if ext in ('.mp4', '.webm', '.mov') else 'image',
                    })
        except Exception:
            pass
        items.sort(key=lambda x: -x['mtime'])
        total = sum(x['size'] for x in items)
        return jsonify({'success': True, 'items': items, 'count': len(items),
                        'total_bytes': total})

    return bp
