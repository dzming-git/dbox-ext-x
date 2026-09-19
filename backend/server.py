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
from datetime import datetime, timezone

from flask import Blueprint, request, g, jsonify, Response, stream_with_context
import importlib.util as _ilu

# X 反爬令牌（x-client-transaction-id）生成。与本页面同包，用相对导入——
# 插件内裸名导入在宿主进程会 ModuleNotFoundError，进而导致整个蓝图 404。
from .x_client_tx import get_transaction_id, invalidate

# run.py 仅依赖标准库（无重型副作用），可直接 import 复用其 X API 能力。
# 注意：不能用裸 `import run`，否则会和 pixiv 的 run.py 抢占全局 sys.modules['run']，
# 导致先加载的一方被后加载方覆盖（典型症状：module 'run' has no attribute 'get_tweet_thread'）。
# 这里按绝对路径加载到独立模块名，彻底规避模块名冲突。
_run_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'run.py')
_spec = _ilu.spec_from_file_location('x_run', _run_path)
xrun = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(xrun)

# 缓存层（列表 / 资源本体两套）按绝对路径加载：backend 不是正式包，避免相对导入不确定性
_cs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache_store.py')
_cs_spec = _ilu.spec_from_file_location('x_cache_store', _cs_path)
cache_store = _ilu.module_from_spec(_cs_spec)
_cs_spec.loader.exec_module(cache_store)


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

    # 向框架登记本插件任务支持的动作（框架据此提供入口，不登记即判定不支持）：
    # resume=从中断处继续，retry=失败后从头重跑。
    try:
        host.tasks.register_resume('/search/resume')
        host.tasks.register_retry('/search/rerun')
    except Exception:
        pass

    # ---- 缓存层（列表 / 资源本体两套，见 cache_store.py 顶部说明）----
    # 落在扩展私有 SQLite（host.db('cache')）：进程重启、面板重载都不丢。
    _cache = cache_store.CacheStore(host.db('cache'))

    def _force_arg():
        """是否强制刷新（用户显式点了刷新/重搜）→ 跳过缓存读、直接重拉。"""
        return str(request.args.get('force', '')).lower() in ('1', 'true', 'yes')

    def _cached(kind, ns, params, fetch_fn, force=None):
        """缓存优先取数（LRU 模式）。返回 (payload, fetched_at, from_cache, stale)：

        · 命中（无论多久之前）→ 直接返回，一个网络包都不发
        · 未命中 / force=True（用户手动刷新）→ 真拉取并写回缓存
        · 拉取失败但有过期旧缓存 → 降级返回旧数据并置 stale=True
        写回后统一治理容量预算（内容 + 媒体共享一份，按全局 LRU 淘汰）。
        """
        if force is None:
            force = _force_arg()
        try:
            res = cache_store.serve_cached(_cache, kind, ns, params, fetch_fn, force=force)
        except Exception:
            payload, ts = _cache.get_any(kind, ns, cache_store._norm_key(params))
            if payload is not None:
                try:
                    _enforce_unified_cap()
                except Exception:
                    pass
                return (payload, ts, True, True)
            raise
        try:
            _enforce_unified_cap()
        except Exception:
            pass
        return res

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

    def _report_task(task_id, **fields):
        """把下载状态/进度同步到框架统一任务表（静默失败，避免打断下载线程）。

        这样任务生命周期与浏览器连接解耦：刷新/重启/多 worker 均可经 task_id 查回，
        并在统一「任务管理器」中可见、可重试，而非只活在进程内 jobs 字典里。"""
        if not task_id:
            return
        try:
            host.tasks.update(task_id, **fields)
        except Exception:
            pass

    def _bearer():
        auth = request.headers.get('Authorization', '')
        return auth[7:] if auth.startswith('Bearer ') else auth

    def _x_cookie_header():
        """从保险库读取 x.com 原始 cookie，拼成 HTTP Cookie 头。

        凭证库里 cookie 有两种存储形态，都要能拼出可用的 ``name=value; ...`` 头：
        - 结构化列表：``rec['cookies']`` 为 ``[{name,value}, ...]``；
        - Netscape 文本：``rec['_raw']`` 为 cookies.txt（``# Netscape...`` 头 + 7 列
          tab 分隔行，末两列为 name/value）。netscape 形态的 cookie 经保险库解码后
          ``cookies`` 为空列表、原始文本落在 ``_raw``，这里必须回退解析，否则会拼出
          空 Cookie 头导致 X 全部以游客态请求、个人页/关注/收藏全空。
        """
        try:
            rec = host.vault._vault.get_by_domain('x.com', kind='cookie')
        except Exception:
            return ''
        if not rec:
            return ''
        # 1) 结构化 cookie 列表（标准 JSON 形态）
        cookies = rec.get('cookies')
        if isinstance(cookies, list) and cookies:
            pairs = [f"{c.get('name')}={c.get('value')}" for c in cookies
                     if isinstance(c, dict) and c.get('name') and c.get('value') is not None]
            if pairs:
                return '; '.join(pairs)
        # 2) Netscape / 原始文本：解析成 name=value 对
        raw = rec.get('_raw') or (cookies if isinstance(cookies, str) else '') or ''
        if raw:
            pairs = []
            for line in raw.splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 7:
                        name, val = parts[5].strip(), parts[6].strip()
                        if name:
                            pairs.append(f'{name}={val}')
                elif '=' in line:
                    pairs.append(line)
            if pairs:
                return '; '.join(pairs)
        return ''

    # ---------- 媒体预览缓存（统一接入框架托管缓存分区） ----------
    # 经 host.cache('media') 直读直写框架管理的磁盘 LRU 分区；淘汰/容量/统计/迁移
    # 统一由 cached 微服务治理。这里只负责读写字节与暴露信息，文件名保持
    # <md5(url)><ext>，与历史缓存一致，迁移脚本可直接承接旧文件。
    _CACHE_PART = host.cache('media', key_hash='md5', key_len=32)
    _CACHE_LRU_DIR = _CACHE_PART.root          # 兼容旧调用点：<md5(url)><ext> 路径拼接
    _CACHE_INDEX_FILE = os.path.join(_CACHE_LRU_DIR, '.cache_index.json')
    os.makedirs(_CACHE_LRU_DIR, exist_ok=True)

    # 清理上次运行残留的 .part：服务重启会中断进行中的下载，留下半截文件。
    try:
        for _fn in os.listdir(_CACHE_LRU_DIR):
            if _fn.endswith('.part'):
                try:
                    os.remove(os.path.join(_CACHE_LRU_DIR, _fn))
                except Exception:
                    pass
    except Exception:
        pass

    # 启动时按磁盘重建索引（迁移来的旧文件也在此被纳入统计与淘汰）
    _CACHE_PART.reindex()

    def _cache_key(url):
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def _unified_budget_bytes():
        """统一缓存预算（字节）：内容（文字/列表/资料）与媒体（头像/图/视频字节）
        共用这一份预算，按全局 LRU 淘汰。优先读取中心「缓存管理」页（按字节存储）
        设置的 _caps.json 的 x/media（沿用该键作为统一预算），读取失败退回 1GB。

        注意：中心缓存页以「字节」为单位读写 _caps（其界面显示/输入为 MB，
        提交时 ×1024² 转字节），故此处直接按字节读取，不得再 ×1024²，
        否则会把 2GB 膨胀成 2PB。
        """
        try:
            import json as _json
            _caps_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_CACHE_PART.root)))),
                'cache', '_caps.json')
            with open(_caps_file, 'r', encoding='utf-8') as f:
                caps = _json.load(f)
            return int(caps.get('x/media', 1024 * 1024 * 1024))
        except Exception:
            return 1024 * 1024 * 1024

    def _enforce_unified_cap():
        """统一 LRU 容量治理：内容 + 媒体共享 _unified_budget_bytes() 一份预算。

        媒体是字节大头，优先让媒体承担淘汰：把媒体分区容量上限设为
        「统一预算 - 内容占用」，超出即由框架 LRU 淘汰最久未访问的媒体；
        内容（文字/列表）因此受保护。仅当内容自身已超预算（极少见）时
        才回头淘汰内容 LRU。
        """
        try:
            budget = _unified_budget_bytes()
            content_bytes = _cache.content_bytes()
            media_allowed = budget - content_bytes
            if media_allowed < 0:
                while content_bytes > budget and _cache.evict_oldest(50):
                    content_bytes = _cache.content_bytes()
                media_allowed = 0
            try:
                _CACHE_PART.cap = max(0, media_allowed)
                _CACHE_PART.enforce_cap()
            except Exception:
                pass
        except Exception:
            pass
    # 启动时按当前内容占用设定媒体容量上限，避免媒体在首批内容写入前撑爆预算
    _enforce_unified_cap()

    def _cache_get(url):
        """命中返回 (path, ext)；并刷新访问顺序。未命中返回 None。"""
        return _CACHE_PART.get(url)

    def _cache_put_file(url, tmp_path, ext, keep_on_fail=False):
        """把已落盘的临时文件原子改名登记进托管分区；返回最终路径或 None。

        流式代理先写 .part，完整读完后原子改名登记；中途失败只留临时文件，
        不会被当成有效缓存。分区内部负责容量上限兜底淘汰；登记后统一治理预算。
        """
        path = _CACHE_PART.put_file(url, tmp_path, ext, keep_on_fail=keep_on_fail)
        try:
            _enforce_unified_cap()
        except Exception:
            pass
        return path

    # ---------- 媒体边下边播（未缓存完也能拖动进度条） ----------
    # 同一 url 只起一个后台下载线程写 .part；响应从 .part 渐进读取并支持 Range，
    # 这样视频在下载途中也能拖动进度条（拖动即发 Range 请求，按当前已下大小截取）；
    # 完整下载完登记 LRU 缓存，之后走上面的 send_file 分支（同样支持 Range）。
    _media_dl_lock = threading.Lock()
    _media_dl_active = set()
    # 失败记录：url -> 失败时刻。必须带 TTL，不能永久拉黑——
    # 此前用 set 永久记录，且唯一的清除语句写死在 _start_media_download() 内，
    # 而 media() 在调用它之前就先 return 502 了，那行永远执行不到，
    # 结果是「URL 失败一次就永远 502」，代理后来恢复也再不会重试。
    _media_dl_err = {}                       # url -> float(timestamp)
    _MEDIA_ERR_TTL = 45.0                    # 失败 45 秒后允许再次尝试
    # url -> 上游声明的资源总字节数。下载期间供「边下边播」降级路径使用，
    # 让它可以给出正确的 Content-Range 总长（下载结束后即清除，不长期驻留）。
    _media_declared_size = {}
    # 并发下载上限：代理（Clash 等）在突发并发下会 ERRNO2 / 10053，
    # 故必须限流，不能放任一次性拉十几张。
    #
    # 但从 4 提到 10：4 路对「一屏图片」明显不够——实测 9 张并发时前 4 张 0.5s、
    # 后 5 张要排到 1.0~1.5s，用户每滚一屏都要等一轮排队。而列表一次返回上千条、
    # 后台定时预取只覆盖最新 60 张，其余全靠现拉，排队会一直持续。
    # twimg 图片多为几十~几百 KB，实测 10 路并发稳定、未见掐断；
    # 且视频已走独立闸门，不会与图片互相挤占。
    _media_dl_sem = threading.Semaphore(10)
    # 视频/音频**单独一套闸门**，不与封面图共用。
    #
    # 为什么必须分开：一条多图+视频的推文，打开时会同时请求 4 张封面和一个视频，
    # 共 5 个下载争抢 4 个槽 —— 视频被排在懒加载封面**后面**。而 media() 对
    # mp4 是「等完整下载再返回，最多等 60s」（见 _COMPLETE_ONLY_EXT 分支），
    # 排队把 60s 耗光后就降级为边下边播，且**不登记缓存**。
    # 于是：首播要等一分钟才出画面，刷新后又得重新等一分钟（缓存始终没建立）。
    # 电脑端看似正常，只是因为封面已在浏览器缓存里、没有参与排队。
    # 分开后封面再多也不会挡住用户真正想看的视频。
    _media_dl_sem_video = threading.Semaphore(4)

    def _media_ext_ct(url, mtype):
        """按 URL 后缀 / 显式 type 推断扩展名与 mimetype（用于边下边播的响应类型）。

        twimg 图片常带 X 的尺寸后缀（如 .../xxx.jpg:orig），若直接 splitext 会把
        ":orig" 算进扩展名（得到 ".jpg:orig"），拼出的缓存文件名含冒号——Windows
        文件名非法，open() 直接抛错，表现为「所有 :orig 图片一律加载失败」。
        故先剥掉尺寸后缀再取扩展名。
        """
        path = urllib.parse.urlparse(url).path
        path = re.sub(r':(orig|large|medium|small|thumb)$', '', path, flags=re.I)
        path_ext = os.path.splitext(path)[1].lower()
        if path_ext == '.m3u8':
            ext = '.m3u8'
        elif mtype == 'video' or path_ext in ('.mp4', '.m4v', '.webm', '.mov', '.ts', '.m4s', '.aac', '.m4a'):
            ext = path_ext if path_ext else '.mp4'
        elif mtype == 'image' or path_ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp'):
            ext = path_ext if path_ext else '.jpg'
        elif path_ext:
            ext = path_ext
        else:
            ext = '.mp4' if mtype == 'video' else '.jpg'
        ct = ('application/vnd.apple.mpegurl' if ext == '.m3u8'
              else 'video/mp2t' if ext == '.ts'
              else 'video/mp4' if ext == '.mp4' else mimetypes.guess_type(url)[0] or 'image/jpeg')
        return ext, ct

    # HLS 清单里的片源地址改写：把分片(.ts)/变体子列表(.m3u8)的绝对 twimg 地址
    # 改写成走本代理的相对地址 media?u=<编码后地址>&type=video，使 hls.js 经 /media
    # 带 cookie 拉取并落本地 LRU 缓存。否则分片被浏览器直连 video.twimg.com 现拉——
    # 既绕过服务端 cookie（部分环境直连 CDN 不稳/被墙），也不进缓存，表现为
    # 「每次点开视频都重新从 X 拉流、首播与回看都慢」。非片源地址（如解密 key）保持不变。
    _TS_EXT = ('.ts', '.m3u8', '.mp4', '.m4v', '.aac', '.m4a', '.m4s')
    # 必须「下完整再返回」的格式：视频/音频容器 + HLS 分片。残缺即无法播放（详见 media()）。
    # 历史坑：早期只让 m3u8 走完整下载、后又补了 mp4，却漏了 HLS 的 ts/m4s 分片——
    # 分片落入图片那条「边下边播」路径，首播必被截断，只有刷新命中缓存才正常。
    _COMPLETE_ONLY_EXT = ('.mp4', '.m4v', '.webm', '.mov', '.ts', '.m4s', '.aac', '.m4a')
    # 等「完整下载」的宽限期（秒）：够小文件下完即可，超过就转边下边播。
    # 设得短是为了让大文件也能立刻起播（画面秒出，后台继续下载并登记缓存）。
    _COMPLETE_GRACE_SEC = 3.0
    # 等 HLS 清单（m3u8）下完的上限（秒）。m3u8 只是几 KB 的文本索引，
    # 连接复用后通常不到 1 秒；此前写死 60 秒，一旦上游卡住，用户就要盯着
    # 转圈等满一分钟才看到失败——而截断的清单本来也解析不出画面。
    _M3U8_WAIT_SEC = 12.0
    _M3U8_URL_RE = re.compile(r'(https?://[^\s"\']+)')

    def _rewrite_m3u8(text):
        def _rep(m):
            u = m.group(1)
            _host = urllib.parse.urlparse(u).hostname or ''
            if not _host.endswith('twimg.com'):
                return u
            _p = u.split('?', 1)[0]
            if not (_p.endswith(_TS_EXT) or '/vid/' in u):
                return u
            return 'media?u=%s&type=video' % urllib.parse.quote(u, safe='')
        return _M3U8_URL_RE.sub(_rep, text)

    _MEDIA_DL_TRIES = 3          # 单次代理抖动（ERRNO2 / 10053）很常见，允许重试

    # 下载完成/失败事件：用来替代「每 0.1s 轮询文件是否出现」的忙等。
    #
    # 轮询的问题不只是空转：唤醒粒度固定 100ms（最多白等 100ms 才响应下载完成），
    # 且一屏十几张图同时等待时，每秒上百次无谓的 exists/getsize 与锁竞争。
    # 改成事件后，下载线程一结束（改名登记成功，或重试耗尽最终失败）立刻唤醒
    # 等待方；等待方仍保留一个较长的兜底间隔复查文件，防止事件因故丢失时卡死
    # （退化为「慢速轮询」，不会比改造前更差）。
    _media_done_events = {}
    _media_done_lock = threading.Lock()

    def _media_event(url):
        with _media_done_lock:
            ev = _media_done_events.get(url)
            if ev is None:
                ev = threading.Event()
                _media_done_events[url] = ev
            # 条目无限增长会拖慢查表：超量时整体清空。事件丢失的后果仅是
            # 等待方多等一个兜底周期（等价于回到轮询），安全。
            if len(_media_done_events) > 2000:
                _media_done_events.clear()
                ev = threading.Event()
                _media_done_events[url] = ev
            return ev

    def _start_media_download(url, tmp_path, ext):
        """后台把上游 twimg 读到 .part，完成后登记 LRU 缓存；同一 url 并发只跑一个线程。

        失败后可重试：代理在并发突发时会掐断连接，单次失败即永久放弃会让图片
        长时间出不来（此前一次失败就永久拉黑）。
        """
        with _media_dl_lock:
            if url in _media_dl_active:
                return
            _media_dl_active.add(url)
            _media_dl_err.pop(url, None)
        # 新一轮下载：清掉上一轮遗留的完成信号，避免本次等待被旧信号立刻唤醒
        try:
            _media_event(url).clear()
        except Exception:
            pass
        def run():
            last_err = None
            try:
                for attempt in range(_MEDIA_DL_TRIES):
                    try:
                        # 并发闸门：整段下载（含连接与读流）都在信号量内，
                        # 避免收藏列表一次性拉十几张图把代理打垮。
                        # 视频/音频走独立闸门，不与封面图排队互挡（见其定义处注释）。
                        _sem = (_media_dl_sem_video if ext in _COMPLETE_ONLY_EXT
                                else _media_dl_sem)
                        with _sem:
                            cookie = _x_cookie_header()
                            headers = xrun.build_headers(cookie, with_bearer=True)
                            # 优先走连接复用（实测 twimg 0.40s → 0.11s）；失败则回退 urllib
                            up = None
                            resp = None
                            sess = xrun._media_session()
                            if sess is not None:
                                try:
                                    # SOCKS 只作用于建立连接这一步：连接一旦建立就还原全局 socket
                                    xrun._apply_socks()
                                    try:
                                        resp = sess.get(url, headers=headers,
                                                        timeout=60, stream=True)
                                    finally:
                                        xrun._restore_socks()
                                except Exception:
                                    resp = None
                                    xrun._media_session_invalidate()
                            if resp is None:
                                opener = xrun.make_opener(None)
                                xrun._apply_socks()
                                try:
                                    up = opener.open(urllib.request.Request(
                                        url, headers=headers), timeout=60)
                                finally:
                                    xrun._restore_socks()
                            # 记下上游声明的长度，用于事后校验完整性。
                            # 代理（Clash 等）掐断连接时 up.read() 会提前返回空，
                            # 若只凭「size>0」就判成功，残缺文件会被登记进 LRU 缓存
                            # 并【永久污染】——该媒体此后永远只能拿到截断内容，
                            # 怎么刷新都救不回来（实测 40MB 视频被截成 128KB 入库）。
                            try:
                                _hdrs = (resp.headers if resp is not None
                                         else (up.headers if up is not None else {}))
                                _declared = int(
                                    _hdrs.get('Content-Length') or 0) or None
                            except Exception:
                                _declared = None
                            # 共享给「边下边播」的降级路径：它必须知道资源的**真实总长**。
                            # 否则只能拿当前已下字节数冒充总长，浏览器据此认为文件已完整，
                            # 实际拿到截断内容 → mp4 无法起播、进度条也错。
                            if _declared:
                                with _media_dl_lock:
                                    _media_declared_size[url] = _declared
                            try:
                                with open(tmp_path, 'wb') as f:
                                    if resp is not None:
                                        for chunk in resp.iter_content(65536):
                                            if chunk:
                                                f.write(chunk)
                                    else:
                                        while True:
                                            chunk = up.read(65536)
                                            if not chunk:
                                                break
                                            f.write(chunk)
                            finally:
                                for _c in (resp, up):
                                    try:
                                        if _c is not None:
                                            _c.close()
                                    except Exception:
                                        pass
                        _got = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
                        # 只有「拿全了」才算成功；上游未给长度（chunked）时退化为原判定
                        if _got > 0 and (_declared is None or _got >= _declared):
                            # 必须检查登记结果：_cache_put_file 在改名失败时返回 None
                            # 且不抛异常，若不看返回值就当成功，.part 会永久残留、
                            # 缓存永不生效 —— 此后每次请求都要重新下载
                            # （实测图片因此加载极慢：直连 0.55s vs 经服务 40s+）。
                            if _cache_put_file(url, tmp_path, ext,
                                               keep_on_fail=True) is not None:
                                last_err = None
                                break
                            raise RuntimeError('登记缓存失败(.part 改名被占用)')
                        raise RuntimeError(
                            f'下载不完整: 实得 {_got} 字节 / 上游声明 {_declared}')
                    except Exception as e:
                        last_err = e
                        # 清理半成品，别让 .part 污染缓存
                        try:
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                        except Exception:
                            pass
                        if attempt < _MEDIA_DL_TRIES - 1:
                            time.sleep(1.2 * (attempt + 1))   # 退避后重试
                            continue
            finally:
                if last_err is not None:
                    # 必须打日志——此前静默吞异常，媒体一律失败却在日志里毫无痕迹，无法定位
                    try:
                        print(f'[x.media] 媒体下载失败({_MEDIA_DL_TRIES}次后) url={url} '
                              f'tmp={tmp_path} err={type(last_err).__name__}: {last_err}',
                              file=sys.stderr, flush=True)
                    except Exception:
                        pass
                    with _media_dl_lock:
                        _media_dl_err[url] = time.time()
                with _media_dl_lock:
                    _media_dl_active.discard(url)
                    _media_declared_size.pop(url, None)   # 下载已结束，总长不再需要
                # 唤醒所有等待方：无论成功（已改名登记）还是最终失败，都不必再等
                try:
                    _media_event(url).set()
                except Exception:
                    pass
        threading.Thread(target=run, daemon=True).start()

    def _serve_media_partial(tmp_path, ct, req, url, ext):
        """从正在增长的 .part 渐进读取并响应：支持 Range（进度条可拖动）；
        未带 Range 的首次请求走分块流（连接保持，边下边播）。

        ext 用于推出下载完成后 .part 改名登记成的正式缓存文件名——
        流式响应必须接着从正式文件读完剩余字节，否则图片只有半张。
        """
        range_header = (req.headers.get('Range') or '').strip()
        rm = re.match(r'bytes=(\d+)-(\d*)$', range_header)
        if rm:
            start = int(rm.group(1))
            end = int(rm.group(2)) if rm.group(2) else None
            waited = 0.0
            # 等待条件有两条，缺一不可：
            # 1) 必须等「已有足够字节」，而不是只等文件出现——下载线程是连接建立后
            #    才 open(tmp_path,'wb')，文件被创建的瞬间 size 仍为 0，首个 64KB chunk
            #    要等网络送达；而 <video>/<img> 的首个请求必带 Range: bytes=0-，
            #    只等 exists() 会撞上「文件已建、字节未到」的窗口 → cur==0 → 416。
            # 2) 必须同时盯【正式缓存文件】——下载完成后 .part 会被 os.replace 改名
            #    登记成正式文件，此后 tmp_path 已不存在，只盯 .part 会永远等不到，
            #    硬等满 30 秒后必然 416（实测图片首次请求正是如此：等满 38 秒返 416，
            #    紧接着的第二次请求命中缓存立即 206）。
            _final_fp = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext)
            while waited < 30:
                if url in _media_dl_err:
                    return jsonify({'success': False, 'message': '代理失败'}), 502
                _src = tmp_path if os.path.exists(tmp_path) else (
                    _final_fp if os.path.exists(_final_fp) else None)
                try:
                    cur = os.path.getsize(_src) if _src else 0
                except OSError:
                    cur = 0
                if cur > start:
                    break
                time.sleep(0.1); waited += 0.1
            if url in _media_dl_err and not (
                    os.path.exists(tmp_path) or os.path.exists(_final_fp)):
                return jsonify({'success': False, 'message': '代理失败'}), 502
            _src = tmp_path if os.path.exists(tmp_path) else (
                _final_fp if os.path.exists(_final_fp) else None)
            try:
                cur = os.path.getsize(_src) if _src else 0
            except OSError:
                cur = 0
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
                """定量发送 [start, start+total) 这段字节。

                关键修复：中途 .part 被下载线程改名为正式缓存文件后，必须接着读
                正式文件——此前一旦改名就 return，浏览器只拿到改名那一刻之前的
                字节（实测出现 206 响应却是 0 字节）。
                """
                f = None
                cur_path = None
                sent = 0
                try:
                    while sent < total:
                        # 优先读 .part；若已被改名登记，则切到正式文件继续读
                        if os.path.exists(tmp_path):
                            target = tmp_path
                        elif os.path.exists(_final_fp):
                            target = _final_fp
                        else:
                            break   # 下载失败被清理
                        if f is None or cur_path != target:
                            if f is not None:
                                try:
                                    f.close()
                                except Exception:
                                    pass
                            try:
                                f = open(target, 'rb')
                            except Exception:
                                break
                            cur_path = target
                        try:
                            avail = os.path.getsize(target)
                        except OSError:
                            break
                        readable = avail - (start + sent)
                        if readable <= 0:
                            time.sleep(0.05)
                            continue
                        to_read = min(total - sent, readable)
                        try:
                            f.seek(start + sent)
                            chunk = f.read(to_read)
                        except Exception:
                            break
                        if not chunk:
                            time.sleep(0.05)
                            continue
                        sent += len(chunk)
                        yield chunk
                finally:
                    if f is not None:
                        try:
                            f.close()
                        except Exception:
                            pass
            resp = Response(stream_with_context(gen()), status=206, mimetype=ct)
            resp.headers['Accept-Ranges'] = 'bytes'
            # Content-Range 的总长**必须**是资源的真实总长（上游声明的），
            # 不能拿「当前已下字节数」冒充——那会让浏览器以为文件已完整，
            # 实际拿到截断内容，mp4 因此无法起播、进度条也错。
            # 真实总长由下载线程记录在 _media_declared_size；
            # 拿不到时（上游 chunked）才退回已下大小，保持原有语义。
            with _media_dl_lock:
                _declared_total = _media_declared_size.get(url)
            real_total = _declared_total if (_declared_total and _declared_total > cur) else cur
            resp.headers['Content-Range'] = 'bytes %d-%d/%d' % (
                start, start + total - 1, real_total)
            resp.headers['Content-Length'] = str(total)
            # 边下边播的分片并非完整资源，绝不能被浏览器长期缓存：
            # 一旦本次只传出部分/空内容，浏览器会把它当有效图片缓存住。
            resp.headers['Cache-Control'] = 'no-cache'
            resp.headers['Content-Disposition'] = 'inline'
            return resp

        # 未带 Range：分块流，连接保持到下载完成（或客户端断开 / 下载失败）
        def gen():
            waited = 0.0
            while not os.path.exists(tmp_path) and waited < 30:
                if url in _media_dl_err:
                    return
                time.sleep(0.1); waited += 0.1
            # 下载完成后 .part 会被 os.replace 改名为正式缓存文件。
            # 此时必须接着从正式文件把剩余字节读完再收尾——此前 .part 一消失
            # 就 break，浏览器只拿到已读到的那部分，图片表现为半张/残缺。
            final_path = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext)
            pos = 0
            idle = 0.0
            f = None
            cur_path = None
            try:
                while True:
                    # 优先读 .part；若已被改名登记，则切到正式文件继续读
                    if os.path.exists(tmp_path):
                        target = tmp_path
                    elif os.path.exists(final_path):
                        target = final_path
                    else:
                        break   # 下载失败被清理
                    cur = os.path.getsize(target)
                    if pos < cur:
                        if f is None or cur_path != target:
                            if f is not None:
                                try:
                                    f.close()
                                except Exception:
                                    pass
                            try:
                                f = open(target, 'rb')
                            except Exception:
                                break
                            cur_path = target
                        try:
                            f.seek(pos)
                            chunk = f.read(cur - pos)
                        except Exception:
                            break
                        if chunk:
                            pos += len(chunk)
                            idle = 0.0
                            yield chunk
                            continue
                    # 已切到正式文件且已读到末尾：下载完成，可以收尾
                    if not os.path.exists(tmp_path) and os.path.exists(final_path) \
                            and pos >= os.path.getsize(final_path):
                        break
                    # 当前已无新数据：必须主动关闭文件句柄再等待。
                    # Windows 不允许对被打开的文件做 os.replace/remove，
                    # 而下载线程写完后要靠 os.replace 把 .part 改名登记进缓存；
                    # 此前句柄全程持有，改名必然失败，结果是文件下完了却永远
                    # 停在 .part、缓存永远不生效（图片每次重下或直接空白）。
                    if f is not None:
                        try:
                            f.close()
                        except Exception:
                            pass
                        f = None
                        cur_path = None
                    time.sleep(0.1)
                    idle += 0.1
                    # 长时间毫无进展：多半下载线程仍在重试退避，
                    # 继续空转无意义，收尾让下一次请求去命中缓存。
                    if idle > 15.0:
                        break
            finally:
                if f is not None:
                    try:
                        f.close()
                    except Exception:
                        pass
                # 句柄已释放：若 .part 还残留（下载线程改名时正被我们占用），
                # 此刻补做改名登记，避免文件永远停在 .part、缓存永远不生效。
                try:
                    if os.path.exists(tmp_path):
                        _cache_put_file(url, tmp_path, ext)
                except Exception:
                    pass
        resp = Response(stream_with_context(gen()), mimetype=ct)
        resp.headers['Accept-Ranges'] = 'bytes'
        # 已知真实总长时显式声明：否则走 chunked、浏览器不知道资源多大，
        # 视频就拖不动进度条。gen() 会一直读到下载完成，字节数与声明一致。
        with _media_dl_lock:
            _declared_total = _media_declared_size.get(url)
        if _declared_total:
            resp.headers['Content-Length'] = str(_declared_total)
        # 流式传输的是「正在增长的文件」，可能中途失败而只传出空/半个 body。
        # 此前这里给的是 max-age=86400，浏览器会把那次失败的空响应缓存 1 天，
        # 于是即便服务端后来修好了，用户刷新也永远是空白图。改为不缓存，
        # 下次重新请求即可命中已下完的本地缓存（走上面 send_file 长缓存分支）。
        resp.headers['Cache-Control'] = 'no-cache'
        resp.headers['Content-Disposition'] = 'inline'
        return resp

    # ---------- 本地 X 收藏夹（SQLite，独立于 X 账号，持久化快照） ----------
    _folder_db_path = os.path.join(host.data_dir, 'x_bookmarks.db')
    _folder_lock = threading.Lock()

    # 启动时恢复媒体缓存索引（访问顺序 + 总量），避免重启后重复下载已缓存资源
    _CACHE_PART.reindex()

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
                target_lib = f.get('library_id') or job['library_id']
                res = host.ingest(
                    target_lib, path,
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
                    library_id=f.get('library_id') or job['library_id'],
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

        # 登记为框架统一任务（持久化、与连接无关）：任务生命周期不再绑定浏览器连接，
        # 刷新/重启/多 worker 均可经 task_id 查回，并在「任务管理器」中可见、可重试。
        task_id = None
        try:
            _t = host.tasks.create(
                title=params.get('title') or ('X 推文 ' + (resource_id or job_id[:8])),
                owner_id=owner_id, status='running', progress=0,
                stage='解析中', detail='下载任务已启动',
                library_id=library_id,
                params={'resource_key': resource_key, 'url': params.get('url'), 'job_id': job_id},
            )
            task_id = _t.get('task_id') if isinstance(_t, dict) else getattr(_t, 'task_id', None)
        except Exception:
            task_id = None

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
                'task_id': task_id,
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
                        _report_task(task_id, progress=int(obj.get('percent', 0)),
                                     stage='下载中', detail=obj.get('message', ''))
                        if resource_key:
                            _progress_store.update(resource_key,
                                percent=int(obj.get('percent', 0)),
                                message=obj.get('message', ''))
                    elif t == 'log':
                        _append_log(job_id, obj.get('message', ''))
                    elif t == 'error':
                        jobs[job_id]['error'] = obj.get('message')
                        _report_task(task_id, status='failed', detail=obj.get('message', ''))
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
                        _report_task(task_id, progress=100, stage=inp.get('title') or '解析完成')
                        wait = None if inp.get('type') == 'preview' else 30
                        input_events[job_id].wait(timeout=wait)
                        jobs[job_id]['pending_input'] = None
                    elif t == 'result':
                        # 降级路径：run.py 直接带 files（未走 /notify）
                        _ingest_files(job_id, obj.get('files', []))
                        _report_task(task_id, status='completed', progress=100, stage='完成', detail='下载完成')
                        if resource_key:
                            _progress_store.mark_completed(resource_key, '下载完成')
            except Exception as e:
                jobs[job_id]['error'] = str(e)
                _report_task(task_id, status='failed', detail=str(e))
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
        return jsonify({'success': True, 'job_id': job_id,
                        'resource_key': resource_key, 'task_id': task_id})

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
                # 隐藏资源是下载时的选项：预览阶段不传，点击下载时才经 /input 回传，
                # 此处同步进 job['hidden']，使随后入库沿用用户当时勾选的状态。
                if isinstance(val, dict) and 'hidden' in val:
                    job['hidden'] = bool(val['hidden'])
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

    # ---------- 首页（关注流）后台定时预取 ----------
    # 场景：用户可能隔几天才看一次 X。若只在打开面板时才拉，每次都要现等网络 +
    # 现下媒体，体验是「点开一片空白、图慢慢出」。
    # 这里在服务端后台定时把首页时间线抓下来存进 feed:main:items——与 /timeline、
    # 前端共用同一份服务端真相（跨设备一致），并把新推文的图片/视频预先下载进
    # 本地 LRU 媒体缓存。打开面板时内容已在服务端、媒体已在本地磁盘 → 直接渲染。
    #
    # 关于「不改用户浏览位置」：本模块只写服务端状态，不向客户端推送、也不触发
    # 任何前端重渲染。前端仍按自己的节奏 poll，且新推文一律走 prependNewTweets
    # （插前记录视口顶部卡片 + 卡内偏移，插后 anchorTo 还原），位置由前端负责保持。
    _SETTINGS_FILE = os.path.join(host.data_dir, 'settings.json')
    _SETTINGS_DEFAULTS = {
        # ---- 媒体缓存 ----
        'cache_max_mb': 512,          # 本地媒体缓存上限（MB）
        # ---- 首页（关注流）后台预取 ----
        'home_enabled': True,
        'home_interval_min': 120,
        'home_pages': 3,              # 每次抓取页数（每页 50 条）
        'home_prefetch_media': True,
        # 图片与视频分开限量：实测单张图片百 KB 级、单个视频可达 89MB。
        # 若不分开控量，几段视频就能把已缓存的图片全挤掉——反而比不预取更慢。
        # 图片体积小，是预取的主要收益来源。原为 60——但列表一次返回上千条，
        # 60 张只够覆盖最新一屏多，用户稍一滚动就进入「现拉」区（每张都要等
        # 一次网络往返，实测约 1.3s），这正是浏览时持续等图片的来源。
        # 单张按百 KB 级估算，150 张约 15MB，远小于默认 2GB 缓存预算，安全。
        'home_image_limit': 150,
        'home_video_limit': 3,        # 视频体积大，严格限量
        # ---- 书签 / 喜欢 后台同步 ----
        # 这两个列表变动远不如首页频繁，默认关闭、间隔拉长（12 小时），
        # 由用户在设置页按需开启。
        'bookmarks_enabled': False,
        'bookmarks_interval_min': 720,
        'bookmarks_pages': 2,
        'likes_enabled': False,
        'likes_interval_min': 720,
        'likes_pages': 2,
        # ---- 订阅轮询（dbox 级订阅 → 新内容提醒）----
        # 只读 dbox 订阅表（source_type='x'），绝不读取 X 关注列表；
        # 默认开启、间隔 30 分钟，由用户在设置页调整。
        'subscribe_enabled': True,
        'subscribe_interval_min': 30,
        # ---- 界面（前端展示偏好，随账号跨设备同步）----
        'ui_arc_side': 'right',   # 首页日期罗盘（圆弧时间轴）停靠侧：right|left
    }
    _settings = None
    _settings_lock = threading.Lock()

    def _new_job_state():
        return {'running': False, 'last_start': 0, 'last_finish': 0,
                'last_ok': False, 'last_error': '', 'last_count': 0,
                'last_extra': '', 'runs': 0}

    _JOB_NAMES = ('home', 'bookmarks', 'likes', 'subscribe')
    _job_state = {n: _new_job_state() for n in _JOB_NAMES}
    _job_last = {n: 0.0 for n in _JOB_NAMES}     # 各任务上次成功排程时间

    def _settings_load():
        nonlocal _settings
        if _settings is not None:
            return _settings
        cfg = dict(_SETTINGS_DEFAULTS)
        # 兼容迁移：早期版本把首页预取单独存在 autocrawl.json（键名不同）
        try:
            with open(os.path.join(host.data_dir, 'autocrawl.json'),
                      'r', encoding='utf-8') as f:
                legacy = json.load(f) or {}
            if isinstance(legacy, dict):
                for old, new in (('enabled', 'home_enabled'),
                                 ('interval_min', 'home_interval_min'),
                                 ('pages', 'home_pages'),
                                 ('prefetch_media', 'home_prefetch_media'),
                                 ('image_limit', 'home_image_limit'),
                                 ('video_limit', 'home_video_limit')):
                    if old in legacy:
                        cfg[new] = legacy[old]
        except Exception:
            pass
        try:
            with open(_SETTINGS_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                for k, v in saved.items():
                    if k not in _SETTINGS_DEFAULTS:
                        continue
                    d = _SETTINGS_DEFAULTS[k]
                    if isinstance(d, bool):
                        cfg[k] = bool(v)
                    elif isinstance(d, str):
                        # 字符串型设置（如 ui_arc_side）：此前只还原 bool 与数值，
                        # 字符串键即便写进了 settings.json 也读不回来，一重启就掉回默认值。
                        cfg[k] = str(v)
                    elif isinstance(v, (int, float)) and not isinstance(v, bool):
                        cfg[k] = int(v)
        except Exception:
            pass
        cfg = _settings_clamp(cfg)
        _settings = cfg
        return cfg

    def _settings_clamp(cfg):
        """把配置钳制到合法区间：手改文件、旧版升级、非法输入都靠它兜住。"""
        c = dict(cfg)
        c['cache_max_mb'] = max(64, min(int(c['cache_max_mb']), 4096))
        c['home_interval_min'] = max(5, min(int(c['home_interval_min']), 1440))
        c['home_pages'] = max(1, min(int(c['home_pages']), 10))
        c['home_image_limit'] = max(0, min(int(c['home_image_limit']), 300))
        c['home_video_limit'] = max(0, min(int(c['home_video_limit']), 50))
        c['bookmarks_interval_min'] = max(30, min(int(c['bookmarks_interval_min']), 10080))
        c['bookmarks_pages'] = max(1, min(int(c['bookmarks_pages']), 10))
        c['likes_interval_min'] = max(30, min(int(c['likes_interval_min']), 10080))
        c['likes_pages'] = max(1, min(int(c['likes_pages']), 10))
        c['subscribe_interval_min'] = max(5, min(int(c['subscribe_interval_min']), 1440))
        c['subscribe_enabled'] = bool(c['subscribe_enabled'])
        for k in ('home_enabled', 'home_prefetch_media',
                  'bookmarks_enabled', 'likes_enabled'):
            c[k] = bool(c[k])
        # 日期罗盘只允许左右两侧：手改文件、旧值、大小写差异一律回落右侧
        c['ui_arc_side'] = 'left' if str(c.get('ui_arc_side', 'right')).strip().lower() == 'left' else 'right'
        return c

    def _settings_save(cfg):
        nonlocal _settings
        cfg = _settings_clamp(cfg)
        with _settings_lock:
            _settings = dict(cfg)
        try:
            with open(_SETTINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        # 上限调小后可能已超限：立即按新上限回收一次
        try:
            _enforce_unified_cap()
        except Exception:
            pass
        return cfg

    def _ac_collect_media(items, image_limit, video_limit):
        """从一批推文收集待预取的媒体 URL：图片与视频分开（视频大，单独限量）。"""
        imgs, vids = [], []
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            for m in (it.get('media') or []):
                if not isinstance(m, dict):
                    continue
                u = m.get('url')
                if not u or not str(u).startswith('http'):
                    continue
                t = (m.get('type') or '').lower()
                if t in ('video', 'gif', 'animated_gif'):
                    if len(vids) < video_limit:
                        vids.append(u)
                    # 视频封面单独算图片：不预取封面，列表里就是个黑框
                    cov = m.get('cover')
                    if cov and str(cov).startswith('http') and len(imgs) < image_limit:
                        imgs.append(cov)
                elif len(imgs) < image_limit:
                    imgs.append(u)
        return imgs, vids

    def _ac_prefetch(urls, mtype):
        """把尚未缓存的媒体交给后台下载线程。幂等：已缓存/下载中的直接跳过。"""
        n = 0
        for u in urls:
            try:
                if _cache_get(u) is not None:
                    continue
                ext, _ct = _media_ext_ct(u, mtype)
                tmp = os.path.join(_CACHE_LRU_DIR, _cache_key(u) + ext + '.part')
                if os.path.exists(tmp):
                    continue                       # 已在下载中
                with _media_dl_lock:
                    if u in _media_dl_active:
                        continue
                _start_media_download(u, tmp, ext)
                n += 1
            except Exception:
                pass
        return n

    def _iso_order(ts):
        """把 X 的 created_at 转成 ISO 时间串（union_by_id 对 order 的契约）。

        ⚠️ 为什么必须转换：core/state_merge 的 _sortable_ts 只认「数字 / 数字串 /
        ISO 时间串」，其余一律判为 -inf。X 的 created_at 形如
        'Sun Aug 30 11:00:03 +0000 2026' 三者都不是 → 所有条目排序键相同 →
        排序退化为插入序（旧条目在前、新追加的在后）→ 封顶 items[:cap] 恰好把
        新推文截掉。表现为「任务照常成功、接口照常 200，但首页永远不更新」。
        """
        raw = ts.strip() if isinstance(ts, str) else ''
        if not raw:
            return ''
        try:
            datetime.fromisoformat(raw[:-1] + '+00:00' if raw.endswith('Z') else raw)
            return raw                      # 已是 ISO，原样返回
        except ValueError:
            pass
        try:
            return datetime.strptime(raw, '%a %b %d %H:%M:%S %z %Y').isoformat()
        except Exception:
            return ''

    _feed_orders_fixed = [False]

    def _feed_fix_orders_once():
        """存量迁移：把已存 feed 的 order 从 X 原串改写为 ISO（每进程一次）。

        不迁移也能让新推文进来（新的有真实时间戳、会排到最前），但旧条目全是
        -inf 会一直压在尾部且顺序是乱的。迁移一次让整份列表恢复时间序。
        """
        if _feed_orders_fixed[0]:
            return
        _feed_orders_fixed[0] = True
        try:
            cur = host.state.get('feed:main:items')
        except Exception:
            return
        if not isinstance(cur, list) or not cur:
            return
        fixed, changed = [], False
        for r in cur:
            if not isinstance(r, dict):
                continue
            rec = dict(r)
            o = rec.get('order')
            sortable = isinstance(o, (int, float)) and not isinstance(o, bool)
            if not sortable and isinstance(o, str) and o.strip():
                try:
                    datetime.fromisoformat(o.strip()[:-1] + '+00:00'
                                           if o.strip().endswith('Z') else o.strip())
                    sortable = True
                except ValueError:
                    sortable = False
            if not sortable:
                iso = _iso_order(rec.get('created_at')) or _iso_order(o)
                if iso:
                    rec['order'] = iso
                    changed = True
            fixed.append(rec)
        if changed:
            try:
                # lww = 整值覆盖：这里就是要拿规范化后的整份列表替换存量
                host.state.put('feed:main:items', fixed, strategy='lww')
            except Exception:
                pass

    def _ac_run_once():
        """抓一轮首页 + 预取媒体。返回 (ok, tweets, media, err)。"""
        cfg = _settings_load()
        cookie = _x_cookie_header()
        if not cookie:
            return False, 0, 0, '未配置 x.com 登录 Cookie'
        items, cursor = [], None
        pages = max(1, min(int(cfg.get('home_pages') or 1), 10))
        for _p in range(pages):
            try:
                # 第一页不带 cursor（拉最新）；后续页带 cursor 往更旧翻
                page_items, cursor = xrun.list_following_timeline(
                    cookie, 50, cursor if _p else None)
            except Exception as e:
                # 第一页就失败才算真失败；翻页中途断掉则保留已抓到的部分
                if not items:
                    return False, 0, 0, str(e)
                break
            if not page_items:
                break
            items.extend(page_items)
            if not cursor:
                break
        if not items:
            return True, 0, 0, ''
        # 按天写入各自的缓存单元（不再写「一整份清单」——详见 /timeline 上方说明）
        try:
            _feed_migrate_from_state()
            _feed_days_put(items)
        except Exception:
            pass
        media_n = 0
        if cfg.get('home_prefetch_media'):
            imgs, vids = _ac_collect_media(
                items,
                int(cfg.get('home_image_limit') or 0),
                int(cfg.get('home_video_limit') or 0))
            media_n += _ac_prefetch(imgs, 'image')
            media_n += _ac_prefetch(vids, 'video')
        return True, len(items), media_n, ''

    # ---- 书签 / 喜欢 后台同步 ----
    # 这两个列表此前只能等用户打开对应标签才现拉（首次进入要转圈数秒，喜欢尤甚）。
    # 这里同样提供后台定时：内容进全局单缓存 feed:tweets:items（与首页/搜索/详情
    # 共用一份内容），成员 id 进 bm:list / like:list——正是前端读取的那两个键，
    # 因此后台跑过一轮后，用户点开书签/喜欢即为秒开。
    def _membership(items, limit=600):
        """转成前端 toMembership() 的同构列表：[{tweet_id, created_at}]。"""
        out = []
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            tid = it.get('tweet_id') or it.get('id')
            if not tid:
                continue
            out.append({'tweet_id': str(tid), 'created_at': it.get('created_at')})
            if len(out) >= limit:
                break
        return out

    def _merge_membership(key, items, limit=600):
        """把新抓到的成员并入既有成员列表（按 tweet_id 去重、时间倒序、封顶）。

        绝不能直接 lww 整体覆盖：后台任务每次只翻 N 页（默认 2 页），抓到的是
        「最近若干页」而非全量，覆盖式写入会把用户此前缓存的更旧条目整段抹掉，
        列表反而越同步越短。故先读回既有列表再按 id 归并。
        """
        cur = []
        try:
            v = host.state.get(key)
            if isinstance(v, list):
                cur = v
            elif isinstance(v, dict) and isinstance(v.get('value'), list):
                cur = v['value']
        except Exception:
            cur = []
        seen = {}
        for m in list(cur) + list(_membership(items, limit)):
            if not isinstance(m, dict):
                continue
            tid = str(m.get('tweet_id') or m.get('id') or '')
            if not tid:
                continue
            rec = {'tweet_id': tid, 'created_at': m.get('created_at')}
            prev = seen.get(tid)
            if prev is None or (rec['created_at'] or '') > (prev.get('created_at') or ''):
                seen[tid] = rec
        merged = list(seen.values())
        merged.sort(key=lambda x: (x.get('created_at') or ''), reverse=True)
        return merged[:limit]

    def _merge_into_tweet_store(items):
        """把一批推文内容并入全局单缓存（与前端 XTWEETS 同一份，跨设备共享）。"""
        norm = []
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            rec = dict(it)
            rec['id'] = str(it.get('tweet_id') or it.get('id') or '')
            rec['order'] = it.get('created_at')
            norm.append(rec)
        if norm:
            try:
                host.state.put('feed:tweets:items', norm,
                               strategy='union_by_id', cap=1500)
            except Exception:
                pass
        return len(norm)

    def _fetch_paged(fetcher, pages, count=50):
        """通用翻页：连续翻到指定页数或没有下一页为止，返回合并后的条目。

        首页第一页不带 cursor（拉最新），书签/喜欢同理；翻页中途失败时
        保留已抓到的部分，不算整轮失败。
        """
        items, cursor = [], None
        for p in range(max(1, pages)):
            try:
                page_items, cursor = fetcher(count, cursor if p else None)
            except Exception:
                if not items:
                    raise
                break
            if not page_items:
                break
            items.extend(page_items)
            if not cursor:
                break
        return items

    def _job_bookmarks(cfg):
        cookie = _x_cookie_header()
        if not cookie:
            return False, 0, '未配置 x.com 登录 Cookie'
        items = _fetch_paged(
            lambda c, cur: xrun.list_bookmarks(cookie, c, cur),
            int(cfg.get('bookmarks_pages') or 1))
        if not items:
            return True, 0, ''
        n = _merge_into_tweet_store(items)
        try:
            host.state.put('bm:list', _merge_membership('bm:list', items),
                           strategy='lww')
        except Exception:
            pass
        return True, n, ''

    def _job_likes(cfg):
        cookie = _x_cookie_header()
        if not cookie:
            return False, 0, '未配置 x.com 登录 Cookie'
        rest_id = _my_rest_id(cookie)
        if not rest_id:
            return False, 0, '无法从 Cookie 识别登录用户'
        items = _fetch_paged(
            lambda c, cur: xrun.list_likes(cookie, rest_id, c, cur),
            int(cfg.get('likes_pages') or 1))
        if not items:
            return True, 0, ''
        n = _merge_into_tweet_store(items)
        try:
            host.state.put('like:list', _merge_membership('like:list', items),
                           strategy='lww')
        except Exception:
            pass
        return True, n, ''

    def _sub_state_path():
        return os.path.join(host.data_dir, 'subscription_state.json')

    def _sub_state_load():
        try:
            with open(_sub_state_path(), 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {'last': {}}

    def _sub_state_save(state):
        try:
            with open(_sub_state_path(), 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False)
        except Exception:
            pass

    def _cache_post_from_item(s, it):
        """把一条时间线条目规整为缓存写入用的 post 字典。"""
        tid = str(it.get('tweet_id') or it.get('id') or '')
        if not tid:
            return None
        handle = (s.get('source_id') or '').strip().lstrip('@')
        if not handle:
            return None
        text = (it.get('text') or it.get('full_text') or it.get('content') or '').strip()
        media = []
        for m in (it.get('media') or []):
            if not isinstance(m, dict):
                continue
            media.append({'thumbnail': m.get('thumbnail'), 'url': m.get('url')})
        url = 'https://x.com/%s/status/%s' % (handle, tid)
        return {
            'subscription_id': s.get('id'),
            'source_type': s.get('source_type') or 'x',
            'source_id': handle,
            'post_id': tid,
            'author': handle,
            'text': text,
            'media': media,
            'url': url,
            'target_mode': s.get('target_mode') or 'video',
            'library_id': s.get('library_id'),
        }

    def _notify_subscription_post(post):
        """把一条已缓存的订阅新内容转成富通知（图 + 摘要 + 跳转站内缓存视图）。

        展示内容完全由本插件提供，核心通知中心只做通用渲染；跳转指向站内
        「订阅动态」视图，原帖外链放在 extra.externalUrl。
        """
        tid = post.get('post_id')
        if not tid:
            return
        text = (post.get('text') or '').strip()
        img = ''
        for m in (post.get('media') or []):
            if not isinstance(m, dict):
                continue
            if m.get('thumbnail'):
                img = m['thumbnail']; break
            if m.get('url'):
                img = m['url']; break
        external = post.get('url') or ''
        host.notify_user(
            title='@%s 有新内容（已缓存）' % (post.get('source_id') or ''),
            body=(text[:120] if text else '新动态'),
            source='x',
            category='subscription',
            payload={
                'image': img,
                'url': '/subscriptions?tab=cache&src=x',
                'summary': (text[:200] if text else ''),
                'target': 'internal',
                'extra': {
                    'externalUrl': external,
                    'handle': post.get('source_id'),
                    'tweet_id': tid,
                },
            },
        )

    def _job_subscribe(cfg):
        """订阅轮询：只读 dbox 订阅表（source_type='x'），绝不读取 X 关注列表。

        逐个订阅 handle 拉时间线，对比本地记录的最后已见推文 id，
        对新增推文以富通知提醒用户（图片 + 摘要 + 跳转）。
        """
        cookie = _x_cookie_header()
        if not cookie:
            return False, 0, '未配置 x.com 登录 Cookie'
        subs_res = host.get_subscriptions('x')
        if isinstance(subs_res, dict):
            subs = subs_res.get('items') or []
        elif isinstance(subs_res, list):
            subs = subs_res
        else:
            subs = []
        subs = [s for s in subs if isinstance(s, dict) and s.get('enabled', True)]
        if not subs:
            return True, 0, '无启用订阅'
        state = _sub_state_load()
        last = state.setdefault('last', {})
        handled = 0
        for s in subs:
            handle = (s.get('source_id') or '').strip().lstrip('@')
            sid = s.get('id')
            if not handle:
                continue
            try:
                _home = xrun.build_headers(cookie, with_bearer=False)

                def _txid(method, path):
                    return get_transaction_id(_home, method, path, ua=xrun.UA)

                items, _, profile = xrun.user_tweets(cookie, handle, 20, None,
                                                     txid_func=_txid)
            except Exception as e:
                try:
                    host.update_subscription(sid, error=str(e)[:200])
                except Exception:
                    pass
                continue
            prev = last.get(handle, '')
            new_items = []
            for it in (items or []):
                tid = str(it.get('tweet_id') or it.get('id') or '')
                if not tid:
                    continue
                if prev and tid == prev:
                    break  # 列表最新在前；遇到上次已见即停止
                new_items.append(it)
            if new_items:
                posts = []
                for it in reversed(new_items):
                    p = _cache_post_from_item(s, it)
                    if p:
                        posts.append(p)
                if posts:
                    try:
                        host.cache_subscription_post(posts)
                    except Exception as _e:
                        host.logger.error('缓存订阅内容失败: %s', _e)
                    for p in posts:
                        _notify_subscription_post(p)
                    newest = str(posts[0].get('post_id') or '')
                    if newest:
                        last[handle] = newest
                        handled += len(posts)
            try:
                host.update_subscription(sid, last_checked_at=time.time(), error=None)
            except Exception:
                pass
        _sub_state_save(state)
        return True, handled, ''

    def _run_job(name, cfg=None):
        """跑一个后台任务并记账。返回是否成功。"""
        st = _job_state[name]
        with _settings_lock:
            if st['running']:
                return False
            st['running'] = True
            st['last_start'] = time.time()
        ok, count, err, extra = False, 0, '', ''
        try:
            if cfg is None:
                cfg = _settings_load()
            if name == 'home':
                ok, tweets, media, err = _ac_run_once()
                count = tweets
                if media:
                    extra = '媒体 %d' % media
            elif name == 'bookmarks':
                ok, count, err = _job_bookmarks(cfg)
            elif name == 'likes':
                ok, count, err = _job_likes(cfg)
            elif name == 'subscribe':
                ok, count, err = _job_subscribe(cfg)
            else:
                ok, count, err = _job_likes(cfg)
        except Exception as e:
            err = '%s: %s' % (type(e).__name__, e)
        finally:
            with _settings_lock:
                st['running'] = False
                st['last_finish'] = time.time()
                st['last_ok'] = bool(ok)
                st['last_error'] = err or ''
                st['last_count'] = count
                st['last_extra'] = extra or ''
                st['runs'] = st.get('runs', 0) + 1
                if ok:
                    _job_last[name] = time.time()
        try:
            print('[x.job] %s 一轮结束 ok=%s 条=%s %s'
                  % (name, ok, count, ('err=' + err) if err else extra),
                  file=sys.stderr, flush=True)
        except Exception:
            pass
        return ok

    def _sched_loop():
        """统一调度：每分钟巡检一次，各任务按自己的间隔到点执行。

        三个任务共用一个线程而非各起一个——间隔可在设置页随时改动，
        单线程巡检能让新间隔在下一分钟即生效；若各起一个线程并按
        interval 长睡，改完设置要等上一整轮（甚至到下次重启）才生效。
        """
        time.sleep(30)          # 启动后先避开插件初始化高峰
        while True:
            try:
                cfg = _settings_load()
                now = time.time()
                for name in _JOB_NAMES:
                    try:
                        if not cfg.get(name + '_enabled'):
                            continue
                        interval = max(1, int(cfg.get(name + '_interval_min') or 60)) * 60
                        if now - _job_last.get(name, 0) >= interval:
                            _run_job(name, cfg)
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(60)

    try:
        threading.Thread(target=_sched_loop, daemon=True).start()
    except Exception:
        pass

    @bp.route('/settings', methods=['GET'])
    @host.login_required
    def settings_get():
        """返回统一设置 + 各后台任务运行状态 + 媒体缓存占用。"""
        with _settings_lock:
            jobs = {n: dict(_job_state[n]) for n in _JOB_NAMES}
        _st = _CACHE_PART.stat()
        return jsonify({'success': True, 'settings': _settings_load(),
                        'jobs': jobs,
                        'cache': {'files': _st['count'], 'bytes': _st['bytes'],
                                  'max_bytes': _unified_budget_bytes()}})

    @bp.route('/settings', methods=['PUT', 'POST'])
    @host.login_required
    def settings_set():
        """更新设置（只接受已知字段，保存前统一钳制）。PUT/POST 皆可。"""
        data = request.get_json(force=True, silent=True) or {}
        cfg = dict(_settings_load())
        if isinstance(data, dict):
            for k, v in data.items():
                if k not in _SETTINGS_DEFAULTS:
                    continue
                if isinstance(_SETTINGS_DEFAULTS[k], bool):
                    cfg[k] = bool(v)
                elif isinstance(_SETTINGS_DEFAULTS[k], str):
                    cfg[k] = str(v)
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    cfg[k] = int(v)
        cfg = _settings_save(cfg)
        return jsonify({'success': True, 'settings': cfg})

    @bp.route('/settings/run/<job>', methods=['POST'])
    @host.login_required
    def settings_run(job):
        """立即跑一轮指定任务（异步，不阻塞请求）。job: home|bookmarks|likes"""
        if job not in _JOB_NAMES:
            return jsonify({'success': False, 'message': '未知任务'}), 404
        with _settings_lock:
            if _job_state[job]['running']:
                return jsonify({'success': False, 'message': '该任务正在运行'}), 409
        threading.Thread(target=_run_job, args=(job,), daemon=True).start()
        return jsonify({'success': True})

    # ---------- 首页 feed：按天分桶（每天 = 一个独立 LRU 缓存单元）----------
    # 为什么不再用「一整份清单 + 条数上限」：
    #   整份清单是**一个**缓存单元，每次刷新都会重写它 → 它永远处于「刚被访问」
    #   → LRU 永不淘汰 → 只能靠人为条数上限兜底；而上限既会截断历史，写错时还会
    #   连新推文都挡在门外（封顶按插入序截，截掉的恰恰是最新追加的那些）。
    #   改成每天一个单元后：当天会被反复更新（本就该新），历史天写完就再没人访问，
    #   自然成为最久未访问者，被统一 LRU（字节预算 evict_oldest）清理。
    #   因此这里**不再需要任何条数上限**，历史深度由缓存预算自然决定。
    _FEED_DAY_KIND = 'list'
    _FEED_DAY_NS = 'feed_day'
    _FEED_INDEX_NS = 'feed_index'
    _feed_migrated = [False]

    def _item_dt(it):
        """推文时间 → datetime（X 原串与 ISO 都认）。"""
        ts = it.get('created_at') or it.get('timeline_at')
        raw = ts.strip() if isinstance(ts, str) else ''
        if not raw:
            return None
        try:
            return datetime.strptime(raw, '%a %b %d %H:%M:%S %z %Y')
        except Exception:
            pass
        try:
            return datetime.fromisoformat(raw[:-1] + '+00:00' if raw.endswith('Z') else raw)
        except Exception:
            return None

    def _item_ts(it):
        d = _item_dt(it)
        if d is None:
            return 0.0
        try:
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.timestamp()
        except Exception:
            return 0.0

    def _day_of(it):
        """推文属于哪一天。用**本地时区**，与前端 dayKeyOf() 的口径保持一致。"""
        d = _item_dt(it)
        if d is None:
            return ''
        try:
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            d = d.astimezone()
        except Exception:
            pass
        return d.strftime('%Y-%m-%d')

    def _feed_day_items(day):
        """取某一天的条目。命中会刷新该单元的 LRU 访问时间（这正是「用过的天留下、没用过的天被淘汰」的依据）。"""
        try:
            payload, fetched_at, _age = _cache.get(_FEED_DAY_KIND, _FEED_DAY_NS, day)
        except Exception:
            return [], None
        return (payload if isinstance(payload, list) else []), fetched_at

    def _feed_day_counts():
        try:
            payload, _ts, _age = _cache.get(_FEED_DAY_KIND, _FEED_INDEX_NS, 'counts')
        except Exception:
            payload = None
        return payload if isinstance(payload, dict) else {}

    def _feed_days_put(items):
        """把一批推文按天并入各自的缓存单元；返回 {day: 条数}。"""
        buckets = {}
        for it in (items or []):
            if not isinstance(it, dict):
                continue
            d = _day_of(it)
            if not d:
                continue
            buckets.setdefault(d, []).append(it)
        if not buckets:
            return {}
        counts = _feed_day_counts()
        changed = {}
        for day, arr in buckets.items():
            cur, _ts = _feed_day_items(day)
            by_id = {}
            for r in (cur or []):
                if isinstance(r, dict):
                    rid = r.get('tweet_id') or r.get('id')
                    if rid is not None:
                        by_id[str(rid)] = r
            for r in arr:
                rid = r.get('tweet_id') or r.get('id')
                if rid is not None:
                    by_id[str(rid)] = r       # 同 id 用本次拉到的覆盖（内容可能更新）
            merged = sorted(by_id.values(), key=_item_ts, reverse=True)
            try:
                _cache.put(_FEED_DAY_KIND, _FEED_DAY_NS, day, merged)
            except Exception:
                continue
            counts[day] = len(merged)
            changed[day] = len(merged)
        if changed:
            try:
                _cache.put(_FEED_DAY_KIND, _FEED_INDEX_NS, 'counts', counts)
            except Exception:
                pass
            _feed_days_reindex(counts)
        return changed

    def _feed_days_reindex(counts=None):
        """重建天索引。目录来自缓存 key，条数来自 counts（两者都很小，不必读各天 payload）。"""
        if counts is None:
            counts = _feed_day_counts()
        try:
            rows = _cache.keys(_FEED_DAY_KIND, _FEED_DAY_NS)
        except Exception:
            return
        days = [{'day': r.get('key'),
                 'count': int(counts.get(r.get('key'), 0) or 0),
                 'fetched_at': r.get('fetched_at')}
                for r in (rows or []) if r.get('key')]
        days.sort(key=lambda d: d['day'], reverse=True)
        try:
            _cache.put(_FEED_DAY_KIND, _FEED_INDEX_NS, 'days', {'days': days})
        except Exception:
            pass

    def _feed_days_list():
        try:
            payload, _ts, _age = _cache.get(_FEED_DAY_KIND, _FEED_INDEX_NS, 'days')
        except Exception:
            payload = None
        if isinstance(payload, dict) and isinstance(payload.get('days'), list):
            return payload['days']
        _feed_days_reindex()
        try:
            payload, _ts, _age = _cache.get(_FEED_DAY_KIND, _FEED_INDEX_NS, 'days')
        except Exception:
            payload = None
        return (payload or {}).get('days') or [] if isinstance(payload, dict) else []

    def _feed_migrate_from_state():
        """一次性把旧的整份 feed:main:items 拆成按天单元（不删旧数据，避免不可逆）。"""
        if _feed_migrated[0]:
            return
        _feed_migrated[0] = True
        try:
            cur = host.state.get('feed:main:items')
        except Exception:
            return
        if not isinstance(cur, list) or not cur:
            return
        try:
            if _cache.keys(_FEED_DAY_KIND, _FEED_DAY_NS):
                return          # 已有按天数据就不再覆盖
        except Exception:
            pass
        _feed_days_put(cur)

    @bp.route('/timeline/days', methods=['GET'])
    @host.login_required
    def timeline_days():
        """首页按天分页的「天索引」：有哪些天、每天多少条、何时拉取的。"""
        _feed_migrate_from_state()
        return jsonify({'success': True, 'days': _feed_days_list()})

    @bp.route('/timeline', methods=['GET'])
    @host.login_required
    def timeline():
        """首页关注流（Following），**按天分页**：一天一页。

        · ?date=YYYY-MM-DD  取某一天：命中缓存直接返回，一个网络包都不发
        · ?force=1          重新拉最新一页，结果按天入桶，返回本次条目
        · ?cursor=...       向更旧翻（X 只能按游标顺序下翻），结果同样按天入桶
        · 不带参数          返回最新一天的缓存（秒开），没有才回源

        存储：每天是**一个独立缓存单元**（kind=list / ns=feed_day / key=日期），
        不再有「一整份清单 + 条数上限」——详见上方按天分桶的说明。
        """
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        _feed_migrate_from_state()
        day = (request.args.get('date') or '').strip()

        # ---- 取某一天：纯缓存读，不回源 ----
        if day:
            items, fetched_at = _feed_day_items(day)
            return jsonify({'success': True, 'day': day, 'items': items,
                            'count': len(items), 'fetched_at': fetched_at,
                            'cached': True, 'stale': False})

        count = min(int(request.args.get('count', 20)), 50)
        cursor = request.args.get('cursor') or None

        def _fetch():
            items, next_cursor = xrun.list_following_timeline(cookie, count, cursor)
            # 拉到的推文按 created_at 分桶，各自并入那一天的缓存单元
            _feed_days_put(items or [])
            if next_cursor:
                try:
                    host.state.put('feed:main:cursor', next_cursor, strategy='max')
                except Exception:
                    pass
            return {'items': items or [], 'next_cursor': next_cursor}

        # 首次进入（无 cursor、未强制刷新）：直接给最新一天的缓存，做到秒开且不重复打 X
        if not cursor and not _force_arg():
            days = _feed_days_list()
            if days:
                newest = days[0]['day']
                items, fetched_at = _feed_day_items(newest)
                return jsonify({'success': True, 'day': newest, 'items': items,
                                'count': len(items), 'fetched_at': fetched_at,
                                'cached': True, 'stale': False, 'days': days})
        try:
            data, ts, cached, stale = _cached('list', 'timeline',
                                              {'count': count, 'cursor': cursor or ''}, _fetch)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取 X 关注流失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'days': _feed_days_list(),
                        'cached': cached, 'fetched_at': ts, 'stale': stale,
                        'budget_bytes': _unified_budget_bytes()})

    @bp.route('/check', methods=['GET'])
    def check():
        return jsonify({'success': True, **_check_x_session()})

    # ---- 网页登录：在用户桌面打开真实浏览器，人工登录，自动取回 cookie ----
    # 不自己破解 X 的人机校验（Arkose/滑块/邮箱验证码）：那是对抗对方风控，违反 ToS，
    # 且对方一改就失效；邮箱验证码更是天然无法自动化。交给用户在真实浏览器里完成最可靠。
    _LOGIN_URL = 'https://x.com/i/flow/login'
    _LOGIN_MATCH = ['auth_token', 'ct0']

    @bp.route('/login/start', methods=['POST'])
    @host.login_required
    def login_start():
        wl = getattr(host, 'weblogin', None)
        if wl is None:
            return jsonify({'success': False,
                            'message': '当前 Dbox 版本不支持网页登录（缺少 weblogin 能力）'}), 501
        r = wl.start(url=_LOGIN_URL, match=_LOGIN_MATCH, domain='x.com')
        if not r.get('ok'):
            return jsonify({'success': False,
                            'message': r.get('error') or '无法打开登录浏览器'}), 500
        return jsonify({'success': True, 'sid': r.get('sid'),
                        'message': '已在桌面打开浏览器，请在其中完成 X 登录'})

    @bp.route('/login/status', methods=['GET'])
    @host.login_required
    def login_status():
        wl = getattr(host, 'weblogin', None)
        sid = (request.args.get('sid') or '').strip()
        if wl is None:
            return jsonify({'success': False, 'message': '不支持网页登录'}), 501
        st = wl.status(sid)
        if not st.get('ok'):
            return jsonify({'success': False, 'message': st.get('error')}), 404
        # 登录完成即写入凭证库（add 按 kind|domain|name 定位，重复登录是覆盖而非新增）
        if st.get('state') == 'done' and st.get('cookies'):
            try:
                host.vault._vault.add(kind='cookie', name='X 登录态', domain='x.com',
                                      value=st['cookies'], fmt='netscape',
                                      note='由「网页登录」自动获取')
            except Exception as e:
                return jsonify({'success': False, 'state': 'done', 'saved': False,
                                'message': '已拿到 cookie，但写入凭证库失败：%s' % e}), 500
            return jsonify({'success': True, 'state': 'done', 'saved': True,
                            'count': len(st['cookies'])})
        return jsonify({'success': True, 'state': st.get('state'), 'saved': False,
                        'error': st.get('error') or ''})

    @bp.route('/login/cancel', methods=['POST'])
    @host.login_required
    def login_cancel():
        wl = getattr(host, 'weblogin', None)
        sid = ((request.get_json(force=True, silent=True) or {}).get('sid')
               or request.args.get('sid') or '')
        if wl is not None:
            wl.cancel((sid or '').strip())
        return jsonify({'success': True})

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
        """按关键词/用户句柄搜索 X 推文（SearchTimeline）。缓存优先：同一 (q, product, 翻页)
        未过期/非 force 时直接返回，不重爬（用户明确要求重搜才 force=1）。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        if not (request.args.get('q') or '').strip():
            return jsonify({'success': False,
                            'message': '缺少搜索关键词'}), 400
        count = min(int(request.args.get('count', 20)), 50)
        cursor = request.args.get('cursor') or None
        product = (request.args.get('product') or 'Top').strip()
        q = request.args.get('q').strip()

        def _fetch():
            # 搜索接口必须带 X 的反爬令牌，否则一律 404；令牌按最终请求路径现算。
            _home_headers = xrun.build_headers(cookie, with_bearer=False)

            def _txid(method, path):
                return get_transaction_id(_home_headers, method, path, ua=xrun.UA)

            def _do():
                return xrun.search_tweets(cookie, q, count, cursor, product, txid_func=_txid)

            try:
                items, next_cursor = _do()
            except Exception as e1:
                # 令牌材料可能因 X 发版而失效（缓存的 ClientTransaction 用了旧 site key，
                # X 拒收 → 404）。丢弃缓存重建一次再试，避免「发版后必须重启进程」。
                if '404' in str(e1):
                    invalidate()
                    items, next_cursor = _do()
                else:
                    raise
            return {'items': items, 'next_cursor': next_cursor}

        try:
            data, ts, cached, stale = _cached('list', 'search',
                                              {'q': q, 'product': product, 'count': count, 'cursor': cursor or ''}, _fetch)
        except Exception as e:
            # 404 基本都指向 X 的反爬校验（拿不到令牌或令牌不被接受）
            if '404' in str(e):
                return jsonify({'success': False, 'message': (
                    '搜索失败: X 拒绝了请求（404）。通常是反爬令牌失效或 X 前端改版，'
                    '可稍后重试；若持续出现，请检查凭证库里的 x.com Cookie 是否仍有效。')}), 502
            return jsonify({'success': False,
                            'message': '搜索失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'cached': cached, 'fetched_at': ts, 'stale': stale,
                        'budget_bytes': _unified_budget_bytes()})

    # ---- 多关键词批量爬取：后台异步任务（连接无关） ----
    # 把"方案 / 全部重搜"的逐词爬取从浏览器循环搬到服务端线程，直接写入同一份
    # UserState 缓存（search:kw:<key> + tweets feed），用户关掉浏览器任务仍继续，
    # 回来按持久 resource_key 轮询进度即可。前端 searchKeyword 写入的就是这两个键。
    _B36 = '0123456789abcdefghijklmnopqrstuvwxyz'
    def _kw_key(q):
        s = ' '.join(str(q).strip().lower().split())  # trim + 折叠空白，与前端 kwKey 一致
        h = 5381
        for ch in s:
            h = ((h << 5) + h + ord(ch)) & 0xffffffff  # >>> 0 无符号 32 位
        n = h
        if n == 0:
            return '0'
        out = ''
        while n:
            out = _B36[n % 36] + out
            n //= 36
        return out

    def _merge_tweet(t):
        if not isinstance(t, dict):
            return t
        tid = t.get('tweet_id')
        if tid is None:
            tid = t.get('id')
        if tid is None:
            return t
        rec = dict(t)
        rec['tweet_id'] = str(tid)
        return rec

    def _bg_state_put(key, value, strategy=None, cap=None, auth=None, device=None):
        """后台线程写 UserState：请求上下文已丢，需显式带鉴权头（代理默认读 flask.request）。"""
        try:
            base = host.state._base()
            ns = host.state._ns
            url = '%s/api/user-state/%s/%s' % (base, ns, key)
            body = {'value': value, 'scope': 'user'}
            if strategy:
                body['strategy'] = strategy
            if cap is not None:
                body['cap'] = cap
            hdr = {'Content-Type': 'application/json'}
            if auth:
                hdr['Authorization'] = auth
            if device:
                hdr['X-Dbox-Device-Id'] = device
            req = urllib.request.Request(url, data=json.dumps(body).encode('utf-8'),
                                         headers=hdr, method='PUT')
            urllib.request.urlopen(req, timeout=20).read()
        except Exception:
            pass

    def _bg_state_get(key, auth=None, device=None):
        """后台线程读 UserState（与 _bg_state_put 配套的显式鉴权版本）。"""
        try:
            base = host.state._base()
            ns = host.state._ns
            url = '%s/api/user-state/%s/%s' % (base, ns, key)
            hdr = {}
            if auth:
                hdr['Authorization'] = auth
            if device:
                hdr['X-Dbox-Device-Id'] = device
            req = urllib.request.Request(url, headers=hdr, method='GET')
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode('utf-8', 'replace') or '{}')
            return data.get('value')
        except Exception:
            return None

    def _kw_cached(kk, auth=None, device=None):
        """该关键词是否已有**可用**缓存（断点续跑时据此跳过）。

        上次明确失败的（err 非空）不该跳过——那正是需要重爬的。
        """
        v = _bg_state_get('search:kw:' + kk, auth=auth, device=device)
        if not isinstance(v, dict):
            return False
        if v.get('err'):
            return False
        return bool(v.get('top') or v.get('latest'))

    def _start_search_worker(keywords, resource_key, task_id, cookie, auth, device,
                             resume=False):
        """启动批量爬取线程。

        resume=True：跳过已有缓存的关键词，用于**从中断处继续**——
        批量搜索往往要爬几十个词，中断后从头再来既费时又浪费接口配额。
        """
        def _txid(method, path):
            return get_transaction_id(xrun.build_headers(cookie, with_bearer=False),
                                      method, path, ua=xrun.UA)

        def worker():
            n = len(keywords)
            done = 0
            try:
                for q in keywords:
                    kk = _kw_key(q)
                    if resume and _kw_cached(kk, auth=auth, device=device):
                        done += 1
                        pct = int(done / n * 100)
                        _report_task(task_id, progress=pct, stage='爬取中',
                                     detail='已跳过（上次已完成）: ' + q)
                        _progress_store.update(resource_key, percent=pct,
                                               message='已跳过: ' + q)
                        continue
                    top_items, top_cur = [], None
                    latest_items, latest_cur = [], None
                    try:
                        top_items, top_cur = xrun.search_tweets(
                            cookie, q, 50, None, 'Top', txid_func=_txid)
                    except Exception:
                        top_cur = None
                    try:
                        latest_items, latest_cur = xrun.search_tweets(
                            cookie, q, 50, None, 'Latest', txid_func=_txid)
                    except Exception:
                        latest_cur = None
                    merged = [_merge_tweet(it) for it in ((top_items or []) + (latest_items or []))]
                    if merged:
                        # 与前端 XTWEETS 同一份内容缓存：feed('tweets') 在 UserState 的键是 feed:tweets:items
                        _bg_state_put('feed:tweets:items', merged, strategy='union_by_id', cap=1500,
                                      auth=auth, device=device)
                    err = '' if (top_items or latest_items) else '未返回结果'
                    _bg_state_put('search:kw:' + kk,
                                  {'q': q, 'top': (top_items or []), 'latest': (latest_items or []),
                                   'cursorTop': top_cur, 'cursorLatest': latest_cur,
                                   'ts': int(time.time() * 1000), 'err': err},
                                  strategy='lww', auth=auth, device=device)
                    done += 1
                    pct = int(done / n * 100)
                    _report_task(task_id, progress=pct, stage='爬取中', detail='已爬取: ' + q)
                    _progress_store.update(resource_key, percent=pct, message='已爬取: ' + q)
                _report_task(task_id, status='completed', progress=100, stage='完成',
                             detail='全部关键词已爬取')
                _progress_store.mark_completed(resource_key, '全部关键词已爬取')
            except Exception as e:
                _report_task(task_id, status='failed', detail=str(e))
                _progress_store.mark_failed(resource_key, str(e))

        threading.Thread(target=worker, daemon=True, name='x-search').start()

    @bp.route('/search/run', methods=['POST'])
    @host.login_required
    def search_run():
        """后台批量爬取多个关键词到本地缓存，返回持久 task_id + resource_key。"""
        data = request.get_json(force=True, silent=True) or {}
        keywords = [str(k).strip() for k in (data.get('keywords') or []) if str(k).strip()]
        if not keywords:
            return jsonify({'success': False, 'message': '缺少关键词'}), 400
        owner_id = data.get('owner_id', getattr(g, 'user_id', None))
        library_id = data.get('library_id') or (data.get('params') or {}).get('library_id')
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False, 'message': '未配置 x.com 登录 Cookie'}), 400
        auth = request.headers.get('Authorization')
        device = request.headers.get('X-Dbox-Device-Id')

        job_id = uuid.uuid4().hex
        resource_key = 'xsearch:' + uuid.uuid4().hex[:16]
        task_id = None
        try:
            _t = host.tasks.create(
                title=data.get('title') or ('X 搜索 ' + (keywords[0] if len(keywords) == 1
                                                          else ('%d 个关键词' % len(keywords)))),
                owner_id=owner_id, status='running', progress=0,
                stage='准备中', detail='开始批量爬取', library_id=library_id,
                params={'resource_key': resource_key, 'keywords': keywords, 'job_id': job_id},
            )
            task_id = _t.get('task_id') if isinstance(_t, dict) else getattr(_t, 'task_id', None)
        except Exception:
            task_id = None

        # 立即落一条 running 进度，确保前端首轮轮询就能取到记录。否则 worker 仍在爬第一个词、
        # 进度表尚无记录 → /status 返回 404 → 前端误判「任务不存在」停止轮询，按钮卡在 0%。
        try:
            _progress_store.upsert(resource_key, platform='x', resource_id=resource_key,
                                   status='running', percent=0, message='准备中')
        except Exception:
            pass

        _start_search_worker(keywords, resource_key, task_id, cookie, auth, device,
                             resume=bool(data.get('resume')))
        return jsonify({'success': True, 'task_id': task_id,
                        'resource_key': resource_key, 'job_id': job_id})

    @bp.route('/search/rerun', methods=['POST'])
    @host.login_required
    def search_rerun():
        """重跑一次批量搜索（失败/取消后）：复用原任务与关键词，从头开始。

        与 resume 的区别：rerun 不跳过任何关键词，全部重爬。
        """
        return _search_continue(request, resume=False)

    @bp.route('/search/resume', methods=['POST'])
    @host.login_required
    def search_resume():
        """从中断处继续：复用原任务，只爬上次没完成的关键词。

        为什么需要单独一个入口：批量搜索常要爬几十个关键词，中途重启/断网后
        任务会卡在 running（现由统一任务表的重启回收标记为 interrupted）。
        从头重跑既慢又浪费接口配额，而每个关键词爬完就已落缓存，
        所以「跳过已完成的」即可精确续跑。
        """
        return _search_continue(request, resume=True)

    def _search_continue(req, resume):
        """按已有任务继续/重跑批量搜索。

        resume=True  ：跳过已缓存的关键词（从中断处继续）
        resume=False ：全部重爬（失败后重跑）
        """
        data = req.get_json(force=True, silent=True) or {}
        task_id = str(data.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': '缺少 task_id'}), 400
        try:
            t = host.tasks.get(task_id)
        except Exception:
            t = None
        if not t:
            return jsonify({'success': False, 'message': '任务不存在'}), 404
        params = t.get('params') or {}
        keywords = [str(k).strip() for k in (params.get('keywords') or []) if str(k).strip()]
        if not keywords:
            return jsonify({'success': False, 'message': '该任务没有可继续的关键词'}), 400
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False, 'message': '未配置 x.com 登录 Cookie'}), 400
        auth = req.headers.get('Authorization')
        device = req.headers.get('X-Dbox-Device-Id')
        resource_key = params.get('resource_key') or ('xsearch:' + uuid.uuid4().hex[:16])
        # 复用同一条任务：回到进行中，进度由 worker 从已完成的关键词数继续往上走
        try:
            host.tasks.update(
                task_id, status='running',
                stage='继续中' if resume else '重跑中',
                detail='从中断处继续' if resume else '重新爬取全部关键词',
                error_code='')
        except Exception:
            pass
        _start_search_worker(keywords, resource_key, task_id, cookie, auth, device,
                             resume=resume)
        return jsonify({'success': True, 'task_id': task_id,
                        'resource_key': resource_key})

    @bp.route('/user_tweets', methods=['GET'])
    @host.login_required
    def user_tweets_ep():
        """拉取某用户的推文时间线（UserTweets），并附带该用户资料。

        与搜索/时间线同源：dbox 的 X 拓展自己用 GraphQL 解析，不打开 x.com 链接。
        user 可为 @句柄 或 内部 rest_id；cursor 用于分页。缓存优先：同一用户列表
        未过期/非 force 直接返回，不重打 X（画师推文更新慢，能看很久）。
        """
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        user = (request.args.get('user') or '').strip().lstrip('@')
        if not user:
            return jsonify({'success': False, 'message': '缺少用户名（user）'}), 400
        count = min(int(request.args.get('count', 20)), 50)
        cursor = request.args.get('cursor') or None

        def _fetch():
            _home_headers = xrun.build_headers(cookie, with_bearer=False)

            def _txid(method, path):
                return get_transaction_id(_home_headers, method, path, ua=xrun.UA)

            items, next_cursor, profile = xrun.user_tweets(
                cookie, user, count, cursor, txid_func=_txid)
            # 跨设备共享缓存：拉取结果先并入服务端 UserState（union_by_id 去重、封顶 1500），
            # key 用稳定的 user_id（rest_id）而非 screen_name。首次加载返回合并 canonical。
            canonical = None
            try:
                user_id = str((profile or {}).get('rest_id')
                              or (profile or {}).get('id') or '')
                if user_id:
                    norm = []
                    for it in (items or []):
                        if not isinstance(it, dict):
                            continue
                        rec = dict(it)
                        tid = it.get('tweet_id')
                        rec['id'] = str(tid if tid is not None else (it.get('id') or ''))
                        rec['order'] = it.get('created_at')
                        norm.append(rec)
                    merged = host.state.put('feed:user:' + user_id, norm,
                                             strategy='union_by_id', cap=1500)
                    if isinstance(merged, dict) and isinstance(merged.get('value'), list):
                        canonical = merged['value']
                    umeta = {k: (profile or {}).get(k) for k in (
                        'rest_id', 'screen_name', 'name', 'avatar', 'bio', 'verified',
                        'statuses_count', 'following_count', 'followers_count')}
                    host.state.put('users:' + user_id, umeta, strategy='lww')
            except Exception:
                canonical = None
            return {'user': profile,
                    'items': canonical if (canonical is not None and not cursor) else items,
                    'next_cursor': next_cursor,
                    'canonical': canonical is not None and not cursor}

        try:
            data, ts, cached, stale = _cached('list', 'user_tweets',
                                              {'user': user, 'count': count, 'cursor': cursor or ''}, _fetch)
        except Exception as e:
            if '404' in str(e):
                return jsonify({'success': False, 'message': (
                    '获取用户推文失败: X 拒绝了请求（404）。可能是反爬令牌失效或 X 前端改版，'
                    '可稍后重试；若持续出现，请检查凭证库里的 x.com Cookie 是否仍有效。')}), 502
            return jsonify({'success': False,
                            'message': '获取用户推文失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'cached': cached, 'fetched_at': ts,
                        'stale': stale, 'budget_bytes': _unified_budget_bytes()})

    @bp.route('/tweet/<tweet_id>', methods=['GET'])
    @host.login_required
    def tweet_detail(tweet_id):
        """拉取单条推文详情 + 评论区（对话线程）。缓存优先：资源本体（推文）发布后基本不变，
        未过期/非 force 直接返回，不重打 X。cursor 用于评论区翻页（各自独立键）。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        cursor = request.args.get('cursor') or None

        def _fetch():
            # 与 /search 一致：带上 X 的反爬令牌（x-client-transaction-id）。
            # 详情路径此前一直没带，被 X 判为机器人 → 大量 429（实测日志上万次）。
            _home_headers = xrun.build_headers(cookie, with_bearer=False)

            def _txid(method, path):
                return get_transaction_id(_home_headers, method, path, ua=xrun.UA)

            return xrun.get_tweet_thread(tweet_id, cookie, cursor, txid_func=_txid)

        try:
            data, ts, cached, stale = _cached('item', 'tweet', {'id': tweet_id, 'cursor': cursor or ''}, _fetch)
        except xrun.XRateLimited as e:
            # 限流要显式告知前端「多久后再试」——别伪装成普通失败，
            # 否则前端会以为数据有问题、用户也会反复点、进一步恶化限流。
            return jsonify({'success': False, 'rate_limited': True,
                            'retry_after': int(getattr(e, 'retry_after', 60) or 60),
                            'message': str(e)}), 429
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取推文详情失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'cached': cached, 'fetched_at': ts,
                        'stale': stale, 'budget_bytes': _unified_budget_bytes()})

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
        """实时从 X 账号收藏夹拉取推文列表。缓存优先：未过期/非 force 直接返回，不重打 X。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        count = min(int(request.args.get('count', 30)), 100)
        cursor = request.args.get('cursor') or None

        def _fetch():
            return xrun.list_bookmarks(cookie, count, cursor)

        try:
            data, ts, cached, stale = _cached('list', 'bookmarks', {'count': count, 'cursor': cursor or ''}, _fetch)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取 X 收藏失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'cached': cached, 'fetched_at': ts,
                        'stale': stale, 'budget_bytes': _unified_budget_bytes()})

    @bp.route('/likes', methods=['GET'])
    @host.login_required
    def likes():
        """实时从 X 账号「喜欢」列表拉取推文。缓存优先。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False,
                            'message': '未配置 x.com 登录 Cookie'}), 400
        rest_id = _my_rest_id(cookie)
        if not rest_id:
            return jsonify({'success': False,
                            'message': '无法从 Cookie 识别登录用户'}), 400
        count = min(int(request.args.get('count', 30)), 100)
        cursor = request.args.get('cursor') or None

        def _fetch():
            return xrun.list_likes(cookie, rest_id, count, cursor)

        try:
            data, ts, cached, stale = _cached('list', 'likes', {'count': count, 'cursor': cursor or ''}, _fetch)
        except Exception as e:
            return jsonify({'success': False,
                            'message': '拉取 X 喜欢失败: ' + str(e)}), 502
        return jsonify({'success': True, **data, 'cached': cached, 'fetched_at': ts,
                        'stale': stale, 'budget_bytes': _unified_budget_bytes()})

    @bp.route('/cache/stats', methods=['GET'])
    @host.login_required
    def cache_stats():
        """缓存概况：内容（SQLite LRU）与媒体（磁盘 LRU）各自占用 + 统一预算。"""
        st = _CACHE_PART.stat()
        return jsonify({'success': True,
                        'content': _cache.stats(_unified_budget_bytes()),
                        'media': {'files': st['count'], 'bytes': st['bytes'],
                                  'budget_bytes': _unified_budget_bytes()}})

    @bp.route('/cache/clear', methods=['POST', 'DELETE'])
    @host.login_required
    def cache_clear():
        """清缓存：带 kind/ns/key 则精确清除，都不带则全部清空。"""
        n = _cache.invalidate(request.args.get('kind') or None,
                              request.args.get('ns') or None,
                              request.args.get('key') or None)
        return jsonify({'success': True, 'removed': n})

    @bp.route('/cache/ttl', methods=['GET', 'POST'])
    @host.login_required
    def cache_ttl():
        """LRU 模式下已无 TTL 概念；保留接口仅用于查询统一预算，便于兼容旧调用。"""
        return jsonify({'success': True, 'ttls': {}, 'budget_bytes': _unified_budget_bytes()})

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


    @bp.route('/stars', methods=['GET', 'POST'])
    @host.login_required
    def stars():
        """dbox 内「星标推文」：与 X 书签无关，仅存 dbox user_state，可跨设备回看。

        GET 返回星标列表；POST 加入一条（body 为推文项，含 tweet_id）；
        删除走 DELETE /stars/<tweet_id>。写入用 lww 整表覆盖（个人收藏，并发冲突可接受）。
        """
        key = 'feed:stars:items'
        if request.method == 'POST':
            item = request.get_json(force=True, silent=True) or {}
            tid = item.get('tweet_id')
            if not tid:
                return jsonify({'success': False, 'message': '缺少 tweet_id'}), 400
            cur = host.state.get(key) or []
            if not isinstance(cur, list):
                cur = []
            cur = [x for x in cur if isinstance(x, dict) and str(x.get('tweet_id')) != str(tid)]
            rec = dict(item)
            rec['id'] = str(tid)
            rec['starred_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
            cur.insert(0, rec)
            if len(cur) > 2000:
                cur = cur[:2000]
            host.state.put(key, cur, strategy='lww')
            return jsonify({'success': True, 'items': cur})
        items = host.state.get(key) or []
        if not isinstance(items, list):
            items = []
        return jsonify({'success': True, 'items': items})

    @bp.route('/stars/<tweet_id>', methods=['DELETE'])
    @host.login_required
    def star_del(tweet_id):
        """从 dbox 星标收藏移除一条。"""
        key = 'feed:stars:items'
        cur = host.state.get(key) or []
        if not isinstance(cur, list):
            cur = []
        new = [x for x in cur if isinstance(x, dict) and str(x.get('tweet_id')) != str(tweet_id)]
        host.state.put(key, new, strategy='lww')
        return jsonify({'success': True, 'items': new})

    def _my_rest_id(cookie):
        """从 x.com Cookie 的 twid 字段解析登录用户 rest_id（数字 id）。"""
        if not cookie:
            return None
        for part in cookie.split(';'):
            k, _, v = part.partition('=')
            if k.strip() == 'twid':
                v = urllib.parse.unquote(v.strip())
                rid = v.split('=', 1)[1] if '=' in v else v
                rid = rid.strip()
                if rid.isdigit():
                    return rid
        return None

    @bp.route('/me', methods=['GET'])
    @host.login_required
    def me():
        """返回登录用户（我）的资料卡数据。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False, 'message': '未配置 x.com 登录 Cookie'}), 400
        rest_id = _my_rest_id(cookie)
        if not rest_id:
            return jsonify({'success': False,
                            'message': '无法从 Cookie 识别登录用户，请重新登录 X 或刷新凭证'}), 400
        try:
            profile = xrun.user_by_rest_id(cookie, rest_id)
        except Exception as e:
            return jsonify({'success': False, 'message': '获取个人资料失败: ' + str(e)}), 502
        if profile:
            try:
                umeta = {k: profile.get(k) for k in (
                    'rest_id', 'screen_name', 'name', 'avatar', 'bio', 'verified',
                    'statuses_count', 'following_count', 'followers_count')}
                host.state.put('users:' + str(rest_id), umeta, strategy='lww')
            except Exception:
                pass
        return jsonify({'success': True, 'user': profile})

    @bp.route('/following', methods=['GET'])
    @host.login_required
    def following():
        """登录用户【关注的人】列表。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False, 'message': '未配置 x.com 登录 Cookie'}), 400
        rest_id = _my_rest_id(cookie)
        if not rest_id:
            return jsonify({'success': False, 'message': '无法从 Cookie 识别登录用户'}), 400
        try:
            count = min(int(request.args.get('count', 40)), 100)
            cursor = request.args.get('cursor') or None
            _h = xrun.build_headers(cookie, with_bearer=False)
            def _txid(method, path):
                return get_transaction_id(_h, method, path, ua=xrun.UA)
            items, next_cursor = xrun.list_following(
                cookie, rest_id, count, cursor, txid_func=_txid)
        except Exception as e:
            return jsonify({'success': False, 'message': '拉取关注列表失败: ' + str(e)}), 502
        return jsonify({'success': True, 'items': items, 'next_cursor': next_cursor})

    @bp.route('/followers', methods=['GET'])
    @host.login_required
    def followers():
        """登录用户【粉丝】列表。"""
        cookie = _x_cookie_header()
        if not cookie:
            return jsonify({'success': False, 'message': '未配置 x.com 登录 Cookie'}), 400
        rest_id = _my_rest_id(cookie)
        if not rest_id:
            return jsonify({'success': False, 'message': '无法从 Cookie 识别登录用户'}), 400
        try:
            count = min(int(request.args.get('count', 40)), 100)
            cursor = request.args.get('cursor') or None
            _h = xrun.build_headers(cookie, with_bearer=False)
            def _txid(method, path):
                return get_transaction_id(_h, method, path, ua=xrun.UA)
            items, next_cursor = xrun.list_followers(
                cookie, rest_id, count, cursor, txid_func=_txid)
        except Exception as e:
            return jsonify({'success': False, 'message': '拉取粉丝列表失败: ' + str(e)}), 502
        return jsonify({'success': True, 'items': items, 'next_cursor': next_cursor})


    @bp.route('/static/<path:f>', methods=['GET'])
    def static_asset(f):
        # 本地托管插件前端依赖（hls.js 等），避免依赖外部 CDN：
        # 部分网络/离线环境下 CDN 不可达 → hls.js 加载失败、X 的 m3u8 视频点开只剩白框，
        # 偶发刷新（CDN 恰好可达）才正常。改为同源本地托管后稳定可用。
        from flask import send_file
        base = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
        p = os.path.normpath(os.path.join(base, f))
        if p != base and not p.startswith(base + os.sep):
            return jsonify({'success': False, 'message': '非法路径'}), 400
        if not os.path.isfile(p):
            return jsonify({'success': False, 'message': '未找到'}), 404
        if f.endswith('.js'):
            ct = 'application/javascript'
        elif f.endswith('.css'):
            ct = 'text/css'
        else:
            ct = mimetypes.guess_type(p)[0] or 'application/octet-stream'
        resp = send_file(p, mimetype=ct, conditional=True, max_age=86400)
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp

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
        # force=1：绕过本地 LRU 缓存、忽略失败黑名单，强制重新从上游取图。
        # 用于「某张图被坏缓存（旧 bug 期写入的空/残缺响应）卡住、怎么刷新都不出」时，
        # 以推文为单位重新获取——前端给该媒体 URL 追加 &force=1 即可覆盖坏缓存。
        force = request.args.get('force') in ('1', 'true', 'yes')
        # 命中缓存：直接用本地文件响应（send_file 自动支持 Range）
        if not force:
            hit = _cache_get(url)
            if hit is not None:
                path, ext = hit
                if os.path.isfile(path):
                    # m3u8 不在 mimetypes 默认表里，必须显式兜底：否则缓存命中时会被
                    # 当成 image/jpeg 返回，hls.js 拿不到播放列表。
                    ct = mimetypes.guess_type(path)[0] or (
                        'application/vnd.apple.mpegurl' if ext == '.m3u8'
                        else 'video/mp2t' if ext == '.ts'
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
        # 同一 url 近期失败过：直接报错，避免再次无谓等待。
        # 但记录带 TTL（_MEDIA_ERR_TTL），过期即放行重试——否则代理恢复后
        # 该 url 会被永久判死，表现为「怎么刷新图片都不出来」。
        # force 模式下不读黑名单，直接重试。
        if not force:
            with _media_dl_lock:
                _err_ts = _media_dl_err.get(url)
            if _err_ts is not None:
                if (time.time() - _err_ts) < _MEDIA_ERR_TTL:
                    return jsonify({'success': False, 'message': '代理失败（稍后自动重试）'}), 502
                with _media_dl_lock:
                    _media_dl_err.pop(url, None)

        _start_media_download(url, tmp_path, ext)

        # mp4 / m3u8 / HLS 分片(ts、m4s、音频分片等) 都必须「等下载完整再返回」，绝不能边下边播。
        # 它们都是「残缺即废」的容器格式：
        #  · m3u8 是文本播放列表索引，只下到一半就缺分片信息，hls.js 解析必失败；
        #  · mp4 的 moov atom（元数据）常位于文件【末尾】；
        #  · ts/m4s 是 HLS 分片，截断即解不出画面，hls.js 会不停报网络/媒体错误并
        #    静默重试 → 表现为「黑屏转圈非常久都不播放」；
        #  · 更关键的是边下边播响应会把 Content-Range 的总长谎报成「当时已下的字节数」，
        #    浏览器据此认为文件已完整下载，实际拿到的是截断文件 → 必然无法起播。
        #    实测（缓存清空后首次请求）：只拿到完整文件的 82.5%
        #    —— 458752 字节 / Content-Range: bytes 0-458751/458752，
        #       而完整文件是 555925 字节。
        # 刷新时文件早已完整进缓存、走上面的 send_file 分支返回完整内容，
        # 于是「刷新后就能播」——这正是该问题【必现】而非偶发竞态的原因。
        # 历史坑：先只处理了 m3u8、后补了 mp4，却漏了 HLS 的 ts/m4s 分片（落进图片那条
        # 「.part 一有字节就返回 + 谎报总长」的路径），首播必被截断，与「有时刷新才好」同源。
        # Flask 以 threaded=True 运行，等待只占用本请求线程，不会阻塞整个服务。
        if ext in _COMPLETE_ONLY_EXT:
            final_path = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext)
            _mt0 = time.time()
            # 只给一小段「宽限期」等它下完：小文件基本瞬间完成，可一次性返回完整
            # 内容并登记缓存。超过宽限期立刻转流式边下边播——用户马上看到画面，
            # 而不是干等满 60 秒。
            #
            # （此前是硬等 60 秒，因为当时降级路径会【谎报总长】、浏览器拿到截断
            #  文件根本播不了，只能指望等满后拿到完整文件。现在降级路径会带上
            #  上游声明的真实总长（_media_declared_size），边下边播是有效的，
            #  因此没必要再让用户干等。下载线程仍在后台跑，完成后照常登记缓存。）
            _ev = _media_event(url)
            while (time.time() - _mt0) < _COMPLETE_GRACE_SEC:
                with _media_dl_lock:
                    _dl_failed = url in _media_dl_err
                if _dl_failed:
                    break
                if os.path.exists(final_path):
                    try:
                        resp = send_file(final_path, mimetype=ct, conditional=True,
                                         max_age=86400)
                        resp.headers['Cache-Control'] = 'private, max-age=86400'
                        return resp
                    except Exception:
                        break
                # 事件驱动：下载一结束立刻被唤醒，不再 0.1s 忙等。
                # 仍按较长间隔复查一次，防止事件因故丢失时卡死。
                _left = _COMPLETE_GRACE_SEC - (time.time() - _mt0)
                if _left <= 0:
                    break
                _ev.wait(min(_left, 0.5))
            return _serve_media_partial(tmp_path, ct, request, url, ext)
        if ext == '.m3u8':
            # HLS 清单必须整份下完再返回，且要把片源地址改写成走本代理（见 _rewrite_m3u8）：
            # 否则 hls.js 直连 video.twimg.com 现拉分片，绕过服务端 cookie 且不进本地缓存，
            # 表现为「每次点开视频都重新从 X 拉流、首播与回看都慢」。改写后分片经 /media
            # 带 cookie 拉取并落盘，看过的视频二次秒开、首播也走服务端代理更稳。
            final_path = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext)
            _mt0 = time.time()
            _ev = _media_event(url)
            while (time.time() - _mt0) < _M3U8_WAIT_SEC:
                with _media_dl_lock:
                    _dl_failed = url in _media_dl_err
                if _dl_failed:
                    break
                if os.path.exists(final_path):
                    try:
                        with open(final_path, 'rb') as _f:
                            _raw = _f.read().decode('utf-8', 'replace')
                        _body = _rewrite_m3u8(_raw)
                        resp = Response(_body, mimetype='application/vnd.apple.mpegurl')
                        resp.headers['Cache-Control'] = 'private, max-age=86400'
                        resp.headers['Access-Control-Allow-Origin'] = '*'
                        return resp
                    except Exception:
                        break
                _left = _M3U8_WAIT_SEC - (time.time() - _mt0)
                if _left <= 0:
                    break
                _ev.wait(min(_left, 0.5))
            # 超时/已失败：回落到既有的边下边播路径，保留原有降级行为
            return _serve_media_partial(tmp_path, ct, request, url, ext)

        # 图片：渐进呈现是对的，给下载线程一点启动时间即可放行。
        # 但同样要盯【正式缓存文件】——下载完成后 .part 已被改名登记，tmp_path
        # 随之不存在，只等 tmp_path 会白白空等满 8 秒再掉进流式路径（慢一拍）。
        # 若已确定失败则直接返回 502 而不是 200 空流——流式响应一旦发出头就
        # 改不了状态码，浏览器 <img> 收到 200 空 body 只会静默显示破图，
        # 既无法触发 onerror 重试也让用户以为是坏了。
        _final_path = os.path.join(_CACHE_LRU_DIR, _cache_key(url) + ext)
        _t0 = time.time()
        _ev = _media_event(url)
        while (time.time() - _t0) < 8.0:
            with _media_dl_lock:
                _failed = url in _media_dl_err
            if _failed:
                return jsonify({'success': False, 'message': '代理失败'}), 502
            # 已下完并登记：直接返回完整文件
            if os.path.exists(_final_path):
                try:
                    resp = send_file(_final_path, mimetype=ct, conditional=True,
                                     max_age=86400)
                    resp.headers['Cache-Control'] = 'private, max-age=86400'
                    return resp
                except Exception:
                    break
            try:
                if os.path.getsize(tmp_path) > 0:
                    break
            except OSError:
                pass
            # 事件驱动：下载结束即唤醒；未结束则按较短间隔复查是否已有字节可读
            _left = 8.0 - (time.time() - _t0)
            if _left <= 0:
                break
            _ev.wait(min(_left, 0.2))
        return _serve_media_partial(tmp_path, ct, request, url, ext)

    @bp.route('/media/cache', methods=['GET'])
    @host.login_required
    def media_cache_list():
        """缓存页统一清单：内容(SQLite LRU) + 媒体(磁盘 LRU) + 统一字节预算。
        前端「缓存管理」页据此列出统一缓存占用（文字/列表/资料与头像/图/视频一并可见）。"""
        st = _CACHE_PART.stat()
        media_files = []
        try:
            for fn in os.listdir(_CACHE_LRU_DIR):
                if fn.startswith('.'):
                    continue
                full = os.path.join(_CACHE_LRU_DIR, fn)
                if not os.path.isfile(full):
                    continue
                ext = os.path.splitext(fn)[1].lower()
                try:
                    sz = os.path.getsize(full)
                except OSError:
                    sz = 0
                media_files.append({
                    'file': fn, 'size': sz,
                    'type': 'video' if ext in ('.mp4', '.m3u8', '.mov', '.webm', '.m4v') else 'image',
                })
        except Exception:
            pass
        cs = _cache.stats(_unified_budget_bytes())
        return jsonify({
            'success': True,
            'media': {'files': media_files, 'count': st['count'], 'bytes': st['bytes'],
                      'cap': st['cap']},
            'content': {'total': cs['total'], 'bytes': cs['bytes'],
                        'groups': cs.get('groups', [])},
            'budget_bytes': _unified_budget_bytes(),
        })

    @bp.route('/media/cache', methods=['DELETE'])
    @host.login_required
    def media_cache_clear():
        """媒体缓存清空（兼容旧调用方；统计/上限改由中心「缓存管理」页统一治理）。"""
        _CACHE_PART.clear()
        return jsonify({'success': True})

    return bp
