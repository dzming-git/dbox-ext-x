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
import json
import uuid
import threading
import subprocess

from flask import Blueprint, request, g, jsonify


def create_blueprint(host):
    bp = Blueprint('x_downloader', __name__, url_prefix=host.url_prefix)

    # 进程级任务状态（存于 host.app_state，框架不干预内容）
    jobs = host.app_state.setdefault('jobs', {})
    jobs_lock = host.app_state.setdefault('jobs_lock', threading.Lock())
    input_events = host.app_state.setdefault('input_events', {})

    plugin_dir = os.path.dirname(os.path.abspath(__file__))   # .../x_downloader/backend
    plugin_root = os.path.dirname(plugin_dir)                 # .../x_downloader

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
                'proc': proc, 'wd': wd,
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
                    elif t == 'log':
                        _append_log(job_id, obj.get('message', ''))
                    elif t == 'error':
                        jobs[job_id]['error'] = obj.get('message')
                        _append_log(job_id, 'ERROR: ' + obj.get('message', ''))
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

    return bp
