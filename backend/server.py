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
        for f in (files or []):
            path = f.get('path')
            if not path or not os.path.isfile(path):
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
                host.ingest(
                    job['library_id'], path,
                    kind=kind, modes=tuple(modes),
                    hidden=hid, meta=meta, owner_id=job['owner_id'],
                )
                _append_log(job_id, '已入库: ' + os.path.basename(path))
            except Exception as e:
                _append_log(job_id, '入库失败: ' + str(e))

    @bp.route('/run', methods=['POST'])
    @host.login_required
    def run():
        data = request.get_json(force=True, silent=True) or {}
        params = data.get('params', {}) or {}
        library_id = data.get('library_id') or params.get('library_id')
        owner_id = data.get('owner_id', getattr(g, 'user_id', None))
        token = _bearer()
        job_id = uuid.uuid4().hex
        wd = _job_dir(job_id)

        # 物化 cookie（插件按域名从保险库取，run.py 读取文件）
        cookies_ctx = {}
        cookie_str = host.vault.get('x.com')
        if cookie_str:
            cookie_path = os.path.join(wd, 'x.com.cookie.txt')
            with open(cookie_path, 'w', encoding='utf-8') as f:
                f.write(cookie_str)
            cookies_ctx['x.com'] = {'path': cookie_path}

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
                        # 暂停读取，等待前端通过 /input 回写选择（与 run.py 的长轮询对齐）
                        jobs[job_id]['pending_input'] = obj.get('input')
                        input_events[job_id].wait(timeout=30)
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
