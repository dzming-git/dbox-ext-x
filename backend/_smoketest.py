"""冒烟测试：用 mock host 验证 server.py 的 /run + /status + /input 预览流程（simulate 模式不联网）。

覆盖：
  1) 默认 preview=true：解析后上报 preview 事件（含 cover/mp4），提交 {'action':'download'} 下载全部并入库；
  2) 提交 {'action':'cancel'}：任务结束且不产生任何入库。
"""
import os, sys, json, time, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, request, g, jsonify

# ---- mock host ----
class FakeVault:
    def get(self, domain):
        return None  # 无 cookie，simulate 模式不依赖

class FakeHost:
    def __init__(self):
        self.url_prefix = '/api/ext/x_downloader'
        self.data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_smoke_data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.vault = FakeVault()
        self.app_state = {}
        self.ingested = []
    def login_required(self, f):
        from functools import wraps
        @wraps(f)
        def d(*a, **k):
            g.user_id = 'u1'
            return f(*a, **k)
        return d
    def ingest(self, library_id, path, kind=None, modes=('video','image'),
               hidden=False, meta=None, owner_id=None):
        self.ingested.append({'library_id': library_id, 'path': path,
                              'kind': kind, 'modes': modes, 'hidden': hidden})
        return {'success': True, 'path': path}

import importlib.util
spec = importlib.util.spec_from_file_location(
    'xdl_server',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server.py'))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

host = FakeHost()
bp = mod.create_blueprint(host)

app = Flask(__name__)
app.register_blueprint(bp)

TOKEN = 'Bearer faketoken'
H = {'Authorization': TOKEN}


def run_job(params, respond):
    """提交任务并轮询，遇到 preview 时调用 respond(input) 回写，返回最终状态与入库记录。"""
    with app.test_client() as c:
        r = c.post('/api/ext/x_downloader/run', headers=H,
                   json={'params': {**params, 'library_id': 'lib1', 'simulate': True}})
        assert r.status_code == 200, r.get_json()
        job_id = r.get_json()['job_id']
        submitted = False
        for _ in range(200):
            s = c.get('/api/ext/x_downloader/status?job_id=' + job_id, headers=H).get_json()
            pi = s.get('pending_input')
            if pi and not submitted:
                if pi.get('type') == 'preview':
                    media = pi.get('media') or []
                    vid = next((m for m in media if m.get('type') == 'video'), {})
                    assert vid.get('cover'), f'视频缺少 cover: {vid}'
                    assert vid.get('mp4'), f'视频缺少 mp4: {vid}'
                    val = respond(pi)
                else:
                    val = respond(pi)
                c.post('/api/ext/x_downloader/input', headers=H,
                       json={'job_id': job_id, 'value': val})
                submitted = True
            if s['done']:
                return s, list(host.ingested)
            time.sleep(0.4)
        raise AssertionError(f'任务未结束: percent={s.get("percent")} error={s.get("error")}')

# 场景1：默认预览 -> 确认下载全部
host.ingested.clear()
s, ingested = run_job({'url': 'https://x.com/u/status/123', 'preview': True},
                      lambda pi: {'action': 'download', 'items': list(range(len(pi['media'])))})
assert s['error'] is None, s['error']
assert len(ingested) >= 2, f'应入库视频与图集: {ingested}'
print('场景1 OK：预览 -> 确认下载全部，入库:', [(x['kind']) for x in ingested])

# 场景2：预览 -> 取消
host.ingested.clear()
s, ingested = run_job({'url': 'https://x.com/u/status/456', 'preview': True},
                      lambda pi: {'action': 'cancel'})
assert s['done'], '取消后任务应结束'
assert not ingested, f'取消后不应入库: {ingested}'
print('场景2 OK：预览 -> 取消，无入库')

print('SMOKE TEST DONE')
