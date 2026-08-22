"""临时冒烟测试：用 mock host 验证 server.py 的 /run + /status 流程（simulate 模式不联网）。"""
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

with app.test_client() as c:
    # simulate=true，多图场景会触发 await_input（auto=false）
    r = c.post('/api/ext/x_downloader/run',
               headers=H,
               json={'params': {'url': 'https://x.com/u/status/123',
                                'simulate': True, 'auto': False,
                                'library_id': 'lib1', 'hidden': True}})
    print('RUN:', r.status_code, r.get_json())
    job_id = r.get_json()['job_id']
    # 轮询状态
    for _ in range(120):
        s = c.get('/api/ext/x_downloader/status?job_id=' + job_id, headers=H).get_json()
        if s.get('pending_input'):
            # 模拟用户选择全部
            val = ['0','1','2','3']
            c.post('/api/ext/x_downloader/input', headers=H,
                   json={'job_id': job_id, 'value': val})
            print('>> submitted input:', val)
        if s['done']:
            print('DONE at iter', _, 'percent', s['percent'], 'error', repr(s['error']))
            break
        time.sleep(0.4)
    else:
        print('NOT DONE. last error=', repr(s.get('error')),
              'logs=', [repr(x) for x in s.get('logs', [])[-6:]])
    print('INGESTED:', host.ingested)
    print('SMOKE TEST DONE')
