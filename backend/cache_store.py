"""X 扩展的通用缓存层：资源列表 / 资源本体两套（与 pixiv 同设计，TTL 按 X 语义调）。

## 为什么分两套

「资源列表」和「资源本体」的变化频率与失效语义完全不同：

- **list（资源列表）**：关注流、收藏、喜欢、搜索结果、画师推文、关注/粉丝列表。
  会**新增**条目（你又关注的人发了新推），但已有条目内容基本不变。键含分页参数，TTL 中等。
- **item（资源本体）**：单条推文详情（正文/媒体/对话线程）。一旦发出来几乎不会再变，TTL 很长。
  键即推文 id，与「从哪个列表点进来的」无关。

## 取数约定

只有三种情况会真正打到 X：
1. 缓存里没有（首次）；
2. 缓存**已过期**（超过该来源的 TTL）；
3. 调用方显式 `force=True`（用户点了刷新）。

其余一律走缓存，一个网络包都不发。每条记录带 `fetched_at`，前端显示「缓存于 …」。

网络失败时若存在过期旧缓存，降级返回旧数据并标 `stale`（比白屏有用——X 常限流/反爬）。
"""

import json
import os
import sqlite3
import threading
import time

# 默认 TTL（秒）。X 的关注流更新较频、推文本体基本不变。
DEFAULT_TTL = {
    # ---- 列表类 ----
    ('list', 'timeline'): 30 * 60,        # 关注流：30 分钟
    ('list', 'user_tweets'): 24 * 3600,   # 画师推文：24 小时
    ('list', 'bookmarks'): 60 * 60,       # 收藏：1 小时
    ('list', 'likes'): 60 * 60,           # 喜欢：1 小时
    ('list', 'search'): 6 * 3600,         # 搜索结果：6 小时
    ('list', 'following'): 24 * 3600,     # 关注列表：24 小时
    ('list', 'followers'): 24 * 3600,     # 粉丝列表：24 小时
    ('list', 'me'): 24 * 3600,            # 我的资料：24 小时
    # ---- 资源本体类 ----
    ('item', 'tweet'): 30 * 24 * 3600,    # 推文详情：30 天（发布后基本不变）
}

DEFAULT_TTL_FALLBACK = 3600
MAX_ROWS_PER_NS = 2000


def _norm_key(params):
    if params is None:
        return ''
    if isinstance(params, (list, tuple)):
        items = [str(p) for p in params]
    elif isinstance(params, dict):
        items = ['%s=%s' % (k, params[k]) for k in sorted(params.keys())]
    else:
        items = [str(params)]
    return '|'.join(items)


class CacheStore(object):
    """SQLite 支撑的双套缓存（线程安全）。"""

    def __init__(self, db_path, ttl_overrides=None):
        self._path = db_path
        self._lock = threading.RLock()
        self._ttl = dict(DEFAULT_TTL)
        if ttl_overrides:
            for k, v in (ttl_overrides or {}).items():
                try:
                    self._ttl[tuple(k) if isinstance(k, (list, tuple)) else k] = int(v)
                except Exception:
                    pass
        self._init_db()

    def _conn(self):
        d = os.path.dirname(self._path)
        if d:
            os.makedirs(d, exist_ok=True)
        conn = sqlite3.connect(self._path, timeout=15)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
        except Exception:
            pass
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._conn()
            try:
                conn.execute('''CREATE TABLE IF NOT EXISTS cache (
                    kind TEXT NOT NULL,
                    ns   TEXT NOT NULL,
                    key  TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    fetched_at REAL NOT NULL,
                    PRIMARY KEY (kind, ns, key))''')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_cache_ns_time '
                             'ON cache(kind, ns, fetched_at)')
                conn.commit()
            finally:
                conn.close()

    def ttl_of(self, kind, ns):
        return int(self._ttl.get((kind, ns), DEFAULT_TTL_FALLBACK))

    def set_ttl(self, kind, ns, seconds):
        try:
            self._ttl[(kind, ns)] = max(0, int(seconds))
        except Exception:
            pass

    def ttls(self):
        return {'%s:%s' % (k[0], k[1]): v for k, v in self._ttl.items()}

    def get(self, kind, ns, key):
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    'SELECT payload, fetched_at FROM cache WHERE kind=? AND ns=? AND key=?',
                    (kind, ns, key)).fetchone()
            finally:
                conn.close()
        if not row:
            return (None, None, None)
        fetched_at = float(row[1])
        age = time.time() - fetched_at
        if age > self.ttl_of(kind, ns):
            return (None, None, None)
        try:
            return (json.loads(row[0]), fetched_at, age)
        except Exception:
            return (None, None, None)

    def get_any(self, kind, ns, key):
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    'SELECT payload, fetched_at FROM cache WHERE kind=? AND ns=? AND key=?',
                    (kind, ns, key)).fetchone()
            finally:
                conn.close()
        if not row:
            return (None, None)
        try:
            return (json.loads(row[0]), float(row[1]))
        except Exception:
            return (None, None)

    def put(self, kind, ns, key, payload):
        now = time.time()
        blob = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    'INSERT OR REPLACE INTO cache(kind, ns, key, payload, fetched_at) '
                    'VALUES (?,?,?,?,?)', (kind, ns, key, blob, now))
                conn.execute(
                    'DELETE FROM cache WHERE rowid IN ('
                    '  SELECT rowid FROM cache WHERE kind=? AND ns=? '
                    '  ORDER BY fetched_at DESC LIMIT -1 OFFSET ?)',
                    (kind, ns, MAX_ROWS_PER_NS))
                conn.commit()
            finally:
                conn.close()
        return now

    def invalidate(self, kind=None, ns=None, key=None):
        where, args = [], []
        if kind:
            where.append('kind=?'); args.append(kind)
        if ns:
            where.append('ns=?'); args.append(ns)
        if key:
            where.append('key=?'); args.append(key)
        sql = 'DELETE FROM cache' + (' WHERE ' + ' AND '.join(where) if where else '')
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(sql, args)
                conn.commit()
                return cur.rowcount or 0
            finally:
                conn.close()

    def stats(self):
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    'SELECT kind, ns, COUNT(*), MAX(fetched_at), MIN(fetched_at) '
                    'FROM cache GROUP BY kind, ns').fetchall()
                total = conn.execute('SELECT COUNT(*) FROM cache').fetchone()[0]
            finally:
                conn.close()
        groups = [{'kind': r[0], 'ns': r[1], 'count': r[2],
                   'last_fetched_at': r[3], 'first_fetched_at': r[4],
                   'ttl': self.ttl_of(r[0], r[1])} for r in rows]
        size = 0
        try:
            size = os.path.getsize(self._path)
        except Exception:
            pass
        return {'total': total, 'groups': groups, 'db_bytes': size, 'path': self._path}


def serve_cached(store, kind, ns, params, fetch_fn, force=False):
    """统一的「缓存优先」取数。返回 (payload, fetched_at, from_cache, stale)。"""
    key = _norm_key(params)
    if not force:
        payload, fetched_at, _age = store.get(kind, ns, key)
        if payload is not None:
            return (payload, fetched_at, True, False)
    payload = fetch_fn()
    fetched_at = store.put(kind, ns, key, payload)
    return (payload, fetched_at, False, False)
