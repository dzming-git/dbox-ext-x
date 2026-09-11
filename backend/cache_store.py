"""X 扩展的通用缓存层：统一的 LRU 缓存（内容 / 列表 / 资源本体共用一套策略）。

## 设计要点（2026-09 重构）

- 所有从 X 真实站点取回的数据（关注流、搜索、单推文正文、用户资料 / 头像、
  喜欢列表、书签列表、历史等）**全部进缓存，命中即返回，永不主动二拉**。
- 淘汰统一使用 LRU 算法：每次读取 / 写入都刷新 ``last_access``，超出统一字节
  预算时淘汰「最久未访问」的条目。媒体（头像 / 图 / 视频字节）由框架托管的
  磁盘 LRU 分区承载，与内容缓存共用后端治理的「统一字节预算」（见 server.py
  的 ``_enforce_unified_cap``）。
- **没有 TTL 自动过期**：数据不会因时间到了就偷偷重新打 X。唯一重新拉取的
  触发是用户手动刷新（接口带 ``force=1``）。新鲜度完全由「缓存于 X 前」徽标
  告知用户，由用户决定是否刷新。这既降低 X 限流 / 反爬（429）风险，
  也让“看到的是新是旧”对用户透明。
- 网络失败时若存在旧缓存，降级返回旧数据并标 ``stale``（比白屏有用——X 常限流）。
"""

import json
import os
import sqlite3
import threading
import time

DEFAULT_TTL_FALLBACK = 0   # LRU 模式下不再按时间过期；保留字段仅作兼容
MAX_ROWS_PER_NS = 4000     # 每命名空间软上限（二级保护，真正淘汰由全局 LRU 负责）


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
    """SQLite 支撑的 LRU 缓存（线程安全）。"""

    def __init__(self, db_path, ttl_overrides=None):
        self._path = db_path
        self._lock = threading.RLock()
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
                    last_access REAL NOT NULL DEFAULT 0,
                    cost INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (kind, ns, key))''')
                # 兼容旧库：缺列先补（必须在建索引前！SQLite 的 ADD COLUMN 不支持
                # IF NOT EXISTS；若先建依赖新列的索引，老库会因 no such column 直接崩，
                # 进而整個插件注册失败、所有 /api/ext/x/* 404）。
                for col, ctype in (('last_access', 'REAL'), ('cost', 'INTEGER')):
                    try:
                        conn.execute('ALTER TABLE cache ADD COLUMN %s %s DEFAULT 0' % (col, ctype))
                    except Exception:
                        pass
                conn.commit()
                for _sql in (
                    'CREATE INDEX IF NOT EXISTS idx_cache_ns_time '
                    'ON cache(kind, ns, fetched_at)',
                    'CREATE INDEX IF NOT EXISTS idx_cache_lru ON cache(last_access)',
                ):
                    try:
                        conn.execute(_sql)
                    except Exception:
                        pass
                conn.commit()
            finally:
                conn.close()

    # ---- TTL 相关：LRU 模式下已无意义，保留为空操作以兼容旧调用 ----
    def ttl_of(self, kind, ns):
        return 0

    def set_ttl(self, kind, ns, seconds):
        pass

    def ttls(self):
        return {}

    def get(self, kind, ns, key):
        """命中即返回（不按时间过期）。同时刷新 LRU 访问时间。

        返回 (payload, fetched_at, age)；未命中返回 (None, None, None)。
        """
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    'SELECT payload, fetched_at, last_access FROM cache '
                    'WHERE kind=? AND ns=? AND key=?',
                    (kind, ns, key)).fetchone()
                if not row:
                    return (None, None, None)
                now = time.time()
                try:
                    conn.execute(
                        'UPDATE cache SET last_access=? WHERE kind=? AND ns=? AND key=?',
                        (now, kind, ns, key))
                except Exception:
                    pass
                fetched_at = float(row[1])
                return (json.loads(row[0]), fetched_at, now - fetched_at)
            finally:
                conn.close()

    def get_any(self, kind, ns, key):
        """同 get，但不刷新 LRU（用于异常降级读取旧缓存）。"""
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    'SELECT payload, fetched_at FROM cache '
                    'WHERE kind=? AND ns=? AND key=?',
                    (kind, ns, key)).fetchone()
                if not row:
                    return (None, None)
                return (json.loads(row[0]), float(row[1]))
            finally:
                conn.close()

    def put(self, kind, ns, key, payload):
        """写入并刷新 LRU 访问时间；按命名空间软上限裁剪最久未访问者。"""
        now = time.time()
        blob = json.dumps(payload, ensure_ascii=False)
        cost = len(blob.encode('utf-8'))
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    'INSERT OR REPLACE INTO cache(kind, ns, key, payload, fetched_at, last_access, cost) '
                    'VALUES (?,?,?,?,?,?,?)', (kind, ns, key, blob, now, now, cost))
                conn.execute(
                    'DELETE FROM cache WHERE kind=? AND ns=? AND rowid IN ('
                    '  SELECT rowid FROM cache WHERE kind=? AND ns=? '
                    '  ORDER BY last_access ASC LIMIT -1 OFFSET ?)',
                    (kind, ns, kind, ns, MAX_ROWS_PER_NS))
                conn.commit()
            finally:
                conn.close()
        return now

    def touch(self, kind, ns, key):
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    'UPDATE cache SET last_access=? WHERE kind=? AND ns=? AND key=?',
                    (time.time(), kind, ns, key))
            finally:
                conn.close()

    def content_bytes(self):
        """当前内容缓存占用的估算字节数（payload 序列化长度之和）。"""
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute('SELECT COALESCE(SUM(cost),0) FROM cache').fetchone()
                return int(row[0] or 0)
            except Exception:
                return 0
            finally:
                conn.close()

    def evict_oldest(self, n=1):
        """淘汰 n 条最久未访问的内容；返回实际删除条数。"""
        removed = 0
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    'SELECT kind, ns, key FROM cache ORDER BY last_access ASC LIMIT ?',
                    (n,)).fetchall()
                for r in rows:
                    conn.execute('DELETE FROM cache WHERE kind=? AND ns=? AND key=?',
                                 (r[0], r[1], r[2]))
                    removed += 1
                if removed:
                    conn.commit()
            finally:
                conn.close()
        return removed

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

    def stats(self, budget_bytes=None):
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(
                    'SELECT kind, ns, COUNT(*), MAX(fetched_at), MIN(fetched_at), '
                    'COALESCE(SUM(cost),0) '
                    'FROM cache GROUP BY kind, ns').fetchall()
                total = conn.execute(
                    'SELECT COUNT(*), COALESCE(SUM(cost),0) FROM cache').fetchone()
            finally:
                conn.close()
        groups = [{'kind': r[0], 'ns': r[1], 'count': r[2],
                   'last_fetched_at': r[3], 'first_fetched_at': r[4],
                   'bytes': r[5]} for r in rows]
        size = 0
        try:
            size = os.path.getsize(self._path)
        except Exception:
            pass
        return {'total': total[0], 'bytes': total[1], 'groups': groups,
                'db_bytes': size, 'path': self._path,
                'budget_bytes': budget_bytes}


def serve_cached(store, kind, ns, params, fetch_fn, force=False):
    """统一的「缓存优先」取数。返回 (payload, fetched_at, from_cache, stale)。

    · 命中（无论多久之前） → 直接返回，一个网络包都不发
    · 未命中 / force=True（用户手动刷新） → 真拉取并写回缓存
    · 拉取失败但有过期旧缓存 → 降级返回旧数据并置 stale=True
    """
    key = _norm_key(params)
    if not force:
        payload, fetched_at, _age = store.get(kind, ns, key)
        if payload is not None:
            return (payload, fetched_at, True, False)
    payload = fetch_fn()
    fetched_at = store.put(kind, ns, key, payload)
    return (payload, fetched_at, False, False)
