"""X 反爬令牌（x-client-transaction-id）生成。

X 的 GraphQL 接口要求该请求头（搜索 SearchTimeline 已实测确认）：缺失时接口直接
返回 404——对照实验中「带它 HTTP 200 且有数据，仅去掉它就 404」，其余请求头、
queryId、features 都不是影响因素。

令牌由 X 端混淆 JS 计算：
    sha256("{method}!{path}!{时间戳}{关键字}{animation_key}")
其中 animation_key 来自「首页 <meta name="twitter-site-verification"> 的 content」
与「ondemand.s 脚本里的字节索引」——二者都会随 X 前端发版变化，必须实时抓取，
不能写死。算法实现取自第三方库 XClientTransaction（MIT）；本模块只负责备料、
缓存与出令牌，把依赖隔离在插件 backend 内，使 run.py 仍保持仅依赖标准库
（run.py 还会以子进程方式被直接执行，不宜引入第三方依赖）。
"""

import threading
import time
import urllib.request

_lock = threading.Lock()
_ct = None          # 已备料的 ClientTransaction（内含站点 key 与 animation key）
_ct_ts = 0          # _ct 备料完成时刻（unix 秒），用于 TTL 自愈

# X 前端发版会轮换 site-verification meta 与 ondemand.s 的哈希，旧 _ct 生成的令牌会被拒（404）。
# 设一个较短 TTL：过期即重建，避免「某次发版后搜索一直 404 直到手动重启」的静默失效。
_CT_TTL = 15 * 60

# 首页候选：带登录态时 / 会 302 到 /home；两者都含所需的 site verification meta。
# 抓首页要用「网页浏览」那套头（不带 Authorization），带 Bearer 会被判 401。
# 注意：必须带登录 Cookie，否则 X 返回游客态首页（无 ondemand.s 引用）→ 取不到令牌材料。
_HOME_URLS = ('https://x.com/', 'https://x.com/home')


def _http_get(url, headers, timeout=20):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _build(home_headers, ua, timeout=20):
    """抓首页取站点校验 key、再抓 ondemand.s 取索引，构造 ClientTransaction。"""
    import bs4
    from x_client_transaction import ClientTransaction
    from x_client_transaction.utils import get_ondemand_file_url

    headers = dict(home_headers or {})
    if ua:
        headers['User-Agent'] = ua

    last_err = None
    for url in _HOME_URLS:
        try:
            soup = bs4.BeautifulSoup(_http_get(url, headers, timeout), 'html.parser')
            if not soup.select_one("meta[name='twitter-site-verification']"):
                last_err = '%s 未见 twitter-site-verification meta' % url
                continue
            ondemand_url = get_ondemand_file_url(soup)
            js = _http_get(ondemand_url, {'User-Agent': ua} if ua else {}, timeout)
            return ClientTransaction(home_page_response=soup,
                                     ondemand_file_response=js.decode('utf-8', 'replace'))
        except Exception as e:
            last_err = e
    raise RuntimeError('无法准备 X 反爬令牌材料: %s' % last_err)


def invalidate():
    """丢弃已缓存的备料。X 前端发版、或令牌连续失效时调用以重新抓取。"""
    global _ct, _ct_ts
    with _lock:
        _ct = None
        _ct_ts = 0


def _ensure_ct(home_headers, ua):
    """在锁内确保 _ct 已备料且未过期；返回可用的 ClientTransaction 或抛出异常。"""
    global _ct, _ct_ts
    if _ct is None or (time.time() - _ct_ts) > _CT_TTL:
        _ct = _build(home_headers, ua)
        _ct_ts = time.time()
    return _ct


def get_transaction_id(home_headers, method, path, ua=None, refresh=False):
    """为一次请求生成 x-client-transaction-id；取不到返回 None，由调用方降级。

    home_headers：抓首页用的请求头，由调用方传入（复用 run.py 的 build_headers，
    注意必须是不带 Bearer 的那套，否则首页会 401）。
    备料（站点 key + animation key）在进程内缓存复用，并带 TTL 自愈：X 发版轮换
    site key/ondemand.s 后旧备料会失效，过期自动重建。令牌本身含时间戳、按
    (method, path) 现算，所以缓存备料不会导致令牌重复。

    若生成抛异常（备料在两次调用间失效/网络抖动），会丢弃缓存重建一次再试。
    """
    global _ct
    if refresh:
        invalidate()
    with _lock:
        ct = _ensure_ct(home_headers, ua)
    try:
        return ct.generate_transaction_id(method=method, path=path)
    except Exception:
        # 备料在抓取后失效（如 ondemand.s 已被 X 轮换），重建一次再试
        with _lock:
            invalidate()
            try:
                ct = _ensure_ct(home_headers, ua)
            except Exception:
                return None
        try:
            return ct.generate_transaction_id(method=method, path=path)
        except Exception:
            return None
