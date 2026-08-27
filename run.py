#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""X（原 Twitter）媒体下载脚本，适配 Dbox 外部脚本引擎。

纯爬虫实现，不依赖 yt-dlp：
  - 图片：从推文页面 / 接口解析 pbs.twimg.com 原图直链，urllib 直连下载。
  - 视频：解析出 video.twimg.com 的 m3u8（HLS）播放列表，
          自行下载全部 .ts 分片，再用系统 ffmpeg 合并为 mp4。

运行契约（详见 src/web/script_engine）：
  - 通过 stdin 读取 JSON：{job_id, params, context}
      context.working_dir : 任务临时目录（产物放在这里，结束前会被清理）
      context.notify      : {url, token} 入库通知回调
      context.cookies     : {domain: {path, format}} 保险库注入的 cookie
  - 通过 stdout 逐行上报：
      {"type":"progress","percent":0-100,"message":""}
      {"type":"log","level":"info|warn|error","message":""}
      {"type":"await_input","input":{prompt,options:[{value,label}],multi,min,max,allow_text,text_hint}}
      {"type":"await_input","input":{"type":"preview","url","text","title","author","media":[...]}}
            —— 预览模式（默认）：input.type="preview"，携带推文文字、作者与媒体预览
               （图片 url / 视频 url+cover+mp4），等待用户在界面上确认下载或取消。
               确认答复格式：{"action":"download","items":[0,1,...]}；取消为 {"action":"cancel"}。
      {"type":"result","files":[{"path":...,"type":"video|image"}]}
  - 分阶段交互：上报 await_input 后，长轮询 GET /api/scripts/jobs/<id>/input 等待管理员选择。

依赖：标准库 + 系统 ffmpeg（合并视频分片）。若使用 SOCKS 代理，需 pip install pysocks。
"""
import sys
import os
import io
import re
import json
import time
import hashlib
import shutil
import socket
import subprocess
import urllib.parse
import urllib.request
import urllib.error

try:
    import socks as _socks_mod
    HAS_SOCKS = True
except Exception:
    _socks_mod = None
    HAS_SOCKS = False

try:
    import winreg
except Exception:
    winreg = None

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')

# 当前代理配置（dict 或 None），由 main() 写入，供 fetch 时挂载 SOCKS。
_PROXY_CFG = None
_ORIG_SOCKET = socket.socket

# X 网页端公开的 Bearer Token（长期不变，用于访客/接口鉴权）。
# 必须是真实的网页端常量，否则 guest/activate 与 statuses/show 都会 401。
WEB_BEARER = ('AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs'
              '%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA')

IMG_RE = re.compile(
    r'https://pbs\.twimg\.com/media/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|gif|webp)',
    re.IGNORECASE)
M3U8_RE = re.compile(
    r'https://video\.twimg\.com/[^\s"\'<>]+\.m3u8(?:\?[^\s"\'<>]*)?',
    re.IGNORECASE)
RENDITION_RE = re.compile(r'/vid/\d+x\d+/', re.IGNORECASE)


# ---------------------------------------------------------------------------
# ffmpeg 定位
# ---------------------------------------------------------------------------
# 服务通常以 LocalSystem 运行，其 PATH 不含用户经 WinGet 安装的 ffmpeg
# （位于 %LOCALAPPDATA%\Microsoft\WinGet\Links）；但 LocalSystem 对该路径
# 有读取权限，故在 PATH 之外额外扫描常见安装位置，并用绝对路径调用。
_FFMPEG_EXE = None


def find_ffmpeg():
    """定位 ffmpeg 可执行文件，优先 PATH，其次扫描常见安装位置。"""
    global _FFMPEG_EXE
    if _FFMPEG_EXE is not None:
        return _FFMPEG_EXE
    candidates = []
    p = shutil.which('ffmpeg')
    if p:
        candidates.append(p)
    env = os.environ.get('FFMPEG_PATH')
    if env:
        candidates.append(env)
    # 扫描所有用户 profile 下的 WinGet Links（与运行账户无关）
    try:
        import glob as _glob
        for _pat in (r'C:/Users/*/AppData/Local/Microsoft/WinGet/Links/ffmpeg.exe',
                     r'C:/Users/*/AppData/Local/Microsoft/WinGet/Packages/*/ffmpeg-*/bin/ffmpeg.exe'):
            for _c in _glob.glob(_pat):
                candidates.append(_c)
    except Exception:
        pass
    # 机器级常见位置
    for _base in (os.environ.get('ProgramFiles'), os.environ.get('ProgramFiles(x86)'),
                  'C:/ProgramData', 'C:/ffmpeg', 'C:/Program Files/ffmpeg'):
        if _base:
            candidates.append(os.path.join(_base, 'ffmpeg', 'bin', 'ffmpeg.exe'))
            candidates.append(os.path.join(_base, 'ffmpeg', 'ffmpeg.exe'))
    candidates.append('C:/Windows/System32/ffmpeg.exe')
    for _c in candidates:
        if _c and os.path.isfile(_c):
            _FFMPEG_EXE = _c
            return _c
    return None


def ffmpeg_exe():
    exe = find_ffmpeg()
    if not exe:
        raise RuntimeError('未找到 ffmpeg：请安装 ffmpeg 并加入 PATH，或设置环境变量 FFMPEG_PATH')
    return exe


# ---------------- stdout 上报 ----------------
def emit(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + '\n')
    sys.stdout.flush()


def netscape_to_header(text):
    """将 Netscape 格式 cookie 文件转为 Cookie 请求头字符串。"""
    parts = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        cols = line.split('\t')
        if len(cols) >= 7 and cols[5] and cols[6]:
            parts.append(f'{cols[5]}={cols[6]}')
    return '; '.join(parts)


def read_cookie_file(path):
    """读取保险库物化的 cookie 文件，按格式返回可用的 Cookie 请求头字符串。"""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    except Exception:
        return ''
    if not content:
        return ''
    # header 格式：文件内容即 Cookie 头原文（单行、无制表符）
    if '\t' not in content and '\n' not in content:
        return content
    # 含 \t 视为 Netscape 格式，转换后使用
    return netscape_to_header(content)


def resolve_cookie(cookie_ctx):
    """解析 cookie：使用保险库按域名自动物化的文件（系统已按 required_cookies 匹配 x.com）。

    不再依赖用户手动选择或直接粘贴 Cookie，由管理器按域名统一注入。
    """
    for dom, info in (cookie_ctx or {}).items():
        p = (info or {}).get('path')
        if p and os.path.isfile(p):
            header = read_cookie_file(p)
            if header:
                emit({'type': 'log', 'message': f'使用保险库 Cookie（{dom}）'})
                # [调试] 打印实际读到的 cookie 关键字段，便于对比与浏览器复制的是否一致
                emit({'type': 'log', 'level': 'warn', 'message':
                    f'[Cookie 诊断] 长度={len(header)} 含auth_token={"auth_token=" in header} '
                    f'含ct0={"ct0=" in header} 首段={header[:50]!r} 尾段={header[-50:]!r}'})
                # 关键字段缺失时直接报错，避免静默 401 误导用户以为网络/代码问题
                if 'auth_token=' not in header:
                    error('保险库 x.com Cookie 缺少 auth_token 字段，无法登录态解析视频。'
                          '请从浏览器复制完整 Cookie（含 auth_token= 与 ct0=）重新覆盖到凭证库。')
                    return None
                return header
    return None


def progress(pct, message=''):
    emit({'type': 'progress', 'percent': int(pct), 'message': message})


def log(message, level='info'):
    emit({'type': 'log', 'level': level, 'message': message})


def error(message):
    emit({'type': 'error', 'message': message})


def _is_auto_proxy(p):
    """是否应自动读取系统代理：留空或 auto/system/detect。"""
    return not (p or '').strip() or (p or '').strip().lower() in ('auto', 'system', 'detect', '0')


def _normalize_proxy_server(server):
    """把注册表 ProxyServer 归一为 URL 字符串（socks5:// 或 http://）。"""
    server = (server or '').strip()
    if not server:
        return None
    # 分协议格式：http=host:port;https=...;socks=host:port
    if '=' in server:
        parts = {}
        for seg in server.split(';'):
            if '=' in seg:
                proto, addr = seg.split('=', 1)
                parts[proto.strip().lower()] = addr.strip()
        for proto in ('socks', 'socks5', 'https', 'http'):
            if proto in parts:
                addr = parts[proto]
                return ('socks5://' if proto.startswith('socks') else 'http://') + addr
        addr = next(iter(parts.values()))
        return ('socks5://' if addr.startswith('socks') else 'http://') + addr
    # 单一地址：默认按 SOCKS 处理（X 通常走 SOCKS 代理）
    return 'socks5://' + server


def _read_proxy_from_key(key):
    """从已打开的 Internet Settings 键读取代理 URL，未启用/缺失则返回 None。"""
    try:
        enabled = winreg.QueryValueEx(key, 'ProxyEnable')[0]
    except OSError:
        return None
    if not enabled:
        return None
    try:
        server = winreg.QueryValueEx(key, 'ProxyServer')[0]
    except OSError:
        return None
    return _normalize_proxy_server(server) if server else None


def get_system_proxy():
    """读取 Windows 系统代理（与浏览器一致），返回 URL 字符串或 None。

    注意：下载器以 LocalSystem 服务运行，其 HKEY_CURRENT_USER 取不到用户代理，
    故优先枚举 HKEY_USERS 下【当前交互登录用户】的配置，再回退机器级。
    """
    if winreg is None:
        return None
    # 1) 当前进程用户（万一服务以用户身份运行）
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                            r'Software\Microsoft\Windows\CurrentVersion\Internet Settings') as k:
            p = _read_proxy_from_key(k)
            if p:
                return p
    except OSError:
        pass
    # 2) 交互式登录用户（HKCU 取不到时的主要路径）
    best = None
    try:
        with winreg.OpenKey(winreg.HKEY_USERS, '') as users:
            i = 0
            while True:
                try:
                    sid = winreg.EnumKey(users, i)
                except OSError:
                    break
                i += 1
                path = rf'{sid}\Software\Microsoft\Windows\CurrentVersion\Internet Settings'
                try:
                    with winreg.OpenKey(winreg.HKEY_USERS, path) as k:
                        p = _read_proxy_from_key(k)
                except OSError:
                    p = None
                if not p:
                    continue
                # 优先交互式会话（Console / RDP，排除 Service）
                interactive = False
                try:
                    with winreg.OpenKey(winreg.HKEY_USERS, rf'{sid}\Volatile Environment') as ve:
                        try:
                            sname = winreg.QueryValueEx(ve, 'SESSIONNAME')[0]
                            interactive = bool(sname) and sname != 'Service'
                        except OSError:
                            pass
                except OSError:
                    pass
                if interactive:
                    return p
                if best is None:
                    best = p
    except OSError:
        pass
    if best:
        return best
    # 3) 机器级
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                            r'Software\Microsoft\Windows\CurrentVersion\Internet Settings') as k:
            p = _read_proxy_from_key(k)
            if p:
                return p
    except OSError:
        pass
    return None


def parse_proxy(p):
    """解析代理参数，返回 dict：
       - None：直连
       - {'type':'http','addr':'http://host:port'}：HTTP/HTTPS 代理
       - {'type':'socks','scheme':'socks5'|'socks5h'|'socks4','host':..,'port':..}
       无 scheme 前缀时默认按 HTTP 处理（兼容旧值）。
       留空或 auto/system/detect 时自动读取 Windows 系统代理（与浏览器一致）。
    """
    p = (p or '').strip()
    if _is_auto_proxy(p):
        sys_p = get_system_proxy()
        if sys_p:
            log(f'自动使用系统代理: {sys_p}')
            p = sys_p
        else:
            log('未检测到系统代理，直连访问 X')
            return None
    if '://' in p:
        scheme, rest = p.split('://', 1)
        scheme = scheme.lower()
    else:
        scheme, rest = 'http', p
    if scheme in ('http', 'https'):
        prefix = 'http://' if scheme == 'http' else 'https://'
        return {'type': 'http', 'scheme': scheme, 'addr': prefix + rest}
    if scheme in ('socks5', 'socks5h', 'socks4', 'socks'):
        socks_scheme = 'socks5' if scheme == 'socks' else scheme
        host, _, port = rest.rpartition(':')
        host = host or '127.0.0.1'
        try:
            port = int(port)
        except ValueError:
            raise ValueError(f'代理端口无法解析: {rest}')
        return {'type': 'socks', 'scheme': socks_scheme,
                'host': host, 'port': port}
    # 未知 scheme 当作 HTTP
    return {'type': 'http', 'scheme': 'http', 'addr': 'http://' + p}


def make_opener(proxy_cfg):
    """返回一个带代理（可选）的 urllib opener，用于访问 X 域名。
       SOCKS 代理由 socket 层挂载，这里只用裸 opener。
    """
    if proxy_cfg and proxy_cfg['type'] == 'http':
        return urllib.request.build_opener(
            urllib.request.ProxyHandler(
                {'http': proxy_cfg['addr'], 'https': proxy_cfg['addr']}))
    return urllib.request.build_opener()


def _apply_socks():
    """若当前代理为 SOCKS，则把 socket 替换为 socks 隧道（仅作用于 X 请求）。"""
    global _PROXY_CFG
    if not _PROXY_CFG or _PROXY_CFG['type'] != 'socks':
        return
    if not HAS_SOCKS:
        raise RuntimeError('未安装 PySocks，无法使用 SOCKS 代理，请先 pip install pysocks')
    stype = {'socks5': _socks_mod.SOCKS5,
             'socks5h': _socks_mod.SOCKS5,
             'socks4': _socks_mod.SOCKS4}[_PROXY_CFG['scheme']]
    _socks_mod.set_default_proxy(
        stype, _PROXY_CFG['host'], _PROXY_CFG['port'],
        rdns=(_PROXY_CFG['scheme'] == 'socks5h'))
    socket.socket = _socks_mod.socksocket


def _restore_socks():
    global _PROXY_CFG
    if _PROXY_CFG and _PROXY_CFG['type'] == 'socks':
        socket.socket = _ORIG_SOCKET


def result(files):
    emit({'type': 'result', 'files': files})


# ---------------- 交互：长轮询等待用户选择 ----------------
def fetch_input(input_ctx, timeout=25):
    """阻塞等待管理后台的用户答复。返回答复值（多选为数组），超时/取消返回 None。"""
    url = input_ctx.get('url')
    token = input_ctx.get('token')
    if not url:
        return None
    # GET /api/scripts/jobs/<id>/input
    input_url = url.rstrip('/').replace('/notify', '/input')
    while True:
        try:
            req = urllib.request.Request(input_url, headers={
                'Authorization': f'Bearer {token}',
                'User-Agent': UA,
            })
            resp = urllib.request.urlopen(req, timeout=timeout + 5)
            if resp.status == 204:
                continue
            data = resp.read().decode('utf-8', 'replace').strip()
            if not data:
                continue
            return json.loads(data)
        except urllib.error.HTTPError as e:
            if e.code == 204:
                continue
            if e.code in (400, 404, 403):
                # 令牌无效/无权限：无需重试，按"无选择"处理（默认下载全部）
                return None
            time.sleep(2)
        except Exception:
            time.sleep(2)


# ---------------- 入库通知 ----------------
def notify_input(input_ctx, files):
    url = input_ctx.get('url')
    token = input_ctx.get('token')
    if url and token:
        try:
            req = urllib.request.Request(
                url, data=json.dumps({'files': files}, ensure_ascii=False).encode('utf-8'),
                headers={
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json',
                    'User-Agent': UA,
                }, method='POST')
            urllib.request.urlopen(req, timeout=15)
            log(f'已通知入库 {len(files)} 个文件')
            return
        except Exception as e:
            log(f'入库通知失败（将降级为 result）: {e}', level='warn')
    result(files)


# ---------------- 网络请求封装 ----------------
def fetch_text(url, opener, headers, timeout=60, max_retries=3, retry_base=1.0):
    """抓取文本内容，失败自动重试（指数退避）。

    - 可重试：网络异常 / 超时 / SSL EOF / 5xx / 429。
    - 不可重试：4xx 中除 429 外的错误（如 404），直接抛出。
    """
    _apply_socks()
    last_exc = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with opener.open(req, timeout=timeout) as r:
                return r.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            last_exc = e
            retryable = e.code in (429, 500, 502, 503, 504)
            if retryable and attempt < max_retries - 1:
                wait = retry_base * (2 ** attempt)
                log(f'请求失败（{url[:90]}…）HTTP {e.code}，{wait:.0f}s 后第 {attempt + 2} 次重试',
                    level='warn')
                time.sleep(wait)
                continue
            break
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError,
                OSError) as e:
            last_exc = e
            if attempt < max_retries - 1:
                wait = retry_base * (2 ** attempt)
                log(f'请求失败（{url[:90]}…）{type(e).__name__}，{wait:.0f}s 后第 {attempt + 2} 次重试',
                    level='warn')
                time.sleep(wait)
                continue
            break
        finally:
            _restore_socks()
    raise last_exc  # type: ignore[misc]


def fetch_bytes(url, opener, headers, timeout=60, max_retries=3, retry_base=1.0):
    """下载二进制，失败自动重试（指数退避）。

    - 可重试：网络异常 / 超时 / 5xx / 429（X 的 twimg CDN 偶发不稳定）。
    - 不可重试：4xx 中除 429 外的错误（如 404 资源真的不存在），直接抛出。
    - 单次分片失败会被重试掩盖，避免整个视频下载因一个分片抖动而前功尽弃。
    """
    _apply_socks()
    last_exc = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with opener.open(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            last_exc = e
            retryable = e.code in (429, 500, 502, 503, 504)
            if retryable and attempt < max_retries - 1:
                wait = retry_base * (2 ** attempt)
                log(f'下载失败（{url[:90]}…）HTTP {e.code}，{wait:.0f}s 后第 {attempt + 2} 次重试',
                    level='warn')
                time.sleep(wait)
                continue
            break
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError,
                OSError) as e:
            last_exc = e
            if attempt < max_retries - 1:
                wait = retry_base * (2 ** attempt)
                log(f'下载失败（{url[:90]}…）{type(e).__name__}，{wait:.0f}s 后第 {attempt + 2} 次重试',
                    level='warn')
                time.sleep(wait)
                continue
            break
        finally:
            _restore_socks()
    # 最后一次仍失败：抛出，由调用方决定整体是否失败
    raise last_exc


# ---------------- 媒体解析 ----------------
def _extract_ct0(cookie_header):
    """从 Cookie 头（header 格式或 Netscape 格式）中取出 ct0 值。"""
    if not cookie_header:
        return None
    # Netscape 格式：每行 domain\tflag\tpath\tsecure\texpiry\tname\tvalue
    if '\t' in cookie_header:
        for line in cookie_header.splitlines():
            parts = line.split('\t')
            if len(parts) >= 7 and parts[5].strip() == 'ct0':
                return parts[6].strip()
        return None
    # header 格式：name=value; name2=value2
    for part in cookie_header.split(';'):
        part = part.strip()
        if part.startswith('ct0='):
            return part[len('ct0='):].strip()
    return None


def _has_auth_token(cookie_header):
    """判断 Cookie 是否包含完整登录会话所需的 auth_token。

    X 的 GraphQL 登录态要求 Cookie 同时含 auth_token 与 ct0，缺任一都会 401。
    缺 auth_token 时强行用登录态 GraphQL 必然 401，应直接走游客态降级。
    """
    if not cookie_header:
        return False
    if '\t' in cookie_header:
        for line in cookie_header.splitlines():
            parts = line.split('\t')
            if len(parts) >= 7 and parts[5].strip() == 'auth_token' and parts[6].strip():
                return True
        return False
    for part in cookie_header.split(';'):
        part = part.strip()
        if part.startswith('auth_token=') and part[len('auth_token='):].strip():
            return True
    return False



def build_headers(cookie_header, with_bearer=True, guest_token=None):
    """构造请求头。

    - 始终带网页端 Bearer（X 接口/媒体请求需要）。
    - 带 Cookie 时：额外发 ct0 -> x-csrf-token 与 OAuth2Session 登录态头，
      否则 X 会当成“半个登录会话”返回登录墙（这正是之前带 Cookie 解析失败的根因）。
    - guest_token 用于游客接口/媒体请求鉴权降级。
    """
    h = {'User-Agent': UA, 'Accept': '*/*'}
    if with_bearer:
        h['Authorization'] = f'Bearer {WEB_BEARER}'
    if guest_token:
        h['x-guest-token'] = guest_token
    if cookie_header:
        h['Cookie'] = cookie_header
        ct0 = _extract_ct0(cookie_header)
        if ct0:
            h['x-csrf-token'] = ct0
            h['x-twitter-auth-type'] = 'OAuth2Session'
            h['x-twitter-active-user'] = 'yes'
            h['x-twitter-client-language'] = 'en'
    return h


# X 推文块：<article data-testid="tweet">...</article>
ARTICLE_RE = re.compile(r'<article\b[^>]*>.*?</article>', re.S | re.I)


def _focal_article_html(page_html, tweet_id):
    """只保留目标推文所在的 <article> 块，避免把评论/回复里的图片也抓下来。"""
    if not tweet_id or tweet_id == 'x':
        return page_html
    for art in ARTICLE_RE.findall(page_html):
        if f'/status/{tweet_id}' in art:
            return art
    # 极少数情况没匹配到，退化为整页（不应发生）
    return page_html


def extract_from_html(html, cookie_header, tweet_id=''):
    """从推文页面 HTML 中正则抓取图片原图与视频 m3u8。只解析目标推文自身，排除评论。"""
    if html:
        html = _focal_article_html(html, tweet_id)
    media = []
    seen_img = set()
    for m in IMG_RE.finditer(html or ''):
        base = m.group(0)
        if base in seen_img:
            continue
        seen_img.add(base)
        orig = base + '?name=orig'
        media.append({'type': 'image', 'url': orig, 'label': '图片'})
    m3u8_urls = [m.group(0) for m in M3U8_RE.finditer(html or '')]
    if m3u8_urls:
        best = pick_m3u8(m3u8_urls)
        media.append({'type': 'video', 'url': best, 'label': '视频/动图'})
    return media


def pick_m3u8(urls):
    """优先选具体分辨率的分片列表（含 /vid/WxH/），否则取第一个。"""
    for u in urls:
        if RENDITION_RE.search(u):
            return u
    return urls[0]


def get_guest_token(opener, cookie_header, retries=3, retry_base=1.0):
    """通过 api.x.com 访客接口换取 guest_token（用于游客态 GraphQL 鉴权）。

    代理（socks5）下 SSL 握手偶发超时，失败自动指数退避重试。
    """
    url = 'https://api.x.com/1.1/guest/activate.json'
    headers = {
        'User-Agent': UA,
        'Authorization': f'Bearer {WEB_BEARER}',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    if cookie_header:
        headers['Cookie'] = cookie_header
    last_exc = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=b'', headers=headers, method='POST')
            with opener.open(req, timeout=10) as r:
                data = json.loads(r.read().decode('utf-8', 'replace'))
            return data.get('guest_token')
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                wait = retry_base * (2 ** attempt)
                log(f'获取 guest_token 失败（{type(e).__name__}），{wait:.0f}s 后重试', level='warn')
                time.sleep(wait)
                continue
            break
    log(f'获取 guest_token 失败（将仅用页面解析）: {last_exc}', level='warn')
    return None


class _GraphQLAuthFailed(Exception):
    """登录态 GraphQL 鉴权失败（Cookie 无效/过期），需上层改走游客态或兜底。"""
    pass


def extract_from_api(tweet_id, cookie_header, opener):
    """调用 GraphQL TweetDetail 接口（结构化数据，登录墙/SPA 推文唯一可靠来源）。

    返回 (media_list, full_text, author)，author 为
    {'name': 显示名, 'screen_name': 用户名, 'url': 作者主页} 或 None。

    全新规则：视频只存在于 GraphQL 返回，必须先确保 GraphQL 成功，再谈兜底。
    鉴权策略：
      - Cookie 含完整会话（auth_token + ct0）：优先用登录态（不限流、可解析私密内容）。
      - Cookie 缺 auth_token（仅 ct0 或游客）：登录态必 401，直接走游客态（guest_token）。
      - 登录态 401：说明会话失效，改取 guest_token 以游客态重试，不再静默丢视频。
      - 游客态仍 401：抛出 _GraphQLAuthFailed，由 extract_media 兜底 HTML。
    """
    qid = _get_tweet_detail_qid()
    # 登录态是否可用：必须同时含 auth_token 与 ct0，否则 X 视会话无效必 401
    login_ok = bool(cookie_header) and _has_auth_token(cookie_header) and _extract_ct0(cookie_header)
    # 初始鉴权：登录态可用则不取 guest_token；否则先取游客 guest_token
    gt = None if login_ok else get_guest_token(opener, cookie_header)
    tried_guest_fallback = not login_ok  # 一开始就是游客态则无需再降级
    tweet = None
    for _ in range(4):
        try:
            data = _graphql_tweet_detail(tweet_id, cookie_header, opener, gt, qid)
        except _GraphQLQueryNotFound:
            new_qid = _discover_tweet_detail_qid(opener, cookie_header)
            if new_qid:
                qid = new_qid
                continue
            return [], '', None
        except urllib.error.HTTPError as e:
            if e.code in (401, 403) and not tried_guest_fallback:
                # 登录态失效：改游客态（取 guest_token 重试），不再静默丢视频
                gt = get_guest_token(opener, cookie_header)
                if gt:
                    tried_guest_fallback = True
                    continue
                raise _GraphQLAuthFailed(f'登录态鉴权失败且无可用 guest_token: HTTP {e.code}')
            if e.code in (401, 403) and tried_guest_fallback:
                raise _GraphQLAuthFailed(f'GraphQL 鉴权失败（游客态亦 401）: HTTP {e.code}')
            log(f'GraphQL 接口解析失败（HTTP {e.code}）: {e}', level='warn')
            return [], '', None
        except Exception as e:
            log(f'GraphQL 接口解析失败: {e}', level='warn')
            return [], '', None
        if not isinstance(data, dict):
            return [], '', None
        # 优先精确匹配目标推文；匹配不到再取任意推文节点
        tweet = _find_tweet_node(data, tweet_id) or _find_tweet_node(data)
        if tweet:
            break
        # 没找到节点：可能需要 guest_token（游客态），补取后重试
        if gt is None:
            gt = get_guest_token(opener, cookie_header)
            if gt:
                continue
            return [], '', None
        return [], ''
    if not tweet:
        return [], '', None
    leg = tweet.get('legacy', {})
    text = (leg.get('full_text') or '').strip()
    # 去掉末尾的 t.co 短链占位（X 在 extended 模式会把链接放到 entities 里）
    text = re.sub(r'https://t\.co/\w+', '', text).strip()
    # 提取作者信息（显示名 + 用户名 + 主页），供前端展示为超链接
    author = None
    # 作者信息可能位于多个位置，逐一尝试
    user_result = (((tweet.get('core') or {}).get('user_results') or {}).get('result') or {})
    uleg = user_result.get('legacy') or {}
    # 兜底：部分响应里用户名直接在 legacy.user 或 user_results.legacy
    if not uleg:
        uleg = (tweet.get('legacy') or {}).get('user') or {}
    screen_name = uleg.get('screen_name')
    if screen_name:
        author = {
            'name': uleg.get('name') or screen_name,
            'screen_name': screen_name,
            'url': 'https://x.com/' + screen_name,
        }
    media = []
    entities = leg.get('extended_entities') or leg.get('entities') or {}
    for ent in entities.get('media', []):
        mtype = ent.get('type')
        if mtype == 'photo':
            base = ent.get('media_url_https') or ent.get('media_url') or ''
            if base:
                media.append({'type': 'image',
                              'url': base + ':orig',
                              'label': '图片'})
        elif mtype in ('video', 'animated_gif'):
            variants = ent.get('video_info', {}).get('variants', [])
            m3u8 = [v['url'] for v in variants
                    if v.get('content_type') == 'application/x-mpegURL']
            if m3u8:
                # 补充预览数据：cover 为视频封面（poster），mp4 为可直接播放的直链
                #（浏览器原生播放用，下载仍走 m3u8 高清分片合并）
                mp4s = [v for v in variants
                        if v.get('content_type') == 'video/mp4' and v.get('url')]
                best_mp4 = max(mp4s, key=lambda v: v.get('bitrate') or 0) if mp4s else None
                media.append({'type': 'video',
                              'url': pick_m3u8(m3u8),
                              'label': '视频/动图',
                              'cover': ent.get('media_url_https') or ent.get('media_url') or '',
                              'mp4': (best_mp4.get('url') or '') if best_mp4 else ''})
        elif mtype == 'document':
            # X 文档附件：记录下载地址与媒体 id，供下载阶段抓取实际文档
            media.append({'type': 'document',
                          'url': ent.get('media_url') or ent.get('media_url_https') or '',
                          'media_id': ent.get('id_str') or ent.get('id') or '',
                          'thumbnail': ent.get('media_url_https') or '',
                              'label': '文档'})
    return media, text, author


# ---------------------------------------------------------------------------
# GraphQL TweetDetail（X 现已废弃 statuses/show.json，SPA 页面也不内嵌媒体，
# 结构化数据只能通过 GraphQL 接口获取；query id 会轮换，支持自动发现）。
# ---------------------------------------------------------------------------
_GQL_TWEET_DETAIL_QID = 'XMOz5h24KAZ86qKffKTLdQ'  # 当前生效的 TweetDetail query id（2026-08 playwright 捕获）
_GQL_TWEET_DETAIL_FEATURES = {
    "blue_business_profile_image_shape_enabled": True,
    "dont_mention_me_view_api_enabled": True,
    "interactive_text_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_enhance_cards_enabled": True,
    "responsive_web_media_download_video_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "verified_phone_label_enabled": False,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweetypie_unmention_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "responsive_web_hIDE_emoji_in_status_enabled": True,
    "responsive_web_text_conversations_enabled": True,
    "responsive_web_ucigapi_consumption_enabled": True,
}
_GQL_TWEET_DETAIL_FIELDTOGGLES = {"withArticlePlainText": False}


# ---------------------------------------------------------------------------
# GraphQL Bookmarks（读取账号收藏夹里的推文列表；query id 会轮换，支持自动
# 发现。注意：X 收藏页用的是 Bookmarks（operationName=Bookmarks，variables
# 只有 count/cursor/includePromotedContent），不是 BookmarkSearchTimeline——
# 后者是带搜索词的搜索，空 rawQuery 会报 ERROR_EMPTY_QUERY）。
# ---------------------------------------------------------------------------
_GQL_BOOKMARKS_QID = 'iblrFnKr6PZUR-dWpfXG6g'  # 已知兜底（会轮换，优先自动发现）
_GQL_BOOKMARKS_OPN = 'Bookmarks'
# 与 X 收藏页一致的 features（2026-08 实测捕获）
_GQL_BOOKMARKS_FEATURES = {
    "rweb_video_screen_enabled": False,
    "rweb_cashtags_enabled": True,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_profile_redirect_enabled": True,
    "rweb_tipjar_consumption_enabled": False,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "rweb_cashtags_composer_attachment_enabled": True,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_annotations_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "rweb_conversational_replies_downvote_enabled": False,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "content_disclosure_indicator_enabled": True,
    "content_disclosure_ai_generated_indicator_enabled": True,
    "responsive_web_grok_show_grok_translated_post": True,
    "responsive_web_grok_analysis_button_from_backend": True,
    "post_ctas_fetch_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": False,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
}

# HomeTimeline（关注流）。features 与 Bookmarks 完全一致（X 的 feature 开关是全局共享的，
# 2026-08 playwright 实测：HomeTimeline 与 Bookmarks 用的是同一套 38 项 features）。
_GQL_HOME_TIMELINE_QID = 'wp06oo3fRGU4P1sK8rECqQ'  # For You 流（recommendation），已知兜底
_GQL_HOME_TIMELINE_OPN = 'HomeTimeline'
# Following（关注中）流：与 For You 是不同接口。X 首页「正在关注」tab 走的是
# HomeLatestTimeline，且用 POST（body 传 variables/features），不是 GET query。
# 2026-08 playwright 捕获：qid BLQWpfVqtgBqAqwRRJcJjA，返回的推文 component 是
# ranked_following（真实关注用户），与 For You 的 for_you_* 完全不同。
_GQL_FOLLOWING_QID = 'BLQWpfVqtgBqAqwRRJcJjA'  # 已知兜底（会轮换，优先自动发现）
_GQL_FOLLOWING_OPN = 'HomeLatestTimeline'


def _discover_home_timeline_qid(opener, cookie_header):
    """从 X 首页 bundle 自动发现 HomeTimeline query id。

    注意：首页 bundle 可能不含 HomeTimeline 定义（懒加载 chunk），发现失败时
    回退到已知兜底 qid（会轮换，需定期更新）。
    """
    global _GQL_HOME_TIMELINE_QID
    try:
        html = fetch_text('https://x.com/home', opener,
                          build_headers(cookie_header, with_bearer=False), timeout=20)
    except Exception:
        html = ''
    scripts = re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']', html or '')
    seen = set(); urls = []
    for s in scripts:
        if not s.startswith('http'):
            s = 'https://x.com' + (s if s.startswith('/') else '/' + s)
        if s not in seen:
            seen.add(s); urls.append(s)
    for s in urls:
        try:
            js = fetch_text(s, opener, {'User-Agent': UA}, timeout=30)
        except Exception:
            continue
        pat = r'operationName["\']?\s*[:=]\s*["\']HomeTimeline["\']?'
        for tm in re.finditer(pat, js):
            win_after = js[tm.end(): tm.end() + 2000]
            win_before = js[max(0, tm.start() - 2000): tm.start()]
            m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_after)
            if not m:
                m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_before)
            if m:
                _GQL_HOME_TIMELINE_QID = m.group(1)
                log(f'已自动发现 HomeTimeline query id: {_GQL_HOME_TIMELINE_QID}')
                return _GQL_HOME_TIMELINE_QID
    return None


def list_home_timeline(cookie_header, count=20, cursor=None):
    """读取 X 关注流（home timeline）推文列表。

    返回 (items, next_cursor)。复用 _extract_bookmark_tweets 的推文/user/media
    解析（把响应里的 timeline 包装成兼容结构）。
    """
    opener = make_opener(None)
    qid = _GQL_HOME_TIMELINE_QID
    if not qid:
        qid = _discover_home_timeline_qid(opener, cookie_header)
    if not qid:
        raise RuntimeError('未能发现 HomeTimeline query id')
    variables = {"count": count, "includePromotedContent": True,
                 "requestContext": "launch", "withCommunity": True}
    if cursor:
        variables["cursor"] = cursor
    url = (f'https://x.com/i/api/graphql/{qid}/{_GQL_HOME_TIMELINE_OPN}'
           f'?variables={urllib.parse.quote(json.dumps(variables))}'
           f'&features={urllib.parse.quote(json.dumps(_GQL_BOOKMARKS_FEATURES))}')
    headers = build_headers(cookie_header, with_bearer=True)
    raw = fetch_text(url, opener, headers, timeout=30)
    data = json.loads(raw)
    # 响应结构：data.home.home_timeline_urt 直接含 instructions（无 timeline 层）
    home = (data.get('data', {}) or {}).get('home', {}) or {}
    tl = home.get('home_timeline_urt') or {}
    # 过滤广告与"推荐/探索"来源推文。
    # 1) 广告：entryId 前缀 "promoted-tweet-"（即使 includePromotedContent=false
    #    仍会返回）。2) 推荐来源：clientEventInfo.component 为 for_you_simclusters /
    #    for_you_phoenix_retrieval（X 的 For You 算法推荐，多为未关注用户）。
    #    只保留"真实关注"推文：for_you_in_network / following_* / 无 component。
    # 这里就地修改 instructions.entries，避免 _extract_bookmark_tweets 误纳。
    for _inst in (tl.get('instructions') or []):
        _entries = _inst.get('entries') or []
        if not _entries:
            continue
        _kept = []
        for e in _entries:
            _eid = e.get('entryId') or ''
            _comp = ((e.get('content') or {}).get('clientEventInfo') or {}).get('component') or ''
            if _eid.startswith('promoted-'):
                continue
            if _comp in ('for_you_simclusters', 'for_you_phoenix_retrieval'):
                continue
            _kept.append(e)
        _inst['entries'] = _kept
    # 包装成 _extract_bookmark_tweets 兼容的 data 结构复用推文/user/media 解析
    items = _extract_bookmark_tweets(
        {'data': {'bookmark_timeline_v2': {'timeline': tl}}})
    next_cursor = None
    if items:
        for inst in (tl.get('instructions') or []):
            for entry in (inst.get('entries') or []):
                c = entry.get('content') or {}
                if c.get('entryType') == 'TimelineTimelineCursor' and \
                        c.get('cursorType') == 'Bottom':
                    next_cursor = c.get('value')
    return items, next_cursor


def _discover_following_qid(opener, cookie_header):
    """从 X 前端 bundle 自动发现 HomeLatestTimeline（Following 流）query id。

    首页 bundle 通常不含 HomeLatestTimeline 定义（懒加载），发现失败时回退
    到已知兜底 qid（会轮换，需定期更新）。
    """
    global _GQL_FOLLOWING_QID
    try:
        html = fetch_text('https://x.com/', opener,
                          build_headers(cookie_header, with_bearer=False), timeout=20)
    except Exception:
        html = ''
    scripts = re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']', html or '')
    seen = set(); urls = []
    for s in scripts:
        if not s.startswith('http'):
            s = 'https://x.com' + (s if s.startswith('/') else '/' + s)
        if s not in seen:
            seen.add(s); urls.append(s)
    for s in urls:
        try:
            js = fetch_text(s, opener, {'User-Agent': UA}, timeout=30)
        except Exception:
            continue
        pat = r'operationName["\']?\s*[:=]\s*["\']HomeLatestTimeline["\']?'
        for tm in re.finditer(pat, js):
            win_after = js[tm.end(): tm.end() + 2000]
            win_before = js[max(0, tm.start() - 2000): tm.start()]
            m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_after)
            if not m:
                m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_before)
            if m:
                _GQL_FOLLOWING_QID = m.group(1)
                log(f'已自动发现 HomeLatestTimeline query id: {_GQL_FOLLOWING_QID}')
                return _GQL_FOLLOWING_QID
    return None


def list_following_timeline(cookie_header, count=20, cursor=None):
    """读取 X 关注中（Following）时间线——只含用户真实关注的人。

    与 For You（HomeTimeline）不同，Following 走 HomeLatestTimeline 且用 POST
    （body 传 variables/features）。返回的推文 component 是 ranked_following
    （真实关注）与 ranked_following_promoted（关注者中的推广）。

    返回 (items, next_cursor)。复用 _extract_bookmark_tweets 解析。
    """
    opener = make_opener(None)
    qid = _GQL_FOLLOWING_QID
    if not qid:
        qid = _discover_following_qid(opener, cookie_header)
    if not qid:
        raise RuntimeError('未能发现 HomeLatestTimeline query id')
    variables = {"count": count, "enableRanking": True,
                 "includePromotedContent": True,
                 "requestContext": "launch", "seenTweetIds": []}
    if cursor:
        variables["cursor"] = cursor
    body = json.dumps({'variables': variables,
                       'features': _GQL_BOOKMARKS_FEATURES}).encode('utf-8')
    url = f'https://x.com/i/api/graphql/{qid}/{_GQL_FOLLOWING_OPN}'
    headers = build_headers(cookie_header, with_bearer=True)
    headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=body, headers=headers, method='POST')
    try:
        with opener.open(req, timeout=30) as r:
            raw = r.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8', 'replace')
    data = json.loads(raw)
    # 响应结构：data.home.home_timeline_urt 直接含 instructions（无 timeline 层）
    home = (data.get('data', {}) or {}).get('home', {}) or {}
    tl = home.get('home_timeline_urt') or {}
    # 过滤关注者中的推广（ranked_following_promoted），只保留真实关注（ranked_following）
    for _inst in (tl.get('instructions') or []):
        _entries = _inst.get('entries') or []
        if not _entries:
            continue
        _inst['entries'] = [
            e for e in _entries
            if not (((e.get('content') or {}).get('clientEventInfo') or {})
                    .get('component') or '').endswith('_promoted')
        ]
    items = _extract_bookmark_tweets(
        {'data': {'bookmark_timeline_v2': {'timeline': tl}}})
    next_cursor = None
    if items:
        for inst in (tl.get('instructions') or []):
            for entry in (inst.get('entries') or []):
                c = entry.get('content') or {}
                if c.get('entryType') == 'TimelineTimelineCursor' and \
                        c.get('cursorType') == 'Bottom':
                    next_cursor = c.get('value')
    return items, next_cursor


def _discover_bookmarks_qid(opener, cookie_header):
    """从 X 首页 bundle 自动发现收藏列表 query id（Bookmarks，登录态）。

    注意：X 首页 bundle 里同时含 Bookmarks（列收藏）与 BookmarkSearchTimeline
    （搜索）。列收藏优先匹配 operationName == "Bookmarks"。
    """
    global _GQL_BOOKMARKS_QID
    try:
        html = fetch_text('https://x.com/', opener,
                          build_headers(cookie_header, with_bearer=False), timeout=20)
    except Exception:
        html = ''
    scripts = re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']', html or '')
    seen = set(); urls = []
    for s in scripts:
        if not s.startswith('http'):
            s = 'https://x.com' + (s if s.startswith('/') else '/' + s)
        if s not in seen:
            seen.add(s); urls.append(s)
    # 优先精确匹配 Bookmarks（列收藏），避免误取 BookmarkSearchTimeline
    targets = ['Bookmarks', 'BookmarkSearchTimeline']
    for target in targets:
        for s in urls:
            try:
                js = fetch_text(s, opener, {'User-Agent': UA}, timeout=30)
            except Exception:
                continue
            pat = r'operationName["\']?\s*[:=]\s*["\']%s["\']?' % target
            for tm in re.finditer(pat, js):
                win_after = js[tm.end(): tm.end() + 2000]
                win_before = js[max(0, tm.start() - 2000): tm.start()]
                m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_after)
                if not m:
                    m = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', win_before)
                if m:
                    _GQL_BOOKMARKS_QID = m.group(1)
                    log(f'已自动发现 {target} query id: {_GQL_BOOKMARKS_QID}')
                    return _GQL_BOOKMARKS_QID
    return None


def _normalize_media(tweet_legacy):
    """从推文 legacy 提取媒体列表（图片直链 / 视频封面+播放地址）。"""
    media = []
    ext = tweet_legacy.get('extended_entities') or {}
    for m in (ext.get('media') or tweet_legacy.get('entities', {}).get('media') or []):
        kind = m.get('type')
        if kind == 'photo':
            url = m.get('media_url_https') or m.get('media_url')
            if url:
                media.append({'type': 'photo', 'url': url})
        elif kind in ('video', 'animated_gif'):
            variants = m.get('video_info', {}).get('variants') or []
            mp4 = next((v.get('url') for v in variants
                        if v.get('content_type') == 'video/mp4'), None)
            cover = m.get('media_url_https') or m.get('media_url')
            media.append({'type': 'video', 'cover': cover, 'url': mp4})
    return media


def _extract_bookmark_tweets(data):
    """从 Bookmarks 响应里提取推文列表（含作者、媒体）。

    Bookmarks 的响应路径为 data.bookmark_timeline_v2.timeline。
    兼容旧的 search_by_raw_query.bookmarks_search_timeline 结构。
    """
    out = []
    try:
        d = data.get('data', {})
        tl = (d.get('bookmark_timeline_v2') or {}).get('timeline')
        if tl is None:
            tl = ((d.get('search_by_raw_query') or {})
                  .get('bookmarks_search_timeline') or {}).get('timeline')
    except Exception:
        return out
    if tl is None:
        return out
    instructions = tl.get('instructions', []) or []
    for inst in instructions:
        entries = list(inst.get('entries') or inst.get('moduleItems') or [])
        for entry in entries:
            # 嵌套 entries（某些 instruction 结构）
            if entry.get('entries'):
                entries = entries + list(entry['entries'])
            content = entry.get('content') or {}
            item = (content.get('itemContent') or content.get('tweetResult') or {})
            # 兼容两种键名：tweet_results（X 新结构，下划线）/ tweetResults（旧结构）
            tr = item.get('tweet_results')
            if tr is None:
                tr = item.get('tweetResults')
            result = (tr or {}).get('result') if isinstance(tr, dict) else None
            if result is None:
                result = item.get('result')
            if not isinstance(result, dict):
                continue
            # TweetWithVisibilityResults / Tweet 都可能出现
            if result.get('__typename') == 'TweetWithVisibilityResults':
                result = result.get('tweet') or result
            if result.get('__typename') != 'Tweet':
                continue
            legacy = result.get('legacy') or {}
            user = ((result.get('core') or {})
                    .get('user_results', {})
                    .get('result', {}) or {})
            # 兼容多种 user 结构：
            # - 旧结构：user.legacy（name/screen_name/profile_image_url_https）
            # - 新结构：user.core（name/screen_name）+ user.avatar.image_url
            # - 扁平结构：user 直接含 name/screen_name
            user_legacy = user.get('legacy') or user
            user_core = (user.get('core') or {})
            u_name = (user_legacy.get('name') or user_core.get('name')
                      or user.get('name'))
            u_screen = (user_legacy.get('screen_name') or user_core.get('screen_name')
                        or user.get('screen_name'))
            u_avatar = (user_legacy.get('profile_image_url_https')
                        or (user.get('avatar') or {}).get('image_url')
                        or (user_core.get('avatar') or {}).get('image_url'))
            media = _normalize_media(legacy)
            if not media:
                # 引用推文也可能带媒体，简单跳过引用层
                pass
            out.append({
                'tweet_id': result.get('rest_id'),
                'text': legacy.get('full_text', ''),
                'created_at': legacy.get('created_at'),
                'favorite_count': legacy.get('favorite_count'),
                'retweet_count': legacy.get('retweet_count'),
                'author': {
                    'name': u_name,
                    'screen_name': u_screen,
                    'avatar': u_avatar,
                    'verified': user_legacy.get('verified', False),
                },
                'media': media,
                'url': 'https://x.com/{}/status/{}'.format(
                    u_screen or 'unknown', result.get('rest_id')),
            })
    return out


def list_bookmarks(cookie_header, count=20, cursor=None):
    """读取 X 账号收藏夹的推文列表。

    使用 X 收藏页同款 Bookmarks query（variables 为 count/cursor/
    includePromotedContent，不需要 rawQuery）。返回 (items, next_cursor)。
    """
    opener = make_opener(None)
    qid = _GQL_BOOKMARKS_QID
    if not qid:
        qid = _discover_bookmarks_qid(opener, cookie_header)
    if not qid:
        raise RuntimeError('未能发现 Bookmarks query id')
    variables = {"count": count, "includePromotedContent": True}
    if cursor:
        variables["cursor"] = cursor
    # 与 X 收藏页同源：x.com/i/api/graphql/（api.x.com 亦可）
    url = (f'https://x.com/i/api/graphql/{qid}/{_GQL_BOOKMARKS_OPN}'
           f'?variables={urllib.parse.quote(json.dumps(variables))}'
           f'&features={urllib.parse.quote(json.dumps(_GQL_BOOKMARKS_FEATURES))}')
    headers = build_headers(cookie_header, with_bearer=True)
    raw = fetch_text(url, opener, headers, timeout=30)
    data = json.loads(raw)
    items = _extract_bookmark_tweets(data)
    # 提取下一页 cursor（Bottom 类型）。若本页无任何推文，视为已到底，
    # 不返回下一页 cursor（X 第二页 stopOnEmptyResponse 时空响应会一直返回
    # 相同的 Bottom cursor，直接透传会造成死循环）。
    next_cursor = None
    if items:
        try:
            d = data.get('data', {})
            tl = (d.get('bookmark_timeline_v2') or {}).get('timeline')
            if tl is None:
                tl = ((d.get('search_by_raw_query') or {})
                      .get('bookmarks_search_timeline') or {}).get('timeline')
            for inst in (tl or {}).get('instructions', []):
                for entry in (inst.get('entries') or []):
                    c = entry.get('content') or {}
                    if c.get('entryType') == 'TimelineTimelineCursor' and \
                            c.get('cursorType') == 'Bottom':
                        next_cursor = c.get('value')
        except Exception:
            pass
    return items, next_cursor


class _GraphQLQueryNotFound(Exception):
    """GraphQL query id 不存在（404 / unknown query），需重新发现。"""


def _get_tweet_detail_qid():
    """返回当前 TweetDetail query id（带模块级缓存）。"""
    global _GQL_TWEET_DETAIL_QID
    return _GQL_TWEET_DETAIL_QID


def _gql_variables(tweet_id, cursor=None):
    return {
        "focalTweetId": tweet_id,
        "cursor": cursor,
        "referrer": None,
        "controller_data": None,
        "rux_context": None,
        "with_rux_injections": False,
        "rankingMode": "Relevance",
        "includePromotedContent": True,
        "withCommunity": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withBirdwatchNotes": False,
        "withVoice": False,
        "withDownvotePerspective": False,
        "withReactionsMetadata": False,
        "withReactionsPerspective": False,
        "withSuperFollowsUserFields": True,
        "withUserResults": True,
        "withCad": True,
        "withV2Timeline": True,
    }


def _graphql_tweet_detail(tweet_id, cookie_header, opener, gt, qid):
    url = (f'https://api.x.com/graphql/{qid}/TweetDetail'
           f'?variables={urllib.parse.quote(json.dumps(_gql_variables(tweet_id)))}'
           f'&features={urllib.parse.quote(json.dumps(_GQL_TWEET_DETAIL_FEATURES))}'
           f'&fieldToggles={urllib.parse.quote(json.dumps(_GQL_TWEET_DETAIL_FIELDTOGGLES))}')
    headers = build_headers(cookie_header, with_bearer=True, guest_token=gt)
    try:
        raw = fetch_text(url, opener, headers, timeout=30)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise _GraphQLQueryNotFound()
        raise
    return json.loads(raw)


def _extract_tweet_obj(r):
    """从单个 tweet result 提取标准推文对象（与 _extract_bookmark_tweets 一致）。"""
    if not isinstance(r, dict):
        return None
    if r.get('__typename') == 'TweetWithVisibilityResults':
        r = r.get('tweet') or r
    if r.get('__typename') != 'Tweet':
        return None
    legacy = r.get('legacy') or {}
    user = ((r.get('core') or {})
            .get('user_results', {})
            .get('result', {}) or {})
    user_legacy = user.get('legacy') or user
    user_core = user.get('core') or {}
    u_name = (user_legacy.get('name') or user_core.get('name') or user.get('name'))
    u_screen = (user_legacy.get('screen_name') or user_core.get('screen_name') or user.get('screen_name'))
    u_avatar = (user_legacy.get('profile_image_url_https')
                or (user.get('avatar') or {}).get('image_url')
                or (user_core.get('avatar') or {}).get('image_url'))
    return {
        'tweet_id': r.get('rest_id'),
        'text': legacy.get('full_text', ''),
        'created_at': legacy.get('created_at'),
        'favorite_count': legacy.get('favorite_count'),
        'retweet_count': legacy.get('retweet_count'),
        'reply_count': legacy.get('reply_count'),
        'quote_count': legacy.get('quote_count'),
        'bookmark_count': legacy.get('bookmark_count'),
        'view_count': (r.get('views') or {}).get('count'),
        'author': {
            'name': u_name,
            'screen_name': u_screen,
            'avatar': u_avatar,
            'verified': user_legacy.get('verified', False),
        },
        'media': _normalize_media(legacy),
        'url': 'https://x.com/{}/status/{}'.format(u_screen or 'unknown', r.get('rest_id')),
    }


def get_tweet_thread(tweet_id, cookie_header, cursor=None):
    """拉取单条推文详情 + 评论区（对话线程）。

    X 推文详情页的评论区就包含在 TweetDetail 响应里
    （data.threaded_conversation_with_injections_v2.instructions：
    第一个是焦点推文，后续 conversationthread-* 是回复）。
    返回 {'tweet': focal, 'replies': [...], 'next_cursor': ...}。
    """
    opener = make_opener(None)
    qid = _get_tweet_detail_qid()
    if not qid:
        qid = _discover_tweet_detail_qid(opener, cookie_header)
    if not qid:
        raise RuntimeError('未能发现 TweetDetail query id')
    url = (f'https://x.com/i/api/graphql/{qid}/TweetDetail'
           f'?variables={urllib.parse.quote(json.dumps(_gql_variables(tweet_id, cursor)))}'
           f'&features={urllib.parse.quote(json.dumps(_GQL_TWEET_DETAIL_FEATURES))}')
    headers = build_headers(cookie_header, with_bearer=True)
    try:
        raw = fetch_text(url, opener, headers, timeout=30)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise RuntimeError('TweetDetail query id 失效')
        raise
    data = json.loads(raw)
    if data.get('errors'):
        raise RuntimeError('X 返回错误: ' + str(data['errors'][0].get('message', data['errors'])))
    tl = ((data.get('data') or {})
          .get('threaded_conversation_with_injections_v2') or {}).get('instructions') or []

    focal = None
    replies = []
    next_cursor = None
    seen = set()
    for inst in tl:
        # 普通条目（焦点推文 / cursor）
        for e in (inst.get('entries') or []):
            c = e.get('content') or {}
            ic = c.get('itemContent') or {}
            tr = ic.get('tweet_results') or {}
            r = tr.get('result') or {}
            obj = _extract_tweet_obj(r)
            if obj and obj['tweet_id'] and obj['tweet_id'] not in seen:
                seen.add(obj['tweet_id'])
                if focal is None:
                    focal = obj
                else:
                    replies.append(obj)
            if c.get('entryType') == 'TimelineTimelineCursor' and \
                    c.get('cursorType') == 'Bottom':
                next_cursor = c.get('value')
        # TimelineTimelineModule：评论线程（items 里是回复推文）
        for e in (inst.get('entries') or []):
            c = e.get('content') or {}
            if c.get('__typename') == 'TimelineTimelineModule':
                for it in (c.get('items') or []):
                    ic = (it.get('item') or {}).get('itemContent') or {}
                    tr = ic.get('tweet_results') or {}
                    r = tr.get('result') or {}
                    obj = _extract_tweet_obj(r)
                    if obj and obj['tweet_id'] and obj['tweet_id'] not in seen:
                        seen.add(obj['tweet_id'])
                        replies.append(obj)
    # 若焦点推文不在第一顺位（罕见），用 tweet_id 匹配回填
    if focal is None:
        for i, rp in enumerate(replies):
            if rp['tweet_id'] == tweet_id:
                focal = replies.pop(i)
                break
    return {'tweet': focal, 'replies': replies, 'next_cursor': next_cursor}


def _discover_tweet_detail_qid(opener, cookie_header):
    """从 X 前端 JS bundle 中自动发现当前 TweetDetail query id。

    关键：X 的 queryId 只存在于**登录态**的 client-web/main.*.js bundle 里，
    游客态首页只返回 entry-client-logged-out.js（不含 queryId）。因此：
      - 带 Cookie 时能拿到 client-web bundle 并提取 queryId；
      - 游客态拿不到，只能依赖写死的默认值（会过期）。
    这里尽量放宽正则（queryId 与 operationName 之间距离不限），
    并扫描首页 HTML 里所有 abs.twimg.com 的 <script src>。
    """
    global _GQL_TWEET_DETAIL_QID
    try:
        html = fetch_text('https://x.com/', opener,
                          build_headers(cookie_header, with_bearer=False), timeout=20)
    except Exception:
        html = ''
    scripts = re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']', html or '')
    # 去重、保留顺序
    seen = set()
    urls = []
    for s in scripts:
        if not s.startswith('http'):
            s = 'https://x.com' + (s if s.startswith('/') else '/' + s)
        if s not in seen:
            seen.add(s)
            urls.append(s)
    for s in urls:
        try:
            js = fetch_text(s, opener, {'User-Agent': UA}, timeout=30)
        except Exception:
            continue
        if 'TweetDetail' not in js:
            continue
        # 放宽正则：queryId 与 operationName 之间允许任意内容（不限 80 字符）
        m = re.search(
            r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']',
            js)
        # 优先取紧邻 TweetDetail 出现的 queryId：找 TweetDetail 附近前后各 500 字符内的 queryId
        found = None
        for tm in re.finditer(r'TweetDetail', js):
            lo = max(0, tm.start() - 600)
            hi = min(len(js), tm.end() + 600)
            window = js[lo:hi]
            mm = re.search(r'queryId["\']?\s*[:=]\s*["\']([A-Za-z0-9_-]{20,})["\']', window)
            if mm:
                found = mm.group(1)
                break
        if found:
            _GQL_TWEET_DETAIL_QID = found
            log(f'已自动发现 TweetDetail query id: {found}')
            return found
    return None


def _find_tweet_node(node, target_id=None):
    """在 GraphQL 响应树中递归定位含 legacy 的 Tweet 节点。

    target_id 给定时优先返回 rest_id/id_str 与之匹配的节点（即目标推文），
    避免误取对话流中的回复/引用推文。
    """
    if isinstance(node, dict):
        if node.get('__typename') == 'Tweet' and node.get('legacy'):
            rid = node.get('rest_id') or (node.get('legacy') or {}).get('id_str')
            if target_id is None or rid == target_id:
                return node
        for v in node.values():
            r = _find_tweet_node(v, target_id)
            if r:
                return r
    elif isinstance(node, list):
        for v in node:
            r = _find_tweet_node(v, target_id)
            if r:
                return r
    return None


def extract_tweet_text_from_html(html, tweet_id=''):
    """从推文页面 HTML 尽力提取正文（页面版正文字段分散，作为接口失败时的兜底）。"""
    if not html:
        return ''
    # 优先从目标推文的 tweetText 块提取（最贴近真实正文）
    block = _focal_article_html(html, tweet_id) if tweet_id else html
    m = re.search(r'<div data-testid=["\']tweetText["\'][^>]*>(.*?)</div>', block, re.S)
    if m:
        txt = re.sub(r'<[^>]+>', '', m.group(1))
        txt = html.unescape(txt).replace('\n', ' ').strip()
        if txt:
            return txt
    # 兜底：og:description（含推文正文）
    m = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
                  html, re.IGNORECASE)
    if m:
        desc = m.group(1)
        # og:description 形如 "作者名: 正文" 或含 "· X" 后缀，做简单清洗
        desc = re.sub(r'\s*—\s*X\s*$', '', desc)
        desc = re.sub(r'\s*·\s*X\s*$', '', desc)
        return desc.strip()
    return ''


def extract_author_from_html(html):
    """从游客页 HTML 尽力提取作者信息（X 游客态 GraphQL 常被限流，故从 og 标签兜底）。

    常见结构：
      - og:description 形如 "显示名 (@username): 正文 …"
      - og:title 形如 "显示名 (@username) / X"
    返回 {'name':..,'screen_name':..,'url':..} 或 None
    """
    if not html:
        return None
    name = None
    screen = None
    # og:title: "显示名 (@username) / X"
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
                  html, re.IGNORECASE)
    if m:
        title = m.group(1).strip()
        title = re.sub(r'\s*/\s*X\s*$', '', title)
        mm = re.match(r'^(.*?)\s*\(@(\w+)\)\s*$', title)
        if mm:
            name = mm.group(1).strip()
            screen = mm.group(2).strip()
    # og:description: "显示名 (@username): 正文"
    if not screen:
        m = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
                      html, re.IGNORECASE)
        if m:
            dm = re.match(r'^(.*?)\s*\(@(\w+)\)\s*:\s*', m.group(1))
            if dm:
                if not name:
                    name = dm.group(1).strip()
                screen = dm.group(2).strip()
    # 仅当解析到 @用户名 时才采用，避免把页面类型（如 "Post"）误当作者
    if screen:
        return {'name': name or screen, 'screen_name': screen, 'url': 'https://x.com/' + screen}
    return None


def extract_media(url, cookie_header, proxy_cfg):
    """解析推文，返回 (媒体列表, 推文文字, 作者信息)。

    策略（公开推文最稳路径优先）：
      1) 无 Cookie 游客页——公开内容无需登录即可解析；
      2) 若游客态解析不到，带 Cookie 登录态重新抓取；
      3) 仍解析不到（X 现为 SPA，结构化数据仅在 GraphQL 接口），调用 TweetDetail 接口。
    说明：视频清晰度与是否登录无关——m3u8 主列表含全部分辨率，下载时取最高。
    """
    opener = make_opener(proxy_cfg)
    author = None
    tweet_id = (re.search(r'/status/(\d+)', url) or [None, 'x'])[1]

    def _norm_url(u):
        """归一化媒体 URL 用于去重：去掉查询串与 :orig/:large 等格式后缀。"""
        if not u:
            return u
        u = u.split('?', 1)[0]
        u = re.sub(r':(orig|large|medium|small|thumb)$', '', u)
        return u

    log('正在抓取推文页面…')
    media = []
    text = ''
    seen = set()

    def _add(items):
        for it in items:
            key = _norm_url(it.get('url', ''))
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            media.append(it)

    # 1) 游客页 HTML（公开内容无需登录即可解析）
    html = None
    try:
        html = fetch_text(url, opener, build_headers('', with_bearer=False), timeout=45)
    except Exception as e:
        log(f'页面抓取失败: {e}', level='warn')
    if html:
        _add(extract_from_html(html, '', tweet_id))
        html_text = extract_tweet_text_from_html(html, tweet_id)
        # 仅当 HTML 文本看起来像真实正文（非占位/异常）时才作为候选
        if html_text and html_text.lower() not in ('post', 'repost', 'quote'):
            text = text or html_text
        # 游客态 GraphQL 常被限流，从 og 标签兜底提取作者
        author = author or extract_author_from_html(html)

    # 2) 带 Cookie 登录态再抓一次（可能拿到更完整内容）
    if cookie_header:
        try:
            html2 = fetch_text(url, opener, build_headers(cookie_header, with_bearer=False), timeout=45)
            if html2:
                _add(extract_from_html(html2, cookie_header, tweet_id))
                html_text2 = extract_tweet_text_from_html(html2, tweet_id)
                if html_text2 and html_text2.lower() not in ('post', 'repost', 'quote'):
                    text = text or html_text2
        except Exception as e:
            log(f'带 Cookie 抓取失败: {e}', level='warn')

    # 3) GraphQL 接口（结构化数据，视频 m3u8 的唯一可靠来源）。
    #    全新规则：预览必须完整呈现（图片 + 视频），故 GraphQL 是主路径。
    #    - 登录态（Cookie 含 auth_token+ct0）优先，不限流、可解析登录态内容；
    #    - 登录态 401（Cookie 缺 auth_token 或过期）→ extract_from_api 自动降级游客态；
    #    - 游客态仍失败 → 抛 _GraphQLAuthFailed，这里兜底 HTML（图片仍在）。
    if tweet_id != 'x':
        log('尝试 GraphQL 接口方式解析媒体（含视频）…')
        try:
            api_media, api_text, api_author = extract_from_api(tweet_id, cookie_header, opener)
            _add(api_media)
            # GraphQL 的 full_text 是权威正文，优先于 HTML 兜底
            text = api_text or text
            author = author or api_author
        except _GraphQLAuthFailed as e:
            log(f'GraphQL 鉴权失败（视频无法解析）: {e}', level='error')
            if cookie_header:
                log('原因：X 拒绝了当前保险库 x.com Cookie（登录态 GraphQL 返回 401）。'
                    '视频 m3u8 仅存在于 GraphQL 返回中，故无法解析。'
                    '请检查凭证库中 x.com Cookie 是否完整且有效（需 auth_token + ct0 均为有效值，未过期）。',
                    level='error')
            else:
                log('原因：未配置 x.com Cookie，且游客态（匿名）已不可用，无法获取视频 m3u8。'
                    '请在凭证库添加有效的 x.com Cookie（auth_token + ct0）。', level='error')
            # 不 return，继续用 HTML 兜底（至少保留图片与文字）
        except Exception as e:
            log(f'GraphQL 接口解析失败: {e}', level='warn')

    # 兜底提示：解析到的全是图片但无视频时，明确告知视频缺失原因。
    if media and not any(m.get('type') == 'video' for m in media):
        if cookie_header:
            log('提示：当前仅解析到图片、缺少视频——X 的视频数据只存在于登录态 GraphQL 接口返回中，'
                '当前 Cookie 未能通过 X 鉴权。请确认凭证库 x.com Cookie 完整有效（auth_token + ct0）。',
                level='warn')
        else:
            log('提示：当前仅解析到图片、缺少视频——未配置 x.com Cookie，且游客态已不可用。'
                '请在凭证库添加有效的 x.com Cookie（auth_token + ct0）后再试。', level='warn')

    return media, text, author


# ---------------- 下载 ----------------
# 媒体缓存目录（由 server 传入）：下载前若该 URL 已在本地缓存（用户预览时已下载），
# 直接复用本地字节，不重复访问 twimg —— P0-2「缓存即下载」。
_MEDIA_CACHE_DIR = None


def _cache_hit_path(url, allowed_exts):
    """返回本地媒体缓存中匹配 url 的文件路径；无缓存或扩展名不符返回 None。"""
    if not _MEDIA_CACHE_DIR or not os.path.isdir(_MEDIA_CACHE_DIR):
        return None
    try:
        key = hashlib.md5(url.encode('utf-8')).hexdigest()
        for fn in os.listdir(_MEDIA_CACHE_DIR):
            if fn.startswith(key):
                ext = os.path.splitext(fn)[1].lower()
                if allowed_exts and ext not in allowed_exts:
                    continue
                p = os.path.join(_MEDIA_CACHE_DIR, fn)
                if os.path.isfile(p):
                    return p
    except Exception:
        return None
    return None


def download_image(url, cookie_header, working_dir, index, proxy_cfg, guest_token=None, progress_cb=None):
    ext = os.path.splitext(urllib.parse.urlparse(url).path)[1] or '.jpg'
    ext = ext if ext.lower() in ('.jpg', '.jpeg', '.png', '.gif', '.webp') else '.jpg'
    dest = os.path.join(working_dir, f'x_media_{index}{ext}')
    # 缓存即下载：命中本地缓存直接复制
    cached = _cache_hit_path(url, {'.jpg', '.jpeg', '.png', '.gif', '.webp'})
    if cached:
        import shutil
        shutil.copyfile(cached, dest)
        if progress_cb:
            try:
                progress_cb(1, 1)
            except Exception:
                pass
        return dest
    headers = build_headers(cookie_header, guest_token=guest_token)
    opener = make_opener(proxy_cfg)
    data = fetch_bytes(url, opener, headers, timeout=90)
    with open(dest, 'wb') as f:
        f.write(data)
    # 图片通常瞬时完成，向调用方报告一次「已完成」
    if progress_cb:
        try:
            progress_cb(1, 1)
        except Exception:
            pass
    return dest


def parse_m3u8(m3u8_text, base_url):
    """解析单个 rendition 的 m3u8：返回 (init_url_or_None, [segment_urls])。

    X 的视频为 fMP4（分片为 .m4s，初始化段由 #EXT-X-MAP 指定）。
    """
    init_url = None
    segments = []
    lines = m3u8_text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXT-X-MAP:'):
            mm = re.search(r'URI="([^"]+)"', line)
            if mm:
                init_url = urllib.parse.urljoin(base_url, mm.group(1))
        elif line.startswith('#'):
            continue
        elif line:
            if line.startswith('http://') or line.startswith('https://'):
                segments.append(line)
            else:
                segments.append(urllib.parse.urljoin(base_url, line))
    return init_url, segments


def select_renditions(master_text, base_url):
    """从 master playlist 选出：最高分辨率视频 rendition + 对应音频 rendition。

    返回 (video_url, audio_url_or_None)。
    """
    lines = master_text.splitlines()
    audio_map = {}      # group-id -> uri（取带宽最大的）
    audio_bw = {}       # group-id -> bandwidth
    streams = []        # (width, audio_group, uri)
    cur = {}
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXT-X-MEDIA:'):
            if 'TYPE=AUDIO' in line:
                g = re.search(r'GROUP-ID="([^"]+)"', line)
                u = re.search(r'URI="([^"]+)"', line)
                bw = re.search(r'BANDWIDTH=(\d+)', line)
                if g and u:
                    gid = g.group(1)
                    uri = urllib.parse.urljoin(base_url, u.group(1))
                    b = int(bw.group(1)) if bw else 0
                    if b >= audio_bw.get(gid, -1):
                        audio_map[gid] = uri
                        audio_bw[gid] = b
        elif line.startswith('#EXT-X-STREAM-INF:'):
            cur = {}
            rm = re.search(r'RESOLUTION=(\d+)x(\d+)', line)
            am = re.search(r'AUDIO="([^"]+)"', line)
            cur['w'] = int(rm.group(1)) if rm else 0
            cur['audio'] = am.group(1) if am else None
        elif line and not line.startswith('#'):
            if 'w' in cur:
                streams.append((cur['w'], cur['audio'],
                                urllib.parse.urljoin(base_url, line)))
    if not streams:
        return base_url, None
    streams.sort(key=lambda x: x[0], reverse=True)
    w, audio_group, vurl = streams[0]
    aurl = audio_map.get(audio_group) if audio_group else None
    return vurl, aurl


def download_fmp4_stream(stream_url, prefix, working_dir, opener, headers, seg_progress_cb=None):
    """下载一个 fMP4 流（init + 分片），用 ffmpeg concat 协议拼为单个 mp4。

    返回生成的 mp4 路径。

    seg_progress_cb(done_seg, total_seg) 在每成功下载一个分片（含 init 段算作 1/N）
    后调用一次，用于让外部把分片级别进度映射到全局进度条。
    """
    text = fetch_text(stream_url, opener, headers, timeout=45)
    init_url, segments = parse_m3u8(text, stream_url)
    if not segments:
        raise RuntimeError(f'流 {prefix} 未解析到分片')
    if '#EXT-X-KEY' in text:
        log(f'流 {prefix} 检测到加密分片，可能无法合并', level='warn')

    n = len(segments)
    # 含 init 段在内的总段数；进度条按整流（含 init）走，下载 init 通常是瞬时
    total_for_progress = n + (1 if init_url else 0)
    log(f'下载流 {prefix}：{n} 个分片' + (f' + init' if init_url else ''))
    names = []
    done_segs = 0
    if init_url:
        init_path = os.path.join(working_dir, f'{prefix}_init.mp4')
        # 续传：已成功下载的 init 段直接复用
        if not (os.path.isfile(init_path) and os.path.getsize(init_path) > 0):
            with open(init_path, 'wb') as f:
                f.write(fetch_bytes(init_url, opener, headers, timeout=90))
        else:
            log(f'  init 已存在，复用')
        names.append(f'{prefix}_init.mp4')
        done_segs += 1
        # init 段也算一步进度（让首字节后的进度条有所反应）
        if seg_progress_cb:
            try:
                seg_progress_cb(done_segs, total_for_progress)
            except Exception:
                pass
    for i, seg_url in enumerate(segments, start=1):
        seg_path = os.path.join(working_dir, f'{prefix}_{i:04d}.m4s')
        # 续传：已成功下载的分片直接复用，避免失败后从头重来
        if os.path.isfile(seg_path) and os.path.getsize(seg_path) > 0:
            names.append(f'{prefix}_{i:04d}.m4s')
            done_segs += 1
            if seg_progress_cb:
                try:
                    seg_progress_cb(done_segs, total_for_progress)
                except Exception:
                    pass
            if i % 5 == 0 or i == n:
                log(f'  分片 {i}/{n}（已存在，复用）')
            continue
        with open(seg_path, 'wb') as f:
            f.write(fetch_bytes(seg_url, opener, headers, timeout=90))
        names.append(f'{prefix}_{i:04d}.m4s')
        done_segs += 1
        # 新增：每下载完成一个分片，立即向调用方上报分段进度，
        # 避免长时间停在「开始下载 40%」看不出在动。
        if seg_progress_cb:
            try:
                seg_progress_cb(done_segs, total_for_progress)
            except Exception:
                pass
        if i % 5 == 0 or i == n:
            log(f'  分片 {i}/{n}')

    # concat 协议要求相对路径（避免 Windows 盘符冒号冲突），故以 working_dir 为 cwd
    concat_in = 'concat:' + '|'.join(names)
    out_path = os.path.join(working_dir, f'{prefix}.mp4').replace(chr(92), '/')
    cmd = [ffmpeg_exe(), '-y', '-i', concat_in, '-c', 'copy', out_path]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                          cwd=working_dir)
    if proc.returncode != 0 or not os.path.exists(out_path):
        err = (proc.stderr or '')[max(0, len(proc.stderr or '')) - 800:]
        raise RuntimeError(f'ffmpeg 拼接流 {prefix} 失败: ' + err)
    return out_path


def download_video(m3u8_url, cookie_header, working_dir, tweet_id, proxy_cfg, guest_token=None, progress_cb=None):
    """下载 X 视频。X 视频为 fMP4，且音视频分离：
       1) 解析 master -> 选最高分辨率视频流 + 对应音频流；
       2) 各自下载 init+分片，ffmpeg concat 协议拼为 mp4；
       3) 有音频时再混流为最终 mp4。
       若 m3u8 直接是单个 rendition 或只有视频，则仅拼视频流。

    progress_cb(done, total) 在下载过程中按文件内分片粒度回调，多次调用；
    done/total ∈ [0, total]，其中 total 默认 1（即 done ∈ [0, 1]）。
    """
    opener = make_opener(proxy_cfg)
    headers = build_headers(cookie_header, guest_token=guest_token)
    log('解析视频播放列表（m3u8）…')

    def _safe_cb(d, t):
        if not progress_cb:
            return
        try:
            progress_cb(d, t)
        except Exception:
            pass

    # 直链 mp4（如某些 GIF）直接下载，无需 ffmpeg
    if m3u8_url.lower().endswith('.mp4'):
        dest = os.path.join(working_dir, f'{tweet_id}.mp4')
        with open(dest, 'wb') as f:
            f.write(fetch_bytes(m3u8_url, opener, headers, timeout=120))
        _safe_cb(1, 1)
        return dest

    master_text = fetch_text(m3u8_url, opener, headers, timeout=45)
    if '#EXT-X-STREAM-INF' in master_text:
        v_url, a_url = select_renditions(master_text, m3u8_url)
    else:
        v_url, a_url = m3u8_url, None

    # 视频流占整个文件 60%，音频流占 40%（多数 X 视频音频段更短，加权合理）。
    # 这样在视频分片下载阶段能持续推进，进度条不再卡在 40。
    VIDEO_WEIGHT = 0.6
    AUDIO_WEIGHT = 0.4

    def v_seg_cb(d, n):
        frac = (d / n) if n else 1.0
        _safe_cb(VIDEO_WEIGHT * frac, 1.0)

    def a_seg_cb(d, n):
        frac = (d / n) if n else 1.0
        _safe_cb(VIDEO_WEIGHT + AUDIO_WEIGHT * frac, 1.0)

    v_out = download_fmp4_stream(v_url, 'video', working_dir, opener, headers,
                                 seg_progress_cb=v_seg_cb)
    if not a_url:
        # 仅视频流时把整体推到 100%，避免「视频完成但音频未到」让用户误以为卡住
        _safe_cb(1, 1)
        return v_out
    a_out = download_fmp4_stream(a_url, 'audio', working_dir, opener, headers,
                                 seg_progress_cb=a_seg_cb)

    # 混流
    final = os.path.join(working_dir, f'{tweet_id}.mp4').replace(chr(92), '/')
    cmd = [ffmpeg_exe(), '-y', '-i', 'video.mp4', '-i', 'audio.mp4',
           '-c', 'copy', final]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                          cwd=working_dir)
    if proc.returncode != 0 or not os.path.exists(final):
        err = (proc.stderr or '')[max(0, len(proc.stderr or '')) - 800:]
        raise RuntimeError('ffmpeg 混流失败: ' + err)
    _safe_cb(1, 1)
    return final


def _looks_like_html(data):
    head = data[:512].lstrip().lower()
    return head.startswith(b'<!doctype') or head.startswith(b'<html') or head.startswith(b'<?xml')


def download_document(media, cookie_header, working_dir, idx, proxy_cfg, guest_token, progress_cb=None):
    """下载 X 文档附件（PDF / Office 等）。

    X 文档真实下载地址不在 entities 里直接给出，这里按优先级尝试若干候选地址，
    并用「内容是否为 HTML 错误页」做校验，避免把 404 页面存成假文档。
    全部失败时退化为下载缩略图（至少有一张预览图）。
    """
    opener = make_opener(proxy_cfg)
    headers = build_headers(cookie_header, guest_token=guest_token)
    media_id = media.get('media_id') or ''

    def _safe_cb(d, t):
        if not progress_cb:
            return
        try:
            progress_cb(d, t)
        except Exception:
            pass

    candidates = []
    if media_id:
        candidates.append(f'https://pbs.twimg.com/document/{media_id}/{media_id}?format=pdf&name=orig')
    if media.get('url'):
        candidates.append(media['url'])

    last_err = None
    for u in candidates:
        try:
            data = fetch_bytes(u, opener, headers, timeout=90)
            if _looks_like_html(data):
                last_err = f'文档地址返回 HTML（疑似失效）: {u}'
                continue
            # 按内容/URL 猜测扩展名
            ext = '.pdf'
            lowered = u.lower()
            if '.docx' in lowered or data[:4] == b'PK\x03\x04':
                ext = '.docx' if '.docx' in lowered else ('.zip' if data[:4] == b'PK\x03\x04' else '.bin')
            elif '.xlsx' in lowered:
                ext = '.xlsx'
            elif '.pptx' in lowered:
                ext = '.pptx'
            elif '.txt' in lowered:
                ext = '.txt'
            dest = os.path.join(working_dir, f'doc_{idx}{ext}')
            with open(dest, 'wb') as f:
                f.write(data)
            log(f'已下载文档: {os.path.basename(dest)}')
            _safe_cb(1, 1)
            return dest
        except Exception as e:
            last_err = f'{u}: {e}'
            log(f'文档候选地址失败: {last_err}', level='warn')

    # 退化：下载缩略图作为预览图
    thumb = media.get('thumbnail') or ''
    if thumb:
        try:
            dest = download_image(thumb, cookie_header, working_dir, idx, proxy_cfg, guest_token)
            log('文档实际文件未取到，已退化为下载缩略图预览', level='warn')
            _safe_cb(1, 1)
            return dest
        except Exception as e:
            log(f'文档缩略图也下载失败: {e}', level='warn')
    raise RuntimeError(f'文档下载失败: {last_err}')


def simulate_media():
    """演示用：合成 3 个媒体（2 图 + 1 视频），用于体现“预览确认”与“二次选择”交互。"""
    return [
        {'type': 'image', 'url': 'https://pbs.twimg.com/demo/1.jpg', 'label': '图片 1（演示）'},
        {'type': 'image', 'url': 'https://pbs.twimg.com/demo/2.jpg', 'label': '图片 2（演示）'},
        {'type': 'video', 'url': 'https://video.twimg.com/demo/playlist.m3u8',
         'cover': 'https://pbs.twimg.com/demo/cover.jpg',
         'mp4': 'https://video.twimg.com/demo/video.mp4',
         'label': '视频（演示）'},
    ]


def write_sim_placeholder(working_dir, index, mtype):
    ext = {'image': '.jpg', 'video': '.mp4', 'document': '.pdf'}.get(mtype, '.bin')
    dest = os.path.join(working_dir, f'x_demo_{index}{ext}')
    with open(dest, 'w', encoding='utf-8') as f:
        f.write(f'demo placeholder for {mtype}\n')
    return dest


# ---------------- 主流程 ----------------
def main():
    raw = sys.stdin.read() if not sys.stdin.isatty() else '{}'
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except Exception:
        payload = {}

    params = payload.get('params', {}) or {}
    context = payload.get('context', {}) or {}
    working_dir = context.get('working_dir') or os.getcwd()
    notify_ctx = context.get('notify', {}) or {}
    cookie_header = resolve_cookie(context.get('cookies', {}) or {})
    # 媒体缓存目录（server 传入）：下载时优先复用本地缓存（P0-2 缓存即下载）
    global _MEDIA_CACHE_DIR
    _MEDIA_CACHE_DIR = context.get('media_cache_dir') or None
    url = (params.get('url') or '').strip()
    simulate = bool(params.get('simulate'))
    # 目标资源库：前端放在顶层 body.library_id（与 params 平级），也可能在 params 里
    library_id = payload.get('library_id') or params.get('library_id') or ''
    # 隐藏开关（替换原 add_video/add_gallery）：下载的资源默认隐藏，
    # 不进视频库/图集库列表，仅在帖子流可见。取消隐藏后进入对应资源库。
    hidden = bool(params.get('hidden', True))
    # 标题：可选项；由用户在加载脚本时选择是否填写。留空则帖子不含标题。
    title_param = params.get('title')
    title_param = str(title_param).strip() if title_param else ''
    # 解析 tweet id（同一任务内的文件聚合为一条帖子）
    tweet_id = (re.search(r'/status/(\d+)', url) or [None, 'x'])[1]
    group = tweet_id
    proxy_cfg = parse_proxy(params.get('proxy'))
    global _PROXY_CFG
    _PROXY_CFG = proxy_cfg
    if proxy_cfg:
        kind = proxy_cfg['type']
        if kind == 'http':
            log(f'使用 HTTP 代理访问 X: {proxy_cfg["addr"]}')
        else:
            log(f'使用 SOCKS 代理访问 X: {proxy_cfg["scheme"]}://{proxy_cfg["host"]}:{proxy_cfg["port"]}')

    if not url:
        error('缺少推文链接参数 url')
        sys.exit(1)

    m = re.search(r'/status/(\d+)', url)
    tweet_id = m.group(1) if m else 'x'

    progress(5, '开始解析推文')
    tweet_text = ''
    tweet_author = None
    try:
        if simulate:
            log('演示模式：合成媒体列表（不联网）')
            media = simulate_media()
            tweet_text = '这是演示推文的文字内容（演示模式）。'
        else:
            media, tweet_text, tweet_author = extract_media(url, cookie_header, proxy_cfg)
    except Exception as e:
        error(str(e))
        sys.exit(1)

    if not media:
        error('未在该推文中找到任何图片或视频')
        sys.exit(1)

    # 推文文字作为帖子正文；标题仅当用户在加载脚本中填写了 title 参数才设置，否则帖子无标题
    post_content = (tweet_text or '').strip()
    post_title = title_param
    if post_title:
        log(f'使用自定义标题: {post_title}')
    else:
        log('未填写标题，帖子将以无标题创建', level='warn')

    log(f'解析到 {len(media)} 个媒体：' + '，'.join(x['label'] for x in media))

    # 预览模式（默认开启）：解析后先把资源预览发给前端（图片直接显示、视频封面点击播放），
    # 等待用户确认后再下载整个帖子；用户也可选择仅下载部分项，或直接取消。
    # auto 仅影响「预览关闭」时是否询问逐项选择，与预览模式互不冲突。
    preview_mode = bool(params.get('preview', True))
    auto_mode = bool(params.get('auto', True))

    if preview_mode:
        progress(20, '解析完成，展示预览…')
        emit({
            'type': 'await_input',
            'input': {
                'type': 'preview',
                'url': url,
                'text': post_content or '',
                'title': post_title,
                'author': tweet_author,
                'media': [
                    {
                        'type': m.get('type', ''),
                        'url': m.get('url', ''),
                        'label': m.get('label', ''),
                        'cover': m.get('cover') or '',
                        'mp4': m.get('mp4') or '',
                    }
                    for m in media
                ],
                'prompt': f'该推文包含 {len(media)} 个媒体，确认后将下载整个帖子：',
                'multi': True,
                'min': 1,
                'max': len(media),
                'allow_text': False,
                'text_hint': '',
            },
        })
        resp = fetch_input(notify_ctx)
        if not isinstance(resp, dict):
            log('未收到确认，已取消下载', level='warn')
            sys.exit(0)
        action = resp.get('action', 'download')
        if action == 'cancel':
            log('用户取消下载')
            sys.exit(0)
        items = resp.get('items')
        if isinstance(items, list) and items:
            indices = [int(i) for i in items if str(i).isdigit()]
            selected = [media[i] for i in indices if 0 <= i < len(media)] or media
        else:
            selected = media  # 默认下载整个帖子的全部媒体
        log('已确认下载 ' + '，'.join(x['label'] for x in selected))
    else:
        selected = media
        # 多个资源 -> 二次触发用户选择（仅非全自动模式才交互，否则自动全选）
        if len(media) > 1 and not auto_mode:
            options = [{'value': str(i), 'label': x['label']} for i, x in enumerate(media)]
            progress(20, '等待用户选择要下载的媒体…')
            emit({
                'type': 'await_input',
                'input': {
                    'prompt': f'该推文包含 {len(media)} 个媒体，请选择要下载的项（可多选）：',
                    'options': options,
                    'multi': True,
                    'min': 1,
                    'max': len(media),
                    'allow_text': False,
                    'text_hint': '',
                },
            })
            resp = fetch_input(notify_ctx)
            if resp is None:
                log('未收到选择，默认下载全部', level='warn')
                indices = list(range(len(media)))
            else:
                try:
                    vals = resp if isinstance(resp, list) else [resp]
                    indices = [int(v) for v in vals if str(v).isdigit()]
                except Exception:
                    indices = list(range(len(media)))
                if not indices:
                    indices = list(range(len(media)))
            selected = [media[i] for i in indices]
            log('用户选择：' + '，'.join(x['label'] for x in selected))
        elif len(media) > 1:
            selected = media
            log(f'全自动模式：自动下载全部 {len(media)} 个媒体')
        else:
            log('仅 1 个媒体，直接下载')

    progress(40, f'开始下载 {len(selected)} 个媒体')

    # 下载阶段：登录态无需 guest_token（媒体请求用 Cookie 鉴权即可），仅游客态补取
    guest_token = (get_guest_token(make_opener(proxy_cfg), '')
                   if (tweet_id != 'x' and not cookie_header) else None)

    downloaded = []
    image_files = []  # 图片路径收集，循环后再聚合为一本图集
    total = len(selected)
    for idx, item in enumerate(selected, start=1):
        # 闭包：每次迭代重新绑定 idx，避免上一轮的索引泄漏到本轮回调
        def file_progress(done_in_file, total_in_file=1.0):
            try:
                base = 40
                # (idx-1) 为此前已完成文件的份额，done/total 是当前文件内的进度（0..1）
                frac = (idx - 1) + (done_in_file / max(total_in_file, 1))
                pct = base + int(50 * frac / max(total, 1))
                progress(pct, f'下载中 {idx}/{total}')
            except Exception:
                pass

        # 进入每个文件下载前推送一次「开始」事件，让 40~50 之间也有过渡
        file_progress(0, 1)

        try:
            if item['type'] == 'image':
                if simulate:
                    path = write_sim_placeholder(working_dir, idx, 'image')
                    file_progress(1, 1)
                else:
                    path = download_image(item['url'], cookie_header, working_dir, idx,
                                          proxy_cfg, guest_token, progress_cb=file_progress)
                image_files.append((path, item.get('label')))
                log(f'已下载图片: {os.path.basename(path)}')
            elif item['type'] == 'document':
                if simulate:
                    path = write_sim_placeholder(working_dir, idx, 'document')
                    file_progress(1, 1)
                else:
                    path = download_document(item, cookie_header, working_dir, idx,
                                             proxy_cfg, guest_token, progress_cb=file_progress)
                # 文档：仅进帖子（post），作为可下载附件
                downloaded.append({'path': path, 'type': 'document',
                                   'target_modes': ['post'], 'group': group,
                                   'content': post_content, 'post_title': post_title,
                                   'source_url': url,
                                   'author_name': (tweet_author or {}).get('name') if tweet_author else None,
                                   'author_url': (tweet_author or {}).get('url') if tweet_author else None,
                                   'caption': item.get('label') or '文档'})
                log(f'已下载文档: {os.path.basename(path)}（仅帖子）')
            else:
                if simulate:
                    path = write_sim_placeholder(working_dir, idx, 'video')
                    file_progress(1, 1)
                else:
                    path = download_video(item['url'], cookie_header, working_dir, tweet_id,
                                          proxy_cfg, guest_token, progress_cb=file_progress)
                # 视频：始终进 video + post 实体（数据完整，便于取消隐藏即时恢复），
                # 是否对普通用户可见由 hidden 标志控制（默认隐藏，不进视频库列表）
                modes = ['video', 'post']
                downloaded.append({'path': path, 'type': 'video',
                                   'target_modes': modes, 'group': group,
                                   'library_id': library_id,
                                   'hidden': hidden,
                                   'content': post_content, 'post_title': post_title,
                                   'source_url': url,
                                   'author_name': (tweet_author or {}).get('name') if tweet_author else None,
                                   'author_url': (tweet_author or {}).get('url') if tweet_author else None,
                                   'caption': item.get('label')})
                log(f'已下载视频: {os.path.basename(path)}'
                    + ('（已隐藏，仅帖子可见）' if hidden else '（已进视频库）'))
            # 文件级收尾：把进度推到该文件的末尾（done=1），便于多文件场景进度条连续
            file_progress(1, 1)
        except Exception as e:
            error(f'下载失败（{item["label"]}）: {e}')
            # 失败也要把当前文件的位置推到末尾，避免后续文件进度被卡在前面的份额
            file_progress(1, 1)

    # 图片：同一个 URL 的图片统一放进同一目录，作为「一本图集」入库
    # （避免每张图各成一本图集；帖子也能把整组图片聚在一起展示）
    if image_files:
        img_dir = os.path.join(working_dir, 'images')
        os.makedirs(img_dir, exist_ok=True)
        kept = []
        for i, (p, label) in enumerate(image_files, start=1):
            if not p:
                continue
            if not os.path.isfile(p):
                kept.append(p)  # 模拟占位等情况：直接计入，不移动
                continue
            ext = os.path.splitext(p)[1].lower() or '.jpg'
            if ext not in ('.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp'):
                ext = '.jpg'
            dst = os.path.join(img_dir, f'{i:02d}{ext}')
            try:
                if os.path.abspath(p) != os.path.abspath(dst):
                    shutil.move(p, dst)
                kept.append(dst)
            except Exception as e:
                log(f'图片归集失败: {p} -> {e}', level='warn')
                kept.append(p)
        if kept:
            modes = ['gallery', 'post']
            downloaded.append({'path': img_dir, 'type': 'gallery',
                               'target_modes': modes, 'group': group,
                               'library_id': library_id,
                               'hidden': hidden,
                               'content': post_content, 'post_title': post_title,
                               'source_url': url,
                               'author_name': (tweet_author or {}).get('name') if tweet_author else None,
                               'author_url': (tweet_author or {}).get('url') if tweet_author else None,
                               'caption': f'图片（{len(kept)}）'})
            log(f'已聚合 {len(kept)} 张图片为图集'
                + ('（已隐藏，仅帖子可见）' if hidden else '（已进图集库）'))

    if not downloaded:
        error('没有任何文件下载成功')
        sys.exit(1)

    progress(95, '通知入库…')
    notify_input(notify_ctx, downloaded)
    progress(100, f'完成，共下载 {len(downloaded)} 个文件')
    sys.exit(0)


if __name__ == '__main__':
    # 子进程 stdout 被父进程（manager.py）以 encoding='utf-8' 读取，
    # 但 Windows 下 stdout 为管道时 Python 会按 locale（GBK）编码，
    # 导致中文日志在父进程被误判为 UTF-8 而乱码。强制 UTF-8 输出。
    if isinstance(sys.stdout, io.TextIOWrapper):
        try:
            sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
        except Exception:
            pass
    if isinstance(sys.stderr, io.TextIOWrapper):
        try:
            sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)
        except Exception:
            pass
    main()
