#!/usr/bin/env node
/* X 面板端到端测试脚手架（供 scripts/x_smoke.js 等复用）
 * 判定口径见 docs/VERIFICATION_RULES.md（本文源自 X 加固计划阶段 0.2，该计划已移出仓库）。
 *
 * 把本 session 反复手写的那套样板固化下来：登录取 token → 注入 localStorage →
 * 无头浏览器 → 定位面板 frame → 抓请求时序 → 等「网络安静」再计时。
 *
 * 两条测量铁律（见计划 0.1，各有一次误判为代价）：
 *   1. 结果要分清「单独串行」与「页面并发」——同一端点在两种情形下能差 10 倍；
 *   2. 改 extensions/ 下文件会触发扩展宿主热重载，重载窗口内请求 502/503 且耗时 2s+，
 *      **必须先 waitStable() 再计时**，否则测到的是重载噪声。
 */
'use strict';

const https = require('https');
const path = require('path');

const BASE = process.env.X_E2E_BASE || 'https://127.0.0.1:5173';
const PANEL_READY = `typeof XSTATE !== 'undefined' && !!XSTATE && typeof XSTATE.pullKeys === 'function'`;

let _pw = null;
function playwright() {
  if (_pw) return _pw;
  // 需要复用本机全局安装的 playwright-core 时，用环境变量 PLAYWRIGHT_CORE_PATH 指定；
  // 代码里不写死任何本机绝对路径（含用户名）
  const cands = ['playwright-core', 'playwright', process.env.PLAYWRIGHT_CORE_PATH,
    process.env.APPDATA ? path.join(process.env.APPDATA, 'npm', 'node_modules',
      '@playwright', 'cli', 'node_modules', 'playwright-core') : ''].filter(Boolean);
  for (const c of cands) {
    try { _pw = require(c); return _pw; } catch (_) { }
  }
  throw new Error('找不到 playwright-core；请 npm i -D playwright-core 或设置 NODE_PATH');
}

function request(opts, body) {
  return new Promise((resolve, reject) => {
    const r = https.request(Object.assign({
      host: '127.0.0.1', port: 5173, rejectUnauthorized: false,
    }, opts), (res) => {
      let s = '';
      res.on('data', (c) => { s += c; });
      res.on('end', () => {
        try { resolve(JSON.parse(s)); } catch (e) { reject(new Error('响应非 JSON: ' + s.slice(0, 120))); }
      });
    });
    r.on('error', reject);
    if (body) r.write(body);
    r.end();
  });
}

// 登录取 token（本机 dev 默认账号；可用 X_E2E_USER/X_E2E_PASS 覆盖）
async function login(user = process.env.X_E2E_USER || 'root', pass = process.env.X_E2E_PASS) {
  if (!pass) throw new Error('请用环境变量 X_E2E_PASS 提供测试账号密码（不要把密码写进代码或文档）');
  const body = JSON.stringify({ username: user, password: pass });
  const d = await request({
    path: '/api/v2/auth/login', method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
  }, body);
  if (!d || !d.data || !d.data.access_token) throw new Error('登录失败: ' + JSON.stringify(d).slice(0, 160));
  return d.data;
}

async function openBrowser(tok, opts = {}) {
  const pw = playwright();
  const browser = await pw.chromium.launch({ channel: opts.channel || 'msedge', headless: opts.headless !== false });
  // opts.mobile=false 时关掉移动端模拟：isMobile/hasTouch 下 Chromium **忽略 mouse.wheel**
  // （触屏视口靠触摸手势滚动），凡需要真实滚轮的用例都必须用桌面上下文。
  const mobile = opts.mobile !== false;
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    viewport: mobile ? { width: 390, height: 844 } : { width: 1280, height: 900 },
    isMobile: mobile, hasTouch: mobile,
  });
  if (tok) {
    await context.addInitScript((a) => {
      localStorage.setItem('token', a.access);
      localStorage.setItem('refresh_token', a.refresh);
      localStorage.setItem('user', a.user);
    }, { access: tok.access_token, refresh: tok.refresh_token, user: JSON.stringify(tok.user) });
  }
  const page = await context.newPage();
  page.on('pageerror', () => {});
  await page.route('**/*', (r) => (r.request().resourceType() === 'font' ? r.abort() : r.continue()));
  return { browser, context, page };
}

// 抓关键请求的「发出/返回 + 耗时」，便于失败时定位
function trackRequests(page, re = /ui-extensions|ui-panel|state\.js|user-state|ext\/x\//) {
  const list = [];
  const T0 = Date.now();
  const key = (u) => u.replace(BASE, '').slice(0, 52);
  const pending = new Map();
  page.on('request', (r) => { const u = key(r.url()); if (re.test(u)) { pending.set(r, T0 ? Date.now() - T0 : 0); list.push({ dir: '→', t: Date.now() - T0, u }); } });
  page.on('response', (r) => { const u = key(r.url()); if (re.test(u)) list.push({ dir: '←', t: Date.now() - T0, u, status: r.status() }); });
  page.on('requestfinished', (r) => { const u = key(r.url()); const t = pending.get(r); if (t !== undefined) { list.push({ dir: '⏱', t, u, ms: Date.now() - T0 - t }); pending.delete(r); } });
  return {
    list,
    dump() { return list.filter((x) => x.dir === '→').map((x) => { const d = list.find((y) => y.u === x.u && y.dir === '⏱' && y.t === x.t); return `  ${String(x.t).padStart(5)}ms → ${x.u}${d ? '  (' + d.ms + 'ms)' : ''}`; }).join('\n'); },
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, { timeout = 8000, interval = 40, label = '条件' } = {}) {
  const t0 = Date.now();
  for (;;) {
    let v = false;
    try { v = await fn(); } catch (_) { v = false; }
    if (v) return Date.now() - t0;
    if (Date.now() - t0 > timeout) throw new Error(`等待超时(${timeout}ms): ${label}`);
    await sleep(interval);
  }
}

// 定位面板 iframe 并等它「就绪」（SDK 已加载）
async function waitPanelFrame(page, { timeout = 10000 } = {}) {
  let found = null;
  await waitFor(async () => {
    for (const fr of page.frames()) {
      try { if (await fr.evaluate(PANEL_READY)) { found = fr; return true; } } catch (_) { }
    }
    return false;
  }, { timeout, label: '面板 frame 就绪' });
  return found;
}

async function openPanel(page, hash = '#/home', opts = {}) {
  await page.goto(BASE + '/ext/x' + hash, { waitUntil: 'domcontentloaded' });
  return waitPanelFrame(page, opts);
}

// 等「网络安静」：最近 quietMs 内没有任何请求完成。用于避开宿主热重载窗口。
async function waitStable(page, { quietMs = 400, max = 8000 } = {}) {
  let last = Date.now();
  const mark = () => { last = Date.now(); };
  page.on('requestfinished', mark); page.on('requestfailed', mark); page.on('request', mark);
  const t0 = Date.now();
  for (;;) {
    if (Date.now() - last >= quietMs) return Date.now() - t0;
    if (Date.now() - t0 > max) return -1;   // 一直不安静：返回 -1，由调用方决定
    await sleep(50);
  }
}

const cards = (frame) => frame.evaluate(() => document.querySelectorAll('.bm-card').length).catch(() => 0);

// 等列表「长稳」：卡片数连续 quietMs 不再变化。
// 为什么需要：关注流首屏是**逐天分块加载**的，第一张卡片出现 ≠ 列表加载完。
// 若一拿到首卡就断言「能否滚动」「本地缓存是否就绪」，会把「刚渲染首日几张」
// 误判成「内容不足以滚动」（本轮就踩了一次，白查了半小时）。
async function waitSettled(frame, { quietMs = 600, max = 12000 } = {}) {
  const t0 = Date.now();
  let last = -1, lastChange = Date.now(), n = 0;
  for (;;) {
    n = await cards(frame);
    if (n !== last) { last = n; lastChange = Date.now(); }
    if (n > 0 && Date.now() - lastChange >= quietMs) return { n, ms: Date.now() - t0, timedOut: false };
    if (Date.now() - t0 > max) return { n, ms: Date.now() - t0, timedOut: true };
    await sleep(60);
  }
}

async function waitCards(frame, { min = 1, timeout = 8000, onTick } = {}) {
  const t0 = Date.now();
  for (;;) {
    const n = await cards(frame);
    if (onTick && Date.now() - t0 > 600) onTick(n);
    if (n >= min) return Date.now() - t0;
    if (Date.now() - t0 > timeout) return null;
    await sleep(40);
  }
}

module.exports = { BASE, request, login, openBrowser, openPanel, waitPanelFrame, waitFor, waitStable, waitCards, waitSettled, trackRequests, cards, sleep, playwright };
