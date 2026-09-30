#!/usr/bin/env node
/* X 插件契约守卫（静态检查，无副作用，秒级）
 * 判定口径见 docs/VERIFICATION_RULES.md（本文源自 X 加固计划阶段 1.4 / 2.1，该计划已移出仓库）。
 * 运行位置：插件仓库根（`cd extensions/x && node scripts/x_guard.js`）。
 *
 * 检查项：
 *   A. ui/panel.html 的内联 X_KEYS 与 x_keys.json 完全一致（--fix 可自动同步）
 *   B. panel.html / backend/server.py 里出现的键字面量都已在注册表登记
 *   C. heavyKeys / producedKeys 由注册表派生（禁止重新硬编码），且派生结果与注册表一致
 *   D. fetchVia==='pullProduced' 的键在 panel.html 里确有取回调用点
 *   E. 注册表里的 localStorage 键字面量都在 panel.html 里存在（防改名漂移）
 *   F. 键哈希契约：前端 kwKey 与后端 _kw_key 对同一批关键词结果完全一致
 *   G. writers 声明与代码写点一致（防「一个键两个写者」两侧漂移）
 *   H. 服务端真产出 ⊆ 注册表 + 重键可定向取回（需本机 dev 服务；不可达则 WARN 跳过）
 *
 * 用法：
 *   node scripts/x_guard.js            只检查（有问题退出码 1）
 *   node scripts/x_guard.js --fix      从 x_keys.json 重新生成 panel.html 的内联块，再检查
 *   node scripts/x_guard.js --offline  跳过 H（不起浏览器/不发请求，纯静态）
 */
'use strict';
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const REG_PATH = path.join(ROOT, 'x_keys.json');
const PANEL_PATH = path.join(ROOT, 'ui/panel.html');
const SERVER_PATH = path.join(ROOT, 'backend/server.py');
const START = '/*X_KEYS_JSON_START*/';
const END = '/*X_KEYS_JSON_END*/';

const fails = [];
const warns = [];
const oks = [];
const fail = (m) => fails.push(m);
const warn = (m) => warns.push(m);
const ok = (m) => oks.push(m);

const reg = JSON.parse(fs.readFileSync(REG_PATH, 'utf8'));
let panel = fs.readFileSync(PANEL_PATH, 'utf8');
const server = fs.readFileSync(SERVER_PATH, 'utf8');

/* ---------- 注册表 → 派生集合 ---------- */
const heavyList = reg.keys.filter((e) => e.heavy).map((e) => e.name + (e.prefix ? '*' : ''));
const producedConcrete = reg.keys
  .filter((e) => e.producer === 'server' && !e.prefix)
  .map((e) => e.name);
const nsFirstSeg = [...new Set(reg.keys.map((e) => e.name.split(':')[0]))];

/* ---------- A. 内联块一致性（含 --fix） ---------- */
function extractInline(src) {
  const i = src.indexOf(START);
  const j = src.indexOf(END);
  if (i < 0 || j < 0) return null;
  return src.slice(i + START.length, j);
}
// 行尾：panel.html 在本仓可能是 CRLF，而注册表生成的是 LF。若逐字节比较，
// 任何编辑器/工具改写行尾都会让这里整天误报（本轮就误报过一次）。比较前统一成 LF，
// 写回时沿用文件原有行尾，避免把文件搞成混行尾。
const EOL = panel.indexOf('\r\n') >= 0 ? '\r\n' : '\n';
const norm = (s) => s.replace(/\r\n/g, '\n');
const wantBodyLF = JSON.stringify(reg, null, 2).split('\n').join('\n  ');
const wantBody = EOL === '\n' ? wantBodyLF : wantBodyLF.split('\n').join('\r\n');
const wantFull = START + wantBody + END;
if (process.argv.includes('--fix')) {
  const cur = extractInline(panel);
  if (cur === null) {
    fail('panel.html 里找不到 ' + START + ' … ' + END + ' 标记，无法自动同步');
  } else if (norm(cur) !== norm(wantBody)) {
    panel = panel.slice(0, panel.indexOf(START)) + wantFull + panel.slice(panel.indexOf(END) + END.length);
    fs.writeFileSync(PANEL_PATH, panel);
    ok('已用 x_keys.json 重写 panel.html 的内联 X_KEYS');
  } else {
    ok('内联 X_KEYS 已是最新，无需改写');
  }
}
{
  const cur = extractInline(panel);
  if (cur === null) {
    fail('panel.html 缺少内联键注册表标记 ' + START);
  } else if (norm(cur) !== norm(wantBody)) {
    fail('panel.html 的内联 X_KEYS 与 x_keys.json 不一致 → 跑 `node scripts/x_guard.js --fix`');
  } else {
    ok(`A 内联 X_KEYS 与 x_keys.json 一致（行尾 ${EOL === '\r\n' ? 'CRLF' : 'LF'}）`);
  }
}

/* ---------- 键字面量匹配 ---------- */
function normPattern(p) {
  return p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
function isRegistered(lit) {
  for (const e of reg.keys) {
    if (e.prefix) {
      if (lit === e.name || lit === e.name + '*' || lit.startsWith(e.name)) return true;
    } else if (lit === e.name) return true;
  }
  return false;
}
const LS_KEYS = new Set(reg.localStorage.map((e) => e.name));
function scanLiterals(src, file) {
  // 候选：以注册表已知命名空间开头的含冒号字符串字面量
  const re = new RegExp(`['"]([a-z][a-zA-Z0-9_]*(?::[a-zA-Z0-9_*-]+)+)['"]`, 'g');
  const bad = new Map();
  let m;
  while ((m = re.exec(src)) !== null) {
    const lit = m[1];
    if (!nsFirstSeg.includes(lit.split(':')[0])) continue;
    if (LS_KEYS.has(lit)) continue;   // localStorage 键单独由 E 检查，不属于跨设备状态键
    if (isRegistered(lit)) continue;
    const line = src.slice(0, m.index).split('\n').length;
    bad.set(lit, `${file}:${line}  ${lit}`);
  }
  return [...bad.values()];
}
{
  const bad = [...scanLiterals(panel, 'panel.html'), ...scanLiterals(server, 'server.py')];
  if (bad.length) {
    fail('B 以下键字面量未在 x_keys.json 登记：\n      ' + bad.join('\n      '));
  } else {
    ok('B 所有键字面量均已登记');
  }
}

/* ---------- C. 派生而非硬编码 ---------- */
{
  if (/setHeavyKeys\s*\(\s*\[/.test(panel)) {
    fail('C panel.html 里 setHeavyKeys 又出现硬编码数组 → 必须用 xHeavyKeys() 派生');
  } else if (!/setHeavyKeys\s*\(\s*xHeavyKeys\(\)/.test(panel)) {
    fail('C panel.html 未调用 setHeavyKeys(xHeavyKeys())');
  } else {
    ok(`C heavyKeys 由注册表派生（当前 ${heavyList.length} 个：${heavyList.join(', ')}）`);
  }
  if (/setProducedKeys\s*\(\s*\[/.test(panel)) {
    fail('C panel.html 里 setProducedKeys 出现硬编码数组 → 必须用 xProducedKeys() 派生');
  } else if (!/setProducedKeys\s*\(\s*xProducedKeys\(\)/.test(panel)) {
    warn('C 未注册 setProducedKeys(xProducedKeys())：pullProduced() 无参调用将取不到默认集合');
  } else {
    ok(`C producedKeys 由注册表派生（当前 ${producedConcrete.length} 个：${producedConcrete.join(', ')})`);
  }
}

/* ---------- D. 产出型键必须有取回调用点 ---------- */
{
  // 代码里常用 KW_CACHE_PFX 这类常量间接拼键名，直接匹配会误报 → 先做「常量→字面量」展开
  const constMap = {};
  {
    const cr = /const\s+([A-Z][A-Z0-9_]*)\s*=\s*'([^']+)'/g;
    let cm;
    while ((cm = cr.exec(panel)) !== null) constMap[cm[1]] = cm[2];
  }
  const expand = (s) => s.replace(/\b[A-Z][A-Z0-9_]*\b/g, (x) => (constMap[x] !== undefined ? constMap[x] : x));
  const windows = [];
  const re = /(pullProduced|pullKeys)\s*\(/g;
  let m;
  while ((m = re.exec(panel)) !== null) windows.push(expand(panel.slice(m.index, m.index + 320)));
  const need = reg.keys.filter((e) => e.fetchVia === 'pullProduced');
  const missing = [];
  for (const e of need) {
    const hit = windows.some((w) => w.includes(e.name));
    if (!hit) missing.push(`${e.name}${e.prefix ? '*' : ''}`);
  }
  if (missing.length) {
    fail('D 以下「服务端产出、需显式取回」的键在 panel.html 找不到取回调用点：\n      '
      + missing.join('\n      ') + '\n      （这类漏取回的表现：提示任务已完成，页面却毫无变化）');
  } else {
    ok(`D ${need.length} 个产出型键都有取回调用点（${need.map((e) => e.name + (e.prefix ? '*' : '')).join(', ')}）`);
  }
}

/* ---------- E. localStorage 键 ---------- */
{
  const missing = reg.localStorage.filter((e) => !panel.includes(e.name)).map((e) => e.name);
  if (missing.length) warn('E 以下 localStorage 键在 panel.html 中找不到字面量：' + missing.join(', '));
  else ok(`E ${reg.localStorage.length} 个 localStorage 键字面量均在场`);
  const neverRead = reg.localStorage.filter((e) => !new RegExp(`(getItem|setItem)\\(\\s*['"]${e.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}['"]`).test(panel)
    && !new RegExp(`['"]${e.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}['"]`).test(panel));
  if (neverRead.length) warn('E 疑似未使用：' + neverRead.map((e) => e.name).join(', '));
}

/* ---------- F. 键哈希契约：kwKey(前端) 与 _kw_key(后端) 必须同源 ---------- */
// 「同概念多实现」的第一号对象：关键词 key 的哈希前后端各写了一遍，只靠注释声明一致。
// 这里把两份实现分别抽出来、喂同一批关键词（含中文/emoji/长串/大小写/空白）跑一遍比对。
// 必须真跑而不是只比对源码：两者的差异是**语义**差异（UTF-16 代码单元 vs Unicode 码位），
// 源码看着一样，只有对非 BMP 字符（emoji）才算出不同的 key——表现是「关键词在清单里、
// 点开却永远没有结果」。
const KW_FIXTURES = [
  'hello', 'Hello World', '  spaced  ', 'a\tb\n c', 'café', 'cafÉ',
  '中文关键词', '日本語テスト', '한국어', 'emoji 😀 测试', '🚀🚀', '#hashtag',
  '@user', 'İstanbul', 'ß', 'Ω≈ç√∫', 'mixed 中 文   spaces', 'UPPER lower 123',
  'x'.repeat(300), '',
];
function extractJsFunction(src, name) {
  const i = src.indexOf('function ' + name + '(');
  if (i < 0) return null;
  const b = src.indexOf('{', i);
  if (b < 0) return null;
  let depth = 0;
  for (let k = b; k < src.length; k++) {
    if (src[k] === '{') depth++;
    else if (src[k] === '}') { depth--; if (!depth) return src.slice(i, k + 1); }
  }
  return null;
}
function extractPyFunction(src, name, dedentToZero) {
  const lines = norm(src).split('\n');
  const i = lines.findIndex((l) => l.trim().startsWith('def ' + name + '('));
  if (i < 0) return null;
  const indent = lines[i].length - lines[i].trimStart().length;
  let j = i + 1;
  for (; j < lines.length; j++) {
    const l = lines[j];
    if (!l.trim()) continue;
    if ((l.length - l.trimStart().length) <= indent) break;
  }
  const body = lines.slice(i, j);
  return (dedentToZero === false ? body : body.map((l) => l.slice(indent))).join('\n');
}
{
  const jsSrc = extractJsFunction(panel, 'kwKey');
  // 后端实现可能拆成若干小函数（如 _kw_units 处理 UTF-16 代码单元），按依赖顺序全抽出来
  const pyFns = ['_kw_units', '_kw_key']
    .map((n) => extractPyFunction(server, n)).filter(Boolean);
  const b36 = /^[ \t]*_B36\s*=\s*(.+)$/m.exec(norm(server));
  if (!jsSrc || pyFns.length !== 2 || !b36) {
    fail('F 无法从 panel.html 抽出 kwKey 或从 server.py 抽出 _kw_key/_B36（结构变了？守卫需同步）');
  } else {
    let jsHashes = null, pyHashes = null, why = '';
    try {
      const fn = new Function(jsSrc + '\n;return kwKey;')();
      jsHashes = KW_FIXTURES.map((q) => fn(q));
    } catch (e) { why = '前端 kwKey 求值失败: ' + e.message; }
    const py = process.env.X_PYTHON || process.env.PYTHON || 'python';
    // 输入输出一律走 UTF-8 文件，不走 stdin/stdout：Windows 上 Python 的
    // sys.stdin/stdout 用系统代码页（cp936）解码，中文/重音字符会被解成另一个字符串，
    // 于是「比对」本身就把两边搞成不同输入——本轮就因此误报过一次「哈希不一致」。
    const tmp = require('os').tmpdir();
    const tmpPy = path.join(tmp, 'x_guard_kwkey_' + process.pid + '.py');
    const tmpIn = path.join(tmp, 'x_guard_kwkey_in_' + process.pid + '.json');
    const tmpOut = path.join(tmp, 'x_guard_kwkey_out_' + process.pid + '.json');
    try {
      fs.writeFileSync(tmpPy, [
        '# -*- coding: utf-8 -*-',
        'import json, sys',
        '_B36 = ' + b36[1].trim(),
        pyFns.join('\n\n'),
        "with open(sys.argv[1], encoding='utf-8') as f:",
        '    qs = json.load(f)',
        "with open(sys.argv[2], 'w', encoding='utf-8') as f:",
        '    json.dump([_kw_key(q) for q in qs], f)',
      ].join('\n'), 'utf8');
      fs.writeFileSync(tmpIn, JSON.stringify(KW_FIXTURES), 'utf8');
      require('child_process').execFileSync(py, [tmpPy, tmpIn, tmpOut], { timeout: 20000 });
      pyHashes = JSON.parse(fs.readFileSync(tmpOut, 'utf8'));
    } catch (e) {
      why = why || ('后端 _kw_key 求值失败（' + py + ' 不可用？）: ' + String(e.message).split('\n')[0]);
    } finally {
      for (const f of [tmpPy, tmpIn, tmpOut]) { try { fs.unlinkSync(f); } catch (_) {} }
    }
    if (why) {
      warn('F 键哈希契约未能验证：' + why);
    } else {
      const bad = [];
      for (let i = 0; i < KW_FIXTURES.length; i++) {
        if (jsHashes[i] !== pyHashes[i]) {
          bad.push(JSON.stringify(KW_FIXTURES[i]).slice(0, 40) + `：前端 ${jsHashes[i]} ≠ 后端 ${pyHashes[i]}`);
        }
      }
      if (bad.length) {
        fail('F kwKey 与 _kw_key 对同一关键词算出了不同的 key（会导致「词在清单里、结果永远为空」）：\n      '
          + bad.slice(0, 6).join('\n      ')
          + (bad.length > 6 ? `\n      …共 ${bad.length} 例` : '')
          + '\n      （常见根因：前端按 UTF-16 代码单元遍历、后端按 Unicode 码位遍历，emoji 等非 BMP 字符处必然分叉）');
      } else {
        ok(`F kwKey 与 _kw_key 哈希一致（${KW_FIXTURES.length} 个代表性关键词，含中文/emoji/长串/空白）`);
      }
    }
  }
}

/* ---------- G. writers 声明与代码写点一致 ---------- */
// 「一个键两个写者」是最高风险的结构：声明里写了谁在写，代码里就必须真有那个写点，
// 否则迟早出现「一边追加、一边整体覆盖」的互相冲掉（bm:list / like:list 就是这类）。
// 写点判定：窗口锚定在「键的写法」上再找写调用，避免全局泛匹配。
// 键的写法有三种（都要认，否则会把「真在写」误判成没写）：
//   ① 字面量 'key'；② 常量（const KW_LIST_KEY = 'search:kwlist'）间接写；
//   ③ 框架生成（feed:<seg>:items/anchor/cursor，代码里本就没有字面量，面板只建 XSTATE.feed('<seg>') 流）。
const PANEL_CONSTS = {};
{
  const add = (name, val) => { (PANEL_CONSTS[name] = PANEL_CONSTS[name] || []).push(val); };
  // 键名常量：const KW_LIST_KEY = 'search:kwlist' 以及
  // 由参数拼出的键工厂：const pstKey = (id) => 'search:preset:' + id
  const pats = [
    /const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'([^']+)'/g,
    /const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:\([^)]*\)|[A-Za-z_][A-Za-z0-9_]*)\s*=>\s*[^;\n]*?'([^']+)'/g,
  ];
  for (const re of pats) {
    let m;
    while ((m = re.exec(panel)) !== null) add(m[1], m[2]);
  }
}
function panelWritesKey(name, calls) {
  const tokens = [`'${name}'`];
  for (const [k, vals] of Object.entries(PANEL_CONSTS)) if (vals.includes(name)) tokens.push(k);
  for (const t of tokens) {
    let idx = panel.indexOf(t);
    while (idx >= 0) {
      const win = panel.slice(Math.max(0, idx - 200), idx + 400);
      if (calls.some((c) => win.includes(c))) return true;
      idx = panel.indexOf(t, idx + 1);
    }
  }
  const fm = /^feed:([^:]+):/.exec(name);
  if (fm && panel.includes(`XSTATE.feed('${fm[1]}'`)) return true;
  return false;
}
function hasWriteSite(src, key, calls) {
  let idx = src.indexOf(`'${key}'`);
  while (idx >= 0) {
    const win = src.slice(Math.max(0, idx - 240), idx + 320);
    if (calls.some((c) => win.includes(c))) return true;
    idx = src.indexOf(`'${key}'`, idx + 1);
  }
  return false;
}
{
  const bad = [];
  for (const e of reg.keys) {
    const w = e.writers || [];
    if (!w.length || e.legacy) continue;   // legacy 键是残留（如 search:last 只被一次性迁移读过），不强求写点
    if (w.includes('server')
      && !hasWriteSite(server, e.name, ['state.put(', '_bg_state_put(', '_merge_membership(', '_merge_into_tweet_store('])) {
      bad.push(`${e.name} 声明 writers 含 server，但 server.py 里找不到写点`);
    }
    if (w.includes('client')
      && !panelWritesKey(e.name, ['XSTATE.set(', 'xSet(', 'state.put('])) {
      bad.push(`${e.name} 声明 writers 含 client，但 panel.html 里找不到写点`);
    }
    if (w.includes('client') && w.includes('server')
      && !(e.strategy === 'union_by_id' || /合并|追加/.test(e.desc || ''))) {
      bad.push(`${e.name} 前后端同时写，但未在 desc 里写明合并语义（strategy=${e.strategy}）`);
    }
  }
  if (bad.length) fail('G writers 声明与代码写点不一致：\n      ' + bad.join('\n      '));
  else ok(`G ${reg.keys.filter((e) => (e.writers || []).length).length} 个键的 writers 与代码写点一致`);
}

/* ---------- H. 服务端真产出 ⊆ 注册表 + 重键可取回（需本机服务） ---------- */
// 这条直接抓「代码里用了一个没登记的键」和「登记为重键却取不回」——
// 前者人工清点必漏（feed:stars:items 就是这么被抓到的），后者是「提示完成、页面无变化」的成因。
(async () => {
  if (process.argv.includes('--offline')) {
    warn('H 已按 --offline 跳过（服务端产出键与重键取回验证）');
  } else {
    let token = null, request = null, base = '';
    try {
      const L = require('./x_e2e_lib.js');
      request = L.request; base = L.BASE;
      token = (await L.login()).access_token;
    } catch (e) {
      warn('H 服务端未就绪，跳过：' + String((e && e.message) || e).split('\n')[0]);
    }
    if (token && request) {
      const auth = { Authorization: 'Bearer ' + token };
      try {
        const all = await request({ path: '/api/user-state/x', method: 'GET', headers: auth, timeout: 20000 });
        if (!all || all.success !== true || !all.data) {
          // 不能把「没拿到数据」当成「没有未登记的键」——那会让这条检查永远假绿。
          warn('H 未能读到服务端状态（' + JSON.stringify(all || {}).slice(0, 90) + '），跳过');
        } else {
          const produced = Object.keys(all.data);
          const unreg = produced.filter((k) => !isRegistered(k));
          if (unreg.length) {
            fail('H 服务端产出了未在 x_keys.json 登记的键：\n      '
              + unreg.slice(0, 8).join('\n      ')
              + '\n      （未登记＝守卫看不见它，键漂移从这里开始）');
          } else {
            ok(`H 服务端 ${produced.length} 个键全部已登记`);
          }
          // 重键定向取回：前缀键（如 feed:user:*）要用「库里真实存在的具体键」去查，
          // 拿前缀本身查必然查不到——那是假警报。
          const heavy = reg.keys.filter((e) => e.heavy && !e.legacy);
          const concrete = [];
          for (const e of heavy) {
            if (e.prefix) produced.filter((k) => k.startsWith(e.name)).slice(0, 3).forEach((k) => concrete.push(k));
            else concrete.push(e.name);
          }
          const missing = [];
          for (const k of concrete) {
            const r = await request({
              path: `/api/user-state/x?keys=${encodeURIComponent(k)}`,
              method: 'GET', headers: auth, timeout: 20000,
            });
            const v = ((r && r.data) || {})[k];
            if (!(v && Array.isArray(v.value) ? v.value.length : (v && v.value))) missing.push(k);
          }
          const noData = heavy.filter((e) => e.prefix && !produced.some((k) => k.startsWith(e.name)))
            .map((e) => e.name + '*');
          // 重键取不回是可修的问题（代码里漏了取回点），但「服务端本次真没产出」不算法失，
          // 故记 WARN 而不是 FAIL，避免空账号/新库干扰他人。
          if (missing.length) warn(`H 以下重键当前取不到值：${missing.join(', ')}`);
          else ok(`H ${concrete.length} 个重键均能定向取回`);
          if (noData.length) warn(`H 以下前缀型重键库里暂无具体键（无法验证取回）：${noData.join(', ')}`);
        }
      } catch (e) {
        warn('H 查询服务端状态失败，跳过：' + String((e && e.message) || e).split('\n')[0]);
      }
    }
  }

  /* ---------- 汇总 ---------- */
  for (const l of oks) console.log('  ok   ' + l);
  for (const l of warns) console.log(' WARN  ' + l);
  for (const l of fails) console.log(' FAIL  ' + l);
  console.log(`\n${fails.length ? '✗' : '✓'} x_guard: ${fails.length} 失败 / ${warns.length} 警告 / ${oks.length} 通过`);
  process.exit(fails.length ? 1 : 0);
})();
