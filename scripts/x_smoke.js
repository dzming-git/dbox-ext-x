#!/usr/bin/env node
/* X 面板黄金路径冒烟（真服务 + 无头浏览器）
 * 判定口径见 docs/VERIFICATION_RULES.md（本文源自 X 加固计划阶段 2.2，该计划已移出仓库）。
 * 用法：node scripts/x_smoke.js     前置：本机 dev 服务在跑（5173/8080/8093）。
 *
 * 覆盖：
 *   1. 打开关注流：内容出现（无未捕获错误）
 *   2. 锚点恢复精度：滚动 → 面板自行保存锚点 → 刷新 → 该卡片回到原位置（误差 < 3px）
 *   3. 产出键取回机制：清掉本地重键 → pull() 取不回、pullProduced() 取回、其它键无损
 *   4. 全新浏览器上下文打开搜索页：关键词缓存从 0 取回 N（防「新设备搜索页为空」）
 *
 * 已知留白（有意，不假装覆盖）：
 *   · 用户页（feed:user:<uid>）需先确认用户页 hash 路由格式；
 *   · 「全部重搜」真实后台爬取耗时数分钟且有风控，此处只验证其依赖的取回原语。
 * 锚点断言不要写成「新 profile 打开会精确恢复到服务端锚点」：新 profile 本机无锚点、
 *   面板按主控语义停在顶部；且锚点指向的推文常已从关注流缓存中消失，无法精确落位。
 *   有效断言是下面这条：用产品自己的保存路径产生锚点，再刷新校验。
 */
'use strict';
const L = require('./x_e2e_lib.js');

const results = [];
function record(name, ok, detail) {
  results.push({ name, ok, detail });
  console.log(`${ok ? '  ok  ' : ' FAIL '} ${name}${detail ? '  — ' + detail : ''}`);
}
function skip(name, why) { results.push({ name, ok: true, detail: 'SKIP: ' + why }); console.log(`  skip  ${name}  — ${why}`); }

const anchorOf = (f) => f.evaluate(() => {
  try { return (XFEED && XFEED.keys) ? XSTATE.get(XFEED.keys.anchor) : null; } catch (_) { return null; }
});

async function case1(page) {
  const tr = L.trackRequests(page);
  const t0 = Date.now();
  const f = await L.openPanel(page, '#/home');
  await L.waitStable(page);
  // 只数浏览列表里的卡（#br-list）：搜索流也用 .bm-card，全局计数会被它满足，
  // 于是「关注流没加载出来」也能算通过——本用例失去了意义。同时确认浏览视图是活动视图。
  const SEL = '#br-list .bm-card';
  const active = await f.evaluate(() => {
    const v = document.getElementById('view-browse');
    return !!(v && v.clientHeight > 0);
  }).catch(() => false);
  const t = await L.waitCards(f, { min: 1, timeout: 9000, sel: SEL });
  if (t === null) {
    record('1 打开关注流：内容出现', false,
      `9s 内浏览列表无卡片（浏览视图${active ? '已' : '未'}激活）`);
    console.log(tr.dump());
    return f;
  }
  // 首卡出现 ≠ 列表加载完（逐天分块）。必须等它长稳，后面两条用例的判据才成立。
  const st = await L.waitSettled(f, { sel: SEL });
  record('1 打开关注流：内容出现', true,
    `首卡 ${Date.now() - t0}ms（含 boot）｜长稳后 ${st.n} 张卡（#br-list，+${st.ms}ms${st.timedOut ? '，未长稳' : ''}）`);
  return f;
}

async function case2(page, f) {
  // 1) 用产品自己的路径产生锚点：滚动容器 → 等防抖保存
  // 不要靠「比较高度」去找滚动容器：this 面板是内滚动/虚拟列表，本轮先后在
  // #view-browse 与 document 上各判断错一次（334 张卡片却报「内容不足以滚动」）。
  // 改用**经验法**：真滚一下，看哪个元素动了 —— 结构再变也不会失效。
  // 真实滚轮必须在主上下文（移动视口）里派发：desktop 布局下滚轮作用不到面板，
  // 只有移动视口下 #view-browse 才会动（默认 390×844 = 面板真实形态）。见文件末尾调用处。
  const before = await anchorOf(f);
  const probe = () => f.evaluate(() => {
    const se = document.scrollingElement || document.documentElement;
    const hits = [];
    if (se && se.scrollTop > 0) hits.push({ sel: 'document', y: se.scrollTop });
    for (const el of document.querySelectorAll('*')) {
      if (el.scrollTop > 0) hits.push({ sel: el.id ? '#' + el.id : (el.className || el.tagName), y: el.scrollTop });
    }
    hits.sort((a, b) => b.y - a.y);
    return hits[0] || { sel: null, y: 0 };
  });
  let pos = { sel: null, y: 0 };
  for (const [mx, my] of [[195, 500], [195, 400], [195, 650], [320, 500]]) {
    await page.mouse.move(mx, my);
    for (let i = 0; i < 10 && !pos.sel; i++) { await page.mouse.wheel(0, 300); await L.sleep(110); pos = await probe(); }
    if (pos.sel) break;
  }
  if (!pos.sel) { skip('2 锚点恢复精度', '滚轮无效：找不到可滚动区域'); return; }
  // 滚到锚点**真的被产品记下来**为止，而不是「滚够 1500px 就假定它记了」。
  // 为什么必须这样等：打开视图后有一段「加载期」，期间框架会冻结自动锚点采集
  // （骨架里的位置无意义），此时滚动不会更新锚点。用固定 sleep 会在慢启动
  // （首卡 7s+ 那种）时撞上冻结窗口，于是用例随机 skip——「有网却不出结果」。
  let saved = null;
  const t0 = Date.now();
  await page.mouse.move(195, 500);
  while (Date.now() - t0 < 15000) {
    await page.mouse.wheel(0, 300);
    await L.sleep(140);
    pos = await probe();
    const a = await anchorOf(f);
    if (a && a.id && (!before || before.id !== a.id)) { saved = a; break; }
  }
  if (!saved) {
    skip('2 锚点恢复精度', `滚动后锚点始终未更新（滚动容器 ${pos.sel}，已滚 ${pos.y}px，等待 15s）`);
    return;
  }
  await L.sleep(600);   // 让防抖保存落定
  const want = Number(saved.offset);

  // 2) 刷新，校验该卡片回到原位置
  await page.reload({ waitUntil: 'domcontentloaded' });
  const f2 = await L.waitPanelFrame(page);
  let okRender = true;
  try { await L.waitFor(() => f2.evaluate((id) => !!document.querySelector(`[data-tid="${id}"]`), saved.id), { timeout: 15000, interval: 60, label: '锚点卡片渲染' }); }
  catch (_) { okRender = false; }
  if (!okRender) { record('2 锚点恢复精度', false, `刷新后锚点卡片 ${saved.id} 15s 内未渲染`); return; }
  const top = await f2.evaluate((id) => document.querySelector(`[data-tid="${id}"]`).getBoundingClientRect().top, saved.id);
  const diff = Math.abs(top - want);
  record('2 锚点恢复精度', diff < 3, `卡片 top=${top.toFixed(1)} vs 保存 offset=${want.toFixed(1)}（差 ${diff.toFixed(2)}px）`);
}

async function case3(f) {
  const r = await f.evaluate(async () => {
    const key = 'feed:tweets:items';
    if (XSTATE._cache[key] === undefined) return { skip: '本地无 ' + key };
    const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
    // 先 flush 面板可能挂着的「待推送改动」：pullKeys 的合并写入会**跳过**仍处于待推送
    // 状态的键（避免覆盖用户刚做的操作），而面板随时会自己回写这个键（内容清洗/合并）。
    // 不 flush 就删再取，会偶发「取不回来」——那是测法问题，不是取回机制坏了。
    try { if (XSTATE.push) await XSTATE.push(); } catch (_) {}
    const size = () => (XSTATE._cache['bm:list'] ? JSON.stringify(XSTATE._cache['bm:list'].value).length : -1);
    const other = size();
    delete XSTATE._cache[key];
    await XSTATE.pull();
    const afterPull = XSTATE._cache[key] !== undefined;
    let afterProduced = false, tries = 0;
    for (; tries < 5 && !afterProduced; tries++) {
      await XSTATE.pullProduced([key]);
      afterProduced = XSTATE._cache[key] !== undefined;
      if (!afterProduced) await sleep(300);
    }
    return { afterPull, afterProduced, tries, intact: other === size(), other, other2: size() };
  });
  if (r.skip) { skip('3 产出键取回机制', r.skip); return; }
  record('3 产出键取回：pull 取不回、pullProduced 取回、其它键无损',
    r.afterPull === false && r.afterProduced === true && r.intact === true,
    `pull 后=${r.afterPull}（应 false，重键不在对账里）｜pullProduced 后=${r.afterProduced}`
    + `（第 ${r.tries + 1} 次）｜其它键 ${r.other}→${r.other2}`);
}

// 5) 布局稳定器：用户在滚的期间，视口上方某行变高，可见内容不许被顶走。
// 由来（2026-09-30）：虚拟列表的行高是「实测 _hReal，缺失则按估值 340」，窗口上下缘换卡
// （占位↔真实卡）时行高会跳变；而面板自己的持续补偿 armScrollAnchorWatch 在用户一滚动就被
// disarm（不抢手势），于是这段窗口没人补 —— 表现为「快滑时 / 停下后资源继续落位时，内容被顶走几条」。
// 本用例必须在**用户滚动态**下做：若只用程序化 scrollTop，面板会停在 brAwaitingRestore 持续重新
// 锚定，位移会被它盖住 —— 这正是最初漏判的原因。
async function case5(page, f) {
  await page.mouse.move(195, 500);
  for (let i = 0; i < 6; i++) { await page.mouse.wheel(0, 300); await L.sleep(90); }
  const r = await f.evaluate(async () => {
    const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
    const cont = document.getElementById('view-browse');
    const list = document.getElementById('br-list');
    if (!cont || !list) return { skip: '浏览视图不可用' };
    // 必须确认浏览视图是「当前活动视图」：非活动视图 display:none，所有 rect 都是 0，
    // 会误判成「视口内没有真卡」而 skip（本用例就踩过）。不是就自己切过去。
    if (!cont.clientHeight) {
      try { if (typeof switchView === 'function') switchView('browse'); } catch (_) {}
      await sleep(1200);
      if (!cont.clientHeight) return { skip: '浏览视图不是活动视图（切不过去）' };
    }
    const probe = () => {
      const cr = cont.getBoundingClientRect();
      const cards = [...list.querySelectorAll('.bm-card')];
      const top = cards.find((c) => c.getBoundingClientRect().bottom > cr.top + 4);
      return { tid: top ? String(top.dataset.tid) : null,
               vTop: top ? Math.round(top.getBoundingClientRect().top - cr.top) : null,
               scrollTop: Math.round(cont.scrollTop) };
    };
    // 自己找可用位置：要有「视口内有真卡」+「视口上方 900px 内有已挂载真卡」两个条件。
    // 不能写死 scrollTop——关注流按天分块，行数与已展开天数会随会话变化（写死会 skip，网就废了）。
    let a = null, above = [];
    for (const st of [1600, 2200, 2800, 3400, 1200, 800, 4000, 600]) {
      cont.scrollTop = st;
      await sleep(700);
      a = probe();
      if (!a.tid) continue;
      const cr = cont.getBoundingClientRect();
      above = [...list.querySelectorAll('.bm-card')].filter((c) => {
        const rr = c.getBoundingClientRect();
        return rr.bottom <= cr.top && rr.top >= cr.top - 900;
      });
      if (above.length) break;
    }
    if (!a || !a.tid) return { skip: '找不到「视口内有真卡」的位置（关注流内容不足？）' };
    if (!above.length) return { skip: '找不到「视口上方有已挂载真卡」的位置' };
    // 量之前先等「位置自己静下来」：逐天分块还会继续并进来，带着布局一起变。
    let lastY = -1, still = 0;
    for (let i = 0; i < 40 && still < 6; i++) {
      await sleep(100);
      const y = Math.round(cont.scrollTop);
      still = (y === lastY) ? still + 1 : 0;
      lastY = y;
    }
    if (still < 6) return { skip: '列表位置一直不稳（并发加载中）' };
    a = probe();
    if (!a.tid) return { skip: '静止后视口内又没有卡片了' };
    const box = document.createElement('div');
    box.style.height = '420px';
    above[above.length - 1].appendChild(box);   // 只改布局高度，不动面板任何状态
    await sleep(700);
    const b = probe();
    return { a, b, delta: b.scrollTop - a.scrollTop,
             same: a.tid === b.tid, dvTop: a.tid === b.tid ? b.vTop - a.vTop : null };
  });
  if (r.skip) { skip('5 布局稳定器（视口上方行变高，可见卡不动）', r.skip); return; }
  const stable = r.same ? Math.abs(r.dvTop) <= 2 : true;   // 基准卡被回收时只能看补偿量
  // 判据只看「补偿有没有发生」：撑高 420px 后 scrollTop 必须被推下去。
  // 不断言恰好 420：并发加载（逐天分块还在并进来）会叠加自己的位移，那部分是正常行为，
  // 精确值打在记录里供人核对。修复前 scrollTop 完全不动（=0），这才是要拦的回归。
  const compensated = r.delta >= 380;
  record('5 布局稳定器：视口上方行变高 420px，可见卡不动且 scrollTop 被补偿',
    stable && compensated,
    `ΔvTop=${r.dvTop === null ? '换卡' : r.dvTop}px（应≈0）｜ΔscrollTop=${r.delta}px（应≈+420）`
    + `｜scrollTop ${r.a.scrollTop}→${r.b.scrollTop}`);
}

const kwState = (f) => f.evaluate(() => {
  const list = (typeof srchKwList !== 'undefined' && srchKwList) ? srchKwList : [];
  const keys = list.map((e) => 'search:kw:' + e.key);
  return { n: keys.length, have: keys.filter((k) => XSTATE._cache[k] !== undefined).length };
});

async function case4(tok) {
  const { browser, page } = await L.openBrowser(tok);   // 新 context：除 token 外 localStorage 全空
  try {
    const f = await L.openPanel(page, '#/search');
    await L.waitStable(page);
    try { await L.waitFor(async () => { const s = await kwState(f); return s.n > 0 && s.have >= s.n; }, { timeout: 12000, interval: 300, label: '关键词缓存取回' }); } catch (_) {}
    const s = await kwState(f);
    record('4 新上下文搜索页：关键词缓存从 0 取回', s.n > 0 && s.have >= s.n,
      s.n === 0 ? '关键词清单为空（该账号没设关键词？）' : `${s.have}/${s.n} 个 search:kw:* 已取回`);

    // 产出键取回机制的**确定性**版本：新 profile 里本机通常没有任何重键，
    // 所以这里用刚取回的真实 search:kw 键做循环（不依赖"本地恰好有某个键"）。
    const cyc = await f.evaluate(async () => {
      const k = Object.keys(XSTATE._cache).find((x) => x.indexOf('search:kw:') === 0);
      if (!k) return { skip: '本地无 search:kw:* 键' };
      const otherKey = Object.keys(XSTATE._cache).find((x) => x !== k && x.indexOf('search:') !== 0 && x.indexOf('feed:') !== 0) || k;
      const size = (kk) => (XSTATE._cache[kk] ? JSON.stringify(XSTATE._cache[kk].value).length : -1);
      const other = size(otherKey);
      delete XSTATE._cache[k];
      await XSTATE.pull();
      const afterPull = XSTATE._cache[k] !== undefined;
      await XSTATE.pullProduced([k]);
      const afterProduced = XSTATE._cache[k] !== undefined;
      return { k, afterPull, afterProduced, intact: other === size(otherKey), other, other2: size(otherKey) };
    });
    // 阶段 3.1 的契约检查（零代码改动版）：kwKey（前端）与后端 _kw_key 必须一致。
    // 做法：清单里每个关键词的缓存值都存着 q（关键词原文）与它对应的 key，用前端 kwKey(q)
    // 重算须等于该 key；且该键下确有内容（后端后台爬取用它自己的哈希写入，两边不一致就会
    // 各写一份，表现为「关键词在清单里、结果却永远为空」）。
    const hz = await f.evaluate(() => {
      const list = (typeof srchKwList !== 'undefined' && srchKwList) ? srchKwList : [];
      const bad = [], empty = [];
      for (const e of list) {
        const v = XSTATE._cache['search:kw:' + e.key];
        const q = v && v.value ? v.value.q : null;
        if (q) { let k = null; try { k = kwKey(q); } catch (_) { } if (k !== e.key) bad.push(`${q}: ${k} ≠ ${e.key}`); }
        const n = (v && v.value) ? ((v.value.top || []).length + (v.value.latest || []).length) : 0;
        if (!n) empty.push(q || e.key);
      }
      return { n: list.length, bad: bad.slice(0, 3), empty: empty.length, emptySample: empty.slice(0, 3) };
    });
    record('3.1 kwKey 前后端哈希一致（真实数据交叉校验）', hz.n > 0 && hz.bad.length === 0,
      hz.n === 0 ? '关键词清单为空' : `校验 ${hz.n} 个；哈希不一致 ${hz.bad.length} 个${hz.bad.length ? '：' + hz.bad.join('; ') : ''}；无结果的关键词 ${hz.empty} 个${hz.empty ? '（' + hz.emptySample.join(',') + '）' : ''}`);

    if (cyc.skip) skip('3 产出键取回（真实 search:kw 键）', cyc.skip);
    else record('3 产出键取回（真实 search:kw 键）',
      cyc.afterPull === false && cyc.afterProduced === true && cyc.intact === true,
      `${cyc.k}：pull 后=${cyc.afterPull}（应 false）｜pullProduced 后=${cyc.afterProduced}｜其它键 ${cyc.other}→${cyc.other2}`);
  } finally { await browser.close(); }
}

(async () => {
  const tok = await L.login();
  const { browser, page } = await L.openBrowser(tok);
  const errs = [];
  page.on('pageerror', (e) => errs.push(String(e).slice(0, 140)));
  try {
    const f = await case1(page);
    // 锚点用例需要**真实滚轮 + 已长稳的列表** → 就用主上下文（见 case2 内的踩坑说明）
    await case2(page, f);
    const f2 = await L.waitPanelFrame(page).catch(() => null);
    if (f2) {
      await L.waitSettled(f2);
      await case3(f2);
      // 3.2 内容来源：渲染出的内容卡不许是空壳（无文本且无媒体）——空壳正是历史脏数据的表征
      // （有 id 没内容 → @unknown 卡）。同时把「有多少张能在全局单缓存 XTWEETS 里查到」
      // 作为观测值打出来：关注流的内容走服务端按天缓存（/timeline?date=），并不等于 XTWEETS，
      // 两者的覆盖比是判断「同一条推文到底存在几份」的现成指标（详见计划 3.2 与附录 B-2 #5）。
      const orph = await f2.evaluate(() => {
        const cards = [...document.querySelectorAll('.bm-card')];
        const tid = (c) => (c.dataset ? (c.dataset.tid || '') : '');
        const live = cards.filter((c) => !c.classList.contains('bm-skeleton') && tid(c));
        let inStore = 0;
        const shells = [];
        live.forEach((c) => {
          let has = false;
          try { has = !!tweetCacheGet(tid(c)); } catch (_) {}
          if (has) inStore++;
          const hasText = (c.textContent || '').trim().length > 20;
          const hasMedia = !!c.querySelector('.media, .bm-media, video');
          if (!hasText && !hasMedia) shells.push(tid(c));
        });
        return { total: cards.length, live: live.length, skeleton: cards.length - live.length,
                 inStore, shells: shells.slice(0, 5), nShell: shells.length };
      });
      record('3.2 内容卡均非空壳（空壳＝脏数据表征）', orph.nShell === 0,
        `卡片 ${orph.total}（内容卡 ${orph.live} / 骨架 ${orph.skeleton}）；空壳卡 ${orph.nShell} 张`
        + (orph.nShell ? '：' + orph.shells.join(',') : '')
        + `｜其中可在全局单缓存 XTWEETS 查到 ${orph.inStore}/${orph.live}`
        + '（关注流内容来自服务端按天缓存，覆盖比低属已知现状，收敛见计划 4.2/4.4）');
    } else record('3 产出键取回机制', false, '面板 frame 未就绪');
    // 稳定器用例放在最后：它要显式打开浏览视图并真实滚动，不依赖前面用例留下的视图/位置。
    const fb = await L.openPanel(page, '#/browse');
    await L.waitSettled(fb);
    await case5(page, fb);
    await case4(tok);
    record('页面无未捕获错误', errs.length === 0, errs.slice(0, 2).join(' | '));
  } catch (e) {
    record('套件执行', false, String((e && e.message) || e).slice(0, 200));
  } finally { await browser.close(); }

  const bad = results.filter((r) => !r.ok).length;
  console.log(`\n${bad ? '✗' : '✓'} x_smoke: ${bad} 失败 / ${results.length - bad} 通过`);
  process.exit(bad ? 1 : 0);
})();
