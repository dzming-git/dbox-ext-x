# -*- coding: utf-8 -*-
"""X 插件前后端契约测试（静态，不依赖运行中的服务、不联网）。

背景：X 面板的持久化键、关键词哈希、取回时序这类契约过去只靠注释声明「与另一边一致」，
一旦分叉就以「提示任务已完成、页面却毫无变化」这类表象分几次暴露（2026-09-24 复盘）。
完整的检查在 `scripts/x_guard.js`（含把前端 kwKey 与后端 _kw_key 真跑比对）；本文件是在
纯 Python 环境（CI 里未必装 node）下也能跑的那部分：

  1. 注册表本身合法：每个键都有 name/strategy/producer/fetchVia 且取值在允许集合内；
  2. 注册表里声明 heavy 的键，与 panel.html 内联块里 `heavy: true` 的集合一致
     （两份必须逐字一致，否则守卫的 A 项会失败，这里先给一个更早、更易读的报错）；
  3. 声明 `producer=server` 且重的键，必须有明确的取回方式（fetchVia 不是 none）——
     这正是「后台产出但本地从不取回」的结构性缺口；
  4. 关键函数（kwKey / _kw_key）都还在各自的文件里（防止被整体删掉后守卫静默通过）。

运行：python -m unittest tests.test_x_contracts
"""
import json
import os
import unittest

# 本文件位于 <插件仓库>/tests/，故 ROOT 即插件仓库根：x_keys.json / ui/ / backend/ 都在根下
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
REG_PATH = os.path.join(ROOT, 'x_keys.json')
PANEL_PATH = os.path.join(ROOT, 'ui', 'panel.html')
SERVER_PATH = os.path.join(ROOT, 'backend', 'server.py')
START = '/*X_KEYS_JSON_START*/'
END = '/*X_KEYS_JSON_END*/'

STRATEGIES = {'lww', 'max', 'union_by_id'}
PRODUCERS = {'server', 'client', 'both', 'none'}
FETCH_VIA = {'pullProduced', 'plugin-api', 'pull', 'none'}


def _read(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


class XKeyRegistryTest(unittest.TestCase):
    """键注册表与面板内联块的一致性。"""

    @classmethod
    def setUpClass(cls):
        cls.reg = json.loads(_read(REG_PATH))
        cls.panel = _read(PANEL_PATH)

    def test_registry_shape(self):
        """每个键的字段齐全且取值合法。"""
        self.assertTrue(self.reg.get('keys'), '注册表里没有任何键')
        for e in self.reg['keys']:
            name = e.get('name') or '<未命名>'
            self.assertTrue(name, '存在没有 name 的条目')
            self.assertIn(e.get('strategy'), STRATEGIES, '%s 的 strategy 非法: %r' % (name, e.get('strategy')))
            self.assertIn(e.get('producer'), PRODUCERS, '%s 的 producer 非法: %r' % (name, e.get('producer')))
            self.assertIn(e.get('fetchVia'), FETCH_VIA, '%s 的 fetchVia 非法: %r' % (name, e.get('fetchVia')))
            self.assertIsInstance(e.get('heavy'), bool, '%s 缺 heavy 布尔字段' % name)
            self.assertTrue(e.get('desc'), '%s 缺 desc（新键必须先登记并说明用途）' % name)

    def test_produced_keys_declare_how_to_fetch(self):
        """服务端产出且为重的键，必须写明本地怎么取回（否则就是「任务完成、页面无变化」）。"""
        bad = []
        for e in self.reg['keys']:
            if e.get('heavy') and e.get('producer') == 'server' and not e.get('legacy'):
                if e.get('fetchVia') == 'none':
                    bad.append(e['name'])
        self.assertEqual([], bad, '以下重键由服务端产出却没有取回方式: %s' % ', '.join(bad))

    def test_inline_block_matches_registry(self):
        """panel.html 内联注册表块与 x_keys.json 逐字一致（行尾归一后比较）。"""
        i, j = self.panel.find(START), self.panel.find(END)
        self.assertNotEqual(-1, i, 'panel.html 缺少内联注册表起始标记，跑 node scripts/x_guard.js --fix')
        self.assertNotEqual(-1, j, 'panel.html 缺少内联注册表结束标记')
        inline = self.panel[i + len(START):j].replace('\r\n', '\n')
        want = json.dumps(self.reg, ensure_ascii=False, indent=2).replace('\n', '\n  ')
        self.assertEqual(want, inline,
                         'panel.html 的内联注册表与 x_keys.json 不一致 → 跑 node scripts/x_guard.js --fix')

    def test_heavy_keys_are_derived_not_hardcoded(self):
        """重键/产出键必须由注册表派生：重键清单是取回与对账策略的唯一依据。"""
        heavy_reg = sorted(e['name'] for e in self.reg['keys'] if e.get('heavy'))
        self.assertTrue(heavy_reg, '注册表里没有登记任何重键（heavy 机制会失效）')
        self.assertIn('function xHeavyKeys()', self.panel)
        self.assertIn('function xProducedKeys()', self.panel)
        self.assertIn('setHeavyKeys(xHeavyKeys())', self.panel)
        self.assertIn('setProducedKeys(xProducedKeys())', self.panel)
        # 禁止再出现硬编码数组（守卫 C 项的同一条规则，这里提前给更易读的报错）
        self.assertNotRegex(self.panel, r'setHeavyKeys\s*\(\s*\[')

    def test_hash_implementations_still_present(self):
        """两侧的关键词哈希实现都还在（守卫的哈希比对依赖它们能被抽出来）。"""
        self.assertIn('function kwKey(', self.panel)
        server = _read(SERVER_PATH)
        self.assertIn('def _kw_key(', server)
        # 后端必须按 UTF-16 代码单元遍历：UTF-16 代码单元与 Unicode 码位在 emoji 处不等价，
        # 用 ord() 遍历会让含 emoji 的关键词前后端算出不同的 key（曾因此查不到结果）。
        self.assertIn('utf-16-le', server,
                      '后端 _kw_key 应通过 UTF-16 编码取代码单元，不能按 Unicode 码位遍历')


if __name__ == '__main__':
    unittest.main()
