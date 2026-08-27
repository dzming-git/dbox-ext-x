# X 客户端（x_downloader）产品需求文档

> 状态：规划中 → 迭代实现
> 更新：2026-08-27
> 定位：把 `x_downloader` 从"下载工具"升级为 **DBox 内置的 X 阅读 + 下载一体客户端**。一个入口、一套数据、所有内容留在 DBox（代理转发），只有用户明确点击「打开原文」才跳 X.com。

## 核心交互原则
- **"点进去预览" = 进入 DBox 内推文详情页**（单个推文 + 评论区 + 相关推文），不是跳外链。
- 所有浏览/详情/评论区都在 DBox 内部完成，走 `/media` 缓存 + GraphQL 代理。
- 媒体"点开即缓存"（复用 media_cache LRU），"缓存即下载"（下载时从本地缓存入库，不重复访问 twimg）。

---

## P0（核心，先做）

### 1. DBox 内推文详情页（替代跳外链）
- 从浏览/收藏/历史点任意推文 → 打开 DBox 内详情页（新视图/抽屉/覆盖层）
- 详情内容：
  - 作者：头像 / 显示名 / handle / 简介 / 粉丝数
  - 完整正文（富文本）
  - 全部媒体画廊（多图、视频可拖动播放）
  - 时间 / 点赞数 / 转推数 / 引用数 / 收藏数 / 浏览数
  - 话题标签（#tag）
- 评论区 / 对话线程：拉取该推文回复列表（`ConversationTimeline`，需验证），回复可展开、可继续查看
- 「打开原文」按钮仅在此详情页，明确点击才跳 X
- 媒体走 `/media` 缓存

### 2. 一键下载进 DBox 资源库
- 详情页 / 浏览卡 / 收藏卡都有「下载」：整条推文（图片 + 视频）入库
- 复用现有 `/run` 任务流 + 进度反馈
- "缓存即下载"：已缓存媒体直接从本地缓存入库

---

## P1（重要）

### 3. 多流浏览
- 关注流（Following，已实现）+ 为你推荐（For You，已实现 `list_home_timeline`）+ 搜索/话题流（`SearchTimeline`，待做）
- 每流独立 tab / 下拉切换；广告过滤（已实现）；分页加载

### 4. 用户 profile 浏览
- 点作者 → 用户资料页：头像/简介/粉丝数/关注数
- 他的推文（`UserTweets`）、媒体（`UserMedia`）、点赞（`UserLikes`，待验证）、关注列表（`Following`）
- 可「下载其最近 N 条推文」

### 5. 推文互动（读）
- 展示点赞/转推/引用/书签计数
- 展示"谁转推/点赞"（如可行）

### 6. 搜索
- 按关键词 / 话题（#标签）搜索推文（`SearchTimeline`，待验证），结果可批量下载

### 7. 本地历史
- 自动记录浏览/预览过的推文（dbox 本地历史），可回看（已缓存媒体秒开）

---

## P2（增强）

### 8. 批量 / 合集下载
- 多选推文批量下载
- 按话题 / 用户 / 搜索页一键抓取全部
- 按 group 聚合为 dbox 帖子（复用 `upsert_post_by_group`）

### 9. 媒体管理
- 已缓存媒体查看 / 清理（大小、LRU 上限提示）
- 下载进度 / 断点续传
- 视频转码 / 压缩选项（可选）

### 10. 深度互动（写）
- 点赞 / 转推 / 回复 / 收藏（GraphQL mutation，需验证 X 是否允许非官方客户端写入）

### 11. 定时 / 通知
- 关注的用户新推文检测 + 自动下载（定时任务）
- 新帖 / 话题订阅

### 12. 组织
- 标签 / 分类收藏
- 收藏夹多集合
- 导出

### 13. 数据源健壮性
- queryId 轮换自动发现失败重试
- 多 cookie 轮换
- 限流退避
- 缓存 TTL
- cookie 过期自动提示更新

---

## 技术可行性标注
- ✅ 已实现：Following/For You 流、收藏、TweetDetail 单推文、媒体 LRU 缓存、广告过滤、下载入库
- ⏳ 需验证：`ConversationTimeline`（评论区）、`SearchTimeline`（搜索）、`UserLikes`、互动写操作（mutation）
- ⚠️ 关键风险：
  - X queryId 频繁轮换（需 discovery + 硬编码兜底）
  - 非官方客户端写互动可能被限流 / 拒绝
  - 评论区接口 features 需 playwright 捕获

---

## 实施顺序
1. **P0-1 详情页（含评论区）** ← 当前进行中
2. P0-2 缓存即下载
3. P1-3 多流 + P1-4 profile
4. P1-6 搜索
5. P1-7 历史
6. 其余 P2 按需

## 进度跟踪
- [x] P0-1 DBox 内推文详情页 + 评论区（commit 590edae + b304338）
- [x] P0-2 一键下载进资源库（缓存即下载，commit 9cad0f8）
- [x] P1-3 多流浏览（Following + For You）
- [x] P1-5 推文互动（读）：详情页展示回复/转推/点赞/引用/浏览数
- [ ] P1-4 用户 profile 浏览
- [ ] P1-6 搜索（X 反爬 x-client-transaction-id 限制，见下）
- [x] P1-7 本地浏览历史（commit ba4cd03）
- [x] P2-8 批量/合集下载（commit 795a8f4）
- [x] P2-9 媒体管理（commit 1e2b59c）
- [ ] P2-10 深度互动（写）
- [ ] P2-11 定时/通知
- [ ] P2-12 组织
- [x] P2-13 数据源健壮性：qid 自动发现 + 兜底常量已就绪

## 重要发现：X 真正的反爬机制（不是 qid 轮换）
2026-08-27 实测修正：
- **qid 没有频繁轮换**（HomeLatestTimeline qid `BLQWpfVqtgBqAqwRRJcJjA`、
  TweetDetail qid `XMOz5h24KAZ86qKffKTLdQ` 在不同时间都仍有效）。
- **真正的反爬是 `x-client-transaction-id` header**：X 部分接口（SearchTimeline、可能未来其他）需要
  这个加密请求头，python 难以伪造，会 403/404。
- 浏览器直发 `fetch(SearchTimeline URL)` 也 403（page.evaluate 的 fetch
  没带浏览器自动注入的 `x-client-transaction-id`）。
- 只能靠 playwright 实际渲染触发 X 前端 JS 生成这个 header。

**影响**：P1-6 搜索、P1-4 profile 可能需要 playwright 代理后端
（x_downloader 后端 spawn 浏览器）。这与 PRD 早期标注的"qid 轮换"
不同——`x-client-transaction-id` 是 X 新版反爬，HomeLatestTimeline/TweetDetail
目前不需要（这就是为什么 Following/详情页能稳定工作）。

## 已就绪的 qid（2026-08-27 实测稳定）
- Following（HomeLatestTimeline POST）：`BLQWpfVqtgBqAqwRRJcJjA`
- TweetDetail（GET）：`XMOz5h24KAZ86qKffKTLdQ`
- features：38 项（与 Bookmarks 同一套），`x-client-transaction-id` 不必需

## 生产可用方案
1. 浏览器代理（playwright 渲染）：能绕 x-client-transaction-id 但每个请求启浏览器开销大
2. 仅暴露已稳定的接口（详情/Following/历史/缓存/批量）：PRD P0-P1.7+P2.8/9 已完成
3. 持续监控 X qid 变化（自动发现函数已就绪）
3. 编写 X 客户端 qid 服务（定期 playwright 抓取最新 qid 写入配置文件，客户端读配置）

P2-13 数据源健壮性就是为解决此问题。
