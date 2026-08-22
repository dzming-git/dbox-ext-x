# x_downloader

X (Twitter) 媒体下载器插件，作为 [dbox](https://github.com/dzming-git/dbox) 的独立扩展。

## 仓库结构

- `manifest.json` — 插件清单（名称、入口、UI）
- `run.py` — 下载任务主程序（被 dbox 后端以子进程方式调用）
- `backend/server.py` — dbox backend 的 Blueprint，提供 `/run`、`/status`、`/input`、`/notify` 接口
- `ui/panel.html` — 浏览器侧面板 UI

## 作为 dbox 的 submodule 使用

本仓库由 dbox 主仓库通过 git submodule 引用：

```bash
git submodule add https://github.com/dzming-git/dbox-x-downloader.git extensions/x_downloader
```

插件目录放置在 dbox 的 `extensions/x_downloader` 下即可被后端自动加载。
