# tvdrama-sources
tvdrama-android 采集源清单（App 远端更新消费）

App 侧经 jsDelivr 拉取：`https://cdn.jsdelivr.net/gh/chu-libu/tvdrama-sources@main/sources.json`
（CDN 缓存约 12h；急用 commit 一个新 tag 换 `@tag` 立即生效。）

## 自动收割（2026-11-07 起）

`.github/workflows/harvest.yml` **每周一 11:00（北京）** 自动运行：

1. 从上游 GitHub 配置仓（TVbox.org / LunaTV-config 生态 / 经典 TVBox 仓）收集候选源
2. 与仓内 `sources.json` 现有源**并集**后统一探活（`ac=videolist&wd=亮剑`，`code==1` 且 list 非空 = 存活）
3. 导出存活源：死源**连续 2 轮**才除名（防单轮抖动误删）；存活数骤降（< 存量一半）触发**熔断**，本轮不导出
4. 每轮写 `.github/heartbeat.json` 并 commit（心跳保活，防 GitHub 对 60 天无提交仓库自动停用定时调度）

手动触发：Actions 页 `weekly source harvest` → `Run workflow`。

- `tools/weekly_harvest.py` — 流水线脚本（纯 Python 标准库；口径移植自母仓 tvdrama-tool）
- `tools/removed.txt` — 人工除名清单（永不收录的源写这里）
- `tools/state_dead_streak.json` — 死源连击计数（流水线自动维护，勿手改）
- `sources_review.json` — 初版人工复核留档，流水线不改写

逻辑移植自母仓 `tvdrama-tool`（collect_sources.py + backend/app/sources_harvest.py），
母仓的手动收割/探活按钮保留不变。
