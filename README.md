# media-monitor

境外平台关键词采集与监测服务。当前支持 `YouTube / TikTok / Facebook / X / Instagram` 的关键词采集、账号采集、内容同步和评论补采，支持命令行一键启动、单平台运行、并行或串行调度，以及运行态日志和结果落盘。

## 1. 项目目标

- 从上游接口获取关键词任务。
- 按平台完成搜索、内容抓取、媒体下载、评论采集和结果归一化。
- 将采集结果同步到下游接口。
- 支持后台持续运行，便于断线后继续采集。

## 2. 当前能力

- 5 个平台关键词采集已打通。
- 命令行支持单平台或多平台启动。
- 可选择平台并行或平台内串行。
- 单次关键词采集默认上限为 10 条。
- 关键词内容默认按 3 个月内发布时间筛选；没有 `published_at` 的内容不拦截。
- 采集失败会按平台和单条内容降级处理，避免整轮中断。
- 结果会写入 `data/pipeline-runs/<run_id>/`。

## 3. 架构概览

### 分层

1. API 层
   - `app/main.py`
   - `app/api/collections.py`
   - 提供健康检查、平台查询和采集触发入口。

2. 领域层
   - `app/domain/`
   - `app/scheduler/`
   - 定义平台、能力、任务模型、错误码和调度约束。

3. 适配器层
   - `app/adapters/`
   - 每个平台一个适配器，负责搜索、内容详情、账号信息、媒体下载和评论采集。

4. 采集编排层
   - `app/ingestion/`
   - 负责关键词拉取、平台分发、结果过滤、同步下游。

5. 批处理/流水线层
   - `app/pipeline/`
   - 提供命令行一键执行、并行调度、运行摘要和落盘。

6. 浏览器与登录辅助
   - `scripts/start-login-browser.sh`
   - `data/storage/private/browser_profiles/`
   - 用于浏览器 Profile、登录态和 CDP 连接。

### 数据流

```text
上游关键词接口 / 本地兜底数据库
        -> 关键词标准化
        -> 平台适配器搜索
        -> 内容/媒体/评论采集
        -> 发布时间过滤
        -> 下游对象与内容接口同步
        -> 本地运行摘要与日志
```

## 4. 平台实现

- `YouTube`：基于 `yt-dlp`，支持关键词、账号、内容、评论。
- `TikTok`：浏览器 Profile + CDP 辅助搜索，内容和媒体以适配器方式处理。
- `Facebook`：浏览器搜索入口 + 内容详情抓取。
- `X`：支持关键词搜索与内容抓取，平台别名兼容 `TWITTER`。
- `Instagram`：支持关键词搜索、内容抓取和评论补采。

## 5. 运行方式

### 安装

```bash
python -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -e .
# 可选：仅在启用 YouTube/TikTok 直播媒体灰度时安装
.venv/bin/pip install 'streamlink>=8.6,<9'
```

### 启动 API

```bash
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 18083
```

Streamlink 灰度默认关闭。通过 `STREAMLINK_ENABLED=true`、
`STREAMLINK_PLATFORMS=TIKTOK` 和 `STREAMLINK_PILOT_TARGETS=<handle>`
仅对指定直播账号启用；解析或取流失败会回退到原有适配器。

正式常驻服务：

```bash
scripts/media-monitor-api.sh install
scripts/media-monitor-api.sh status
scripts/media-monitor-api.sh health
```

常用管理命令：

```bash
scripts/media-monitor-api.sh restart
scripts/media-monitor-api.sh logs
scripts/media-monitor-api.sh stop
```

### 采集服务 API

关键词采集：

```bash
curl -sS -X POST http://127.0.0.1:18083/collector/jobs/keywords \
  -H 'Content-Type: application/json' \
  -d '{"platforms":["instagram"],"keywordIds":[31],"limit":10,"collectComments":true,"downloadMedia":true,"recencyMonths":0,"continueOnError":true,"metadata":{"instagram_search_candidate_limit":80,"browser_comment_limit":50}}'
```

账号采集：

```bash
curl -sS -X POST http://127.0.0.1:18083/collector/jobs/accounts \
  -H 'Content-Type: application/json' \
  -d '{"accounts":[{"platform":"youtube","account":"@example"}],"limit":10,"collectComments":true,"downloadMedia":true,"recencyDays":365,"continueOnError":true,"metadata":{"max_comments":50}}'
```

账号采集默认规则：前端不传时间范围时按近 365 天过滤，最多同步 10 条；开启媒体下载时每个账号最多下载 10 条，YouTube 账号会逐条进入视频详情下载，确保有视频和缩略图资产后再推送下游。

查询状态和日志：

```bash
curl -sS http://127.0.0.1:18083/collector/jobs/<run_id>
curl -sS 'http://127.0.0.1:18083/collector/jobs/<run_id>/logs?tail=200'
```

采集任务结束会自动清理运行资源：成功同步到下游的本地视频、缩略图和转码中间文件会立即删除；失败或未同步媒体默认保留 24 小时后清理；`summary.json`、job log 和 `data/storage/private` 登录资料不会被清理。可通过 `.env` 调整：

```env
COLLECTOR_CLEANUP_ENABLED=true
COLLECTOR_CLEANUP_RETENTION_HOURS=24
COLLECTOR_CLEANUP_EMPTY_DIRS=true
COLLECTOR_MEMORY_GC_ENABLED=true
```

### 单平台采集

```bash
.venv/bin/python -m app.pipeline.run --platform x --limit 10
```

Instagram 关键词不限时间、最多同步 10 条视频并采集评论：

```bash
.venv/bin/python -m app.pipeline.run \
  --platform instagram \
  --limit 10 \
  --continue-on-error \
  --metadata-json '{"keyword_recency_months":0,"instagram_search_candidate_limit":80,"browser_search_timeout_seconds":180,"browser_comment_timeout_seconds":60,"browser_comment_limit":50,"max_height":480}'
```

### 多平台串行采集

```bash
.venv/bin/python -m app.pipeline.run \
  --platform youtube,tiktok,facebook,x,instagram \
  --limit 10 \
  --continue-on-error
```

### 多平台并行采集

```bash
.venv/bin/python -m app.pipeline.run \
  --platform youtube,tiktok,facebook,x,instagram \
  --limit 10 \
  --parallel-platforms \
  --continue-on-error
```

### 后台持续运行

```bash
RUN_ID="realtime-$(date +%Y%m%d-%H%M%S)"
LOG="data/logs/pipeline/$RUN_ID.log"
mkdir -p data/logs/pipeline data/runtime/pipeline

setsid nohup bash -c '
while true; do
  CYCLE_RUN_ID="'"$RUN_ID"'-cycle-$(date +%Y%m%d-%H%M%S)"
  PYTHONUNBUFFERED=1 .venv/bin/python -m app.pipeline.run \
    --platform youtube,tiktok,facebook,x,instagram \
    --limit 10 \
    --continue-on-error \
    --run-id "$CYCLE_RUN_ID"
  sleep 3600
done' > "$LOG" 2>&1 < /dev/null &

echo $! > "data/runtime/pipeline/$RUN_ID.pid"
```

## 6. 配置项

推荐通过 `.env` 配置：

```bash
YOUTUBE_ENABLED=true
TIKTOK_ENABLED=true
FACEBOOK_ENABLED=true
X_ENABLED=true
INSTAGRAM_ENABLED=true

YOUTUBE_BROWSER_PROFILE=...
TIKTOK_BROWSER_PROFILE=...
TIKTOK_BROWSER_CDP_URL=http://127.0.0.1:9223
FACEBOOK_BROWSER_PROFILE=...
FACEBOOK_BROWSER_CDP_URL=http://127.0.0.1:9224
X_BROWSER_PROFILE=...
X_BROWSER_CDP_URL=...
INSTAGRAM_BROWSER_PROFILE=...
INSTAGRAM_BROWSER_CDP_URL=http://127.0.0.1:9225

COLLECTOR_SYNC_ENABLED=true
COLLECTOR_BACKEND_BASE_URL=http://127.0.0.1:18082/api/v1
COLLECTOR_ACCESS_KEY=...
COLLECTOR_ACCESS_TOKEN=...
COLLECTOR_BACKEND_TIMEOUT_SECONDS=300

BROWSER_COMMENT_COLLECTION_ENABLED=true
BROWSER_COMMENT_LIMIT=50
BROWSER_COMMENT_TIMEOUT_SECONDS=45
```

## 7. 采集策略

- 默认先做关键词搜索，再进入内容详情抓取。
- 关键词结果默认按 3 个月发布时间筛选。
- 设置 `keyword_recency_months=0` 可关闭发布时间限制。
- 如果内容没有发布时间字段，保留，不做硬拦截。
- 采集失败按单条内容降级，不影响整个平台继续跑。
- 平台内建议串行，平台间可按资源决定是否并行。
- 需要稳定性时优先顺序启动；需要吞吐时再打开并行。
- Instagram 关键词搜索支持候选放大，`instagram_search_candidate_limit` 用于多取候选并优先尝试 `reel` 视频，避免前 10 个候选被图片帖占满。
- 内容成功上传并同步下游后，会删除本地视频和缩略图，只保留任务结果和日志。
- 采集服务对 `PENDING/RUNNING` 任务加平台锁，同一平台同一时间只允许一个任务运行。
- 前端重复点击同一个采集请求时，服务返回已有 `runId`，不会重复启动采集。

## 8. 监控与结果

- 日志目录：`data/logs/pipeline/`
- 运行摘要：`data/pipeline-runs/<run_id>/summary.json`
- 过程数据：`data/pipeline-runs/<run_id>/`
- 运行中的进度可直接看日志，重点关注：
  - 关键词拉取数量
  - 平台开始/结束状态
  - 内容同步数量
  - 失败原因和跳过原因

常用命令：

```bash
tail -f data/logs/pipeline/<run>.log
jq . data/pipeline-runs/<run-id>/summary.json
```

## 9. 后续优化方向

1. 来源侧提前过滤
   - 把发布时间过滤尽量下推到平台搜索层，减少无效抓取。

2. 自适应并发
   - 根据 CPU、内存、失败率和平台健康状态动态切换串行/并行。

3. 任务调度升级
   - 增加平台健康分、失败熔断、退避重试和自动恢复。

4. 去重与增量
   - 保存外部内容 ID、游标和最近抓取点，减少重复拉取。

5. 排序与评分
   - 对候选内容做质量打分，优先抓最近、带媒体、带互动的数据。

6. 观测性
   - 补充平台级耗时、成功率、同步成功率和失败分类指标。

7. 服务化运行
   - 补一个 systemd / supervisor 部署方式，保证重启后自动恢复。

## 10. 算法改进建议

当前流程是“关键词检索 -> 候选内容 -> 内容详情 -> 媒体/评论 -> 过滤 -> 同步”。
下一步可以做成更稳的两阶段策略：

- 第一阶段：尽量用平台原生检索条件缩小候选集。
- 第二阶段：对候选内容打分，按新鲜度、媒体完整度、互动信息和稳定性排序后再抓取。

可落地的改进包括：

- 结合平台原生时间语义做搜索。
- 同一个关键词保留最近命中集，做增量补抓。
- 对失败内容做局部重试，不回滚整轮。
- 对不同平台设置不同的抓取深度和并发阈值。

## 11. 故障排查

- 登录态失效：重新启动对应平台的登录浏览器。
- CDP 端口异常：检查 `9223 / 9224 / 9225` 等端口是否在监听。
- 单条内容失败：看日志里的 `ACCESS_DENIED`、`NO_MATCH`、`COLLECTOR_BUG`。
- 下游同步失败：检查 `COLLECTOR_BACKEND_BASE_URL` 和访问令牌。

```bash
ss -ltnp | rg '(:9223|:9224|:9225)'
pgrep -af 'app.pipeline.run|python -m app.pipeline'
```

## 12. 相关文档

- `docs/architecture.md`
- `docs/collector-service-integration.md`
- `docs/deployment.md`
- `docs/account-pool.md`
- `docs/session-cookie-lifecycle.md`
- `docs/platform-adapter.md`
- `docs/youtube-adapter.md`
