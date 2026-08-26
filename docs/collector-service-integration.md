# 采集服务系统集成方案

## 1. 目标

把现有采集能力封装成一个可以被系统前后端调用的服务。

目标效果：

- 前端点击按钮即可发起采集。
- 后端不直接关心各平台抓取细节。
- 采集服务负责平台登录态、关键词拉取、抓取、评论采集、媒体下载、下游同步。
- 每次采集都有 `run_id`，可以查询状态、日志、结果和失败原因。
- 保留命令行抓取能力，方便后端或运维手动触发。

## 2. 推荐架构

```text
前端页面
  -> 业务后端
  -> 采集服务 API
  -> 采集任务 runner
  -> 平台 adapters
  -> 下游对象接口 / 内容接口
  -> 任务状态与日志
```

职责划分：

- 前端：发起采集、展示进度、展示采集结果。
- 业务后端：鉴权、参数校验、调用采集服务、保存业务侧任务记录。
- 采集服务：执行 pipeline、写日志、写 summary、同步下游。
- 下游接口：接收视频、缩略图、内容、评论数据。

前端不直接执行服务器命令。命令只能由业务后端或采集服务内部执行。

## 3. 集成方式

### 方式 A：推荐，HTTP 服务集成

前端调用业务后端，业务后端调用采集服务。

优点：

- 权限边界清晰。
- 前端可以查询进度。
- 后续方便接入队列、取消、重试、告警。

### 方式 B：命令行集成

业务后端通过服务器命令启动采集。

优点：

- 接入最快。
- 不需要改太多服务代码。

缺点：

- 状态管理需要额外封装。
- 并发、超时、取消、日志都要自己处理。

建议先用方式 A，对外提供 HTTP 接口；命令行作为调试和兜底入口保留。

## 4. 服务接口设计

建议对前后端暴露两类创建接口：

- `POST /collector/jobs/keywords`：关键词采集。
- `POST /collector/jobs/accounts`：账号采集。

状态、日志、取消、健康检查共用：

- `GET /collector/jobs/{run_id}`
- `GET /collector/jobs/{run_id}/logs`
- `GET /collector/platforms/health`

这样前端不会把关键词和账号参数混在一起，后端也更容易做参数校验。

### 4.1 创建采集任务

通用入口可以保留：

```http
POST /collector/jobs
```

请求：

```json
{
  "platforms": ["instagram"],
  "keywordIds": [31],
  "limit": 10,
  "collectComments": true,
  "downloadMedia": true,
  "recencyMonths": 0,
  "parallelPlatforms": false,
  "continueOnError": true,
  "metadata": {
    "instagram_search_candidate_limit": 80,
    "browser_search_timeout_seconds": 180,
    "browser_comment_timeout_seconds": 60,
    "browser_comment_limit": 50,
    "max_height": 480
  }
}
```

字段说明：

- `platforms`：平台列表，支持 `youtube/tiktok/facebook/x/instagram`。
- `keywordIds`：可选，指定关键词 ID；为空时使用上游接口返回的活跃关键词。
- `limit`：每个平台每个关键词最多同步内容数，当前范围 `1-10`。
- `collectComments`：是否采集评论。
- `downloadMedia`：是否下载视频和缩略图。
- `recencyMonths`：发布时间限制月数，`0` 表示不限时间。
- `parallelPlatforms`：是否平台并行。
- `continueOnError`：单个平台失败后是否继续其他平台。
- `metadata`：平台级调优参数。

响应：

```json
{
  "runId": "collector-20260724-103000",
  "status": "RUNNING",
  "message": "collector job started"
}
```

实际落地时更推荐拆成下面两个明确入口。

### 4.1.1 创建关键词采集任务

```http
POST /collector/jobs/keywords
```

请求：

```json
{
  "platforms": ["instagram", "x"],
  "keywordIds": [31],
  "limit": 10,
  "collectComments": true,
  "downloadMedia": true,
  "recencyMonths": 0,
  "parallelPlatforms": false,
  "continueOnError": true,
  "metadata": {
    "instagram_search_candidate_limit": 80,
    "browser_search_timeout_seconds": 180,
    "browser_comment_timeout_seconds": 60,
    "browser_comment_limit": 50,
    "max_height": 480
  }
}
```

关键词流程逻辑：

1. 业务后端校验用户权限。
2. 业务后端调用采集服务 `POST /collector/jobs/keywords`。
3. 采集服务生成 `run_id`。
4. 如果传了 `keywordIds`，只跑指定关键词。
5. 如果没传 `keywordIds`，从上游活跃关键词接口拉取。
6. 按关键词的平台字段过滤目标平台。
7. 为每个关键词构造 `CollectionRequest`：
   - `target_type=KEYWORD`
   - `capability=SEARCH_KEYWORD`
   - `limit=10`
   - `collect_comments=true`
   - `download_media=true`
8. 平台 adapter 搜索关键词内容。
9. 内容详情、视频、缩略图、评论进入统一 `CollectionBatch`。
10. 按 `recencyMonths` 处理发布时间，`0` 表示不限时间。
11. 同步下游对象接口和内容接口。
12. 写 `summary.json`、平台 JSON 和日志。
13. 前端轮询 job 状态直到结束。

关键词同步到下游时：

```json
{
  "collectionType": "KEYWORD",
  "keywordId": 31,
  "keyword": "领导人",
  "platform": "INSTAGRAM",
  "videoId": "DW3YQ8vCd93",
  "comments": []
}
```

### 4.1.2 创建账号采集任务

```http
POST /collector/jobs/accounts
```

请求：

```json
{
  "accounts": [
    {
      "platform": "youtube",
      "account": "@example"
    },
    {
      "platform": "instagram",
      "account": "https://www.instagram.com/example/"
    }
  ],
  "limit": 10,
  "collectComments": true,
  "downloadMedia": true,
  "continueOnError": true,
  "metadata": {
    "browser_search_timeout_seconds": 180,
    "browser_comment_timeout_seconds": 60,
    "browser_comment_limit": 50,
    "max_comments": 50,
    "max_height": 480
  }
}
```

账号流程逻辑：

1. 业务后端校验用户权限和账号归属。
2. 业务后端调用采集服务 `POST /collector/jobs/accounts`。
3. 采集服务生成 `run_id`。
4. 遍历账号列表。
5. 为每个账号构造 `CollectionRequest`：
   - `target_type=ACCOUNT`
   - `capability=ACCOUNT_TIMELINE`
   - `target_value=<账号名或主页 URL>`
   - `limit=10`
   - `collect_comments=true`
   - `download_media=true`
6. 平台 adapter 抓取账号主页或时间线。
7. 内容详情、视频、缩略图、评论进入统一 `CollectionBatch`。
8. 账号流程默认不按关键词发布时间过滤；需要时间限制时由 metadata 显式传入。
9. 同步下游对象接口和内容接口。
10. 写 `summary.json`、平台 JSON 和日志。
11. 前端轮询 job 状态直到结束。

账号同步到下游时：

```json
{
  "collectionType": "USERNAME",
  "platform": "YOUTUBE",
  "user": {
    "uid": "@example",
    "username": "@example",
    "nickname": "example"
  },
  "videoId": "abc123",
  "comments": []
}
```

### 4.2 查询任务状态

```http
GET /collector/jobs/{run_id}
```

响应：

```json
{
  "runId": "collector-20260724-103000",
  "status": "RUNNING",
  "platforms": ["instagram"],
  "startedAt": "2026-07-24T10:30:00+08:00",
  "finishedAt": null,
  "keywordsTotal": 1,
  "keywordsCollected": 1,
  "contentsCollected": 4,
  "commentsCollected": 36,
  "assetsCollected": 8,
  "contentsSynced": 4,
  "failedPlatform": null,
  "failedReason": null,
  "summaryPath": "data/pipeline-runs/collector-20260724-103000/summary.json",
  "logPath": "data/logs/pipeline/collector-20260724-103000.log"
}
```

### 4.3 查询任务日志

```http
GET /collector/jobs/{run_id}/logs?tail=200
```

响应：

```json
{
  "runId": "collector-20260724-103000",
  "lines": [
    "[PIPELINE] run_id=collector-20260724-103000 platforms=INSTAGRAM limit=10",
    "[KEYWORD] start instagram keyword_id=31 keyword=领导人",
    "[COLLECT] instagram keyword_id=31 contents=4 comments=36 assets=8 accounts=3",
    "[SYNC] instagram keyword_id=31 contents_synced=4"
  ]
}
```

### 4.4 后续扩展：取消任务

当前版本先不开放取消接口。因为关键词任务和平台抓取内部可能正在执行浏览器或下载操作，简单标记状态不能可靠停止底层采集。

后续如果要做取消，建议由 job runner 保存子进程 PID，并通过进程组终止实现。

```http
POST /collector/jobs/{run_id}/cancel
```

响应：

```json
{
  "runId": "collector-20260724-103000",
  "status": "CANCEL_REQUESTED"
}
```

### 4.5 平台健康检查

```http
GET /collector/platforms/health
```

响应：

```json
{
  "items": [
    {
      "platform": "instagram",
      "ok": true,
      "message": "browser_profile=chromium:/opt/media-monitor/data/storage/private/browser_profiles/instagram/instagram_login_02"
    }
  ]
}
```

## 5. 任务状态

建议统一使用这些状态：

```text
PENDING
RUNNING
SUCCESS
FAILED
CANCEL_REQUESTED
CANCELLED
TIMEOUT
```

状态流转：

```text
PENDING -> RUNNING -> SUCCESS
PENDING -> RUNNING -> FAILED
PENDING -> RUNNING -> CANCEL_REQUESTED -> CANCELLED
PENDING -> RUNNING -> TIMEOUT
```

## 5.1 最终处理逻辑

服务内部建议统一成一个 `CollectorJob`：

```json
{
  "runId": "collector-20260724-103000",
  "jobType": "KEYWORD",
  "status": "PENDING",
  "request": {},
  "pid": null,
  "summaryPath": "",
  "logPath": ""
}
```

`jobType` 只允许：

```text
KEYWORD
ACCOUNT
```

runner 分流逻辑：

```text
创建 job
  -> 校验参数
  -> 写入 PENDING
  -> 启动后台任务
  -> 更新 RUNNING
  -> 根据 jobType 分流
      -> KEYWORD: run_keyword_monitor
      -> ACCOUNT: adapter.discover(ACCOUNT_TIMELINE)
  -> 统一 sync_batch_to_backend
  -> 写平台 JSON
  -> 写 summary.json
  -> 更新 SUCCESS / FAILED
```

关键词任务：

```text
jobType=KEYWORD
  -> 拉取上游关键词
  -> 按平台过滤关键词
  -> SEARCH_KEYWORD
  -> collectionType=KEYWORD
  -> payload 带 keywordId / keyword
```

账号任务：

```text
jobType=ACCOUNT
  -> 使用请求里的账号列表
  -> ACCOUNT_TIMELINE
  -> collectionType=USERNAME
  -> payload 带 user 信息
```

最终所有采集结果都进入同一个下游同步函数：

```text
CollectionBatch
  -> 上传视频对象
  -> 上传缩略图对象
  -> 组装内容 payload
  -> 附带评论 comments
  -> sync_content
```

对前端来说，关键词和账号只有创建参数不同；查询状态、看日志、看结果的接口完全一样。

## 6. 抓取命令

命令分两类：

- 关键词采集：当前已有 pipeline CLI。
- 账号采集：当前可直接调用 API；建议下一步补成统一 CLI。

### Instagram 不限时间抓取

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -m app.pipeline.run \
  --platform instagram \
  --limit 10 \
  --continue-on-error \
  --metadata-json '{"keyword_recency_months":0,"instagram_search_candidate_limit":80,"browser_search_timeout_seconds":180,"browser_comment_timeout_seconds":60,"browser_comment_limit":50,"max_height":480}'
```

### 账号抓取 API 命令

当前账号采集可以通过 API 调用：

```bash
curl -sS -X POST http://127.0.0.1:18083/collections/run \
  -H 'Content-Type: application/json' \
  -d '{
    "platform": "YOUTUBE",
    "capability": "ACCOUNT_TIMELINE",
    "target_type": "ACCOUNT",
    "target_value": "@example",
    "limit": 10,
    "collect_comments": true,
    "download_media": true,
    "dry_run": false,
    "timeout_seconds": 1800,
    "metadata": {
      "max_comments": 50,
      "max_height": 480
    }
  }'
```

建议后续补统一账号 CLI：

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -m app.pipeline.run_account \
  --platform youtube \
  --account @example \
  --limit 10 \
  --metadata-json '{"max_comments":50,"max_height":480}'
```

### 5 平台顺序抓取

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -m app.pipeline.run \
  --platform youtube,tiktok,facebook,x,instagram \
  --limit 10 \
  --continue-on-error \
  --metadata-json '{"keyword_recency_months":0,"instagram_search_candidate_limit":80,"browser_search_timeout_seconds":180,"browser_comment_timeout_seconds":60,"browser_comment_limit":50,"max_comments":50,"max_height":480}'
```

### 5 平台并行抓取

```bash
PYTHONUNBUFFERED=1 .venv/bin/python -m app.pipeline.run \
  --platform youtube,tiktok,facebook,x,instagram \
  --limit 10 \
  --parallel-platforms \
  --continue-on-error \
  --metadata-json '{"keyword_recency_months":0,"instagram_search_candidate_limit":80,"browser_search_timeout_seconds":180,"browser_comment_timeout_seconds":60,"browser_comment_limit":50,"max_comments":50,"max_height":480}'
```

生产环境建议默认顺序抓取。并行适合资源充足、平台登录态稳定、需要快速补采的场景。

## 7. 后端调用建议

后端不要直接拼接用户输入到 shell 命令。

建议做法：

1. 后端接收前端请求。
2. 后端校验平台、limit、关键词 ID、权限。
3. 后端调用采集服务 `POST /collector/jobs`。
4. 采集服务内部根据白名单参数生成命令。
5. 采集服务返回 `run_id`。
6. 前端轮询状态接口。

后端伪代码：

```python
payload = {
    "platforms": ["instagram"],
    "keywordIds": [31],
    "limit": 10,
    "collectComments": True,
    "downloadMedia": True,
    "recencyMonths": 0,
    "continueOnError": True,
    "metadata": {
        "instagram_search_candidate_limit": 80,
        "browser_comment_limit": 50
    }
}

response = requests.post(
    "http://127.0.0.1:18083/collector/jobs",
    json=payload,
    headers={"Authorization": "Bearer <internal-token>"},
    timeout=10,
)
```

## 8. 前端交互建议

前端页面需要这些能力：

- 平台选择：单选或多选。
- 关键词选择：可选当前活跃关键词，或按 keywordId 指定。
- 抓取数量：固定范围 `1-10`。
- 时间范围：不限时间 / 3 个月 / 6 个月。
- 评论开关：是否采集评论。
- 媒体开关：是否下载视频。
- 启动按钮：创建 job。
- 进度区域：展示平台、关键词、内容数、评论数、同步数。
- 日志区域：展示最近 100-200 行。
- 结果入口：跳转到下游内容列表或当前系统内容详情。

前端轮询建议：

- 任务运行中每 3-5 秒轮询一次。
- 任务结束后停止轮询。
- 失败时展示 `failedReason` 和日志入口。

## 9. 并发和稳定性

建议加这些约束：

- 同一平台同一时间只允许 1 个采集任务。
- 当前服务已对 `PENDING/RUNNING` 任务加平台锁。
- 当前服务已对相同请求做防重复点击处理，重复请求会返回已有 `run_id`。
- 多平台默认串行。
- 并行只开放给管理员或后台定时任务。
- 每个任务设置最大运行时间，例如 2 小时。
- 浏览器 CDP 端口按平台固定。
- 登录态失效时任务失败并返回 `AUTH_EXPIRED`。
- 单条内容失败只跳过，不中断整个平台。
- 内容成功上传并同步下游后，本地视频、缩略图和转码中间文件会被立即删除，只保留 summary 和日志。
- 每个 job 结束后会触发一次运行时清理和 Python GC；清理失败只写入 job 日志，不会把已完成采集改成失败。
- 失败或未同步媒体默认保留 `COLLECTOR_CLEANUP_RETENTION_HOURS=24` 小时，便于排查后自动回收。

清理参数：

```env
COLLECTOR_CLEANUP_ENABLED=true
COLLECTOR_CLEANUP_RETENTION_HOURS=24
COLLECTOR_CLEANUP_EMPTY_DIRS=true
COLLECTOR_MEMORY_GC_ENABLED=true
```

平台锁建议：

```text
collector_lock:instagram
collector_lock:x
collector_lock:facebook
collector_lock:tiktok
collector_lock:youtube
```

## 10. 数据和日志落盘

当前 pipeline 已经落盘：

```text
data/pipeline-runs/<run_id>/summary.json
data/pipeline-runs/<run_id>/<platform>.json
data/logs/pipeline/<run_id>.log
```

建议 job 状态表：

```sql
collector_jobs(
  run_id varchar primary key,
  status varchar,
  platforms json,
  request_json json,
  summary_json json,
  log_path varchar,
  pid int,
  started_at datetime,
  finished_at datetime,
  created_by varchar,
  failed_reason text
)
```

## 11. 安全要求

- 采集服务只监听内网地址。
- 所有接口加内部 token。
- 前端不能传任意命令。
- 平台参数必须白名单校验。
- `.env`、browser profile、cookie 不暴露给前端。
- 日志里不要输出 access token。
- 取消任务只能取消自己创建的任务或管理员任务。

## 12. 部署建议

### 轻量部署

```bash
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 18083
```

业务后端通过 `http://127.0.0.1:18083` 调用采集服务。

### 生产部署

建议：

- `systemd` 管理采集 API。
- `systemd` 或队列 worker 管理后台任务。
- Nginx 只暴露业务后端，不直接暴露采集服务。
- 日志进入 `data/logs/pipeline/`，再由系统日志采集器归档。

当前仓库已提供 systemd 安装和管理脚本：

```bash
scripts/media-monitor-api.sh install
scripts/media-monitor-api.sh status
scripts/media-monitor-api.sh health
scripts/media-monitor-api.sh logs
```

服务单元：

```text
deploy/systemd/media-monitor-api.service
```

默认监听：

```text
0.0.0.0:18083
```

## 13. 实施步骤

第一步：封装 job API。

- 新增 `POST /collector/jobs`。
- API 内部启动 pipeline。
- 写 job 状态和日志路径。

第二步：封装状态查询。

- 新增 `GET /collector/jobs/{run_id}`。
- 读取 summary 和平台 JSON。
- 返回统一进度字段。

第三步：封装日志查询。

- 新增 `GET /collector/jobs/{run_id}/logs`。
- 支持 `tail` 参数。

第四步：接前端页面。

- 前端点击后创建 job。
- 轮询 job 状态。
- 展示采集数量、同步数量、失败原因。

第五步：增强稳定性。

- 增加平台锁。
- 增加任务取消。
- 增加失败重试。
- 增加 systemd 自恢复。

## 14. 推荐默认参数

Instagram 当前建议：

```json
{
  "keyword_recency_months": 0,
  "instagram_search_candidate_limit": 80,
  "browser_search_timeout_seconds": 180,
  "browser_comment_timeout_seconds": 60,
  "browser_comment_limit": 50,
  "max_height": 480
}
```

全平台稳定采集建议：

```json
{
  "keyword_recency_months": 0,
  "browser_search_timeout_seconds": 180,
  "browser_comment_timeout_seconds": 60,
  "browser_comment_limit": 50,
  "max_comments": 50,
  "max_height": 480
}
```

## 15. 后续优化

- 用队列替代直接 `subprocess.Popen`。
- 支持任务优先级。
- 支持关键词级别暂停和重试。
- 支持平台账号健康分。
- 支持采集结果去重和增量游标。
- 支持失败分类统计。
- 支持前端实时进度推送，替代轮询。
