# Agent Ingestion Integration Design

**Date:** 2026-08-20

## Goal

将采集系统接入本机 `127.0.0.1:8088` 的内容安全 Agent，使关键词采集、账号采集和直接采集接口在下游内容入库成功后自动提交研判，并保存 Agent 返回的 `case`、`report`、`evaluation` 和 `trace`。

## Scope

本次只改采集系统侧：

- 增加 Agent 配置和 HTTP 客户端；
- 在下游内容同步成功后调用 Agent；
- 统一映射 YouTube、TikTok、X、Instagram、Facebook 数据；
- 保存研判结果和失败状态；
- 增加测试和运行状态检查。

本次不修改 Agent 内部的 RAG、Evaluator、Repair、Trace 逻辑，也不把采集服务器的媒体文件交给 Agent。Agent 接收 MinIO URL / objectKey 作为媒体追溯信息，当前研判主要基于采集系统传入的标题、ASR/OCR、评论和关键帧描述；后续可扩展为 Agent 侧主动读取 MinIO 媒体。

## Confirmed Configuration

`.env` 使用以下配置：

```env
AGENT_BASE_URL=http://127.0.0.1:8088
AGENT_INGEST_URL=http://127.0.0.1:8088/api/agent/ingest
AGENT_INGEST_ENABLED=true
AGENT_INGEST_TIMEOUT=5
AGENT_INGEST_STRATEGY=rag_repair
```

采集系统和 Agent 必须部署在同一台机器；若 Agent 不在本机，必须显式修改 `AGENT_INGEST_URL`，不能假设 `127.0.0.1` 可以跨服务器访问。

实施前置条件：Agent 必须先部署在采集服务器 `192.168.6.228` 本机，并监听 `8088`。只有该条件满足时，`http://127.0.0.1:8088/api/agent/ingest` 才成立。

## Architecture

```text
platform adapter
    -> CollectionBatch
    -> upload video/thumbnail to collector backend MinIO
    -> sync content to downstream backend
    -> POST /api/agent/ingest on localhost
    -> save case/report/evaluation/trace in batch.raw and job/pipeline summaries
    -> cleanup local media files
```

接入点是 `app.ingestion.backend.sync_batch_to_backend`。关键词任务、账号任务和 `/collections/run` 都已经经过该函数，因此不会为不同入口复制 Agent 调用逻辑。

Agent 调用必须发生在 `sync_content` 成功之后、清理本地媒体之前。这样：

1. Agent payload 能包含上传后返回的 MinIO URL 和对象键；
2. 下游已经有内容记录，Agent 失败不会影响内容入库；
3. Agent 处理完成后仍可按任务目录追踪原始采集和同步结果。

## Components

### 1. Settings

在 `app/core/config.py` 增加 Agent 配置，默认值对应本机 `8088`：

- `agent_base_url`
- `agent_ingest_url`
- `agent_ingest_enabled`
- `agent_ingest_timeout`
- `agent_ingest_strategy`
- `agent_ingest_max_attempts`，默认 `3`

如果 `agent_ingest_url` 为空，则由 `agent_base_url` 拼接 `/api/agent/ingest`。如果 Agent 未启用或下游同步未启用，则不调用 Agent，并返回可区分的跳过状态。

### 2. Agent client

新建 `app/ingestion/agent.py`，只负责：

- 组装和发送标准 JSON；
- 设置 `Content-Type: application/json`；
- 使用 `httpx.Client(trust_env=False)`，避免本机调用被 HTTP 代理接管；
- 按 `AGENT_INGEST_TIMEOUT` 设置单次请求超时；
- 对连接错误、超时、5xx 和无效 JSON 最多重试三次；
- 返回结构化成功或失败结果，不向采集主链路抛出未处理异常。

Agent 请求主体采用文档推荐字段：

```json
{
  "data_source": "youtube",
  "video_id": "source-content-id",
  "video_title": "title",
  "asr_text": "description or extracted text",
  "comment_list": [{"text": "comment"}],
  "keyframes": [{"description": "thumbnail or keyframe description"}],
  "minio_url": "uploaded-video-url",
  "strategy": "rag_repair",
  "riskLevel": "optional-risk-level",
  "riskType": "optional-risk-type"
}
```

平台映射：

| Collector platform | Agent `data_source` |
| --- | --- |
| `YOUTUBE` | `youtube` |
| `TIKTOK` | `tiktok` |
| `X` | `x` |
| `INSTAGRAM` | `instagram` |
| `FACEBOOK` | `facebook` |

Agent 文档目前对 YouTube、TikTok、X 有明确归一化；Instagram 和 Facebook 会按原始平台值透传为 `source_profile`，仍可进入通用 RAG 研判流程。

字段映射规则：

- `video_id`: `CollectedContent.external_id`
- `video_title`: `CollectedContent.title`
- `asr_text`: 优先使用 `content.raw` 中的 `asr_text`/`subtitle`/`text`，没有时使用 `description`
- `comment_list`: 当前内容对应的 `CollectedComment`，每条至少发送 `text`，同时保留作者、时间和原始字段
- `keyframes`: 由缩略图和 `content.raw` 中已有关键帧/OCR 字段组成；没有描述时不伪造分析结论
- `minio_url`: 上传返回的 `UploadedObject.url`；同时附加 `video_object_key` 和 `thumbnail_object_key` 作为扩展字段。当前 Agent ingest 不映射这两个字段，会忽略它们；采集侧先保留，后续 Agent 如需直接读取 MinIO 对象时再扩展 normalizer 映射
- `riskLevel`/`riskType`: 从关键词元数据或内容原始字段读取，没有值时不传人工标签

### 3. Sync result and persistence

`sync_batch_to_backend` 保持既有 `items` 结构不变，增加同级字段：

```json
{
  "items": [{"contentId": 1, "sourceVideoId": "video-1"}],
  "agent_ingest": {
    "enabled": true,
    "items": [
      {
        "video_id": "video-1",
        "status": "SUCCESS",
        "case": {},
        "report": {},
        "evaluation": {},
        "trace": {}
      }
    ]
  }
}
```

失败或跳过时保存：

```json
{
  "video_id": "video-1",
  "status": "FAILED",
  "attempts": 3,
  "error": "connection refused",
  "retryable": true
}
```

同时写入 `batch.raw["agent_ingest"]`，因此以下现有结果都能拿到研判信息：

- `data/pipeline-runs/<run_id>/summary.json`
- `data/pipeline-runs/<run_id>/<platform>.json`
- `data/collector-jobs/<run_id>.json`
- `/collections/run` 返回的 `batch.raw`

不新增数据库表，避免在当前 SQLite/后端双存储结构中引入新的迁移依赖。后续若前端需要按内容长期检索，再将 Agent 结果同步到业务后端或单独结果表。

## Error Handling

Agent 是采集链路的旁路能力：

- 下游上传或内容同步失败：保持当前采集失败行为，不调用 Agent；
- Agent 返回 2xx 且 JSON 合法：状态 `SUCCESS`，保存完整四段结果；
- Agent 返回 4xx：状态 `FAILED`，不重试不可恢复请求；
- Agent 返回 5xx、超时、连接失败：按退避策略重试，最终状态 `FAILED`；
- Agent 失败：不改变 `contentsSynced`，不删除已上传的 MinIO 对象，不将整个平台标记为采集失败；
- 本地媒体清理仍在 Agent 调用结束后执行，清理失败也只记录 cleanup 状态，不改变已完成任务状态。

默认重试间隔为 `0.5s`、`1.0s`、`2.0s`，单次请求最多等待 `AGENT_INGEST_TIMEOUT` 秒。

## Health and Operations

增加采集系统的 `GET /agent/health`：

- `enabled`
- `url`
- `reachable`
- `status_code`
- `message`

该接口只检查 Agent，不影响现有 `/health` 返回。启动日志和任务日志记录 Agent 的启用状态、请求次数、成功数、失败数和跳过原因。任务状态中区分：

- `contentsSynced`: 下游内容入库数量；
- `agentIngested`: Agent 成功接收数量；
- `agentFailed`: Agent 调用失败数量。

## Testing

新增 `tests/test_agent_ingest.py`，覆盖：

1. 配置生成默认本机 URL；
2. YouTube/TikTok/X/Instagram/Facebook payload 映射；
3. 评论、标题、描述、MinIO URL 和对象键映射；
4. Agent 2xx 响应保存 `case/report/evaluation/trace`；
5. 5xx/超时重试；
6. 4xx 不重试；
7. Agent 不可用时下游同步仍返回成功；
8. Agent 未启用或下游同步关闭时返回明确跳过状态。

更新 `tests/test_ingestion_backend.py`，验证：

- `items` 原有字段和计数不变；
- `agent_ingest` 被写入返回值和 `batch.raw`；
- 上传完成后本地清理仍执行。

更新接口测试，验证 `/agent/health` 和任务结果中的 Agent 统计字段。

## Non-Goals

- 不修改 Agent 的 RAG、Evaluator、Repair、Trace 实现；
- 不通过公网 `43.162.95.63` 调 Agent；
- 不在本地永久保存大模型报告数据库；
- 不因 Agent 暂时不可用而停止采集；
- 不把本地视频路径传给 Agent；
- 不让 Agent 代替下游 MinIO 上传或内容入库。
