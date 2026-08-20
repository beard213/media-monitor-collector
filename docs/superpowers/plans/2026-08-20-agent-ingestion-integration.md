# Agent Ingestion Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在采集系统完成下游内容入库后，旁路调用本机 `127.0.0.1:8088/api/agent/ingest`，保存 Agent 的 `case/report/evaluation/trace`，且 Agent 故障不影响采集和下游同步。

**Architecture:** 新增独立的 `AgentClient` 和 payload 构造函数，在现有 `sync_batch_to_backend` 的 `sync_content` 成功之后调用 Agent。Agent 结果写入 `backend_sync.agent_ingest` 和 `batch.raw.agent_ingest`，现有 `backend_sync.items`、`contentsSynced` 和本地清理行为保持兼容。

**Tech Stack:** Python 3.12, FastAPI, Pydantic Settings, httpx, pytest, httpx.MockTransport.

## Global Constraints

- Agent 前置条件：必须先部署在 `192.168.6.228` 本机并监听 `8088`。
- 默认配置必须为 `AGENT_BASE_URL=http://127.0.0.1:8088`、`AGENT_INGEST_URL=http://127.0.0.1:8088/api/agent/ingest`、`AGENT_INGEST_ENABLED=true`、`AGENT_INGEST_TIMEOUT=5`、`AGENT_INGEST_STRATEGY=rag_repair`。
- Agent 只消费标题、正文/ASR/OCR、评论、关键帧描述和媒体追溯信息，不主动读取或解析视频本体。
- Agent 请求使用 `httpx.Client(trust_env=False)`，不能经过服务器 HTTP 代理。
- Agent 失败不得改变已成功的下游同步结果，不得删除 MinIO 对象，不得把平台任务标记为采集失败。
- `sync_batch_to_backend` 返回值中的既有 `items` 字段和元素结构必须保持兼容。
- 不新增数据库表；结果保存在现有 `batch.raw`、任务 JSON 和 pipeline JSON/summary 中。
- 所有实现步骤先写失败测试，再写最小实现，再运行针对性测试。

---

### Task 1: Verify the local Agent prerequisite and add configuration

**Files:**
- Modify: `app/core/config.py`
- Modify: `.env.example`
- Modify: `README.md`
- Test: `tests/test_agent_ingest.py`

**Interfaces:**
- Produces `Settings.agent_base_url: str`
- Produces `Settings.agent_ingest_url: str`
- Produces `Settings.agent_ingest_enabled: bool`
- Produces `Settings.agent_ingest_timeout: float`
- Produces `Settings.agent_ingest_strategy: str`
- Produces `Settings.agent_ingest_max_attempts: int`

- [ ] **Step 1: Verify Agent is listening before code integration**

Run on `192.168.6.228`:

```bash
ss -ltnp | rg '127\.0\.0\.1:8088|0\.0\.0\.0:8088' || true
curl --noproxy '*' -sS -o /dev/null -w 'HTTP_STATUS=%{http_code}\n' \
  http://127.0.0.1:8088/docs
```

Expected:

- `ss` shows a process listening on port `8088`;
- `/docs` returns an HTTP status, normally `200`.

If no process is listening, start the separate Agent project on this same machine before the integration smoke test. Do not change the collector URL to a public IP for this local deployment.

- [ ] **Step 2: Write failing settings tests**

Add to `tests/test_agent_ingest.py`:

```python
def test_agent_settings_default_to_local_ingest_endpoint(monkeypatch):
    from app.core.config import Settings

    monkeypatch.delenv("AGENT_BASE_URL", raising=False)
    monkeypatch.delenv("AGENT_INGEST_URL", raising=False)
    settings = Settings(_env_file=None)

    assert settings.agent_base_url == "http://127.0.0.1:8088"
    assert settings.agent_ingest_url == "http://127.0.0.1:8088/api/agent/ingest"
    assert settings.agent_ingest_enabled is True
    assert settings.agent_ingest_timeout == 5
    assert settings.agent_ingest_strategy == "rag_repair"


def test_agent_settings_accept_explicit_endpoint(monkeypatch):
    monkeypatch.setenv("AGENT_BASE_URL", "http://agent.internal:8088")
    monkeypatch.setenv("AGENT_INGEST_URL", "http://agent.internal:8088/api/agent/ingest")
    monkeypatch.setenv("AGENT_INGEST_ENABLED", "false")
    monkeypatch.setenv("AGENT_INGEST_TIMEOUT", "7")

    from app.core.config import Settings

    settings = Settings(_env_file=None)

    assert settings.agent_base_url == "http://agent.internal:8088"
    assert settings.agent_ingest_url == "http://agent.internal:8088/api/agent/ingest"
    assert settings.agent_ingest_enabled is False
    assert settings.agent_ingest_timeout == 7
```

- [ ] **Step 3: Run the tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py -q
```

Expected: FAIL because the new Settings fields do not yet exist.

- [ ] **Step 4: Add the settings fields**

Add these fields to `Settings` in `app/core/config.py`:

```python
    agent_base_url: str = "http://127.0.0.1:8088"
    agent_ingest_url: str = "http://127.0.0.1:8088/api/agent/ingest"
    agent_ingest_enabled: bool = True
    agent_ingest_timeout: float = 5.0
    agent_ingest_strategy: str = "rag_repair"
    agent_ingest_max_attempts: int = 3
```

Add the matching variables after the collector backend variables in `.env.example`:

```env
AGENT_BASE_URL=http://127.0.0.1:8088
AGENT_INGEST_URL=http://127.0.0.1:8088/api/agent/ingest
AGENT_INGEST_ENABLED=true
AGENT_INGEST_TIMEOUT=5
AGENT_INGEST_STRATEGY=rag_repair
AGENT_INGEST_MAX_ATTEMPTS=3
```

Document the same values and the `192.168.6.228` local deployment prerequisite in `README.md`.

- [ ] **Step 5: Run the settings tests**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py -q
```

Expected: PASS for both settings tests.

- [ ] **Step 6: Commit**

```bash
git add app/core/config.py .env.example README.md tests/test_agent_ingest.py
git commit -m "feat: configure local agent ingestion"
```

### Task 2: Implement the isolated Agent HTTP client

**Files:**
- Create: `app/ingestion/agent.py`
- Modify: `tests/test_agent_ingest.py`

**Interfaces:**
- Produces `AgentClient.from_settings(settings=None) -> AgentClient`
- Produces `AgentClient.ingest(payload: dict[str, Any]) -> dict[str, Any]`
- Produces `AgentClient.healthcheck() -> dict[str, Any]`
- Produces `AgentClient.enabled -> bool`

- [ ] **Step 1: Write failing client tests**

Append to `tests/test_agent_ingest.py`:

```python
import json
import httpx


def test_agent_client_posts_json_and_returns_full_result():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["content_type"] = request.headers["content-type"]
        seen["payload"] = json.loads(request.read())
        return httpx.Response(
            200,
            json={
                "case": {"case_id": "youtube_video-1"},
                "report": {"risk_level": "high_risk"},
                "evaluation": {"passed": True},
                "trace": {"trace_id": "trace-1"},
            },
        )

    from app.ingestion.agent import AgentClient

    client = AgentClient(
        url="http://127.0.0.1:8088/api/agent/ingest",
        timeout_seconds=5,
        enabled=True,
        strategy="rag_repair",
        max_attempts=3,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = client.ingest({"data_source": "youtube", "video_id": "video-1"})

    assert seen["url"] == "http://127.0.0.1:8088/api/agent/ingest"
    assert seen["content_type"] == "application/json"
    assert seen["payload"]["strategy"] == "rag_repair"
    assert result["status"] == "SUCCESS"
    assert result["report"]["risk_level"] == "high_risk"


def test_agent_client_retries_5xx_and_reports_attempts():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(503, json={"error": "warming up"})
        return httpx.Response(200, json={"case": {}, "report": {}, "evaluation": {}, "trace": {}})

    from app.ingestion.agent import AgentClient

    client = AgentClient(
        url="http://agent/ingest",
        timeout_seconds=5,
        enabled=True,
        strategy="rag_repair",
        max_attempts=3,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.ingest({"video_id": "video-1"})

    assert attempts == 3
    assert result["status"] == "SUCCESS"
    assert result["attempts"] == 3


def test_agent_client_does_not_retry_4xx():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(422, json={"error": "invalid payload"})

    from app.ingestion.agent import AgentClient

    client = AgentClient(
        url="http://agent/ingest",
        timeout_seconds=5,
        enabled=True,
        strategy="rag_repair",
        max_attempts=3,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.ingest({"video_id": "video-1"})

    assert attempts == 1
    assert result["status"] == "FAILED"
    assert result["retryable"] is False


def test_agent_client_failure_is_structured_and_does_not_raise():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    from app.ingestion.agent import AgentClient

    client = AgentClient(
        url="http://agent/ingest",
        timeout_seconds=5,
        enabled=True,
        strategy="rag_repair",
        max_attempts=2,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.ingest({"video_id": "video-1"})

    assert result["status"] == "FAILED"
    assert result["attempts"] == 2
    assert result["retryable"] is True
    assert "connection refused" in result["error"]
```

- [ ] **Step 2: Run the client tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py -q
```

Expected: FAIL because `app.ingestion.agent.AgentClient` does not exist.

- [ ] **Step 3: Implement `AgentClient`**

Create `app/ingestion/agent.py` with the following behavior:

```python
class AgentClient:
    def __init__(self, *, url, timeout_seconds, enabled, strategy,
                 max_attempts=3, http_client=None):
        self.url = url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.enabled = enabled
        self.strategy = strategy
        self.max_attempts = max_attempts
        self._client = http_client or httpx.Client(
            timeout=timeout_seconds,
            trust_env=False,
        )

    @classmethod
    def from_settings(cls, settings=None):
        resolved = settings or get_settings()
        return cls(
            url=resolved.agent_ingest_url
                or f"{resolved.agent_base_url.rstrip('/')}/api/agent/ingest",
            timeout_seconds=resolved.agent_ingest_timeout,
            enabled=resolved.agent_ingest_enabled,
            strategy=resolved.agent_ingest_strategy,
            max_attempts=resolved.agent_ingest_max_attempts,
        )
```

`ingest` must:

1. Return `{"status": "SKIPPED", "reason": "disabled"}` when disabled;
2. add `strategy` only when the payload does not already provide one;
3. POST JSON with `Content-Type: application/json`;
4. treat 2xx JSON response as `SUCCESS`, copying `case`, `report`, `evaluation`, `trace`;
5. retry only 5xx, 408, 429, connect errors and timeouts;
6. return `FAILED` with `retryable`, `attempts` and `error` after final failure;
7. never raise an Agent request exception to its caller.

Use `time.sleep(0.5 * attempt)` between retry attempts. Use `healthcheck` to issue a short GET to `AGENT_BASE_URL` plus `/health` or `/docs`, returning `reachable`, `status_code` and `message`; it must also return a structured failure instead of raising.

- [ ] **Step 4: Run the client tests**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/ingestion/agent.py tests/test_agent_ingest.py
git commit -m "feat: add resilient agent client"
```

### Task 3: Add payload mapping and integrate Agent after downstream sync

**Files:**
- Modify: `app/ingestion/backend.py`
- Modify: `tests/test_ingestion_backend.py`
- Modify: `tests/test_agent_ingest.py`

**Interfaces:**
- Produces `build_agent_payload(batch, content, request, video, thumbnail) -> dict[str, Any]`
- Extends `sync_batch_to_backend(...) -> {"items": [...], "agent_ingest": {...}}`
- Writes `batch.raw["agent_ingest"]`

- [ ] **Step 1: Write failing payload and integration tests**

Add tests that build the existing YouTube batch fixture and mock two endpoints:

```python
def test_build_agent_payload_maps_content_comments_and_uploaded_media():
    from app.ingestion.agent import build_agent_payload

    payload = build_agent_payload(
        batch,
        batch.contents[0],
        request,
        video=UploadedObject(
            object_key="ingestion/job-1/videos/video-1.mp4",
            url="http://minio/media-monitor/ingestion/job-1/videos/video-1.mp4",
            size=10,
            content_type="video/mp4",
            object_type="VIDEO",
        ),
        thumbnail=UploadedObject(
            object_key="ingestion/job-1/thumbnails/video-1.jpg",
            url="http://minio/media-monitor/ingestion/job-1/thumbnails/video-1.jpg",
            size=5,
            content_type="image/jpeg",
            object_type="THUMBNAIL",
        ),
    )

    assert payload["data_source"] == "youtube"
    assert payload["video_id"] == "video-1"
    assert payload["video_title"] == "title"
    assert payload["minio_url"].endswith("/video-1.mp4")
    assert payload["video_object_key"].endswith("/video-1.mp4")
    assert payload["thumbnail_object_key"].endswith("/video-1.jpg")
    assert payload["comment_list"][0]["text"] == "hello"
    assert payload["strategy"] == "rag_repair"


def test_sync_batch_calls_agent_only_after_content_sync_and_keeps_sync_success(tmp_path, monkeypatch):
    calls = []

    class FakeAgent:
        enabled = True

        def ingest(self, payload):
            calls.append(("agent", payload["video_id"]))
            return {
                "status": "SUCCESS",
                "attempts": 1,
                "case": {"case_id": "youtube_video-1"},
                "report": {"risk_level": "high_risk"},
                "evaluation": {"passed": True},
                "trace": {"trace_id": "trace-1"},
            }

    monkeypatch.setattr(backend_module.AgentClient, "from_settings", lambda settings=None: FakeAgent())
    result = backend_module.sync_batch_to_backend(batch, request, client=backend_client)

    assert result["items"][0]["status"] == "QUEUED"
    assert result["agent_ingest"]["items"][0]["status"] == "SUCCESS"
    assert batch.raw["agent_ingest"]["items"][0]["report"]["risk_level"] == "high_risk"
    assert len(result["items"]) == 1
    assert calls == [("agent", "video-1")]
```

Also add a test where `FakeAgent.ingest` returns `FAILED` and assert `result["items"]` still contains the queued downstream response and `contentsSynced` callers can still count one item.

- [ ] **Step 2: Run the new tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py tests/test_ingestion_backend.py -q
```

Expected: FAIL because the payload builder and Agent integration do not exist.

- [ ] **Step 3: Implement payload construction**

In `app/ingestion/agent.py`, implement:

```python
def build_agent_payload(batch, content, request, *, video, thumbnail):
    raw = content.raw or {}
    text = (
        raw.get("asr_text")
        or raw.get("subtitle")
        or raw.get("asrText")
        or raw.get("ocr_text")
        or raw.get("ocrText")
        or raw.get("text")
        or content.description
        or ""
    )
    comments = [
        {
            "comment_id": item.external_id,
            "author": item.author_external_id,
            "text": item.text,
            "published_at": item.published_at.isoformat() if item.published_at else None,
            "raw": item.raw or {},
        }
        for item in batch.comments
        if item.content_external_id == content.external_id
    ]
    payload = {
        "data_source": _agent_source(content.platform),
        "video_id": content.external_id,
        "video_title": content.title,
        "asr_text": text,
        "comment_list": comments,
        "keyframes": _keyframe_descriptions(content, thumbnail),
        "minio_url": video.url or video.object_key,
        "video_object_key": video.object_key,
        "thumbnail_object_key": thumbnail.object_key,
        "source_url": content.url,
        "strategy": "rag_repair",
    }
    return _copy_optional_risk_fields(payload, request, content)
```

The source helper must return lowercase values:

```python
def _agent_source(platform):
    return {
        Platform.YOUTUBE: "youtube",
        Platform.TIKTOK: "tiktok",
        Platform.X: "x",
        Platform.INSTAGRAM: "instagram",
        Platform.FACEBOOK: "facebook",
    }[platform]
```

Do not invent keyframe descriptions. Use existing `raw` values such as `keyframes`, `keyframe_descriptions`, `ocr_text`, or thumbnail URL metadata; otherwise return an empty list.

- [ ] **Step 4: Integrate after `sync_content`**

In `app/ingestion/backend.py`:

1. Import `AgentClient` and `build_agent_payload`;
2. create `agent = AgentClient.from_settings(resolved_settings)` once per batch;
3. keep `agent_results = []`;
4. after `backend.sync_content(payload)` returns, call:

```python
agent_payload = build_agent_payload(
    batch,
    content,
    request,
    video=video,
    thumbnail=thumbnail,
)
agent_result = agent.ingest(agent_payload)
agent_results.append({
    "video_id": content.external_id,
    **agent_result,
})
```

5. call `_cleanup_synced_assets` after the Agent call;
6. set:

```python
batch.raw["agent_ingest"] = {
    "enabled": agent.enabled,
    "items": agent_results,
}
return {
    "items": results,
    "agent_ingest": batch.raw["agent_ingest"],
}
```

If the backend sync is disabled, the existing caller will not invoke `sync_batch_to_backend`; no Agent call is made. If the Agent is disabled, each batch result must contain `status=SKIPPED` rather than failing.

- [ ] **Step 5: Run targeted integration tests**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py tests/test_ingestion_backend.py -q
```

Expected: PASS, with existing downstream `items` assertions unchanged.

- [ ] **Step 6: Commit**

```bash
git add app/ingestion/agent.py app/ingestion/backend.py tests/test_agent_ingest.py tests/test_ingestion_backend.py
git commit -m "feat: send synced content to agent"
```

### Task 4: Expose Agent health and job-level statistics

**Files:**
- Modify: `app/api/collector_jobs.py`
- Modify: `app/api/collections.py`
- Modify: `app/main.py`
- Modify: `tests/test_collector_jobs_api.py`
- Modify: `tests/test_agent_ingest.py`

**Interfaces:**
- Produces `GET /agent/health`
- Adds `agentIngested` and `agentFailed` to job status where sync results are available
- Adds `agent_ingest`, `agent_ingested` and `agent_failed` to direct `/collections/run` response

- [ ] **Step 1: Write failing endpoint tests**

Add:

```python
def test_agent_health_reports_local_agent(monkeypatch):
    from app.api import agent_health

    monkeypatch.setattr(
        agent_health,
        "agent_healthcheck",
        lambda: {
            "enabled": True,
            "url": "http://127.0.0.1:8088/api/agent/ingest",
            "reachable": True,
            "status_code": 200,
            "message": "ok",
        },
    )

    response = TestClient(app).get("/agent/health")

    assert response.status_code == 200
    assert response.json()["reachable"] is True
    assert response.json()["status_code"] == 200
```

Add a direct collection response test whose fake `sync_batch_to_backend` returns one successful Agent item and assert:

```python
assert response.json()["agent_ingested"] == 1
assert response.json()["agent_failed"] == 0
assert response.json()["batch"]["raw"]["agent_ingest"]["items"][0]["status"] == "SUCCESS"
```

Add a job test whose fake `sync_batch_to_backend` returns one successful Agent item and assert:

```python
assert status["agentIngested"] == 1
assert status["agentFailed"] == 0
```

- [ ] **Step 2: Run endpoint tests to verify they fail**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py tests/test_collector_jobs_api.py -q
```

Expected: FAIL because the health router and statistics fields do not exist.

- [ ] **Step 3: Add the health adapter and route**

Create `app/api/agent_health.py`:

```python
from fastapi import APIRouter

from app.ingestion.agent import AgentClient

router = APIRouter(prefix="/agent", tags=["agent"])


def agent_healthcheck() -> dict:
    return AgentClient.from_settings().healthcheck()


@router.get("/health")
def agent_health() -> dict:
    return agent_healthcheck()
```

Include the router from `app/main.py`.

- [ ] **Step 4: Add statistics without changing failure semantics**

In `CollectorJobService._initial_job`, initialize:

```python
"agentIngested": 0,
"agentFailed": 0,
```

When account jobs consume `backend_sync`, count:

```python
agent_items = (backend_sync.get("agent_ingest") or {}).get("items") or []
agent_ingested = sum(item.get("status") == "SUCCESS" for item in agent_items)
agent_failed = sum(item.get("status") == "FAILED" for item in agent_items)
```

Add those values to the job update and account summary. For keyword jobs, aggregate the same fields from platform results and include them in `run_keyword_monitor` result and pipeline platform summaries. Do not set overall job status to failed only because `agent_failed > 0`.

In `app/api/collections.py`, after `backend_sync` is available, compute:

```python
agent_items = (backend_sync.get("agent_ingest") or {}).get("items") or []
agent_ingested = sum(item.get("status") == "SUCCESS" for item in agent_items)
agent_failed = sum(item.get("status") == "FAILED" for item in agent_items)
```

Return these fields alongside `backend_sync`:

```python
"agent_ingested": agent_ingested,
"agent_failed": agent_failed,
```

For keyword results, add `agent_ingested` and `agent_failed` to each collected item, sum them in `run_keyword_monitor`, and copy them into `_run_platform` and `_finish_keyword_job`.

- [ ] **Step 5: Run endpoint tests**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py tests/test_collector_jobs_api.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/api/agent_health.py app/api/collector_jobs.py app/api/collections.py app/main.py tests/test_agent_ingest.py tests/test_collector_jobs_api.py
git commit -m "feat: expose agent health and job metrics"
```

### Task 5: Update runtime configuration and operational documentation

**Files:**
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `docs/collector-service-integration.md`
- Create: `scripts/check-agent.sh`

**Interfaces:**
- Produces executable `scripts/check-agent.sh`
- Documents local Agent startup order and collection smoke test

- [ ] **Step 1: Write the operational check script test**

Add a shell-level documentation check in `tests/test_agent_ingest.py`:

```python
def test_agent_check_script_contains_local_prerequisite():
    from pathlib import Path

    script = Path("scripts/check-agent.sh").read_text(encoding="utf-8")

    assert "127.0.0.1:8088" in script
    assert "/api/agent/ingest" in script
    assert "ss -ltnp" in script
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
.venv/bin/pytest tests/test_agent_ingest.py::test_agent_check_script_contains_local_prerequisite -q
```

Expected: FAIL because the script does not exist.

- [ ] **Step 3: Add `scripts/check-agent.sh`**

Create an executable script:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "[AGENT] listener"
ss -ltnp | rg '127\.0\.0\.1:8088|0\.0\.0\.0:8088'

echo "[AGENT] health"
curl --noproxy '*' -fsS http://127.0.0.1:8088/docs >/dev/null

echo "[COLLECTOR] configured endpoint"
grep -E '^AGENT_(BASE_URL|INGEST_URL|INGEST_ENABLED|INGEST_TIMEOUT|INGEST_STRATEGY)=' .env

echo "[AGENT] ready"
```

Document:

```bash
cd /nfs/yhjc/media-monitor
./scripts/check-agent.sh
```

Then document a local smoke request using the collector’s direct collection API and explain that Agent failure is visible in `agent_ingest.items` while `contentsSynced` remains successful.

- [ ] **Step 4: Run documentation/script tests**

Run:

```bash
chmod +x scripts/check-agent.sh
.venv/bin/pytest tests/test_agent_ingest.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/check-agent.sh .env.example README.md docs/collector-service-integration.md tests/test_agent_ingest.py
git commit -m "docs: document local agent operations"
```

### Task 6: Full verification and real local smoke test

**Files:**
- No source changes expected.
- Test: all existing tests plus `tests/test_agent_ingest.py`

**Interfaces:**
- Verifies Agent, collector, downstream sync, MinIO metadata, cleanup and result persistence as one flow.

- [ ] **Step 1: Check the local Agent**

Run:

```bash
cd /nfs/yhjc/media-monitor
./scripts/check-agent.sh
```

Expected: `[AGENT] ready`.

- [ ] **Step 2: Run the full test suite**

Run:

```bash
.venv/bin/pytest -q
```

Expected: all tests pass. A pre-existing warning from Starlette/httpx may remain, but no test failure is acceptable.

- [ ] **Step 3: Restart the collector service with the new environment**

Run:

```bash
pid="$(ss -ltnp | awk '/:18083/ {print $NF}' | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1)"
if [ -n "$pid" ]; then kill "$pid"; sleep 1; fi
setsid .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 18083 \
  > data/logs/media-monitor-api.log 2>&1 < /dev/null &
sleep 2
curl --noproxy '*' -fsS http://127.0.0.1:18083/health
curl --noproxy '*' -fsS http://127.0.0.1:18083/agent/health
```

Expected: collector `/health` is `ok=true`; `/agent/health` reports the local Agent reachability.

- [ ] **Step 4: Run one controlled account smoke test**

Use one known logged-in YouTube account and one item:

```bash
curl --noproxy '*' -fsS -X POST http://127.0.0.1:18083/collector/jobs/accounts \
  -H 'Content-Type: application/json' \
  -d '{
    "accounts": [{"platform": "youtube", "account": "@serpentza"}],
    "limit": 1,
    "collectComments": false,
    "downloadMedia": true,
    "continueOnError": false,
    "metadata": {"max_height": 144, "min_free_disk_gb": 1},
    "runInBackground": false
  }'
```

Expected:

- `status=SUCCESS`;
- `contentsSynced=1`;
- `agentIngested=1` when Agent is healthy;
- `summary.json` contains `agent_ingest.items[0].report`;
- local video/thumbnail cleanup follows existing cleanup policy.

- [ ] **Step 5: Inspect the persisted result and logs**

Run:

```bash
latest="$(find data/pipeline-runs data/collector-jobs -type f \( -name summary.json -o -name '*.log' \) -printf '%T@ %p\n' | sort -nr | head -4)"
printf '%s\n' "$latest"
tail -80 data/logs/media-monitor-api.log
```

Confirm that logs distinguish:

- downstream sync success;
- Agent success or retry/failure;
- local cleanup result.

- [ ] **Step 6: Commit only if verification required a documentation/config correction**

```bash
git status --short
```

Do not commit generated `data/` outputs or `.env` secrets. Commit only a targeted correction if the verification exposed a documentation or configuration defect.
