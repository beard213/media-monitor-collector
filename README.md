# Media Monitor Collector

A multi-platform social media collection and monitoring service for content-safety and public-opinion workflows. The service normalizes keyword and account collection across YouTube, TikTok, Facebook, X, and Instagram, then synchronizes structured content, media assets, comments, and collection logs to downstream systems.

This public repository is a sanitized engineering version. Runtime data, browser profiles, cookies, credentials, downloaded media, production IPs, and private task outputs are intentionally excluded.

## Highlights

- **Unified collection model**: shared domain schema for platform, capability, target, account, content, comment, media asset, error, and batch output.
- **Multi-platform adapters**: YouTube, TikTok, Facebook, X, and Instagram adapters behind one registry and capability contract.
- **Keyword and account workflows**: supports keyword search, account collection, content detail collection, media download, and comment enrichment.
- **Browser-assisted collection**: supports persistent browser profile / CDP based collection for platforms that require logged-in sessions.
- **Pipeline orchestration**: CLI runner supports single-platform, multi-platform serial, and multi-platform parallel collection.
- **Failure isolation**: per-platform and per-item errors are normalized, allowing a collection run to continue after partial failures.
- **Runtime cleanup**: successful media files and temporary artifacts can be cleaned after downstream synchronization.
- **Agent integration point**: optional side-channel delivery to a content-safety Agent ingest endpoint after collection sync.
- **Test coverage**: contract, adapter, API, pipeline, keyword-monitor, backend-sync, and cleanup tests.

## Architecture

```text
Keyword / Account Task
  -> API / CLI entry
  -> Scheduler contract validation
  -> Platform adapter registry
  -> Platform-specific collector
       -> search / account / content / media / comments
  -> Unified CollectionBatch
  -> Recency filter / error normalization
  -> Downstream sync
  -> Optional Agent ingest
  -> Runtime summary, logs, cleanup
```

## Repository Layout

```text
app/
  adapters/          Platform adapters and browser-assisted collectors
  api/               FastAPI endpoints for collection jobs and health checks
  core/              Runtime settings loaded from environment variables
  domain/            Shared collection models and enums
  ingestion/         Backend sync, keyword monitor, Agent side-channel, cleanup
  pipeline/          CLI orchestration for batch collection
  scheduler/         API request contracts and validation

data/                Runtime directories; only .gitkeep placeholders are committed
deploy/systemd/      Example systemd service template
docs/                Architecture and integration notes
scripts/             Service and login-browser helper scripts
tests/               Unit and workflow tests
```

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
cp .env.example .env
pytest -q
```

Start the API service:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 18083
```

Health check:

```bash
curl -sS http://127.0.0.1:18083/health
```

## API Examples

Create a keyword collection job:

```bash
curl -sS -X POST http://127.0.0.1:18083/collector/jobs/keywords \
  -H 'Content-Type: application/json' \
  -d '{"platforms":["youtube"],"keywordIds":[1],"limit":10,"collectComments":true,"downloadMedia":false,"continueOnError":true}'
```

Create an account collection job:

```bash
curl -sS -X POST http://127.0.0.1:18083/collector/jobs/accounts \
  -H 'Content-Type: application/json' \
  -d '{"accounts":[{"platform":"youtube","account":"@example"}],"limit":10,"collectComments":true,"downloadMedia":false,"continueOnError":true}'
```

Query job status and logs:

```bash
curl -sS http://127.0.0.1:18083/collector/jobs/<run_id>
curl -sS 'http://127.0.0.1:18083/collector/jobs/<run_id>/logs?tail=200'
```

Run from CLI:

```bash
python -m app.pipeline.run --platform youtube,tiktok,facebook,x,instagram --limit 10 --parallel-platforms --continue-on-error
```

## Configuration

All production values should be supplied through `.env` or the runtime environment. Do not commit browser profiles, cookies, API tokens, downloaded videos, exports, logs, or database files.

Important settings:

```text
COLLECTOR_SYNC_ENABLED       Enable downstream sync
COLLECTOR_BACKEND_BASE_URL   Downstream business system base URL
COLLECTOR_ACCESS_KEY         Downstream API access key
COLLECTOR_ACCESS_TOKEN       Downstream API token
AGENT_INGEST_ENABLED         Enable optional Agent side-channel delivery
AGENT_INGEST_URL             Agent ingest endpoint
*_BROWSER_PROFILE            Persistent browser profile for login-required platforms
```

## Agent Side-Channel

When `AGENT_INGEST_ENABLED=true`, the collector can post normalized collection results to an external Agent service after downstream sync. The Agent failure path is isolated from the primary collection flow, so failed Agent delivery does not block collection completion.

## Public Data Policy

This repository intentionally contains no real collected content. `data/` only keeps empty placeholders so local runs have the expected directory layout.
