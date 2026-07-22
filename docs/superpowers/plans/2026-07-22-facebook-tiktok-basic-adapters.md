# Facebook TikTok Basic Adapters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add basic Facebook and TikTok collection adapters backed by logged-in Chromium browser profiles.

**Architecture:** Follow the existing Instagram and X adapter pattern: each platform healthcheck validates yt-dlp and a configured profile, then collection runs yt-dlp with `cookiesfrombrowser` and normalizes results to `CollectionBatch`. Shared yt-dlp browser-profile behavior lives in a small base module so Facebook and TikTok only define platform-specific URL and metadata rules.

**Tech Stack:** Python 3.11+, FastAPI, pydantic-settings, pytest, yt-dlp.

## Global Constraints

- Use persistent Chromium profiles only; do not export cookies.
- Do not automate login, CAPTCHA, 2FA, or platform access-control bypass.
- Comments and keyword search remain out of scope for this first adapter pass.
- Keep backend sync unchanged unless a platform is explicitly supported by ingestion backend.

---

### Task 1: Configuration and Registry

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/adapters/registry.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Produces: `Settings.tiktok_browser`, `Settings.tiktok_browser_profile`, `Settings.tiktok_output_root`, `Settings.facebook_browser`, `Settings.facebook_browser_profile`, `Settings.facebook_output_root`.
- Produces: `build_registry()` returns concrete `TikTokAdapter` and `FacebookAdapter` instances.

- [ ] **Step 1: Write failing tests**

Add assertions that the registry exposes concrete Facebook and TikTok adapters and that settings can carry profile paths.

- [ ] **Step 2: Run red tests**

Run: `pytest tests/test_contracts.py::test_all_target_platforms_are_registered -q`
Expected: FAIL because Facebook/TikTok are still `StubAdapter`.

- [ ] **Step 3: Implement minimal config and registry wiring**

Add settings fields and replace stub registrations with concrete adapter classes.

- [ ] **Step 4: Run green tests**

Run: `pytest tests/test_contracts.py::test_all_target_platforms_are_registered -q`
Expected: PASS.

### Task 2: Shared Browser yt-dlp Adapter

**Files:**
- Create: `app/adapters/browser_ytdlp.py`
- Test: `tests/test_browser_ytdlp_adapters.py`

**Interfaces:**
- Produces: `BrowserYtDlpAdapter`, which provides `healthcheck`, `_collect`, `_browser_profile`, `_target_to_url`, `_map_error`, `_batch_from_info`, and helpers for subclasses.
- Consumes: subclass properties `platform`, `browser_name`, `browser_profile_path`, `enabled`, `output_root`, `content_type`.

- [ ] **Step 1: Write failing adapter tests**

Test profile-required auth errors, `cookiesfrombrowser` params, target URL conversion via subclass hooks, normalized account/content/assets output, and rate/auth/network error mapping.

- [ ] **Step 2: Run red tests**

Run: `pytest tests/test_browser_ytdlp_adapters.py -q`
Expected: FAIL because `app.adapters.browser_ytdlp` does not exist.

- [ ] **Step 3: Implement shared adapter**

Implement only the behavior required by the tests and already present in Instagram/X: healthcheck, collect dispatch, yt-dlp params, batch normalization, timestamp conversion, assets, and error mapping.

- [ ] **Step 4: Run green tests**

Run: `pytest tests/test_browser_ytdlp_adapters.py -q`
Expected: PASS.

### Task 3: Facebook and TikTok Adapters

**Files:**
- Create: `app/adapters/facebook.py`
- Create: `app/adapters/tiktok.py`
- Modify: `app/adapters/registry.py`
- Modify: `.env`
- Modify: `.env.example`
- Test: `tests/test_browser_ytdlp_adapters.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Produces: `FacebookAdapter` with capabilities `ACCOUNT_TIMELINE`, `CONTENT_DETAIL`, `PROFILE`, `MEDIA_DOWNLOAD`.
- Produces: `TikTokAdapter` with capabilities `ACCOUNT_TIMELINE`, `CONTENT_DETAIL`, `PROFILE`, `MEDIA_DOWNLOAD`.

- [ ] **Step 1: Write failing platform tests**

Test healthcheck messages include configured profile paths, TikTok account values become `https://www.tiktok.com/@name`, and Facebook account values become `https://www.facebook.com/name`.

- [ ] **Step 2: Run red tests**

Run: `pytest tests/test_browser_ytdlp_adapters.py tests/test_contracts.py -q`
Expected: FAIL until concrete adapters and config are wired.

- [ ] **Step 3: Implement platform classes and env configuration**

Use `BrowserYtDlpAdapter` subclasses and set `.env` profile paths to the existing `facebook/facebook_01` and `tiktok/tiktok_01` directories.

- [ ] **Step 4: Run green tests**

Run: `pytest tests/test_browser_ytdlp_adapters.py tests/test_contracts.py -q`
Expected: PASS.

### Task 4: Verification and Smoke Checks

**Files:**
- No planned source changes.

**Interfaces:**
- Consumes: FastAPI app and concrete adapters from previous tasks.

- [ ] **Step 1: Run full automated checks**

Run: `python3 -m compileall -q app && pytest -q`
Expected: both commands pass.

- [ ] **Step 2: Run `/platforms` smoke check**

Run the app locally if needed and query `/platforms`; expected Facebook and TikTok have `enabled=true`, `mode=yt-dlp`, and browser profile messages.

- [ ] **Step 3: Run one small sample collection where a target URL is available**

Use `/collections/run` with `dry_run=false`, `download_media=false`, `limit=1`, and a public TikTok or Facebook URL. Expected result is either a normalized one-item batch or a clear platform/auth error from yt-dlp.
