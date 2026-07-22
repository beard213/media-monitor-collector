# Browser Comment Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add cookie/profile-based browser comment collection for platforms where yt-dlp does not return comments.

**Architecture:** Add a focused `app/adapters/browser_comments.py` module that uses Playwright and a temporary Chromium profile copy to collect visible comment data. Browser-backed adapters call it after yt-dlp collection when comments are requested, then append normalized comments to the existing batch.

**Tech Stack:** Python 3.11+, Playwright, system Chromium, pytest, existing FastAPI adapter contracts.

## Global Constraints

- No official platform API tokens are available.
- Do not automate CAPTCHA, 2FA, account creation, ban evasion, stealth fingerprint spoofing, or access-control circumvention.
- Use the persisted Chromium profile/cookies already configured for each platform.
- Keep `yt-dlp` as the media and baseline metadata collector.

---

### Task 1: Comment Extraction Helpers

**Files:**
- Create: `app/adapters/browser_comments.py`
- Test: `tests/test_browser_comments.py`

**Interfaces:**
- Produces: `BrowserCommentCollector.collect(url: str, platform: Platform, content_id: str, profile: Path, browser: str, limit: int) -> list[CollectedComment]`
- Produces: pure helper functions for extracting comments from nested JSON and visible DOM text.

- [ ] Write failing tests for nested JSON comment extraction and DOM text parsing.
- [ ] Run tests and confirm they fail because the module does not exist.
- [ ] Implement the minimal helpers and collector shell.
- [ ] Run tests and confirm they pass.

### Task 2: Adapter Integration

**Files:**
- Modify: `app/adapters/browser_ytdlp.py`
- Test: `tests/test_browser_ytdlp_adapters.py`

**Interfaces:**
- Consumes: `BrowserCommentCollector.collect(...)`
- Produces: browser-backed comments appended when `collect_comments` or `COMMENTS` capability is requested and yt-dlp comments are empty.

- [ ] Write failing integration test with a fake collector.
- [ ] Run tests and confirm failure.
- [ ] Add settings-controlled browser comment fallback call.
- [ ] Run targeted tests and confirm pass.

### Task 3: Runtime Configuration and Smoke Test

**Files:**
- Modify: `app/core/config.py`
- Modify: `.env.example`
- Modify: `.env`
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `browser_comment_collection_enabled`, `browser_comment_limit`, and `browser_comment_timeout_seconds`.

- [ ] Add Playwright dependency and install it in the venv.
- [ ] Add runtime settings.
- [ ] Run compile and full tests.
- [ ] Run live smoke tests against known public TikTok/Instagram/Facebook/X content URLs.
