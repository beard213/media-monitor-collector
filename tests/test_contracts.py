from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.account_pool.policy import decide_credential_health, utcnow
from app.adapters.facebook import FacebookAdapter
from app.adapters.tiktok import TikTokAdapter
from app.adapters.registry import build_registry
from app.adapters.instagram import InstagramAdapter
from app.adapters.youtube import YouTubeAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialStatus, CredentialType, Platform
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError, CredentialLease, CredentialRef
from app.main import app
from app.scheduler.contracts import CollectionJobCreate, KeywordMonitorRunCreate


def test_all_target_platforms_are_registered():
    registry = build_registry()
    assert set(registry) == {
        Platform.YOUTUBE,
        Platform.TIKTOK,
        Platform.FACEBOOK,
        Platform.X,
        Platform.INSTAGRAM,
    }
    assert isinstance(registry[Platform.YOUTUBE], YouTubeAdapter)
    assert isinstance(registry[Platform.TIKTOK], TikTokAdapter)
    assert isinstance(registry[Platform.FACEBOOK], FacebookAdapter)
    assert isinstance(registry[Platform.INSTAGRAM], InstagramAdapter)


def test_browser_profile_platforms_expose_comment_capability():
    registry = build_registry()

    assert Capability.COMMENTS in registry[Platform.TIKTOK].capabilities
    assert Capability.COMMENTS in registry[Platform.FACEBOOK].capabilities
    assert Capability.COMMENTS in registry[Platform.INSTAGRAM].capabilities
    assert Capability.COMMENTS in registry[Platform.X].capabilities


def test_collection_job_limit_defaults_to_10_and_rejects_above_10():
    request = CollectionJobCreate(
        platform=Platform.YOUTUBE,
        capability=Capability.SEARCH_KEYWORD,
        target_type="ACCOUNT",
        target_value="demo-account",
    )

    assert request.limit == 10

    with pytest.raises(ValidationError):
        CollectionJobCreate(
            platform=Platform.YOUTUBE,
            capability=Capability.SEARCH_KEYWORD,
            target_type="ACCOUNT",
            target_value="demo-account",
            limit=11,
        )


def test_keyword_monitor_limit_defaults_to_10_and_rejects_above_10():
    request = KeywordMonitorRunCreate()

    assert request.limit_per_keyword == 10

    with pytest.raises(ValidationError):
        KeywordMonitorRunCreate(limit_per_keyword=11)


def test_collection_request_limit_defaults_to_10_and_rejects_above_10():
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="ACCOUNT", target_value="demo-account"),
    )

    assert request.limit == 10

    with pytest.raises(ValidationError):
        CollectionRequest(
            job_id="test-job",
            platform=Platform.YOUTUBE,
            capability=Capability.SEARCH_KEYWORD,
            target=CollectionTarget(platform=Platform.YOUTUBE, target_type="ACCOUNT", target_value="demo-account"),
            limit=11,
        )


def test_rate_limit_moves_credential_to_cooldown():
    now = utcnow()
    decision = decide_credential_health(
        current_status=CredentialStatus.ACTIVE,
        error_code=CollectorErrorCode.RATE_LIMITED,
        consecutive_failures=1,
        now=now,
    )

    assert decision.next_status == CredentialStatus.COOLDOWN
    assert decision.cooldown_until is not None
    assert decision.cooldown_until >= now + timedelta(minutes=29)


def test_auth_expired_requires_reauth():
    decision = decide_credential_health(
        current_status=CredentialStatus.ACTIVE,
        error_code=CollectorErrorCode.AUTH_EXPIRED,
        consecutive_failures=1,
    )

    assert decision.next_status == CredentialStatus.NEEDS_REAUTH


def test_youtube_dry_run_collection_api_returns_normalized_batch():
    client = TestClient(app)

    response = client.post(
        "/collections/run",
        json={
            "platform": "YOUTUBE",
            "capability": "CONTENT_DETAIL",
            "target_type": "CONTENT",
            "target_value": "dQw4w9WgXcQ",
            "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "limit": 1,
            "collect_comments": True,
            "download_media": False,
            "dry_run": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["platform"] == "YOUTUBE"
    assert payload["counts"] == {
        "accounts": 1,
        "contents": 1,
        "comments": 1,
        "assets": 0,
    }
    assert payload["batch"]["contents"][0]["platform"] == "YOUTUBE"


def test_keyword_monitor_api_runs_active_keyword_workflow(monkeypatch):
    import app.api.collections as collections_api

    seen = {}

    def fake_run_keyword_monitor(*, platforms, limit_per_keyword, collect_comments, download_media, dry_run, metadata):
        seen["platforms"] = platforms
        seen["limit_per_keyword"] = limit_per_keyword
        seen["collect_comments"] = collect_comments
        seen["download_media"] = download_media
        seen["dry_run"] = dry_run
        seen["metadata"] = metadata
        return {
            "platforms": ["YOUTUBE"],
            "keywords_total": 1,
            "keywords_collected": 1,
            "contents_synced": 1,
            "items": [{"keyword_id": 12, "keyword": "示例关键词"}],
            "skipped": [],
        }

    monkeypatch.setattr(collections_api, "run_keyword_monitor", fake_run_keyword_monitor, raising=False)
    client = TestClient(app)

    response = client.post(
        "/collections/keywords/run",
        json={
            "platforms": ["YOUTUBE"],
            "limit_per_keyword": 2,
            "collect_comments": True,
            "download_media": True,
            "dry_run": False,
            "metadata": {"max_comments": 5},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["keywords_total"] == 1
    assert payload["contents_synced"] == 1
    assert seen["platforms"] == [Platform.YOUTUBE]
    assert seen["limit_per_keyword"] == 2
    assert seen["collect_comments"] is True
    assert seen["download_media"] is True
    assert seen["dry_run"] is False
    assert seen["metadata"] == {"max_comments": 5}


def test_youtube_adapter_uses_browser_profile_credential(tmp_path):
    profile = tmp_path / "chrome-profile"
    profile.mkdir()
    adapter = YouTubeAdapter()
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.CONTENT_DETAIL,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="dQw4w9WgXcQ",
        ),
        limit=1,
        collect_comments=False,
        download_media=False,
    )
    lease = CredentialLease(
        lease_id="lease-1",
        credential=CredentialRef(
            credential_id="youtube-1",
            platform=Platform.YOUTUBE,
            credential_type=CredentialType.BROWSER_PROFILE,
            secret_ref=str(profile),
            metadata={"browser": "chrome"},
        ),
        worker_id="worker-1",
        expires_at=utcnow() + timedelta(minutes=30),
    )

    params = adapter._ydl_params(request, lease, tmp_path / "output")

    assert params["cookiesfrombrowser"] == ("chrome", str(profile), None, None)


def test_youtube_adapter_prefers_exported_cookie_pool_over_browser_profile(tmp_path):
    profile = tmp_path / "chrome-profile"
    profile.mkdir()
    cookie_file = tmp_path / "youtube.netscape"
    cookie_file.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")
    adapter = YouTubeAdapter()
    adapter.settings.youtube_browser = "chromium"
    adapter.settings.youtube_browser_profile = profile
    adapter.settings.youtube_cookie_file = cookie_file
    adapter.settings.youtube_cookie_dir = tmp_path / "cookie-pool"
    adapter.settings.youtube_prefer_browser_cookies = False
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.CONTENT_DETAIL,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="dQw4w9WgXcQ",
        ),
        limit=1,
        collect_comments=False,
        download_media=True,
    )

    params = adapter._ydl_params(request, None, tmp_path / "output")
    health = adapter.healthcheck()

    assert params["cookiefile"] == str(cookie_file.resolve())
    assert "cookiesfrombrowser" not in params
    assert "cookie_pool=1" in health.message


def test_youtube_adapter_can_prefer_refreshed_browser_profile_over_exported_cookie(tmp_path):
    profile = tmp_path / "chrome-profile"
    profile.mkdir()
    cookie_file = tmp_path / "youtube.netscape"
    cookie_file.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")
    adapter = YouTubeAdapter()
    adapter.settings.youtube_browser = "chromium"
    adapter.settings.youtube_browser_profile = profile
    adapter.settings.youtube_cookie_file = cookie_file
    adapter.settings.youtube_cookie_dir = tmp_path / "cookie-pool"
    adapter.settings.youtube_prefer_browser_cookies = True
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.CONTENT_DETAIL,
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="CONTENT", target_value="dQw4w9WgXcQ"),
        limit=1,
        collect_comments=False,
        download_media=False,
    )

    params = adapter._ydl_params(request, None, tmp_path / "output")

    assert params["cookiesfrombrowser"] == ("chromium", str(profile), None, None)
    assert "cookiefile" not in params


def test_youtube_cookie_pool_rotates_after_rate_limit_cooldown(tmp_path):
    pool = tmp_path / "cookie-pool"
    pool.mkdir()
    primary = pool / "youtube_02.netscape"
    fallback = pool / "youtube_03.netscape"
    primary.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")
    fallback.write_text("# Netscape HTTP Cookie File\n", encoding="utf-8")
    adapter = YouTubeAdapter()
    adapter.settings.youtube_cookie_file = primary
    adapter.settings.youtube_cookie_dir = pool
    adapter.settings.youtube_cookie_pool_cooldown_seconds = 900
    adapter._cookie_pool_cursor = 0
    adapter._cookie_pool_cooldown_until.clear()

    assert adapter._select_cookie_from_pool() == primary.resolve()
    adapter._cool_down_cookie_after_error(
        primary,
        CollectorError(CollectorErrorCode.RATE_LIMITED, "rate limited", retryable=True),
    )

    assert adapter._select_cookie_from_pool() == fallback.resolve()


def test_youtube_adapter_ignores_empty_browser_profile_path():
    adapter = YouTubeAdapter()
    adapter.settings.youtube_browser_profile = None

    assert adapter._configured_browser_profile(None) is None


def test_youtube_adapter_enables_js_challenge_runtime_for_youtube(tmp_path):
    deno = tmp_path / "deno"
    deno.write_text("#!/bin/sh\n")
    adapter = YouTubeAdapter()
    adapter.settings.youtube_js_runtime = str(deno)
    adapter.settings.youtube_remote_components = "ejs:github"
    adapter.settings.youtube_disable_plugins = True
    adapter.settings.youtube_po_token_enabled = False
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="66dzSA4XSqw",
        ),
        limit=1,
        collect_comments=False,
        download_media=True,
    )

    params = adapter._ydl_params(request, None, tmp_path / "output")

    assert params["plugin_dirs"] == []
    assert params["js_runtimes"] == {"deno": {"path": str(deno)}}
    assert params["remote_components"] == ["ejs:github"]

    adapter.settings.youtube_browser_profile = ""

    assert adapter._configured_browser_profile(None) is None


def test_youtube_keyword_target_uses_supported_ytdlp_search_scheme():
    adapter = YouTubeAdapter()
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="KEYWORD", target_value="北戴河"),
        limit=3,
        collect_comments=False,
        download_media=False,
    )

    assert adapter._target_to_url(request) == "ytsearch3:北戴河"

    adapter.settings.youtube_browser_profile = Path(".")

    assert adapter._configured_browser_profile(None) is None


def test_youtube_live_resolve_download_is_bounded_by_default(tmp_path):
    adapter = YouTubeAdapter()
    adapter.settings.youtube_live_clip_seconds = 45
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="https://www.youtube.com/watch?v=refmlUAQssM",
        ),
        limit=1,
        collect_comments=False,
        download_media=True,
    )

    params = adapter._ydl_params(request, None, tmp_path / "output")
    ranges = tuple(params["download_ranges"]({"id": "refmlUAQssM"}, None))

    assert ranges == ({"start_time": 0, "end_time": 45.0},)
    assert params["force_keyframes_at_cuts"] is True


def test_youtube_live_resolve_download_can_disable_bounded_clip(tmp_path):
    adapter = YouTubeAdapter()
    adapter.settings.youtube_live_clip_seconds = 60
    request = CollectionRequest(
        job_id="test-job",
        platform=Platform.YOUTUBE,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="https://www.youtube.com/watch?v=refmlUAQssM",
            metadata={"youtube_live_clip_seconds": 0},
        ),
        limit=1,
        collect_comments=False,
        download_media=True,
    )

    params = adapter._ydl_params(request, None, tmp_path / "output")

    assert "download_ranges" not in params
