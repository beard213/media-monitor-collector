from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app.account_pool.policy import decide_credential_health, utcnow
from app.adapters.facebook import FacebookAdapter
from app.adapters.tiktok import TikTokAdapter
from app.adapters.registry import build_registry
from app.adapters.instagram import InstagramAdapter
from app.adapters.youtube import YouTubeAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialStatus, CredentialType, Platform
from app.domain.models import CollectionRequest, CollectionTarget, CredentialLease, CredentialRef
from app.main import app


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

    adapter.settings.youtube_browser_profile = Path(".")

    assert adapter._configured_browser_profile(None) is None
