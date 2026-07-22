from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from app.adapters.facebook import FacebookAdapter
from app.adapters.tiktok import TikTokAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError


def _request(platform: Platform, capability: Capability, target_type: str, target_value: str) -> CollectionRequest:
    return CollectionRequest(
        job_id="job-1",
        platform=platform,
        capability=capability,
        target=CollectionTarget(platform=platform, target_type=target_type, target_value=target_value),
        limit=1,
        collect_comments=False,
        download_media=False,
    )


def _install_fake_ytdlp(monkeypatch: pytest.MonkeyPatch, captured: dict) -> None:
    class FakeYoutubeDL:
        def __init__(self, params):
            captured["params"] = params

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["url"] = url
            captured["download"] = download
            return {
                "entries": [
                    {
                        "id": "video-1",
                        "webpage_url": url,
                        "uploader_id": "creator-1",
                        "uploader": "Creator One",
                        "channel": "creatorone",
                        "channel_url": "https://example.com/creatorone",
                        "title": "Sample video",
                        "description": "Sample description",
                        "timestamp": 1_700_000_000,
                        "duration": 12,
                        "view_count": 100,
                        "like_count": 10,
                        "comment_count": 2,
                        "thumbnail": "https://example.com/thumb.jpg",
                        "comments": [
                            {
                                "id": "comment-1",
                                "author": "viewer",
                                "author_id": "viewer-1",
                                "text": "hello",
                                "timestamp": 1_700_000_100,
                                "like_count": 3,
                            }
                        ],
                    }
                ]
            }

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))


def test_tiktok_adapter_collects_with_configured_browser_profile(tmp_path, monkeypatch):
    profile = tmp_path / "tiktok-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser = "chromium"
    adapter.settings.tiktok_browser_profile = profile
    adapter.settings.tiktok_output_root = tmp_path / "tiktok-output"

    batch = adapter.discover(_request(Platform.TIKTOK, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "creatorone"))

    assert captured["url"] == "https://www.tiktok.com/@creatorone"
    assert captured["download"] is False
    assert captured["params"]["cookiesfrombrowser"] == ("chromium", str(profile), None, None)
    assert captured["params"]["simulate"] is True
    assert batch.accounts[0].platform == Platform.TIKTOK
    assert batch.accounts[0].external_id == "creator-1"
    assert batch.contents[0].platform == Platform.TIKTOK
    assert batch.contents[0].external_id == "video-1"
    assert batch.assets[0].asset_type == "THUMBNAIL"


def test_facebook_adapter_account_target_becomes_profile_url(tmp_path, monkeypatch):
    profile = tmp_path / "facebook-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    adapter = FacebookAdapter()
    adapter.settings.facebook_enabled = True
    adapter.settings.facebook_browser = "chromium"
    adapter.settings.facebook_browser_profile = profile
    adapter.settings.facebook_output_root = tmp_path / "facebook-output"

    batch = adapter.discover(_request(Platform.FACEBOOK, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "creatorone"))

    assert captured["url"] == "https://www.facebook.com/creatorone"
    assert captured["params"]["cookiesfrombrowser"] == ("chromium", str(profile), None, None)
    assert batch.accounts[0].platform == Platform.FACEBOOK
    assert batch.contents[0].content_type == "VIDEO"


@pytest.mark.parametrize(
    ("adapter_cls", "platform", "target_url", "target_value"),
    [
        (TikTokAdapter, Platform.TIKTOK, "https://www.tiktok.com/@creatorone/video/123", "123"),
        (FacebookAdapter, Platform.FACEBOOK, "https://www.facebook.com/facebook/videos/1230923098438186/", "1230923098438186"),
    ],
)
def test_browser_adapter_collects_comments_when_requested(tmp_path, monkeypatch, adapter_cls, platform, target_url, target_value):
    profile = tmp_path / f"{platform.value.lower()}-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    adapter = adapter_cls()
    setattr(adapter.settings, f"{platform.value.lower()}_enabled", True)
    setattr(adapter.settings, f"{platform.value.lower()}_browser", "chromium")
    setattr(adapter.settings, f"{platform.value.lower()}_browser_profile", profile)
    setattr(adapter.settings, f"{platform.value.lower()}_output_root", tmp_path / f"{platform.value.lower()}-output")

    batch = adapter.collect_comments(
        _request(platform, Capability.COMMENTS, "CONTENT", target_value).model_copy(
            update={
                "collect_comments": True,
                "target": CollectionTarget(platform=platform, target_type="CONTENT", target_value=target_value, source_url=target_url),
            }
        )
    )

    assert captured["params"]["getcomments"] is True
    assert batch.comments[0].external_id == "comment-1"
    assert batch.comments[0].author_external_id == "viewer-1"
    assert batch.comments[0].text == "hello"
    assert batch.comments[0].content_external_id == batch.contents[0].external_id


def test_browser_adapter_falls_back_to_browser_comments(tmp_path, monkeypatch):
    from app.domain.models import CollectedComment

    profile = tmp_path / "tiktok-profile"
    profile.mkdir()

    class FakeYoutubeDL:
        def __init__(self, params):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            return {
                "id": "video-1",
                "webpage_url": url,
                "uploader_id": "creator-1",
                "uploader": "Creator One",
                "title": "Sample video",
                "thumbnail": "https://example.com/thumb.jpg",
            }

    class FakeCollector:
        def collect(self, *, url, platform, content_id, profile, browser, limit, timeout_seconds):
            return [
                CollectedComment(
                    platform=platform,
                    external_id="browser-comment-1",
                    content_external_id=content_id,
                    author_external_id="viewer-1",
                    text="from browser",
                )
            ]

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser = "chromium"
    adapter.settings.tiktok_browser_profile = profile
    adapter.settings.tiktok_output_root = tmp_path / "tiktok-output"
    adapter.settings.browser_comment_collection_enabled = True
    adapter.browser_comment_collector = FakeCollector()

    batch = adapter.collect_comments(
        _request(Platform.TIKTOK, Capability.COMMENTS, "CONTENT", "video-1").model_copy(update={"collect_comments": True})
    )

    assert batch.comments[0].external_id == "browser-comment-1"
    assert batch.comments[0].text == "from browser"


def test_browser_profile_healthcheck_requires_enabled_platform_and_profile(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace())
    profile = tmp_path / "facebook-profile"
    profile.mkdir()

    adapter = FacebookAdapter()
    adapter.settings.facebook_enabled = True
    adapter.settings.facebook_browser = "chromium"
    adapter.settings.facebook_browser_profile = profile

    health = adapter.healthcheck()

    assert health.ok is True
    assert health.mode == "yt-dlp"
    assert f"browser_profile=chromium:{profile}" == health.message


def test_missing_browser_profile_requires_reauth(monkeypatch):
    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=object))
    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser_profile = None

    with pytest.raises(CollectorError) as exc_info:
        adapter.discover(_request(Platform.TIKTOK, Capability.CONTENT_DETAIL, "CONTENT", "7240000000000000000"))

    assert exc_info.value.code == CollectorErrorCode.AUTH_EXPIRED
    assert exc_info.value.credential_action == CredentialAction.REAUTH
