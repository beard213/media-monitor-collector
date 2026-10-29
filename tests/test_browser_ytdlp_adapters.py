from __future__ import annotations

from datetime import datetime, timezone
import json
import hashlib
import sys
from types import SimpleNamespace

import pytest

from app.adapters.facebook import FacebookAdapter
from app.adapters.tiktok import TikTokAdapter
from app.adapters.x import XAdapter
from app.adapters.youtube import YouTubeAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectionBatch, CollectedContent, CollectionRequest, CollectionTarget, CollectorError


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


def test_tiktok_mp4_validation_rejects_subtitle_payload(tmp_path):
    valid = tmp_path / "valid.mp4"
    valid.write_bytes(bytes.fromhex("000000186674797069736f6d00000000"))
    subtitle = tmp_path / "subtitle.mp4"
    subtitle.write_bytes(b"WEBVTT")

    assert TikTokAdapter._is_mp4_file(valid) is True
    assert TikTokAdapter._is_mp4_file(subtitle) is False


def test_tiktok_quality_gate_rejects_known_placeholder_clip(tmp_path, monkeypatch):
    placeholder = tmp_path / "placeholder.mp4"
    placeholder.write_bytes(bytes.fromhex("000000186674797069736f6d00000000"))
    monkeypatch.setattr(TikTokAdapter, "_PLACEHOLDER_VIDEO_SHA256", {hashlib.sha256(placeholder.read_bytes()).hexdigest()})

    assert "placeholder" in TikTokAdapter._video_quality_error(placeholder).lower()


def test_tiktok_keyword_detail_uses_browser_fallback_after_ytdlp_is_blocked(tmp_path, monkeypatch):
    profile = tmp_path / "tiktok-profile"
    profile.mkdir()
    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser_profile = profile
    adapter.browser_search_collector.collect = lambda **_kwargs: ["https://www.tiktok.com/@creatorone/video/123"]

    request = _request(Platform.TIKTOK, Capability.SEARCH_KEYWORD, "KEYWORD", "政治局")
    original_collect = adapter._collect

    def blocked_detail(item):
        if item.target.target_type == "CONTENT":
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "TikTok detail challenged", retryable=False)
        return original_collect(item)

    adapter._collect = blocked_detail
    adapter._collect_content_via_browser = lambda item: CollectionBatch(
        contents=[CollectedContent(platform=Platform.TIKTOK, external_id="123", url=item.target.source_url, title="browser fallback")]
    )

    batch = adapter._collect_keyword(request, profile)

    assert [item.external_id for item in batch.contents] == ["123"]


def test_youtube_surfaces_suppressed_sign_in_error_as_auth_expired(monkeypatch):
    class FakeYoutubeDL:
        def __init__(self, params):
            self.params = params

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            self.params["logger"].error("[youtube] sample: Sign in to confirm you're not a bot")
            return {"entries": []}

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = YouTubeAdapter()
    request = _request(Platform.YOUTUBE, Capability.SEARCH_KEYWORD, "KEYWORD", "sample")

    with pytest.raises(CollectorError) as exc_info:
        adapter.discover(request)

    assert exc_info.value.code == CollectorErrorCode.AUTH_EXPIRED
    assert exc_info.value.credential_action == CredentialAction.REAUTH
    assert "YouTube collection failed" in exc_info.value.message


def test_tiktok_live_resolution_is_marked_live_without_waiting_for_unbounded_download(tmp_path, monkeypatch):
    profile = tmp_path / "tiktok-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser = "chromium"
    adapter.settings.tiktok_browser_profile = profile
    adapter.settings.tiktok_output_root = tmp_path / "tiktok-output"

    request = _request(
        Platform.TIKTOK,
        Capability.LIVE_RESOLVE,
        "CONTENT",
        "https://www.tiktok.com/@creatorone/live",
    )
    batch = adapter.discover(request)

    assert captured["url"] == "https://www.tiktok.com/@creatorone/live"
    assert captured["download"] is False
    assert batch.contents[0].content_type == "LIVE"
    assert batch.contents[0].raw["is_live"] is True


def test_browser_search_extracts_youtube_home_video_links():
    from app.adapters.browser_search import content_links_from_anchors

    links = content_links_from_anchors(
        [
            "https://www.youtube.com/watch?v=abc123&list=ignored",
            "/shorts/short123?feature=share",
            "https://youtu.be/shortlink123?t=1",
            "https://www.youtube.com/channel/UC123",
        ],
        platform=Platform.YOUTUBE,
        limit=5,
    )

    assert links == [
        "https://www.youtube.com/watch?v=abc123",
        "https://www.youtube.com/shorts/short123",
        "https://www.youtube.com/watch?v=shortlink123",
    ]


def test_youtube_adapter_extracts_handle_from_display_account_value():
    adapter = YouTubeAdapter()
    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@wenzhaoofficial").model_copy(
        update={
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="文昭談古論今 -Wen Zhao Official @wenzhaoofficial",
                source_url="https://www.youtube.com/@wenzhaoofficia/videos",
            )
        }
    )

    assert adapter._target_to_url(request) == "https://www.youtube.com/@wenzhaoofficial/videos"


def test_youtube_account_source_root_is_normalized_to_videos_tab():
    adapter = YouTubeAdapter()
    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@wenzhaoofficial").model_copy(
        update={
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="@wenzhaoofficial",
                source_url="https://www.youtube.com/@wenzhaoofficial",
            )
        }
    )

    assert adapter._target_to_url(request) == "https://www.youtube.com/@wenzhaoofficial/videos"


def test_youtube_priority_metadata_probe_returns_video_id_without_media_download(tmp_path, monkeypatch):
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"
    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="@creator",
                source_url="https://www.youtube.com/@creator",
                metadata={
                    "priority_metadata_probe": True,
                    "youtube_incremental_account": True,
                    "youtube_prefer_cached_account": False,
                    "youtube_playlist_end": 1,
                },
            )
        }
    )

    batch = adapter.discover(request)

    assert captured["url"] == "https://www.youtube.com/@creator/videos"
    assert captured["download"] is False
    assert batch.contents[0].external_id == "video-1"
    assert [asset.asset_type for asset in batch.assets] == ["THUMBNAIL"]
    assert batch.raw["metadata_only"] is True


def test_youtube_account_timeline_skips_youtubetab_authcheck(tmp_path, monkeypatch):
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={"collect_comments": True}
    )
    batch = adapter.discover(request)

    assert captured["url"] == "https://www.youtube.com/@creator/videos"
    assert captured["params"]["extractor_args"]["youtubetab"]["skip"] == ["authcheck"]
    assert captured["params"]["extractor_args"]["youtube"]["max_comments"] == [str(adapter.settings.youtube_default_comments)]
    assert batch.contents[0].external_id == "video-1"


def test_youtube_can_skip_browser_cookies_for_public_collection(tmp_path, monkeypatch):
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)
    profile = tmp_path / "youtube-profile"
    profile.mkdir()
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"
    adapter.settings.youtube_browser_profile = profile

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="@creator",
                metadata={"youtube_skip_browser_cookies": True},
            )
        }
    )
    adapter.discover(request)

    assert "cookiesfrombrowser" not in captured["params"]


def test_youtube_account_timeline_allows_playlist_candidate_limit(tmp_path, monkeypatch):
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={
            "limit": 1,
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="@creator",
                metadata={"youtube_playlist_end": 30, "youtube_incremental_account": False},
            ),
        }
    )
    adapter.discover(request)

    assert captured["params"]["playlistend"] == 30


def test_youtube_download_prefers_backend_compatible_formats(tmp_path, monkeypatch):
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"

    request = _request(Platform.YOUTUBE, Capability.CONTENT_DETAIL, "CONTENT", "video-1").model_copy(
        update={"download_media": True}
    )
    adapter.discover(request)

    assert "vcodec^=avc1" in captured["params"]["format"]
    assert "acodec^=mp4a" in captured["params"]["format"]


def test_tiktok_adapter_collects_keyword_search_links(tmp_path, monkeypatch):
    profile = tmp_path / "tiktok-profile"
    profile.mkdir()
    captured = {"browser_urls": []}

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["search_url"] = url
            return [
                "https://www.tiktok.com/@creator/video/1111111111111111111",
                "https://www.tiktok.com/@creator/video/2222222222222222222",
            ]

    adapter = TikTokAdapter()
    adapter.settings.tiktok_enabled = True
    adapter.settings.tiktok_browser = "chromium"
    adapter.settings.tiktok_browser_profile = profile
    adapter.settings.tiktok_output_root = tmp_path / "tiktok-output"
    adapter.browser_search_collector = FakeSearchCollector()

    def browser_detail(request):
        captured["browser_urls"].append(request.target.source_url)
        return CollectionBatch(
            contents=[
                CollectedContent(
                    platform=Platform.TIKTOK,
                    external_id=request.target.source_url.rsplit("/", 1)[-1],
                    url=request.target.source_url,
                    title="Keyword video",
                )
            ]
        )

    adapter._collect_content_via_browser = browser_detail

    batch = adapter.discover(_request(Platform.TIKTOK, Capability.SEARCH_KEYWORD, "KEYWORD", "秦皇岛").model_copy(update={"limit": 2}))

    assert Capability.SEARCH_KEYWORD in adapter.capabilities
    assert captured["search_url"] == "https://www.tiktok.com/search/video?q=%E7%A7%A6%E7%9A%87%E5%B2%9B"
    assert captured["browser_urls"] == [
        "https://www.tiktok.com/@creator/video/1111111111111111111",
        "https://www.tiktok.com/@creator/video/2222222222222222222",
    ]
    assert [content.external_id for content in batch.contents] == ["1111111111111111111", "2222222222222222222"]


def test_x_adapter_collects_keyword_search_links(tmp_path, monkeypatch):
    profile = tmp_path / "x-profile"
    profile.mkdir()
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            return {
                "id": url.rsplit("/", 1)[-1],
                "webpage_url": url,
                "uploader_id": "creator-1",
                "uploader": "Creator One",
                "title": "Keyword post",
                "thumbnail": "https://example.com/thumb.jpg",
            }

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["search_url"] = url
            return [
                "https://x.com/creator/status/1810000000000000000",
                "https://x.com/creator/status/1810000000000000001",
            ]

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = XAdapter()
    adapter.settings.x_enabled = True
    adapter.settings.x_browser = "chromium"
    adapter.settings.x_browser_profile = profile
    adapter.settings.x_output_root = tmp_path / "x-output"
    adapter.browser_search_collector = FakeSearchCollector()

    batch = adapter.discover(_request(Platform.X, Capability.SEARCH_KEYWORD, "KEYWORD", "秦皇岛").model_copy(update={"limit": 2}))

    assert Capability.SEARCH_KEYWORD in adapter.capabilities
    assert captured["search_url"] == "https://x.com/search?q=%E7%A7%A6%E7%9A%87%E5%B2%9B&f=live"
    assert captured["urls"] == [
        "https://x.com/creator/status/1810000000000000000",
        "https://x.com/creator/status/1810000000000000001",
    ]
    assert [content.external_id for content in batch.contents] == ["1810000000000000000", "1810000000000000001"]


def test_x_adapter_keyword_collection_skips_bad_content_link_and_continues(tmp_path, monkeypatch):
    profile = tmp_path / "x-profile"
    profile.mkdir()
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            if url.endswith("1810000000000000000"):
                raise CollectorError(
                    CollectorErrorCode.ACCESS_DENIED,
                    "No video could be found in this tweet",
                    retryable=False,
                    credential_action=CredentialAction.NONE,
                )
            return {
                "id": url.rsplit("/", 1)[-1],
                "webpage_url": url,
                "uploader_id": "creator-1",
                "uploader": "Creator One",
                "title": "Keyword post",
                "thumbnail": "https://example.com/thumb.jpg",
            }

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            return [
                "https://x.com/creator/status/1810000000000000000",
                "https://x.com/creator/status/1810000000000000001",
            ]

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = XAdapter()
    adapter.settings.x_enabled = True
    adapter.settings.x_browser = "chromium"
    adapter.settings.x_browser_profile = profile
    adapter.settings.x_output_root = tmp_path / "x-output"
    adapter.browser_search_collector = FakeSearchCollector()

    batch = adapter.discover(_request(Platform.X, Capability.SEARCH_KEYWORD, "KEYWORD", "秦皇岛").model_copy(update={"limit": 2}))

    assert captured["urls"] == [
        "https://x.com/creator/status/1810000000000000000",
        "https://x.com/creator/status/1810000000000000001",
    ]
    assert [content.external_id for content in batch.contents] == ["1810000000000000001"]


def test_x_adapter_maps_ytdlp_timestamp_to_published_at(tmp_path, monkeypatch):
    profile = tmp_path / "x-profile"
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
                "id": "1810000000000000001",
                "webpage_url": url,
                "uploader_id": "creator-1",
                "uploader": "Creator One",
                "title": "Keyword post",
                "timestamp": 1_700_000_000,
            }

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = XAdapter()
    adapter.settings.x_enabled = True
    adapter.settings.x_browser = "chromium"
    adapter.settings.x_browser_profile = profile
    adapter.settings.x_output_root = tmp_path / "x-output"

    batch = adapter.discover(_request(Platform.X, Capability.CONTENT_DETAIL, "CONTENT", "https://x.com/creator/status/1810000000000000001"))

    assert batch.contents[0].published_at == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)


def test_facebook_adapter_numeric_account_target_becomes_profile_id_url(tmp_path, monkeypatch):
    profile = tmp_path / "facebook-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    adapter = FacebookAdapter()
    adapter.settings.facebook_enabled = True
    adapter.settings.facebook_browser = "chromium"
    adapter.settings.facebook_browser_profile = profile
    adapter.settings.facebook_output_root = tmp_path / "facebook-output"

    adapter.discover(_request(Platform.FACEBOOK, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "61590818619292"))

    assert captured["url"] == "https://www.facebook.com/profile.php?id=61590818619292"


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


def test_facebook_live_capture_forces_a_fresh_bounded_clip(tmp_path):
    profile = tmp_path / "facebook-profile"
    profile.mkdir()
    adapter = FacebookAdapter()
    request = _request(
        Platform.FACEBOOK,
        Capability.LIVE_RESOLVE,
        "CONTENT",
        "https://www.facebook.com/creator/videos/1230923098438186",
    ).model_copy(update={"download_media": True})
    request.target.metadata["liveClipSeconds"] = 15

    params = adapter._ydl_params(request, tmp_path / "facebook-output", profile)

    assert params["overwrites"] is True
    assert params["continuedl"] is False
    assert params["external_downloader"] == "ffmpeg"
    assert "worstvideo" in params["format"]
    assert params["external_downloader_args"]["ffmpeg_o"] == ["-t", "15"]
    assert callable(params["download_ranges"])


def test_browser_adapter_preserves_live_viewer_count():
    content = FacebookAdapter()._content_from_item(
        {
            "id": "1230923098438186",
            "webpage_url": "https://www.facebook.com/creator/videos/1230923098438186",
            "title": "Facebook Live",
            "concurrent_view_count": 112,
        },
        "creator",
    )

    assert content.raw["concurrent_view_count"] == 112


def test_facebook_marks_expired_vod_as_ended_when_live_url_is_stale():
    import time

    content = CollectedContent(
        platform=Platform.FACEBOOK,
        external_id="ended-live",
        raw={
            "timestamp": time.time() - 600,
            "duration": 120,
            "concurrent_view_count": 0,
        },
    )

    FacebookAdapter._mark_expired_replay(content)

    assert content.raw["is_live"] is False
    assert content.raw["live_status"] == "ended"
    assert content.raw["end_detection"] == "facebook_vod_duration_elapsed"


def test_facebook_marks_expired_vod_as_ended_even_with_stale_viewers():
    import time

    content = CollectedContent(
        platform=Platform.FACEBOOK,
        external_id="ended-live-with-stale-viewers",
        raw={
            "timestamp": time.time() - 600,
            "duration": 120,
            # Facebook can retain the last live viewer total briefly after end.
            "concurrent_view_count": 242,
        },
    )

    FacebookAdapter._mark_expired_replay(content)

    assert content.raw["is_live"] is False
    assert content.raw["live_status"] == "ended"
    assert content.raw["end_detection"] == "facebook_vod_duration_elapsed"


def test_facebook_adapter_keyword_target_becomes_search_url(tmp_path, monkeypatch):
    profile = tmp_path / "facebook-profile"
    profile.mkdir()
    captured = {}
    _install_fake_ytdlp(monkeypatch, captured)

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["search_url"] = url
            return ["https://www.facebook.com/watch/?v=1230923098438186"]

    adapter = FacebookAdapter()
    adapter.settings.facebook_enabled = True
    adapter.settings.facebook_browser = "chromium"
    adapter.settings.facebook_browser_profile = profile
    adapter.settings.facebook_output_root = tmp_path / "facebook-output"
    adapter.browser_search_collector = FakeSearchCollector()

    batch = adapter.discover(_request(Platform.FACEBOOK, Capability.SEARCH_KEYWORD, "KEYWORD", "秦皇岛"))

    assert Capability.SEARCH_KEYWORD in adapter.capabilities
    assert captured["search_url"] == "https://www.facebook.com/search/videos/?q=%E7%A7%A6%E7%9A%87%E5%B2%9B"
    assert captured["url"] == "https://www.facebook.com/watch/?v=1230923098438186"
    assert batch.contents[0].platform == Platform.FACEBOOK


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
    captured = {}

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
        def collect(self, *, url, platform, content_id, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["cdp_url"] = cdp_url
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
    adapter.settings.tiktok_browser_cdp_url = "http://127.0.0.1:9223"
    adapter.browser_comment_collector = FakeCollector()

    batch = adapter.collect_comments(
        _request(Platform.TIKTOK, Capability.COMMENTS, "CONTENT", "video-1").model_copy(update={"collect_comments": True})
    )

    assert batch.comments[0].external_id == "browser-comment-1"
    assert batch.comments[0].text == "from browser"
    assert captured["cdp_url"] == "http://127.0.0.1:9223"


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



def test_youtube_account_uses_historical_cache_before_network(tmp_path, monkeypatch):
    class FailingYoutubeDL:
        def __init__(self, params):
            raise AssertionError("network extractor should not be constructed when account cache is available")

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FailingYoutubeDL))
    monkeypatch.chdir(tmp_path)
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "data" / "storage" / "youtube"
    cache_dir = adapter.settings.youtube_output_root / "old-run" / "cached-video"
    cache_dir.mkdir(parents=True)
    (cache_dir / "cached-video.mp4").write_bytes(b"video")
    (cache_dir / "cached-video-thumbnail.jpg").write_bytes(b"thumb")
    summary_dir = tmp_path / "data" / "pipeline-runs" / "account-old"
    summary_dir.mkdir(parents=True)
    (summary_dir / "summary.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "platform": "YOUTUBE",
                        "account": "@wenzhaoofficial",
                        "backend_sync": {"items": [{"sourceVideoId": "cached-video"}]},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@wenzhaoofficial").model_copy(
        update={"download_media": True, "limit": 1}
    )

    batch = adapter.discover(request)

    assert [content.external_id for content in batch.contents] == ["cached-video"]
    assert len(batch.assets) == 2
    assert batch.raw["account_cache_fast_path"] is True

def test_youtube_account_download_reuses_cached_assets_before_detail_download(tmp_path, monkeypatch):
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            captured.setdefault("params", []).append(params)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            return {
                "entries": [
                    {"id": "cached-video", "url": "cached-video", "title": "Cached video", "channel_id": "creator-1"}
                ]
            }

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"
    cache_dir = adapter.settings.youtube_output_root / "old-run" / "cached-video"
    cache_dir.mkdir(parents=True)
    video = cache_dir / "cached-video-collector-compatible.mp4"
    thumb = cache_dir / "cached-video-thumbnail.jpg"
    video.write_bytes(b"video")
    thumb.write_bytes(b"thumb")

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={"download_media": True}
    )
    batch = adapter.discover(request)

    assert captured["urls"] == ["https://www.youtube.com/@creator/videos"]
    assert captured["params"][0]["extract_flat"] == "in_playlist"
    assert batch.contents[0].external_id == "cached-video"
    assert {asset.asset_type for asset in batch.assets} == {"VIDEO", "THUMBNAIL"}
    assert batch.assets[0].local_path != str(video)
    assert batch.assets[0].local_path.endswith("cached-video.mp4")


def test_youtube_cached_account_runs_opted_in_comment_fallback(tmp_path, monkeypatch):
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            return {"entries": [{"id": "cached-comment-video", "url": "cached-comment-video", "title": "Cached comment video", "channel_id": "creator-1"}]}

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"
    cache_dir = adapter.settings.youtube_output_root / "old-run" / "cached-comment-video"
    cache_dir.mkdir(parents=True)
    (cache_dir / "cached-comment-video.mp4").write_bytes(b"video")
    (cache_dir / "cached-comment-video-thumbnail.jpg").write_bytes(b"thumb")
    called = []
    monkeypatch.setattr(adapter, "_add_youtube_browser_comment_fallback", lambda batch, request: called.append([content.external_id for content in batch.contents]))

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={"download_media": True, "collect_comments": True}
    )
    adapter.discover(request)

    assert captured["urls"] == ["https://www.youtube.com/@creator/videos"]
    assert called == [["cached-comment-video"]]


def test_youtube_account_download_keeps_candidate_metadata_when_details_fail(tmp_path, monkeypatch):
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            if url.endswith("/videos"):
                return {"entries": [{"id": "candidate-video", "url": "candidate-video", "title": "Candidate"}]}
            raise RuntimeError("Sign in to confirm you’re not a bot")

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={"download_media": True}
    )
    batch = adapter.discover(request)

    assert [content.external_id for content in batch.contents] == ["candidate-video"]
    assert batch.assets == []
    assert batch.raw["media_error"]
    assert batch.raw["skipped"][0]["videoId"] == "candidate-video"


def test_youtube_incremental_account_oversamples_candidates_when_some_fail(tmp_path, monkeypatch):
    captured = {"urls": []}

    class FakeYoutubeDL:
        def __init__(self, params):
            captured.setdefault("params", []).append(params)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, download):
            captured["urls"].append(url)
            if url.endswith("/videos"):
                return {"entries": [
                    {"id": "cached-one", "url": "cached-one", "title": "Cached one"},
                    {"id": "blocked-two", "url": "blocked-two", "title": "Blocked two"},
                    {"id": "cached-three", "url": "cached-three", "title": "Cached three"},
                ]}
            raise RuntimeError("Sign in to confirm you’re not a bot")

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = YouTubeAdapter()
    adapter.settings.youtube_output_root = tmp_path / "youtube-output"
    for video_id in ["cached-one", "cached-three"]:
        cache_dir = adapter.settings.youtube_output_root / "old-run" / video_id
        cache_dir.mkdir(parents=True)
        (cache_dir / f"{video_id}.mp4").write_bytes(b"video")
        (cache_dir / f"{video_id}-thumbnail.jpg").write_bytes(b"thumb")

    request = _request(Platform.YOUTUBE, Capability.ACCOUNT_TIMELINE, "ACCOUNT", "@creator").model_copy(
        update={
            "download_media": True,
            "limit": 2,
            "target": CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="ACCOUNT",
                target_value="@creator",
                metadata={"youtube_try_uncached_details": True, "youtube_download_pacing_seconds": 0},
            ),
        }
    )
    batch = adapter.discover(request)

    assert captured["params"][0]["playlistend"] == 4
    # A blocked newest item remains in the result as metadata-only content;
    # it must not be silently dropped as if the account had no update.
    assert [content.external_id for content in batch.contents] == ["cached-one", "blocked-two"]
    assert len(batch.assets) == 2
    assert batch.contents[1].raw["media_error"].startswith("AUTH_REQUIRED:")
