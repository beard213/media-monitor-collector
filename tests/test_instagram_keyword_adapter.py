from __future__ import annotations

import sys
from types import SimpleNamespace

from app.adapters.instagram import InstagramAdapter
from app.domain.enums import Capability, CollectorErrorCode, Platform
from app.domain.models import CollectedComment, CollectedContent, CollectionBatch, CollectionRequest, CollectionTarget, CollectorError


def test_instagram_adapter_collects_keyword_search_links(tmp_path, monkeypatch):
    profile = tmp_path / "instagram-profile"
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
                "id": url.strip("/").rsplit("/", 1)[-1],
                "webpage_url": url,
                "uploader_id": "creator-1",
                "uploader": "creator",
                "title": "Instagram keyword reel",
                "thumbnail": "https://example.com/thumb.jpg",
            }

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["search_url"] = url
            return [
                "https://www.instagram.com/reel/DW9XXwFjDvK",
                "https://www.instagram.com/p/ABC123def45",
            ]

    monkeypatch.setitem(sys.modules, "yt_dlp", SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    adapter = InstagramAdapter()
    adapter.settings.instagram_enabled = True
    adapter.settings.instagram_browser = "chromium"
    adapter.settings.instagram_browser_profile = profile
    adapter.settings.instagram_output_root = tmp_path / "instagram-output"
    adapter.browser_search_collector = FakeSearchCollector()
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.INSTAGRAM,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.INSTAGRAM, target_type="KEYWORD", target_value="秦皇岛"),
        limit=2,
        collect_comments=False,
        download_media=False,
    )

    batch = adapter.discover(request)

    assert Capability.SEARCH_KEYWORD in adapter.capabilities
    assert captured["search_url"] == "https://www.instagram.com/explore/tags/%E7%A7%A6%E7%9A%87%E5%B2%9B/"
    assert captured["urls"] == [
        "https://www.instagram.com/reel/DW9XXwFjDvK",
        "https://www.instagram.com/p/ABC123def45",
    ]
    assert [content.external_id for content in batch.contents] == ["DW9XXwFjDvK", "ABC123def45"]


def test_instagram_adapter_keyword_collection_skips_bad_content_link_and_continues(tmp_path, monkeypatch):
    profile = tmp_path / "instagram-profile"
    profile.mkdir()
    captured = {"urls": []}

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            return [
                "https://www.instagram.com/reel/bad",
                "https://www.instagram.com/reel/good",
            ]

    adapter = InstagramAdapter()
    adapter.settings.instagram_enabled = True
    adapter.settings.instagram_browser = "chromium"
    adapter.settings.instagram_browser_profile = profile
    adapter.settings.instagram_output_root = tmp_path / "instagram-output"
    adapter.browser_search_collector = FakeSearchCollector()

    def fake_collect(request):
        captured["urls"].append(request.target.target_value)
        if request.target.target_value.endswith("/bad"):
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "no video formats", retryable=False)
        return adapter._batch_from_info(
            {
                "id": "good",
                "webpage_url": request.target.target_value,
                "uploader_id": "creator-1",
                "uploader": "creator",
                "title": "collectable reel",
                "thumbnail": "https://example.com/thumb.jpg",
            },
            request,
        )

    monkeypatch.setattr(adapter, "_collect", fake_collect)
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.INSTAGRAM,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.INSTAGRAM, target_type="KEYWORD", target_value="秦皇岛"),
        limit=2,
        collect_comments=False,
        download_media=False,
    )

    batch = adapter._collect_keyword(request, profile)

    assert captured["urls"] == [
        "https://www.instagram.com/reel/bad",
        "https://www.instagram.com/reel/good",
    ]
    assert [content.external_id for content in batch.contents] == ["good"]


def test_instagram_keyword_overfetches_reels_until_limit_and_merges_comments(tmp_path, monkeypatch):
    profile = tmp_path / "instagram-profile"
    profile.mkdir()
    captured = {"urls": [], "search_limit": None}

    class FakeSearchCollector:
        def collect(self, *, url, platform, profile, browser, limit, timeout_seconds, cdp_url=None):
            captured["search_limit"] = limit
            return [
                "https://www.instagram.com/p/photo-post",
                "https://www.instagram.com/reel/good-1",
                "https://www.instagram.com/reel/good-2",
            ]

    adapter = InstagramAdapter()
    adapter.settings.instagram_enabled = True
    adapter.settings.instagram_browser = "chromium"
    adapter.settings.instagram_browser_profile = profile
    adapter.settings.instagram_output_root = tmp_path / "instagram-output"
    adapter.browser_search_collector = FakeSearchCollector()

    def fake_collect(request):
        content_id = request.target.target_value.rstrip("/").rsplit("/", 1)[-1]
        captured["urls"].append(request.target.target_value)
        return CollectionBatch(
            contents=[
                CollectedContent(
                    platform=Platform.INSTAGRAM,
                    external_id=content_id,
                    url=request.target.target_value,
                    author_external_id=f"creator-{content_id}",
                )
            ],
            comments=[
                CollectedComment(
                    platform=Platform.INSTAGRAM,
                    external_id=f"comment-{content_id}",
                    content_external_id=content_id,
                    author_external_id=f"viewer-{content_id}",
                    text="comment text",
                )
            ],
        )

    monkeypatch.setattr(adapter, "_collect", fake_collect)
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.INSTAGRAM,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.INSTAGRAM, target_type="KEYWORD", target_value="领导人"),
        limit=2,
        collect_comments=True,
        download_media=True,
    )

    batch = adapter._collect_keyword(request, profile)

    assert captured["search_limit"] == 10
    assert captured["urls"] == [
        "https://www.instagram.com/reel/good-1",
        "https://www.instagram.com/reel/good-2",
    ]
    assert [content.external_id for content in batch.contents] == ["good-1", "good-2"]
    assert [comment.author_external_id for comment in batch.comments] == ["viewer-good-1", "viewer-good-2"]
