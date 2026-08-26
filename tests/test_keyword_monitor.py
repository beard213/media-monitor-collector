from datetime import datetime, timezone
from collections import namedtuple

from app.domain.enums import Capability, Platform
from app.domain.models import CollectedAccount, CollectedComment, CollectedContent, CollectionBatch, CollectorError, MediaAsset
from app.domain.enums import CollectorErrorCode, CredentialAction
from app.ingestion.backend import ActiveKeyword, UploadedObject
from app.ingestion.keyword_monitor import _keyword_platform, run_keyword_monitor
from app.ingestion.keyword_recency import keyword_recency_cutoff


class FakeKeywordBackend:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.requested_platforms: list[str] = []
        self.synced_payloads: list[dict] = []

    def active_keywords(self, *, platforms):
        self.requested_platforms = [platform.value if hasattr(platform, "value") else str(platform) for platform in platforms]
        return [
            ActiveKeyword(id=12, platform="YOUTUBE", keyword="示例关键词", match_type="CONTAINS", risk_level="MEDIUM"),
            ActiveKeyword(id=13, platform="TWITTER", keyword="暂不支持", match_type="CONTAINS", risk_level="HIGH"),
        ]

    def upload_object(self, path, *, object_type, object_key):
        return UploadedObject(
            object_key=object_key,
            url=f"http://minio/media-monitor/{object_key}",
            size=123,
            content_type="video/mp4" if object_type == "VIDEO" else "image/jpeg",
            object_type=object_type,
        )

    def sync_content(self, payload):
        self.synced_payloads.append(payload)
        return {"contentId": 501, "sourceVideoId": payload["videoId"], "status": "QUEUED"}

    def download_file(self, url, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"thumb")
        return destination


class FakeYouTubeKeywordAdapter:
    platform = Platform.YOUTUBE
    capabilities = {Capability.SEARCH_KEYWORD}

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.requests = []

    def discover(self, request):
        self.requests.append(request)
        video = self.tmp_path / "keyword-hit-001.mp4"
        thumb = self.tmp_path / "keyword-hit-001.jpg"
        video.write_bytes(b"video")
        thumb.write_bytes(b"thumb")
        return CollectionBatch(
            accounts=[
                CollectedAccount(
                    platform=Platform.YOUTUBE,
                    external_id="author-1",
                    username="@matched-author",
                    nickname="命中内容作者",
                )
            ],
            contents=[
                CollectedContent(
                    platform=Platform.YOUTUBE,
                    external_id="keyword-hit-001",
                    url="https://www.youtube.com/watch?v=keyword-hit-001",
                    content_type="SHORT_VIDEO",
                    title="关键词命中的短视频",
                    author_external_id="author-1",
                    published_at=datetime(2026, 7, 22, 10, 30, 0, tzinfo=timezone.utc),
                    raw={"duration": 95, "view_count": 1200, "like_count": 33, "comment_count": 2},
                )
            ],
            assets=[
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="keyword-hit-001", asset_type="VIDEO", local_path=str(video)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="keyword-hit-001", asset_type="THUMBNAIL", local_path=str(thumb)),
            ],
        )


def test_run_keyword_monitor_fetches_keywords_collects_youtube_and_syncs(tmp_path):
    backend = FakeKeywordBackend(tmp_path)
    adapter = FakeYouTubeKeywordAdapter(tmp_path)

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=2,
        collect_comments=True,
        download_media=True,
        dry_run=False,
        metadata={"max_comments": 5, "min_free_disk_gb": 0},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
    )

    assert backend.requested_platforms == ["YOUTUBE"]
    assert len(adapter.requests) == 1
    request = adapter.requests[0]
    assert request.platform == Platform.YOUTUBE
    assert request.capability == Capability.SEARCH_KEYWORD
    assert request.target.target_type == "KEYWORD"
    assert request.target.target_value == "示例关键词"
    assert request.target.metadata["keyword_id"] == 12
    assert request.target.metadata["keyword"] == "示例关键词"
    assert request.target.metadata["match_type"] == "CONTAINS"
    assert request.target.metadata["risk_level"] == "MEDIUM"
    assert request.limit == 2
    assert request.collect_comments is True
    assert request.download_media is True
    assert len(backend.synced_payloads) == 1
    assert backend.synced_payloads[0]["keywordId"] == 12
    assert backend.synced_payloads[0]["keyword"] == "示例关键词"
    assert backend.synced_payloads[0]["collectionType"] == "KEYWORD"
    assert result["keywords_total"] == 2
    assert result["keywords_collected"] == 1
    assert result["contents_synced"] == 1
    assert result["skipped"][0]["platform"] == "TWITTER"


def test_run_keyword_monitor_emits_progress_events(tmp_path):
    backend = FakeKeywordBackend(tmp_path)
    adapter = FakeYouTubeKeywordAdapter(tmp_path)
    events = []

    run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=2,
        collect_comments=True,
        download_media=True,
        dry_run=False,
        metadata={"max_comments": 5, "min_free_disk_gb": 0},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
        progress_callback=events.append,
    )

    assert events[0] == {"event": "keywords_loaded", "platforms": ["YOUTUBE"], "keyword_source": "backend_api", "keywords_total": 2}
    assert {"event": "keyword_started", "platform": "YOUTUBE", "keyword_id": 12, "keyword": "示例关键词"} in events
    collected_event = next(event for event in events if event.get("event") == "keyword_collected")
    assert collected_event["platform"] == "YOUTUBE"
    assert collected_event["keyword_id"] == 12
    assert collected_event["keyword"] == "示例关键词"
    assert collected_event["counts"] == {"accounts": 1, "contents": 1, "comments": 0, "assets": 2}
    assert collected_event["recency_filter"]["enabled"] is True
    assert {"event": "keyword_synced", "platform": "YOUTUBE", "keyword_id": 12, "keyword": "示例关键词", "contents_synced": 1} in events
    assert {"event": "keyword_skipped", "platform": "TWITTER", "keyword_id": 13, "keyword": "暂不支持", "reason": "unsupported_platform"} in events


class FailingKeywordBackend(FakeKeywordBackend):
    def active_keywords(self, *, platforms):
        raise CollectorError(
            CollectorErrorCode.NETWORK_TEMPORARY,
            "keywords API failed",
            retryable=True,
            credential_action=CredentialAction.NONE,
        )


class FakeKeywordDatabase:
    def active_keywords(self, *, platforms):
        return [ActiveKeyword(id=12, platform="YOUTUBE", keyword="北戴河", match_type="CONTAINS", risk_level="MEDIUM")]


def test_run_keyword_monitor_falls_back_to_keyword_database_when_backend_keyword_api_fails(tmp_path):
    backend = FailingKeywordBackend(tmp_path)
    adapter = FakeYouTubeKeywordAdapter(tmp_path)

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=1,
        collect_comments=False,
        download_media=True,
        dry_run=False,
        metadata={"min_free_disk_gb": 0},
        backend_client=backend,
        keyword_database_client=FakeKeywordDatabase(),
        adapters={Platform.YOUTUBE: adapter},
    )

    assert len(adapter.requests) == 1
    assert adapter.requests[0].target.target_value == "北戴河"
    assert adapter.requests[0].target.metadata["keyword_id"] == 12
    assert result["keyword_source"] == "database_fallback"
    assert result["keywords_total"] == 1
    assert result["contents_synced"] == 1


class FailingKeywordAdapter(FakeYouTubeKeywordAdapter):
    def discover(self, request):
        self.requests.append(request)
        raise CollectorError(
            CollectorErrorCode.ACCESS_DENIED,
            "no keyword content links",
            retryable=False,
            credential_action=CredentialAction.NONE,
        )


def test_run_keyword_monitor_skips_failed_keyword_and_continues(tmp_path):
    backend = FakeKeywordBackend(tmp_path)
    adapter = FailingKeywordAdapter(tmp_path)

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=1,
        collect_comments=False,
        download_media=False,
        dry_run=False,
        metadata={"min_free_disk_gb": 0},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
    )

    assert result["keywords_total"] == 2
    assert result["keywords_collected"] == 0
    assert result["contents_synced"] == 0
    assert result["skipped"][0]["keyword_id"] == 12
    assert result["skipped"][0]["reason"] == "ACCESS_DENIED"


def test_keyword_platform_maps_twitter_alias_to_x():
    keyword = ActiveKeyword(id=20, platform="TWITTER", keyword="秦皇岛", match_type="CONTAINS", risk_level="MEDIUM")

    assert _keyword_platform(keyword, [Platform.X]) == Platform.X


class MixedPublishedAtKeywordAdapter(FakeYouTubeKeywordAdapter):
    def discover(self, request):
        self.requests.append(request)
        recent_video = self.tmp_path / "recent-hit.mp4"
        old_video = self.tmp_path / "old-hit.mp4"
        missing_video = self.tmp_path / "missing-hit.mp4"
        recent_thumb = self.tmp_path / "recent-hit.jpg"
        old_thumb = self.tmp_path / "old-hit.jpg"
        missing_thumb = self.tmp_path / "missing-hit.jpg"
        for path in (recent_video, old_video, missing_video):
            path.write_bytes(b"video")
        for path in (recent_thumb, old_thumb, missing_thumb):
            path.write_bytes(b"thumb")
        return CollectionBatch(
            accounts=[
                CollectedAccount(platform=Platform.YOUTUBE, external_id="author-recent"),
                CollectedAccount(platform=Platform.YOUTUBE, external_id="author-old"),
                CollectedAccount(platform=Platform.YOUTUBE, external_id="author-missing"),
            ],
            contents=[
                CollectedContent(
                    platform=Platform.YOUTUBE,
                    external_id="recent-hit",
                    author_external_id="author-recent",
                    published_at=datetime(2026, 7, 20, 8, 0, 0, tzinfo=timezone.utc),
                ),
                CollectedContent(
                    platform=Platform.YOUTUBE,
                    external_id="old-hit",
                    author_external_id="author-old",
                    published_at=datetime(2026, 3, 1, 8, 0, 0, tzinfo=timezone.utc),
                ),
                CollectedContent(
                    platform=Platform.YOUTUBE,
                    external_id="missing-hit",
                    author_external_id="author-missing",
                    published_at=None,
                ),
            ],
            comments=[
                CollectedComment(platform=Platform.YOUTUBE, external_id="recent-comment", content_external_id="recent-hit"),
                CollectedComment(platform=Platform.YOUTUBE, external_id="old-comment", content_external_id="old-hit"),
                CollectedComment(platform=Platform.YOUTUBE, external_id="missing-comment", content_external_id="missing-hit"),
            ],
            assets=[
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="recent-hit", asset_type="VIDEO", local_path=str(recent_video)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="recent-hit", asset_type="THUMBNAIL", local_path=str(recent_thumb)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="old-hit", asset_type="VIDEO", local_path=str(old_video)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="old-hit", asset_type="THUMBNAIL", local_path=str(old_thumb)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="missing-hit", asset_type="VIDEO", local_path=str(missing_video)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id="missing-hit", asset_type="THUMBNAIL", local_path=str(missing_thumb)),
            ],
        )


def test_run_keyword_monitor_filters_keyword_contents_to_recent_window_and_keeps_missing_dates(tmp_path):
    backend = FakeKeywordBackend(tmp_path)
    adapter = MixedPublishedAtKeywordAdapter(tmp_path)

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=10,
        collect_comments=True,
        download_media=True,
        dry_run=False,
        metadata={"keyword_recency_months": 3, "keyword_recency_now": "2026-07-24T00:00:00+00:00", "min_free_disk_gb": 0},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
    )

    synced_video_ids = [payload["videoId"] for payload in backend.synced_payloads]
    assert synced_video_ids == ["recent-hit", "missing-hit"]
    assert {payload["user"]["uid"] for payload in backend.synced_payloads} == {"author-recent", "author-missing"}
    assert result["items"][0]["counts"] == {"accounts": 2, "contents": 2, "comments": 2, "assets": 4}
    assert result["items"][0]["recency_filter"]["filtered_old"] == 1


def test_keyword_recency_cutoff_allows_zero_months_to_disable_filter():
    assert keyword_recency_cutoff({"keyword_recency_months": 0}) is None


def test_run_keyword_monitor_filters_loaded_keywords_by_metadata_ids(tmp_path):
    class TwoKeywordBackend(FakeKeywordBackend):
        def active_keywords(self, *, platforms):
            return [
                ActiveKeyword(id=12, platform="YOUTUBE", keyword="第一个关键词", match_type="CONTAINS", risk_level="MEDIUM"),
                ActiveKeyword(id=31, platform="YOUTUBE", keyword="领导人", match_type="CONTAINS", risk_level="HIGH"),
            ]

    backend = TwoKeywordBackend(tmp_path)
    adapter = FakeYouTubeKeywordAdapter(tmp_path)

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=10,
        collect_comments=True,
        download_media=True,
        dry_run=True,
        metadata={"keyword_ids": [31], "min_free_disk_gb": 0},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
    )

    assert result["keywords_total"] == 1
    assert result["items"][0]["keyword_id"] == 31
    assert adapter.requests[0].target.target_value == "领导人"


def test_run_keyword_monitor_skips_collection_when_disk_free_space_is_below_guard(tmp_path, monkeypatch):
    backend = FakeKeywordBackend(tmp_path)
    adapter = FakeYouTubeKeywordAdapter(tmp_path)
    disk_usage = namedtuple("disk_usage", "total used free")

    monkeypatch.setattr(
        "app.ingestion.keyword_monitor.shutil.disk_usage",
        lambda _path: disk_usage(40 * 1024**3, 39 * 1024**3, 1 * 1024**3),
    )

    result = run_keyword_monitor(
        platforms=[Platform.YOUTUBE],
        limit_per_keyword=10,
        collect_comments=True,
        download_media=True,
        dry_run=False,
        metadata={"min_free_disk_gb": 2},
        backend_client=backend,
        adapters={Platform.YOUTUBE: adapter},
    )

    assert adapter.requests == []
    assert result["keywords_collected"] == 0
    assert result["skipped"][0]["reason"] == "DISK_LOW"
    assert "free disk" in result["skipped"][0]["message"]
