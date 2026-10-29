from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.domain.enums import Platform
from app.domain.models import CollectedAccount, CollectedComment, CollectedContent, CollectionBatch, MediaAsset
from app.main import app


def _reset_live_service(live_monitor, tmp_path):
    live_monitor.live_monitor_service.base_dir = tmp_path / "live-captures"
    with live_monitor.live_monitor_service._lock:
        live_monitor.live_monitor_service._sessions.clear()


def test_live_monitor_start_collects_syncs_and_exposes_bilibili_style_session(tmp_path, monkeypatch):
    from app.api import live_monitor

    video = tmp_path / "live.mp4"
    video.write_bytes(b"live-video")
    thumbnail = tmp_path / "live.jpg"
    thumbnail.write_bytes(b"live-thumb")
    captured = {"requests": []}

    class FakeAdapter:
        def discover(self, request):
            captured["requests"].append(request)
            if request.download_media:
                return CollectionBatch(
                    accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-live", username="@live", nickname="Live Creator")],
                    contents=[
                        CollectedContent(
                            platform=Platform.YOUTUBE,
                            external_id="live-video-1",
                            url=request.target.source_url,
                            content_type="LIVE",
                            title="Live title",
                            author_external_id="UC-live",
                            raw={"is_live": True, "live_status": "is_live", "thumbnail": "https://img.example/live.jpg"},
                        )
                    ],
                    assets=[
                        MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="VIDEO", local_path=str(video)),
                        MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="THUMBNAIL", local_path=str(thumbnail)),
                    ],
                    comments=[
                        CollectedComment(
                            platform=Platform.YOUTUBE,
                            external_id="chat-1",
                            content_external_id="live-video-1",
                            author_external_id="viewer-1",
                            text="hello live",
                            raw={"source": "youtube_live_chat", "author": "Viewer One", "videoOffsetTimeMsec": "1200"},
                        )
                    ],
                )
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-live", username="@live", nickname="Live Creator")],
                contents=[
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="live-video-1",
                        url=request.target.source_url,
                        content_type="LIVE",
                        title="Live title",
                        author_external_id="UC-live",
                        raw={"is_live": True, "live_status": "is_live", "thumbnail": "https://img.example/live.jpg"},
                    )
                ],
            )

    def fake_sync(batch, request, *, settings):
        captured["sync_request"] = request
        return {
            "items": [
                {
                    "contentId": 9001,
                    "sourceVideoId": batch.contents[0].external_id,
                    "status": "QUEUED",
                    "mediaUrl": "http://minio/live.mp4",
                    "thumbnailUrl": "http://minio/live.jpg",
                }
            ],
            "agent_ingest": {"enabled": False, "items": []},
        }

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(live_monitor, "sync_batch_to_backend", fake_sync)
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/live-monitor/start",
        json={
            "platform": "youtube",
            "sourceUrl": "https://www.youtube.com/watch?v=refmlUAQssM",
            "segmentSeconds": 15,
            "runInBackground": False,
            "collectComments": True,
        },
    )

    assert response.status_code == 202
    session = response.json()
    assert session["status"] == "COMPLETED"
    assert session["stage"] == "COMPLETED"
    assert session["lastHeartbeatAt"]
    assert session["updatedAt"]
    assert session["diagnostics"]["platform"] == "YOUTUBE"
    assert session["diagnostics"]["cookieSource"] in {"none", "cookie_file", "cookie_pool", "browser_profile"}
    assert session["segmentSeconds"] == 15
    assert session["segmentsPerBatch"] == 1
    assert session["metadata"]["title"] == "Live title"
    assert session["danmaku"]["enabled"] is True
    assert session["danmaku"]["count"] == 1
    assert session["danmaku"]["items"][0]["text"] == "hello live"
    assert Path(session["danmaku"]["path"]).exists()
    assert session["comments"][0]["authorName"] == "Viewer One"
    assert session["batches"][0]["commentsPath"] == session["danmaku"]["path"]
    assert session["batches"][0]["status"] == "SYNCED"
    assert session["batches"][0]["backendSync"]["items"][0]["contentId"] == 9001
    assert session["hasDisplayableResult"] is True
    assert session["latestResult"]["contentId"] == 9001
    assert session["latestResult"]["mediaUrl"] == "http://minio/live.mp4"
    assert session["latestResult"]["thumbnailUrl"] == "http://minio/live.jpg"
    assert session["results"][0]["isLive"] is True
    assert session["results"][0]["videoId"] == "live-video-1"
    assert captured["requests"][0].download_media is False
    assert captured["requests"][1].download_media is True
    assert captured["requests"][1].capability.value == "LIVE_RESOLVE"
    assert captured["requests"][1].target.metadata["youtube_live_clip_seconds"] == 15
    assert captured["sync_request"].job_id == session["sessionId"]

    fetched = client.get(f"/api/live-monitor/{session['sessionId']}")
    assert fetched.status_code == 200
    assert fetched.json()["sessionId"] == session["sessionId"]
    assert Path(session["sessionPath"]).exists()


def test_live_monitor_youtube_not_live_returns_not_live(tmp_path, monkeypatch):
    from app.api import live_monitor

    class FakeAdapter:
        def discover(self, request):
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-video", username="@video")],
                contents=[
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="video-1",
                        url=request.target.source_url,
                        content_type="VIDEO",
                        title="Regular upload",
                        author_external_id="UC-video",
                        raw={"is_live": False, "live_status": "not_live"},
                    )
                ],
            )

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/live-monitor/start",
        json={"platform": "youtube", "sourceUrl": "https://www.youtube.com/watch?v=not-live", "runInBackground": False},
    )

    assert response.status_code == 202
    session = response.json()
    assert session["status"] == "NOT_LIVE"
    assert session["stage"] == "NOT_LIVE"
    assert session["progressMessage"] == "live source is not currently live"
    assert session["batches"] == []
    assert session["results"] == []
    assert session["latestResult"] is None
    assert session["hasDisplayableResult"] is False



def test_live_monitor_exposes_collector_error_code(tmp_path, monkeypatch):
    from app.api import live_monitor
    from app.domain.enums import CollectorErrorCode, CredentialAction
    from app.domain.models import CollectorError

    class FakeAdapter:
        def discover(self, request):
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "YouTube sign in required",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/live-monitor/start",
        json={"platform": "youtube", "sourceUrl": "https://www.youtube.com/watch?v=login-required", "runInBackground": False},
    )

    assert response.status_code == 202
    session = response.json()
    assert session["status"] == "FAILED"
    assert session["stage"] == "FAILED"
    assert session["failedCode"] == "AUTH_EXPIRED"
    assert session["failedAction"] == "REAUTH"
    assert session["retryable"] is False
    assert "YouTube sign in required" in session["failedReason"]

def test_live_monitor_stop_marks_active_session_stopped(tmp_path):
    from app.api import live_monitor

    _reset_live_service(live_monitor, tmp_path)
    session = {
        "sessionId": "youtube-live-test",
        "platform": "YOUTUBE",
        "sourceUrl": "https://www.youtube.com/@demo/live",
        "status": "RUNNING",
        "segmentSeconds": 300,
        "segmentsPerBatch": 1,
        "createdAt": "2026-09-16T00:00:00+00:00",
        "startedAt": "2026-09-16T00:00:01+00:00",
        "finishedAt": None,
        "metadata": {},
        "danmaku": {"enabled": False, "count": 0, "path": ""},
        "segments": [],
        "batches": [],
        "contentsCollected": 0,
        "commentsCollected": 0,
        "assetsCollected": 0,
        "contentsSynced": 0,
        "failedReason": None,
        "progressMessage": "",
        "sessionPath": str(tmp_path / "live-captures" / "youtube" / "youtube-live-test" / "session.json"),
        "logPath": str(tmp_path / "live-captures" / "youtube" / "youtube-live-test" / "live.log"),
    }
    live_monitor.live_monitor_service._save_session(session)

    client = TestClient(app)
    response = client.post("/api/live-monitor/youtube-live-test/stop")

    assert response.status_code == 200
    assert response.json()["status"] == "STOPPED"



def test_foreign_live_monitor_builds_platform_specific_live_urls():
    from app.api.live_monitor import LiveMonitorStart, live_monitor_service

    cases = [
        (
            {"platform": "youtube", "videoId": "refmlUAQssM"},
            "https://www.youtube.com/watch?v=refmlUAQssM",
        ),
        (
            {"platform": "youtube", "channelId": "UCiFOL6V9KbvxfXvzdFSsqCw"},
            "https://www.youtube.com/channel/UCiFOL6V9KbvxfXvzdFSsqCw/live",
        ),
        (
            {"platform": "youtube", "handle": "wenzhaoofficial"},
            "https://www.youtube.com/@wenzhaoofficial/live",
        ),
        (
            {"platform": "tiktok", "username": "demo_live"},
            "https://www.tiktok.com/@demo_live/live",
        ),
        (
            {"platform": "instagram", "username": "demo_live"},
            "https://www.instagram.com/demo_live/live/",
        ),
        (
            {"platform": "facebook", "videoId": "123456789"},
            "https://www.facebook.com/watch/?v=123456789",
        ),
        (
            {"platform": "facebook", "pageId": "61581863522342"},
            "https://www.facebook.com/61581863522342/live",
        ),
        (
            {"platform": "x", "username": "demo", "statusId": "1234567890"},
            "https://x.com/demo/status/1234567890",
        ),
        (
            {"platform": "x", "username": "demo"},
            "https://x.com/demo",
        ),
    ]

    for payload, expected in cases:
        request = LiveMonitorStart.model_validate(payload)
        assert live_monitor_service._resolve_source_url(request) == expected


def test_youtube_adapter_parses_live_chat_json_file(tmp_path, monkeypatch):
    from app.adapters.youtube import YouTubeAdapter
    from app.domain.enums import Capability
    from app.domain.models import CollectionRequest, CollectionTarget

    adapter = YouTubeAdapter()
    monkeypatch.setattr(adapter.settings, "youtube_output_root", tmp_path)
    video_id = "live-video-1"
    content_dir = tmp_path / "job-live" / video_id
    content_dir.mkdir(parents=True)
    (content_dir / f"{video_id}.live_chat.json").write_text(
        json.dumps(
            {
                "replayChatItemAction": {
                    "videoOffsetTimeMsec": "1200",
                    "actions": [
                        {
                            "addChatItemAction": {
                                "item": {
                                    "liveChatTextMessageRenderer": {
                                        "id": "chat-1",
                                        "authorName": {"simpleText": "Viewer One"},
                                        "authorExternalChannelId": "viewer-1",
                                        "message": {"runs": [{"text": "hello "}, {"text": "live"}]},
                                        "timestampUsec": "1700000000000000",
                                    }
                                }
                            }
                        }
                    ],
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    request = CollectionRequest(
        job_id="job-live",
        platform=Platform.YOUTUBE,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="CONTENT",
            target_value="https://www.youtube.com/watch?v=live-video-1",
            source_url="https://www.youtube.com/watch?v=live-video-1",
            metadata={"max_comments": 10},
        ),
        limit=1,
        collect_comments=True,
        download_media=True,
    )

    comments = adapter._live_chat_comments_from_files(request, video_id)

    assert len(comments) == 1
    assert comments[0].external_id == "chat-1"
    assert comments[0].author_external_id == "viewer-1"
    assert comments[0].text == "hello live"
    assert comments[0].raw["source"] == "youtube_live_chat"


def test_live_monitor_exposes_platform_specific_comment_shape(tmp_path, monkeypatch):
    from app.api import live_monitor

    video = tmp_path / "tiktok-live.mp4"
    video.write_bytes(b"live-video")
    thumbnail = tmp_path / "tiktok-live.jpg"
    thumbnail.write_bytes(b"live-thumb")

    class FakeAdapter:
        def discover(self, request):
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.TIKTOK, external_id="creator-1", username="demo_live", nickname="Demo Live")],
                contents=[
                    CollectedContent(
                        platform=Platform.TIKTOK,
                        external_id="tiktok-live-1",
                        url=request.target.source_url,
                        content_type="LIVE",
                        title="TikTok live title",
                        author_external_id="creator-1",
                        raw={"is_live": True, "live_status": "is_live", "thumbnail": "https://img.example/tiktok.jpg"},
                    )
                ],
                assets=[
                    MediaAsset(platform=Platform.TIKTOK, content_external_id="tiktok-live-1", asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.TIKTOK, content_external_id="tiktok-live-1", asset_type="THUMBNAIL", local_path=str(thumbnail)),
                ]
                if request.download_media
                else [],
                comments=[
                    CollectedComment(
                        platform=Platform.TIKTOK,
                        external_id="tk-comment-1",
                        content_external_id="tiktok-live-1",
                        author_external_id="viewer_tk",
                        text="hello tiktok",
                        raw={"source": "browser_dom_tiktok", "author": "Viewer TK", "relative_time": "1m ago", "like_count": 3},
                    )
                ]
                if request.download_media
                else [],
            )

    def fake_sync(batch, request, *, settings):
        return {
            "items": [
                {
                    "contentId": 9101,
                    "sourceVideoId": batch.contents[0].external_id,
                    "status": "QUEUED",
                    "mediaUrl": "http://minio/tiktok-live.mp4",
                    "thumbnailUrl": "http://minio/tiktok-live.jpg",
                }
            ],
            "agent_ingest": {"enabled": False, "items": []},
        }

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(live_monitor, "sync_batch_to_backend", fake_sync)
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/live-monitor/start",
        json={"platform": "tiktok", "username": "demo_live", "segmentSeconds": 15, "runInBackground": False, "collectComments": True},
    )

    assert response.status_code == 202
    session = response.json()
    assert session["status"] == "COMPLETED"
    assert session["danmaku"]["enabled"] is True
    assert session["danmaku"]["count"] == 1
    comment = session["danmaku"]["items"][0]
    assert comment["platform"] == "TIKTOK"
    assert comment["source"] == "browser_dom_tiktok"
    assert comment["kind"] == "comment"
    assert comment["authorName"] == "Viewer TK"
    assert comment["text"] == "hello tiktok"
    assert comment["relativeTime"] == "1m ago"
    assert comment["likeCount"] == 3


def test_v1_live_monitor_task_stream_snapshot_and_events(tmp_path, monkeypatch):
    from app.api import live_monitor

    video = tmp_path / "live.mp4"
    video.write_bytes(b"live-video")
    thumbnail = tmp_path / "live.jpg"
    thumbnail.write_bytes(b"live-thumb")

    class FakeAdapter:
        def discover(self, request):
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-live", username="@live", nickname="Live Creator")],
                contents=[
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="live-video-1",
                        url=request.target.source_url,
                        content_type="LIVE",
                        title="Live title",
                        author_external_id="UC-live",
                        raw={"is_live": True, "live_status": "is_live", "thumbnail": "https://img.example/live.jpg"},
                    )
                ],
                assets=[
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="THUMBNAIL", local_path=str(thumbnail)),
                ]
                if request.download_media
                else [],
                comments=[
                    CollectedComment(
                        platform=Platform.YOUTUBE,
                        external_id="chat-1",
                        content_external_id="live-video-1",
                        author_external_id="viewer-1",
                        text="hello live",
                        raw={"source": "youtube_live_chat", "author": "Viewer One"},
                    )
                ]
                if request.download_media
                else [],
            )

    def fake_sync(batch, request, *, settings):
        return {
            "items": [
                {
                    "contentId": 9100,
                    "sourceVideoId": batch.contents[0].external_id,
                    "status": "QUEUED",
                    "mediaUrl": "http://minio/live.mp4",
                    "thumbnailUrl": "http://minio/live.jpg",
                }
            ],
            "agent_ingest": {"enabled": False, "items": []},
        }

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(live_monitor, "sync_batch_to_backend", fake_sync)
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    created = client.post(
        "/api/v1/live-monitor/tasks",
        json={
            "platform": "YOUTUBE",
            "sourceUrl": "https://www.youtube.com/watch?v=refmlUAQssM",
            "audioSegmentSeconds": 15,
            "collectComments": True,
            "runInBackground": False,
        },
    )

    assert created.status_code == 202
    payload = created.json()
    stream_id = payload["streamId"]
    assert payload["stream"]["status"] == "LIVE"
    assert payload["stream"]["latestResult"]["contentId"] == 9100

    listed = client.get("/api/v1/live-monitor/streams").json()
    assert listed["total"] == 1
    assert listed["items"][0]["streamId"] == stream_id

    snapshot = client.get(f"/api/v1/live-monitor/streams/{stream_id}/snapshot").json()
    assert snapshot["stream"]["title"] == "Live title"
    assert snapshot["segments"][0]["videoUrl"] == "http://minio/live.mp4"
    assert snapshot["messages"][0]["text"] == "hello live"

    events = client.get(f"/api/v1/live-monitor/streams/{stream_id}/events")
    assert events.status_code == 200
    assert "text/event-stream" in events.headers["content-type"]
    assert "live.snapshot" in events.text


def test_v1_x_live_monitor_collects_a_bounded_clip_and_uses_x_playback(tmp_path, monkeypatch):
    from app.api import live_monitor

    video = tmp_path / "x-live.mp4"
    video.write_bytes(b"x-live-video")
    captured = {}

    class FakeXAdapter:
        def discover(self, request):
            captured["request"] = request
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.X, external_id="x-user-1", username="demo", nickname="Demo")],
                contents=[
                    CollectedContent(
                        platform=Platform.X,
                        external_id="x-space-1",
                        url=request.target.source_url,
                        content_type="LIVE",
                        title="X Space live",
                        author_external_id="x-user-1",
                        raw={"is_live": True, "live_status": "is_live"},
                    )
                ],
                assets=[MediaAsset(platform=Platform.X, content_external_id="x-space-1", asset_type="VIDEO", local_path=str(video))]
                if request.download_media
                else [],
            )

    def fake_sync(batch, request, *, settings):
        return {"items": [{"contentId": 9200, "sourceVideoId": "x-space-1", "mediaUrl": "http://minio/x-live.mp4"}], "agent_ingest": {"enabled": False, "items": []}}

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeXAdapter())
    monkeypatch.setattr(live_monitor, "sync_batch_to_backend", fake_sync)
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/v1/live-monitor/tasks",
        json={
            "platform": "X",
            "sourceUrl": "https://x.com/demo/status/1234567890",
            "segmentSeconds": 15,
            "runInBackground": False,
            "collectComments": False,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "COMPLETED"
    assert payload["stream"]["playerType"] == "X_STATUS_URL"
    assert payload["stream"]["playbackMode"] == "SOURCE_URL"
    assert payload["stream"]["latestResult"]["contentId"] == 9200
    assert captured["request"].target.metadata["x_live_clip_seconds"] == 15


def test_v1_x_live_monitor_marks_a_non_live_post_not_live(tmp_path, monkeypatch):
    from app.api import live_monitor

    class FakeXAdapter:
        def discover(self, request):
            return CollectionBatch(
                contents=[
                    CollectedContent(
                        platform=Platform.X,
                        external_id="x-post-1",
                        url=request.target.source_url,
                        content_type="POST",
                        raw={"is_live": False, "live_status": "not_live"},
                    )
                ]
            )

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeXAdapter())
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/v1/live-monitor/tasks",
        json={"platform": "X", "sourceUrl": "https://x.com/demo/status/1234567890", "runInBackground": False},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "NOT_LIVE"


def test_v1_instagram_live_monitor_collects_a_bounded_clip(tmp_path, monkeypatch):
    from app.api import live_monitor

    video = tmp_path / "instagram-live.mp4"
    video.write_bytes(b"instagram-live-video")
    captured = {}

    class FakeInstagramAdapter:
        def discover(self, request):
            captured["request"] = request
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.INSTAGRAM, external_id="ig-user-1", username="demo_live", nickname="Demo")],
                contents=[
                    CollectedContent(
                        platform=Platform.INSTAGRAM,
                        external_id="ig-live-1",
                        url=request.target.source_url,
                        content_type="LIVE",
                        title="Instagram live",
                        author_external_id="ig-user-1",
                        raw={"is_live": True, "live_status": "is_live"},
                    )
                ],
                assets=[MediaAsset(platform=Platform.INSTAGRAM, content_external_id="ig-live-1", asset_type="VIDEO", local_path=str(video))]
                if request.download_media
                else [],
            )

    def fake_sync(batch, request, *, settings):
        return {"items": [{"contentId": 9300, "sourceVideoId": "ig-live-1", "mediaUrl": "http://minio/instagram-live.mp4"}], "agent_ingest": {"enabled": False, "items": []}}

    monkeypatch.setattr(live_monitor, "get_adapter", lambda platform: FakeInstagramAdapter())
    monkeypatch.setattr(live_monitor, "sync_batch_to_backend", fake_sync)
    monkeypatch.setattr(live_monitor, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_live_service(live_monitor, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/api/v1/live-monitor/tasks",
        json={"platform": "INSTAGRAM", "username": "demo_live", "segmentSeconds": 15, "runInBackground": False},
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "COMPLETED"
    assert payload["stream"]["playerType"] == "INSTAGRAM_URL"
    assert payload["stream"]["playbackMode"] == "SOURCE_URL"
    assert payload["stream"]["latestResult"]["contentId"] == 9300
    assert captured["request"].target.metadata["instagram_live_clip_seconds"] == 15
