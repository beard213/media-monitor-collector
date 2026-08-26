from __future__ import annotations

import httpx

from app.core.config import Settings
from app.domain.enums import Capability, Platform
from app.domain.models import (
    CollectedAccount,
    CollectedComment,
    CollectedContent,
    CollectionBatch,
    CollectionRequest,
    CollectionTarget,
    MediaAsset,
)
from app.ingestion import backend as backend_module
from app.ingestion.agent import AgentClient, build_agent_payload
from app.ingestion.backend import CollectorBackendClient, UploadedObject, sync_youtube_batch_to_backend


def test_agent_client_posts_payload_and_returns_success():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.read().decode()
        return httpx.Response(
            200,
            json={
                "case_id": "youtube_v1",
                "report": {"risk_label": "high_risk"},
                "evaluation": {"score": 0.91},
                "trace": {"strategy": "rag_repair"},
            },
        )

    client = AgentClient(
        ingest_url="http://agent.local/api/agent/ingest",
        enabled=True,
        strategy="rag_repair",
        timeout_seconds=5,
        max_attempts=1,
        retry_interval_seconds=0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.ingest({"data_source": "youtube", "video_id": "v1", "video_title": "title"})

    assert seen["url"] == "http://agent.local/api/agent/ingest"
    assert '"strategy":"rag_repair"' in seen["body"]
    assert result["status"] == "SUCCESS"
    assert result["attempts"] == 1
    assert result["case_id"] == "youtube_v1"
    assert result["report"]["risk_label"] == "high_risk"


def test_agent_client_isolates_connection_failure_after_retries():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("connection refused", request=request)

    client = AgentClient(
        ingest_url="http://agent.local/api/agent/ingest",
        enabled=True,
        strategy="rag_repair",
        timeout_seconds=5,
        max_attempts=2,
        retry_interval_seconds=0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.ingest({"data_source": "youtube", "video_id": "v1"})

    assert attempts == 2
    assert result["status"] == "FAILED"
    assert result["attempts"] == 2
    assert result["retryable"] is True
    assert "connection refused" in result["error"]


def test_build_agent_payload_maps_collected_content_comments_and_minio_objects():
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.YOUTUBE,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="CONTENT", target_value="video-1"),
        limit=1,
        collect_comments=True,
        download_media=True,
    )
    content = CollectedContent(
        platform=Platform.YOUTUBE,
        external_id="video-1",
        url="https://youtube.com/watch?v=video-1",
        title="Risk title",
        description="Risk description",
        author_external_id="creator-1",
        raw={"asr_text": "ASR transcript", "keyframes": [{"url": "frame-1.jpg", "description": "banner"}]},
    )
    batch = CollectionBatch(
        accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="creator")],
        contents=[content],
        comments=[
            CollectedComment(
                platform=Platform.YOUTUBE,
                external_id="comment-1",
                content_external_id="video-1",
                text="dangerous comment",
            )
        ],
        assets=[
            MediaAsset(
                platform=Platform.YOUTUBE,
                content_external_id="video-1",
                asset_type="VIDEO",
                object_key="ingestion/job-1/videos/video-1.mp4",
            )
        ],
    )
    video = UploadedObject(
        object_key="ingestion/job-1/videos/video-1.mp4",
        url="http://minio/media-monitor/ingestion/job-1/videos/video-1.mp4",
        size=123,
        content_type="video/mp4",
        object_type="VIDEO",
    )
    thumbnail = UploadedObject(
        object_key="ingestion/job-1/thumbnails/video-1.jpg",
        url="http://minio/media-monitor/ingestion/job-1/thumbnails/video-1.jpg",
        size=45,
        content_type="image/jpeg",
        object_type="THUMBNAIL",
    )

    payload = build_agent_payload(batch, content, request, video=video, thumbnail=thumbnail, strategy="rag_repair")

    assert payload["data_source"] == "youtube"
    assert payload["video_id"] == "video-1"
    assert payload["video_title"] == "Risk title"
    assert payload["asr_text"] == "ASR transcript"
    assert payload["comment_list"] == ["dangerous comment"]
    assert payload["keyframes"] == [{"url": "frame-1.jpg", "description": "banner"}]
    assert payload["minio_url"] == "ingestion/job-1/videos/video-1.mp4"
    assert payload["thumbnail_url"] == "ingestion/job-1/thumbnails/video-1.jpg"
    assert payload["strategy"] == "rag_repair"


def test_sync_batch_invokes_agent_after_backend_success(tmp_path, monkeypatch):
    video = tmp_path / "video.mp4"
    thumbnail = tmp_path / "thumb.jpg"
    video.write_bytes(b"video-bytes")
    thumbnail.write_bytes(b"thumb-bytes")
    agent_payloads = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        if str(request.url).endswith("/objects"):
            object_type = "VIDEO" if b"VIDEO" in body else "THUMBNAIL"
            suffix = "mp4" if object_type == "VIDEO" else "jpg"
            directory = "videos" if object_type == "VIDEO" else "thumbnails"
            return httpx.Response(
                201,
                json={
                    "objectKey": f"ingestion/job-1/{directory}/video-1.{suffix}",
                    "url": f"http://minio/media-monitor/ingestion/job-1/{directory}/video-1.{suffix}",
                    "size": len(body),
                    "contentType": "video/mp4" if object_type == "VIDEO" else "image/jpeg",
                    "objectType": object_type,
                },
            )
        return httpx.Response(200, json={"contentId": 100, "sourceVideoId": "video-1", "status": "QUEUED"})

    class FakeAgent:
        enabled = True

        def ingest(self, payload):
            agent_payloads.append(payload)
            return {"status": "SUCCESS", "case_id": "youtube_video-1", "report": {"risk_label": "medium_risk"}}

    fake_agent = FakeAgent()
    monkeypatch.setattr(backend_module.AgentClient, "from_settings", lambda settings: fake_agent)

    client = CollectorBackendClient(
        base_url="http://backend:18082/api/v1",
        access_key="collector-default",
        access_token="change-me-token",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.YOUTUBE,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="CONTENT", target_value="video-1"),
        limit=1,
        collect_comments=True,
        download_media=True,
    )
    batch = CollectionBatch(
        accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="creator")],
        contents=[
            CollectedContent(
                platform=Platform.YOUTUBE,
                external_id="video-1",
                url="https://youtube.com/watch?v=video-1",
                title="Risk title",
                author_external_id="creator-1",
            )
        ],
        comments=[
            CollectedComment(
                platform=Platform.YOUTUBE,
                external_id="comment-1",
                content_external_id="video-1",
                text="risk comment",
            )
        ],
        assets=[
            MediaAsset(platform=Platform.YOUTUBE, content_external_id="video-1", asset_type="VIDEO", local_path=str(video)),
            MediaAsset(platform=Platform.YOUTUBE, content_external_id="video-1", asset_type="THUMBNAIL", local_path=str(thumbnail)),
        ],
    )
    settings = Settings(_env_file=None, agent_ingest_enabled=True, agent_ingest_strategy="rag_repair")

    result = sync_youtube_batch_to_backend(batch, request, client=client, settings=settings)

    assert result["items"][0]["status"] == "QUEUED"
    assert result["agent_ingest"]["enabled"] is True
    assert result["agent_ingest"]["items"][0]["status"] == "SUCCESS"
    assert batch.raw["agent_ingest"] == result["agent_ingest"]
    assert agent_payloads[0]["video_id"] == "video-1"
    assert agent_payloads[0]["comment_list"] == ["risk comment"]
    assert agent_payloads[0]["minio_url"] == "ingestion/job-1/videos/video-1.mp4"
