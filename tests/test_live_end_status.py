from types import SimpleNamespace

from app.domain.enums import Capability, Platform
from app.domain.models import CollectedContent, CollectionBatch, CollectionRequest, CollectionTarget, MediaAsset
from app.ingestion.backend import UploadedObject
from app.ingestion.live_stream import finish_realtime_live_push, live_end_reason, run_realtime_live_push


def test_explicit_platform_offline_state_is_terminal():
    batch = CollectionBatch(
        contents=[
            CollectedContent(
                platform=Platform.YOUTUBE,
                external_id="ended-live",
                raw={"is_live": False, "live_status": "post_live"},
            )
        ]
    )

    assert live_end_reason(batch) == "PLATFORM_REPORTED_OFFLINE"


def test_end_callback_carries_explicit_ended_fields():
    captured = {}

    class FakeBackend:
        def live_ended(self, stream_id, payload):
            captured["stream_id"] = stream_id
            captured["payload"] = payload
            return {"ok": True}

    request = CollectionRequest(
        job_id="live-ended-test",
        platform=Platform.YOUTUBE,
        capability="LIVE_RESOLVE",
        target=CollectionTarget(platform=Platform.YOUTUBE, target_type="CONTENT", target_value="video"),
    )

    result = finish_realtime_live_push(
        "123",
        request,
        segment_count=9,
        message_count=4,
        backend=FakeBackend(),
        reason="PLATFORM_REPORTED_ENDED",
        detection_source="platform_metadata",
    )

    assert result == {"ok": True}
    assert captured["stream_id"] == "123"
    assert captured["payload"]["status"] == "ENDED"
    assert captured["payload"]["liveStatus"] == "ENDED"
    assert captured["payload"]["endDetected"] is True
    assert captured["payload"]["detectionSource"] == "platform_metadata"


def test_facebook_media_registers_uploaded_mp4_as_playback_source(tmp_path):
    video = tmp_path / "facebook-live.mp4"
    video.write_bytes(b"mp4")
    captured = {}

    class FakeBackend:
        def upload_object(self, path, *, object_type, object_key):
            return UploadedObject(
                object_key=object_key,
                url=f"https://media.example/{object_key}",
                size=path.stat().st_size,
                content_type="video/mp4",
                object_type=object_type,
            )

        def upsert_live_stream(self, payload):
            captured["upsert"] = payload
            return {"streamId": "759"}

        def live_segments(self, stream_id, payload):
            captured["segment"] = payload
            return {"ok": True}

        def live_messages(self, stream_id, payload):
            return {"ok": True}

        def live_metrics(self, stream_id, payload):
            return {"ok": True}

        def live_heartbeat(self, stream_id, payload):
            return {"ok": True}

    request = CollectionRequest(
        job_id="facebook-playback-test",
        platform=Platform.FACEBOOK,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.FACEBOOK,
            target_type="CONTENT",
            target_value="https://www.facebook.com/page/videos/123",
            source_url="https://www.facebook.com/page/videos/123",
            metadata={"accountId": 342589, "liveSequenceStart": 100, "liveUrl": "https://www.facebook.com/page/videos/123"},
        ),
        collect_comments=False,
        download_media=True,
    )
    batch = CollectionBatch(
        contents=[
            CollectedContent(
                platform=Platform.FACEBOOK,
                external_id="123",
                url="https://www.facebook.com/page/videos/123",
                raw={"live_clip_seconds": 10},
            )
        ],
        assets=[MediaAsset(platform=Platform.FACEBOOK, content_external_id="123", asset_type="VIDEO", local_path=str(video))],
    )

    result = run_realtime_live_push(batch, request, backend=FakeBackend(), settings=SimpleNamespace(worker_id="test-worker"))

    assert result["streamId"] == "759"
    assert captured["upsert"]["playbackMode"] == "OFFICIAL_EMBED"
    assert captured["upsert"]["embedId"] == "123"
    assert captured["upsert"]["sourceUrl"] == "https://www.facebook.com/page/videos/123"
    assert captured["upsert"]["playUrl"] == "https://www.facebook.com/page/videos/123"
    assert captured["upsert"]["mediaUrl"].startswith("https://media.example/live/OVERSEAS/FACEBOOK/123/")
