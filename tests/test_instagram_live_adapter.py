from types import SimpleNamespace

from app.adapters.instagram import InstagramAdapter
from app.domain.enums import Capability, Platform
from app.domain.models import CollectionRequest, CollectionTarget


def _live_info():
    return {
        "id": "ig-live-abc",
        "webpage_url": "https://www.instagram.com/demo_live/live/",
        "title": "Instagram Live",
        "uploader_id": "ig-user-1",
        "uploader": "demo_live",
        "is_live": True,
        "live_status": "is_live",
        "thumbnail": "https://img.example/instagram-live.jpg",
        "formats": [
            {"url": "https://media.example/ig-low.m3u8", "protocol": "m3u8_native", "height": 240, "acodec": "aac"},
            {"url": "https://media.example/ig-high.m3u8", "protocol": "m3u8_native", "height": 720, "acodec": "aac"},
        ],
    }


def _request():
    return CollectionRequest(
        job_id="instagram-live-test",
        platform=Platform.INSTAGRAM,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.INSTAGRAM,
            target_type="CONTENT",
            target_value="https://www.instagram.com/demo_live/live/",
            source_url="https://www.instagram.com/demo_live/live/",
            metadata={"instagram_live_clip_seconds": 15, "max_height": 360},
        ),
        limit=1,
        download_media=True,
    )


def test_instagram_live_info_is_normalized_as_live_and_prefers_bounded_quality():
    adapter = object.__new__(InstagramAdapter)
    info = _live_info()

    batch = adapter._batch_from_info(info, _request())

    assert batch.contents[0].content_type == "LIVE"
    assert batch.contents[0].raw["is_live"] is True
    assert batch.contents[0].raw["live_status"] == "is_live"
    assert adapter._select_live_stream_url(info, _request()) == "https://media.example/ig-low.m3u8"


def test_instagram_live_clip_uses_ffmpeg_with_requested_duration(tmp_path, monkeypatch):
    from app.adapters import instagram as instagram_module

    adapter = object.__new__(InstagramAdapter)
    info = _live_info()
    request = _request()
    batch = adapter._batch_from_info(info, request)
    captured = {}

    monkeypatch.setattr(instagram_module.shutil, "which", lambda name: "/usr/bin/ffmpeg")

    def fake_run(command, **kwargs):
        captured["command"] = command
        with open(command[-1], "wb") as stream:
            stream.write(b"instagram-live-clip")
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(instagram_module.subprocess, "run", fake_run)

    adapter._download_live_with_ffmpeg(info, request, tmp_path, batch)

    assert captured["command"][captured["command"].index("-t") + 1] == "15"
    assert batch.assets[0].asset_type == "VIDEO"
    assert batch.assets[0].local_path.endswith("ig-live-abc-live-15s.mp4")
    assert batch.contents[0].raw["live_direct_ffmpeg"] is True
