from types import SimpleNamespace

from app.adapters.x import XAdapter
from app.domain.enums import Capability, Platform
from app.domain.models import CollectionRequest, CollectionTarget


def _live_info():
    return {
        "id": "space-abc",
        "webpage_url": "https://x.com/i/spaces/space-abc",
        "title": "Live X Space",
        "uploader_id": "creator-1",
        "uploader": "Creator",
        "is_live": True,
        "live_status": "is_live",
        "thumbnail": "https://img.example/space.jpg",
        "formats": [
            {"url": "https://media.example/low.m3u8", "protocol": "m3u8_native", "height": 240, "acodec": "aac"},
            {"url": "https://media.example/high.m3u8", "protocol": "m3u8_native", "height": 720, "acodec": "aac"},
        ],
    }


def _request(tmp_path):
    return CollectionRequest(
        job_id="x-live-test",
        platform=Platform.X,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.X,
            target_type="CONTENT",
            target_value="https://x.com/i/spaces/space-abc",
            source_url="https://x.com/i/spaces/space-abc",
            metadata={"x_live_clip_seconds": 15, "max_height": 360},
        ),
        limit=1,
        download_media=True,
    )


def test_x_live_info_is_normalized_as_live_and_prefers_bounded_quality(tmp_path):
    adapter = object.__new__(XAdapter)
    info = _live_info()

    batch = adapter._batch_from_info(info, info["webpage_url"], tmp_path)

    assert batch.contents[0].content_type == "LIVE"
    assert batch.contents[0].raw["is_live"] is True
    assert batch.contents[0].raw["live_status"] == "is_live"
    assert adapter._select_live_stream_url(info, _request(tmp_path)) == "https://media.example/low.m3u8"


def test_x_live_clip_uses_ffmpeg_with_requested_duration(tmp_path, monkeypatch):
    from app.adapters import x as x_module

    adapter = object.__new__(XAdapter)
    info = _live_info()
    request = _request(tmp_path)
    batch = adapter._batch_from_info(info, info["webpage_url"], tmp_path)
    captured = {}

    monkeypatch.setattr(x_module.shutil, "which", lambda name: "/usr/bin/ffmpeg")

    def fake_run(command, **kwargs):
        captured["command"] = command
        output = command[-1]
        with open(output, "wb") as stream:
            stream.write(b"x-live-clip")
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(x_module.subprocess, "run", fake_run)

    adapter._download_live_with_ffmpeg(info, request, tmp_path, batch)

    assert "-t" in captured["command"]
    assert captured["command"][captured["command"].index("-t") + 1] == "15"
    assert batch.assets[0].asset_type == "VIDEO"
    assert batch.assets[0].local_path.endswith("space-abc-live-15s.mp4")
    assert batch.contents[0].raw["live_direct_ffmpeg"] is True
