from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

from app.adapters.streamlink_media import resolve_stream_url
from app.adapters.tiktok import TikTokAdapter
from app.domain.enums import Capability, Platform
from app.domain.models import CollectionRequest, CollectionTarget


def test_resolve_stream_url_selects_requested_quality(monkeypatch):
    fake_streamlink = ModuleType("streamlink")

    class FakeSession:
        def streams(self, url):
            assert url.endswith("/@demo/live")
            return {
                "best": SimpleNamespace(url="https://cdn.example/live/high.m3u8"),
                "360p": SimpleNamespace(url="https://cdn.example/live/low.m3u8"),
            }

    fake_streamlink.Streamlink = FakeSession
    fake_exceptions = ModuleType("streamlink.exceptions")
    fake_exceptions.NoPluginError = type("NoPluginError", (Exception,), {})
    fake_exceptions.NoStreamsError = type("NoStreamsError", (Exception,), {})
    fake_exceptions.PluginError = type("PluginError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "streamlink", fake_streamlink)
    monkeypatch.setitem(sys.modules, "streamlink.exceptions", fake_exceptions)

    url, diagnostics = resolve_stream_url("https://www.tiktok.com/@demo/live", quality="360p")

    assert url == "https://cdn.example/live/low.m3u8"
    assert diagnostics["streamType"] == "SimpleNamespace"
    assert diagnostics["availableQualities"] == ["360p", "best"]


def test_tiktok_streamlink_pilot_is_allow_listed():
    adapter = TikTokAdapter()
    adapter.settings.streamlink_enabled = True
    adapter.settings.streamlink_platforms = "TIKTOK"
    adapter.settings.streamlink_pilot_targets = "bigmouth556"
    request = CollectionRequest(
        job_id="pilot",
        platform=Platform.TIKTOK,
        capability=Capability.LIVE_RESOLVE,
        target=CollectionTarget(
            platform=Platform.TIKTOK,
            target_type="LIVE_ROOM",
            target_value="https://www.tiktok.com/@bigmouth556/live",
        ),
        download_media=True,
    )

    assert adapter._streamlink_pilot_enabled(request) is True
    assert adapter._streamlink_pilot_enabled(request.model_copy(update={"target": request.target.model_copy(update={"target_value": "https://www.tiktok.com/@other/live"})})) is False
