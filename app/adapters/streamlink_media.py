"""Small, optional Streamlink bridge used by live-media pilots.

Streamlink is intentionally not imported at module import time.  The
collector must remain installable and fully functional with the existing
yt-dlp/browser path when the optional dependency is absent or disabled.
"""

from __future__ import annotations

from typing import Any


class StreamlinkUnavailable(RuntimeError):
    """Raised when the optional Streamlink dependency cannot be used."""


def resolve_stream_url(url: str, *, quality: str = "best") -> tuple[str, dict[str, Any]]:
    """Resolve a platform URL to a playable stream URL with Streamlink.

    The returned URL is consumed by the existing bounded FFmpeg recorder, so
    downstream segment, heartbeat, and analysis contracts remain unchanged.
    """

    try:
        import streamlink
        from streamlink.exceptions import NoPluginError, NoStreamsError, PluginError
    except Exception as exc:  # pragma: no cover - depends on optional install
        raise StreamlinkUnavailable("streamlink is not installed") from exc

    try:
        session = streamlink.Streamlink()
        streams = session.streams(url)
    except (NoPluginError, NoStreamsError, PluginError) as exc:
        raise StreamlinkUnavailable(f"streamlink could not resolve live URL: {type(exc).__name__}") from exc
    except Exception as exc:  # pragma: no cover - plugin/network-specific
        raise StreamlinkUnavailable(f"streamlink resolve failed: {type(exc).__name__}") from exc

    selected = streams.get(quality) or streams.get("best")
    if selected is None:
        raise StreamlinkUnavailable("streamlink returned no playable quality")

    # HLSStream/HTTPStream expose .url.  MuxedStream instances do not have a
    # single URL and should fall back to the existing FFmpeg/yt-dlp path.
    resolved_url = str(getattr(selected, "url", "") or "").strip()
    if not resolved_url:
        raise StreamlinkUnavailable(f"streamlink quality {quality!r} has no direct URL")
    return resolved_url, {
        "quality": quality,
        "streamType": type(selected).__name__,
        "availableQualities": sorted(str(key) for key in streams),
    }
