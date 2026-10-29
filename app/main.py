import asyncio
import re
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from app.api.agent_health import router as agent_health_router
from app.api.collector_jobs import router as collector_jobs_router
from app.api.live_monitor import router as live_monitor_router
from app.api.live_monitor import v1_router as live_monitor_v1_router
from app.api.collections import router as collections_router
from app.adapters.registry import platform_statuses
from app.core.config import get_settings
from app.ingestion.hls_relay import get_hls_relay_manager

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Unified foreign platform collection service architecture.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)


def _delayed_live_playlist(playlist: str) -> str:
    """Serve a stable HLS edge instead of the newest raw segment.

    Some upstream sources publish several seconds of media in a burst and
    then leave the source unchanged briefly.  ``EXT-X-START`` alone does not
    protect players that choose their own live-edge start position, so hide a
    small newest tail from the public manifest as well.
    """
    lines = playlist.splitlines()
    uri_indexes = [index for index, line in enumerate(lines) if line and not line.startswith("#")]
    # With three-second collector segments, six visible entries are an
    # eighteen-second playable window. Before that warm-up is complete,
    # expose all available media rather than a tiny playlist.
    hidden = min(10, max(0, len(uri_indexes) - 6))
    if hidden:
        lines = lines[: uri_indexes[-hidden - 1] + 1]
    rendered = "\n".join(lines) + "\n"
    if "#EXT-X-START:" not in rendered:
        marker = "#EXT-X-INDEPENDENT-SEGMENTS"
        # Relative to the already delayed public edge. Clients that honor
        # this start ~18s behind raw edge; those that do not still have ~12s.
        start_tag = "#EXT-X-START:TIME-OFFSET=-6.000,PRECISE=YES"
        rendered = rendered.replace(marker, f"{start_tag}\n{marker}", 1)
    return rendered


@app.middleware("http")
async def _live_media_headers(request, call_next):
    """Keep HLS clients from caching a stale playlist and fix TS MIME type."""
    path = request.url.path.lower()
    # Upstream cross-border HLS can arrive in 4-5 second bursts. Serve a
    # delayed manifest so the player cannot catch that edge.
    if path.startswith("/live/") and path.endswith(".m3u8"):
        relative = request.url.path[len("/live/"):].lstrip("/")
        candidate = (settings.hls_output_root / Path(relative)).resolve()
        root = settings.hls_output_root.resolve()
        if root in candidate.parents and candidate.is_file():
            try:
                playlist = candidate.read_text(encoding="utf-8", errors="replace")
            except OSError:
                playlist = ""
            if playlist and "#EXT-X-ENDLIST" not in playlist:
                playlist = _delayed_live_playlist(playlist)
            response = Response(playlist, media_type="application/vnd.apple.mpegurl")
        else:
            response = await call_next(request)
    else:
        response = await call_next(request)
    if path.startswith("/live/") or path.startswith("/live-proxy/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Access-Control-Allow-Origin"] = "*"
        if path.endswith(".m3u8"):
            response.headers["Content-Type"] = "application/vnd.apple.mpegurl"
        elif path.endswith(".ts"):
            response.headers["Content-Type"] = "video/mp2t"
    return response


# Stable rolling HLS output for live playback. The playlist is intentionally
# served with the collector API so the frontend does not depend on the latest
# finite MP4 object URL.
app.mount("/live", StaticFiles(directory=str(settings.hls_output_root), check_dir=False), name="live-hls")

# Login browsers run on loopback-only VNC ports.  The collector API port is
# already the managed public ingress, so expose noVNC through this ASGI app
# instead of opening additional cloud firewall ports.  The upstream VNC
# servers still require their own password; this route only transports bytes.
_NOVNC_ROOT = "/usr/share/novnc"
_LOGIN_VNC_TARGETS: dict[str, tuple[str, int]] = {
    "youtube-01": ("127.0.0.1", 5913),
    "youtube-02": ("127.0.0.1", 5914),
}
app.mount("/vnc/novnc", StaticFiles(directory=_NOVNC_ROOT, check_dir=False), name="novnc")


@app.websocket("/vnc/{instance}/websockify")
async def vnc_websocket_proxy(websocket: WebSocket, instance: str) -> None:
    """Bridge noVNC's binary WebSocket stream to a loopback VNC server."""
    target = _LOGIN_VNC_TARGETS.get(instance)
    if target is None:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    writer = None
    try:
        reader, writer = await asyncio.open_connection(*target)
        # x11vnc sends the RFB banner immediately.  Forward it before
        # starting the two long-running pumps so the browser can select its
        # security method (and display the password prompt) deterministically.
        banner = await asyncio.wait_for(reader.readexactly(12), timeout=5)
        await websocket.send_bytes(banner)

        async def browser_to_vnc() -> None:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                data = message.get("bytes")
                if data:
                    writer.write(data)
                    await writer.drain()

        async def vnc_to_browser() -> None:
            while data := await reader.read(64 * 1024):
                await websocket.send_bytes(data)

        incoming = asyncio.create_task(browser_to_vnc())
        outgoing = asyncio.create_task(vnc_to_browser())
        done, pending = await asyncio.wait({incoming, outgoing}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    except (OSError, WebSocketDisconnect):
        # The noVNC page handles reconnects; do not turn a transient desktop
        # restart into an unhandled ASGI exception.
        pass
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()


def _proxy_media_url(platform: str, key: str, source: str) -> str:
    return f"/live-proxy/{platform}/{key}/resource?u={quote(source, safe='')}"


def _rewrite_hls_playlist(playlist: str, source_url: str, platform: str, key: str, relay_manager, relay) -> str:
    """Rewrite upstream media/key URIs through the collector, like domestic Bili.

    The browser always keeps one collector-origin playlist URL while signed
    CDN URLs remain private implementation details and may be refreshed by
    the collector without rebuilding the player.
    """
    def rewrite_uri(value: str) -> str:
        absolute = relay_manager.absolute_url(source_url, value)
        relay_manager.learn_proxy_url(relay, absolute)
        return _proxy_media_url(platform, key, absolute)

    rewritten: list[str] = []
    uri_pattern = re.compile(r'URI="([^"]+)"')
    for raw_line in playlist.splitlines():
        line = raw_line.strip()
        if not line:
            rewritten.append(raw_line)
            continue
        if line.startswith("#"):
            rewritten.append(uri_pattern.sub(lambda match: f'URI="{rewrite_uri(match.group(1))}"', raw_line))
        else:
            rewritten.append(rewrite_uri(line))
    return "\n".join(rewritten) + "\n"


async def _fetch_proxy_url(url: str, relay_manager, relay) -> tuple[bytes, str]:
    if not relay_manager.allow_proxy_url(relay, url):
        raise HTTPException(status_code=403, detail="HLS proxy target is not part of this live source")
    headers = {
        "User-Agent": relay.headers or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0 Safari/537.36",
        "Accept": "*/*",
    }
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True, trust_env=False) as client:
            upstream = await client.get(url, headers=headers)
            upstream.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream HLS fetch failed: {type(exc).__name__}") from exc
    return upstream.content, str(upstream.headers.get("content-type") or "")


@app.get("/live-proxy/{platform}/{key}/index.m3u8")
async def live_proxy_playlist(platform: str, key: str, request: Request) -> Response:
    manager = get_hls_relay_manager(settings.hls_output_root, settings.collector_public_base_url)
    relay = manager.proxy_relay(key, platform)
    if relay is None:
        # Backward compatibility for a business page that cached the proxy
        # URL before the service switched back to the locally tested relay.
        # The playlist is still the same live run; only the route changed.
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        local_playlist = (settings.hls_output_root / safe_platform / safe_key / "index.m3u8").resolve()
        root = settings.hls_output_root.resolve()
        if local_playlist.is_file() and root in local_playlist.parents:
            return FileResponse(
                local_playlist,
                media_type="application/vnd.apple.mpegurl",
                headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Access-Control-Allow-Origin": "*"},
            )
        raise HTTPException(status_code=404, detail="HLS_NOT_READY")
    content, _ = await _fetch_proxy_url(relay.source_url, manager, relay)
    text = content.decode("utf-8", errors="replace")
    if "#EXTM3U" not in text:
        raise HTTPException(status_code=502, detail="upstream did not return an HLS playlist")
    return Response(
        _rewrite_hls_playlist(text, relay.source_url, platform, key, manager, relay),
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Access-Control-Allow-Origin": "*"},
    )


@app.get("/live-proxy/{platform}/{key}/resource")
async def live_proxy_resource(platform: str, key: str, u: str) -> Response:
    manager = get_hls_relay_manager(settings.hls_output_root, settings.collector_public_base_url)
    relay = manager.proxy_relay(key, platform)
    if relay is None:
        raise HTTPException(status_code=404, detail="HLS_NOT_READY")
    content, content_type = await _fetch_proxy_url(u, manager, relay)
    if content.lstrip().startswith(b"#EXTM3U"):
        text = content.decode("utf-8", errors="replace")
        return Response(
            _rewrite_hls_playlist(text, u, platform, key, manager, relay),
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Access-Control-Allow-Origin": "*"},
        )
    upstream_type = content_type.split(";", 1)[0]
    # Google/TikTok CDNs commonly send HLS TS as octet-stream. Explicitly
    # identify it for browser media pipelines, matching the domestic relay.
    if not upstream_type or upstream_type == "application/octet-stream":
        upstream_type = "video/mp2t" if urlparse(u).path.lower().endswith((".ts", "/seg.ts")) else "application/octet-stream"
    return Response(
        content,
        media_type=upstream_type,
        headers={"Cache-Control": "public, max-age=3", "Access-Control-Allow-Origin": "*"},
    )


@app.get("/live-proxy/{platform}/{key}/{filename:path}")
async def live_proxy_legacy_file(platform: str, key: str, filename: str) -> FileResponse:
    """Serve local relay files through the old proxy prefix during URL rollout."""
    if filename in {"", "index.m3u8"} or filename.startswith("resource") or ".." in filename:
        raise HTTPException(status_code=404, detail="HLS_NOT_READY")
    safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
    safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
    candidate = (settings.hls_output_root / safe_platform / safe_key / filename).resolve()
    root = settings.hls_output_root.resolve()
    if root not in candidate.parents or not candidate.is_file():
        raise HTTPException(status_code=404, detail="HLS_NOT_READY")
    media_type = "video/mp2t" if candidate.suffix.lower() == ".ts" else "application/octet-stream"
    return FileResponse(candidate, media_type=media_type, headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"})


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "app": settings.app_name,
        "environment": settings.environment,
    }


@app.get("/platforms")
def platforms() -> dict:
    return {"items": platform_statuses()}


app.include_router(collections_router)
app.include_router(collector_jobs_router)
app.include_router(live_monitor_router)
app.include_router(live_monitor_v1_router)
app.include_router(agent_health_router)
