from __future__ import annotations

import os
import signal
import shutil
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse


class HlsRelay:
    """Small persistent HLS relay for a live source.

    The relay owns one FFmpeg process and publishes a sliding playlist. It is
    deliberately independent from MP4 segment upload so a slow downstream
    upload cannot pause browser playback.
    """

    def __init__(
        self,
        output_dir: Path,
        source_url: str,
        headers: str = "",
        *,
        proxy_source: bool = False,
        upstream_proxy: str = "",
    ) -> None:
        self.output_dir = output_dir
        self.source_url = source_url
        self.headers = headers
        self.proxy_source = proxy_source
        # The resolver uses the platform proxy, therefore the media relay
        # must use the same egress.  In particular, signed Googlevideo URLs
        # can be tied to the IP/region that resolved them.  sing-box exposes
        # a mixed listener, so its SOCKS URL can safely be presented to
        # FFmpeg as an HTTP CONNECT proxy on the same host and port.
        self.upstream_proxy = self._ffmpeg_http_proxy(upstream_proxy)
        # Hosts learned from the source playlist. They constrain the media
        # proxy so its public endpoint cannot be used as an arbitrary proxy.
        self.allowed_hosts: set[str] = {str(urlparse(source_url).hostname or "").lower()}
        # Changes on every collector process/relay recreation. The public URL
        # includes it so a frontend that keys its player by playUrl will
        # rebuild HLS state after a relay restart instead of retaining an old
        # media sequence in memory.
        self.generation = str(int(time.time() * 1000))
        self.process: subprocess.Popen | None = None
        self.last_exit_code: int | None = None
        self._lock = threading.Lock()

    @property
    def playlist(self) -> Path:
        return self.output_dir / "index.m3u8"

    def start(self) -> None:
        if self.proxy_source:
            # Native upstream HLS is already browser segmented. Proxying its
            # manifest retains the platform's continuous timeline and avoids
            # adding a second FFmpeg segment boundary.
            return
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg or not self.source_url:
            return
        with self._lock:
            if self.process and self.process.poll() is None:
                return
            if self.process:
                self.last_exit_code = self.process.returncode
            self.output_dir.mkdir(parents=True, exist_ok=True)
            # Keep recent prior segments across a relay restart. A browser
            # can still be downloading them while it observes the new
            # manifest; deleting them creates a fatal 404. Unique epoch-based
            # sequence numbers below avoid overwriting them. Prune only media
            # well outside the public recovery window.
            cutoff = time.time() - 300
            for stale_file in self.output_dir.glob("segment-*.ts"):
                try:
                    if stale_file.stat().st_mtime < cutoff:
                        stale_file.unlink(missing_ok=True)
                except OSError:
                    pass
            try:
                self.playlist.unlink(missing_ok=True)
            except OSError:
                pass
            command = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error"]
            relay_headers = self.headers or "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0 Safari/537.36\r\nAccept: */*\r\n"
            command.extend(["-headers", relay_headers])
            if self.upstream_proxy:
                command.extend(["-http_proxy", self.upstream_proxy])
            command.extend([
                "-fflags", "+genpts",
                # Do not build an excessive input buffer.  The relay has its
                # own 40-second output window below; buffering both sides is
                # what makes a browser fall behind a live edge.
                "-analyzeduration", "0",
                "-probesize", "32768",
                "-reconnect", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "5",
                "-re",
                "-i", self.source_url,
                "-map", "0:v:0",
                "-map", "0:a:0?",
                # YouTube's selected HLS renditions are already H.264/AAC.
                # Re-encoding every feed used virtually all CPU on this
                # two-vCPU collector (seven simultaneous relays), causing
                # playlists to stop advancing and clients to black-screen.
                # Remuxing preserves the upstream keyframes; actual segment
                # duration may be slightly longer than hls_time, but the
                # public rolling buffer absorbs that safely and the relay
                # no longer competes with every other monitored stream.
                "-c:v", "copy",
                "-c:a", "copy",
                # Flush packets immediately; otherwise the encoder/muxer can
                # hold a completed GOP while the player is already behind the
                # live edge.
                "-flush_packets", "1",
                "-muxdelay", "0",
                "-muxpreload", "0",
                "-f", "hls",
                # A one-second output segment can take 1.3-2.1 seconds to
                # arrive from the overseas source, so it is already late by
                # the time a player asks for it. Three-second segments give
                # each request real transport headroom while this ten-entry
                # window still keeps roughly thirty seconds of rolling media.
                "-hls_time", "3",
                "-hls_list_size", "10",
                # Keep old TS files on disk even after they leave the public
                # eight-second playlist.  External players can briefly fall
                # behind while fetching the manifest; deleting a segment at
                # that point causes a 404, a decoder reset, and visible
                # stutter.  This does not add playback latency because the
                # playlist still advertises only the latest eight seconds.
                "-hls_delete_threshold", "30",
                # Never restart media sequence numbers from zero: browsers
                # may retain the previous manifest and reject a backwards
                # sequence jump as stale playback.
                "-hls_start_number_source", "epoch",
                "-hls_flags", "delete_segments+discont_start+independent_segments+program_date_time",
                "-hls_segment_filename", str(self.output_dir / "segment-%06d.ts"),
                str(self.playlist),
            ])
            # `-live_start_index` belongs to FFmpeg's HLS demuxer. Streamlink
            # commonly resolves TikTok to a continuous FLV/HTTP stream; if
            # the option is passed before that input FFmpeg exits immediately
            # with "Option live_start_index not found" and no local playlist
            # is produced. Keep the live-edge optimization for HLS only.
            if urlparse(self.source_url).path.lower().endswith(".m3u8"):
                # The input is already a real-time HLS playlist. `-re` adds a
                # second pacing layer and makes FFmpeg fall behind the live
                # edge. Start from the newest available input segment instead
                # of intentionally reading three segments (~6s) late.
                command.remove("-re")
                input_index = command.index("-i")
                command[input_index:input_index] = ["-live_start_index", "-1"]
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )

    @staticmethod
    def _ffmpeg_http_proxy(proxy: str) -> str:
        """Return an FFmpeg-compatible HTTP proxy without leaking credentials.

        The deployed sing-box listener is ``mixed``: both SOCKS and HTTP
        CONNECT are accepted on its single local port.  FFmpeg's HTTPS
        protocol supports HTTP CONNECT but not a SOCKS URL option.
        """
        value = str(proxy or "").strip()
        parsed = urlparse(value)
        if parsed.scheme.lower() in {"socks", "socks4", "socks4a", "socks5", "socks5h"} and parsed.netloc:
            return f"http://{parsed.netloc}"
        if parsed.scheme.lower() in {"http", "https"} and parsed.netloc:
            return value
        return ""

    def stop(self) -> None:
        with self._lock:
            process = self.process
            self.process = None
        if not process or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def ready(self, *, min_segments: int = 6) -> bool:
        if self.proxy_source:
            return bool(self.source_url)
        if not self.playlist.is_file() or self.playlist.stat().st_size <= 0:
            return False
        try:
            text = self.playlist.read_text(encoding="utf-8", errors="ignore")
            if "#EXT-X-ENDLIST" in text:
                return False
            # Do not advertise playback after the first file. A player that
            # starts immediately has no cushion for one cross-border RTT or
            # a short CDN pause and visibly stutters. Wait for a small rolling
            # buffer before setting playbackReady=true.
            available = sum(1 for line in text.splitlines() if line.startswith("segment-") and line.endswith(".ts"))
            return available >= max(1, int(min_segments))
        except OSError:
            return False

    def stale(self, max_age_seconds: float = 12.0) -> bool:
        """Whether the playlist stopped advancing for longer than the live SLA."""
        try:
            return not self.playlist.is_file() or time.time() - self.playlist.stat().st_mtime > max_age_seconds
        except OSError:
            return True

    def diagnostic(self) -> str:
        """A non-sensitive relay state suitable for task status/logs."""
        with self._lock:
            process = self.process
            if process and process.poll() is None:
                return "ffmpeg_running"
            if process:
                self.last_exit_code = process.returncode
            if self.last_exit_code is not None:
                return f"ffmpeg_exited_{self.last_exit_code}"
            return "ffmpeg_not_started"


class HlsRelayManager:
    def __init__(self, root: Path, public_base_url: str) -> None:
        self.root = root
        self.public_base_url = public_base_url.rstrip("/")
        self._relays: dict[str, HlsRelay] = {}
        self._lock = threading.Lock()

    def ensure(
        self,
        key: str,
        source_url: str,
        headers: str = "",
        platform: str = "tiktok",
        upstream_proxy: str = "",
    ) -> str:
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        relay_key = f"{safe_platform}-{safe_key}"
        # Keep the public playback route on the locally produced, browser
        # tested TS playlist. Dynamic upstream HLS proxying remains available
        # only as an isolated diagnostic path; it must not replace a live
        # player until browser playback has passed an end-to-end test.
        proxy_source = False
        with self._lock:
            relay = self._relays.get(relay_key)
            if relay is None:
                relay = HlsRelay(
                    self.root / safe_platform / safe_key,
                    source_url,
                    headers,
                    proxy_source=proxy_source,
                    upstream_proxy=upstream_proxy,
                )
                self._relays[relay_key] = relay
            elif source_url and source_url != relay.source_url:
                # Signed CDN URLs rotate while a room stays live. Keep a
                # currently healthy FFmpeg process on its existing URL so a
                # resolver refresh does not truncate the public playlist. If
                # the old process has already exited, start immediately from
                # the fresh URL below.
                relay.source_url = source_url
                relay.allowed_hosts.add(str(urlparse(source_url).hostname or "").lower())
            # A live manifest can remain technically connected while its CDN
            # request has stopped producing segments. Restart only after the
            # playlist is stale; a healthy relay is never interrupted by URL
            # rotation.
            if not relay.proxy_source and relay.process and relay.process.poll() is None and relay.stale():
                relay.stop()
            relay.start()
        if proxy_source:
            return f"{self.public_base_url}/live-proxy/{safe_platform}/{safe_key}/index.m3u8"
        return f"{self.public_base_url}/live/{safe_platform}/{safe_key}/index.m3u8?generation={relay.generation}"

    def stop(self, key: str, platform: str = "tiktok") -> None:
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        with self._lock:
            relay = self._relays.pop(f"{safe_platform}-{safe_key}", None)
        if relay:
            relay.stop()

    def ready(self, key: str, platform: str = "tiktok", *, min_segments: int = 6) -> bool:
        """Return true only when the live playlist has a non-terminal segment."""
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        with self._lock:
            relay = self._relays.get(f"{safe_platform}-{safe_key}")
        return bool(relay and relay.ready(min_segments=min_segments))

    def diagnostic(self, key: str, platform: str = "tiktok") -> str:
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        with self._lock:
            relay = self._relays.get(f"{safe_platform}-{safe_key}")
        return relay.diagnostic() if relay else "relay_not_created"

    def proxy_relay(self, key: str, platform: str) -> HlsRelay | None:
        safe_key = "".join(ch for ch in str(key) if ch.isalnum() or ch in {"-", "_"}) or "live"
        safe_platform = "".join(ch for ch in str(platform).lower() if ch.isalnum() or ch in {"-", "_"}) or "live"
        with self._lock:
            relay = self._relays.get(f"{safe_platform}-{safe_key}")
        return relay if relay and relay.proxy_source else None

    @staticmethod
    def absolute_url(base_url: str, item_url: str) -> str:
        return urljoin(base_url, item_url)

    def allow_proxy_url(self, relay: HlsRelay, url: str) -> bool:
        parsed = urlparse(url)
        host = str(parsed.hostname or "").lower()
        if parsed.scheme not in {"http", "https"} or not host:
            return False
        with relay._lock:
            return host in relay.allowed_hosts

    def learn_proxy_url(self, relay: HlsRelay, url: str) -> None:
        host = str(urlparse(url).hostname or "").lower()
        if host:
            with relay._lock:
                relay.allowed_hosts.add(host)


_manager: HlsRelayManager | None = None
_manager_lock = threading.Lock()


def get_hls_relay_manager(root: Path, public_base_url: str) -> HlsRelayManager:
    global _manager
    with _manager_lock:
        if _manager is None or _manager.root != root or _manager.public_base_url != public_base_url.rstrip("/"):
            _manager = HlsRelayManager(root, public_base_url)
        return _manager
