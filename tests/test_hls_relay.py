from app.ingestion.hls_relay import HlsRelay


def test_hls_relay_translates_mixed_socks_proxy_for_ffmpeg(tmp_path):
    relay = HlsRelay(
        tmp_path,
        "https://r.example/live.m3u8",
        upstream_proxy="socks5://127.0.0.1:17891",
    )

    assert relay.upstream_proxy == "http://127.0.0.1:17891"


def test_hls_relay_rejects_unsupported_proxy_scheme(tmp_path):
    relay = HlsRelay(tmp_path, "https://r.example/live.m3u8", upstream_proxy="ftp://127.0.0.1:2121")

    assert relay.upstream_proxy == ""
