import os
import time
from types import SimpleNamespace

from app.ingestion.runtime_cleanup import cleanup_after_collection


def test_cleanup_after_collection_removes_only_stale_platform_media(tmp_path):
    storage = tmp_path / "storage"
    youtube = storage / "youtube"
    x = storage / "x"
    private = storage / "private" / "browser_profiles" / "x"
    stale_media = youtube / "job-old" / "video.mp4"
    recent_media = youtube / "job-new" / "video.mp4"
    old_private_cookie = private / "Cookies"
    empty_nested_dir = x / "empty" / "nested"

    stale_media.parent.mkdir(parents=True)
    recent_media.parent.mkdir(parents=True)
    old_private_cookie.parent.mkdir(parents=True)
    empty_nested_dir.mkdir(parents=True)
    stale_media.write_bytes(b"old-video")
    recent_media.write_bytes(b"new-video")
    old_private_cookie.write_bytes(b"session")

    old = time.time() - 3 * 3600
    os.utime(stale_media, (old, old))
    os.utime(old_private_cookie, (old, old))

    settings = SimpleNamespace(
        collector_cleanup_enabled=True,
        collector_cleanup_retention_hours=1,
        collector_cleanup_empty_dirs=True,
        collector_memory_gc_enabled=False,
        youtube_output_root=youtube,
        tiktok_output_root=storage / "tiktok",
        facebook_output_root=storage / "facebook",
        x_output_root=x,
        instagram_output_root=storage / "instagram",
    )

    result = cleanup_after_collection(settings=settings)

    assert result["status"] == "success"
    assert result["filesRemoved"] == 1
    assert result["bytesRemoved"] == len(b"old-video")
    assert not stale_media.exists()
    assert recent_media.exists()
    assert old_private_cookie.exists()
    assert not empty_nested_dir.exists()
