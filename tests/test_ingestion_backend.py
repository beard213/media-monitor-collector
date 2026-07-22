from datetime import datetime, timezone

import httpx

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
from app.ingestion.backend import CollectorBackendClient, sync_youtube_batch_to_backend


def test_backend_client_uploads_object_with_collector_headers(tmp_path):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video-bytes")
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["access_key"] = request.headers.get("accessKey")
        seen["access_token"] = request.headers.get("accessToken")
        body = request.read()
        seen["body"] = body
        return httpx.Response(
            201,
            json={
                "objectKey": "ingestion/job-1/videos/video.mp4",
                "url": "http://minio/media-monitor/ingestion/job-1/videos/video.mp4",
                "size": len(body),
                "contentType": "video/mp4",
                "objectType": "VIDEO",
            },
        )

    client = CollectorBackendClient(
        base_url="http://backend:18082/api/v1",
        access_key="collector-default",
        access_token="12345678",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.upload_object(video, object_type="VIDEO", object_key="ingestion/job-1/videos/video.mp4")

    assert seen["url"] == "http://backend:18082/api/v1/ingestion/collector/objects"
    assert seen["access_key"] == "collector-default"
    assert seen["access_token"] == "12345678"
    assert b'name="objectType"' in seen["body"]
    assert b"VIDEO" in seen["body"]
    assert result.object_key == "ingestion/job-1/videos/video.mp4"
    assert result.url == "http://minio/media-monitor/ingestion/job-1/videos/video.mp4"


def test_sync_youtube_batch_uploads_assets_and_posts_content(tmp_path):
    video = tmp_path / "dQw4w9WgXcQ.mp4"
    video.write_bytes(b"video-bytes")
    thumbnail = tmp_path / "thumb.jpg"
    thumbnail.write_bytes(b"thumb-bytes")
    calls: list[tuple[str, str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.method, str(request.url), body))
        if str(request.url).endswith("/objects"):
            object_type = "VIDEO" if b"VIDEO" in body else "THUMBNAIL"
            suffix = "videos/dQw4w9WgXcQ.mp4" if object_type == "VIDEO" else "thumbnails/dQw4w9WgXcQ.jpg"
            return httpx.Response(
                201,
                json={
                    "objectKey": f"ingestion/job-1/{suffix}",
                    "url": f"http://minio/media-monitor/ingestion/job-1/{suffix}",
                    "size": len(body),
                    "contentType": "video/mp4" if object_type == "VIDEO" else "image/jpeg",
                    "objectType": object_type,
                },
            )
        return httpx.Response(
            200,
            json={
                "contentId": 501,
                "sourceVideoId": "dQw4w9WgXcQ",
                "status": "QUEUED",
                "mediaObject": "ingestion/job-1/videos/dQw4w9WgXcQ.mp4",
                "mediaUrl": "http://minio/media-monitor/ingestion/job-1/videos/dQw4w9WgXcQ.mp4",
                "thumbnailObject": "ingestion/job-1/thumbnails/dQw4w9WgXcQ.jpg",
                "thumbnailUrl": "http://minio/media-monitor/ingestion/job-1/thumbnails/dQw4w9WgXcQ.jpg",
            },
        )

    client = CollectorBackendClient(
        base_url="http://backend:18082/api/v1",
        access_key="collector-default",
        access_token="12345678",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    request = CollectionRequest(
        job_id="job-1",
        platform=Platform.YOUTUBE,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(
            platform=Platform.YOUTUBE,
            target_type="ACCOUNT",
            target_value="@RickAstleyYT",
        ),
        limit=1,
        collect_comments=True,
        download_media=True,
    )
    batch = CollectionBatch(
        accounts=[
            CollectedAccount(
                platform=Platform.YOUTUBE,
                external_id="UCuAXFkgsw1L7xaCfnd5JJOw",
                username="@RickAstleyYT",
                nickname="Rick Astley",
            )
        ],
        contents=[
            CollectedContent(
                platform=Platform.YOUTUBE,
                external_id="dQw4w9WgXcQ",
                url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                content_type="VIDEO",
                title="Never Gonna Give You Up",
                author_external_id="UCuAXFkgsw1L7xaCfnd5JJOw",
                published_at=datetime(2009, 10, 25, 6, 57, 33, tzinfo=timezone.utc),
                raw={"duration": 213, "view_count": 100, "like_count": 10, "comment_count": 1},
            )
        ],
        comments=[
            CollectedComment(
                platform=Platform.YOUTUBE,
                external_id="comment-1",
                content_external_id="dQw4w9WgXcQ",
                author_external_id="@viewer",
                text="hello",
                published_at=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
                raw={"author": "viewer", "like_count": 3},
            )
        ],
        assets=[
            MediaAsset(
                platform=Platform.YOUTUBE,
                content_external_id="dQw4w9WgXcQ",
                asset_type="VIDEO",
                local_path=str(video),
            ),
            MediaAsset(
                platform=Platform.YOUTUBE,
                content_external_id="dQw4w9WgXcQ",
                asset_type="THUMBNAIL",
                local_path=str(thumbnail),
            ),
        ],
    )

    result = sync_youtube_batch_to_backend(batch, request, client=client)

    assert len(calls) == 3
    assert calls[0][1] == "http://backend:18082/api/v1/ingestion/collector/objects"
    assert calls[1][1] == "http://backend:18082/api/v1/ingestion/collector/objects"
    assert calls[2][1] == "http://backend:18082/api/v1/ingestion/collector/contents"
    posted = calls[2][2].decode()
    assert '"platform":"YOUTUBE"' in posted
    assert '"videoId":"dQw4w9WgXcQ"' in posted
    assert '"videoUrl":"http://minio/media-monitor/ingestion/job-1/videos/dQw4w9WgXcQ.mp4"' in posted
    assert '"thumbnailUrl":"http://minio/media-monitor/ingestion/job-1/thumbnails/dQw4w9WgXcQ.jpg"' in posted
    assert '"comments":[{"commentId":"comment-1"' in posted
    assert result["items"][0]["status"] == "QUEUED"
    assert batch.assets[0].object_key == "ingestion/job-1/videos/dQw4w9WgXcQ.mp4"
    assert batch.assets[1].url == "http://minio/media-monitor/ingestion/job-1/thumbnails/dQw4w9WgXcQ.jpg"


def test_sync_instagram_batch_posts_instagram_platform(tmp_path):
    video = tmp_path / "DW9XXwFjDvK.mp4"
    video.write_bytes(b"video-bytes")
    thumbnail = tmp_path / "thumb.jpg"
    thumbnail.write_bytes(b"thumb-bytes")
    posted_payloads: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        if str(request.url).endswith("/objects"):
            object_type = "VIDEO" if b"VIDEO" in body else "THUMBNAIL"
            directory = "videos" if object_type == "VIDEO" else "thumbnails"
            suffix = "mp4" if object_type == "VIDEO" else "jpg"
            return httpx.Response(
                201,
                json={
                    "objectKey": f"ingestion/job-ig/{directory}/DW9XXwFjDvK.{suffix}",
                    "url": f"http://minio/media-monitor/ingestion/job-ig/{directory}/DW9XXwFjDvK.{suffix}",
                    "size": len(body),
                    "contentType": "video/mp4" if object_type == "VIDEO" else "image/jpeg",
                    "objectType": object_type,
                },
            )
        posted_payloads.append(body)
        return httpx.Response(200, json={"contentId": 600, "sourceVideoId": "DW9XXwFjDvK", "status": "QUEUED"})

    client = CollectorBackendClient(
        base_url="http://backend:18082/api/v1",
        access_key="collector-default",
        access_token="12345678",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    request = CollectionRequest(
        job_id="job-ig",
        platform=Platform.INSTAGRAM,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(platform=Platform.INSTAGRAM, target_type="CONTENT", target_value="DW9XXwFjDvK"),
        limit=1,
        collect_comments=False,
        download_media=True,
    )
    batch = CollectionBatch(
        accounts=[CollectedAccount(platform=Platform.INSTAGRAM, external_id="274109692", username="stellawebb", nickname="Stella")],
        contents=[
            CollectedContent(
                platform=Platform.INSTAGRAM,
                external_id="DW9XXwFjDvK",
                url="https://www.instagram.com/reel/DW9XXwFjDvK/",
                content_type="SHORT_VIDEO",
                title="Video by stellawebb",
                author_external_id="274109692",
                raw={"duration": 32, "like_count": 413, "comment_count": 18},
            )
        ],
        assets=[
            MediaAsset(platform=Platform.INSTAGRAM, content_external_id="DW9XXwFjDvK", asset_type="VIDEO", local_path=str(video)),
            MediaAsset(platform=Platform.INSTAGRAM, content_external_id="DW9XXwFjDvK", asset_type="THUMBNAIL", local_path=str(thumbnail)),
        ],
    )

    result = sync_youtube_batch_to_backend(batch, request, client=client)

    assert result["items"][0]["status"] == "QUEUED"
    assert '"platform":"INSTAGRAM"' in posted_payloads[0].decode()
    assert '"videoId":"DW9XXwFjDvK"' in posted_payloads[0].decode()


def test_sync_tiktok_batch_posts_tiktok_platform(tmp_path):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"video-bytes")
    thumbnail = tmp_path / "thumb.jpg"
    thumbnail.write_bytes(b"thumb-bytes")
    posted_payloads: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        if str(request.url).endswith("/objects"):
            object_type = "VIDEO" if b"VIDEO" in body else "THUMBNAIL"
            directory = "videos" if object_type == "VIDEO" else "thumbnails"
            suffix = "mp4" if object_type == "VIDEO" else "jpg"
            return httpx.Response(
                201,
                json={
                    "objectKey": f"ingestion/job-tt/{directory}/video.{suffix}",
                    "url": f"http://minio/media-monitor/ingestion/job-tt/{directory}/video.{suffix}",
                    "size": len(body),
                    "contentType": "video/mp4" if object_type == "VIDEO" else "image/jpeg",
                    "objectType": object_type,
                },
            )
        posted_payloads.append(body)
        return httpx.Response(200, json={"contentId": 700, "sourceVideoId": "video", "status": "QUEUED"})

    client = CollectorBackendClient(
        base_url="http://backend:18082/api/v1",
        access_key="collector-default",
        access_token="12345678",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    request = CollectionRequest(
        job_id="job-tt",
        platform=Platform.TIKTOK,
        capability=Capability.MEDIA_DOWNLOAD,
        target=CollectionTarget(platform=Platform.TIKTOK, target_type="CONTENT", target_value="video"),
        limit=1,
        collect_comments=True,
        download_media=True,
    )
    batch = CollectionBatch(
        accounts=[CollectedAccount(platform=Platform.TIKTOK, external_id="107955", username="tiktok", nickname="TikTok")],
        contents=[
            CollectedContent(
                platform=Platform.TIKTOK,
                external_id="7665098256803810591",
                url="https://www.tiktok.com/@tiktok/video/7665098256803810591",
                content_type="SHORT_VIDEO",
                title="Sample",
                author_external_id="107955",
                published_at=datetime(2026, 7, 21, 21, 36, 16, tzinfo=timezone.utc),
                raw={"duration": 93, "view_count": 21600, "like_count": 758, "comment_count": 132},
            )
        ],
        comments=[
            CollectedComment(
                platform=Platform.TIKTOK,
                external_id="comment-1",
                content_external_id="7665098256803810591",
                author_external_id="viewer-1",
                text="hello",
                published_at=datetime(2026, 7, 21, 22, 0, 0, tzinfo=timezone.utc),
                raw={"author": "viewer", "like_count": 3},
            )
        ],
        assets=[
            MediaAsset(platform=Platform.TIKTOK, content_external_id="7665098256803810591", asset_type="VIDEO", local_path=str(video)),
            MediaAsset(platform=Platform.TIKTOK, content_external_id="7665098256803810591", asset_type="THUMBNAIL", local_path=str(thumbnail)),
        ],
    )

    result = sync_youtube_batch_to_backend(batch, request, client=client)

    assert result["items"][0]["status"] == "QUEUED"
    assert '"platform":"TIKTOK"' in posted_payloads[0].decode()
    assert '"comments":[{"commentId":"comment-1"' in posted_payloads[0].decode()
