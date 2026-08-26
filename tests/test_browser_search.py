from app.adapters.browser_search import content_links_from_anchors
from app.domain.enums import Platform


def test_content_links_from_anchors_extracts_tiktok_video_urls():
    links = content_links_from_anchors(
        [
            "https://www.tiktok.com/@creator/video/1234567890123456789?lang=en",
            "/@other/video/2234567890123456789",
            "https://www.tiktok.com/@creator",
        ],
        platform=Platform.TIKTOK,
        limit=5,
    )

    assert links == [
        "https://www.tiktok.com/@creator/video/1234567890123456789",
        "https://www.tiktok.com/@other/video/2234567890123456789",
    ]


def test_content_links_from_anchors_extracts_facebook_video_urls():
    links = content_links_from_anchors(
        [
            "https://www.facebook.com/watch/?v=1234567890&ref=search",
            "https://www.facebook.com/facebook/videos/1230923098438186/",
            "https://www.facebook.com/reel/987654321",
            "https://www.facebook.com/profile.php?id=1000",
        ],
        platform=Platform.FACEBOOK,
        limit=5,
    )

    assert links == [
        "https://www.facebook.com/watch/?v=1234567890",
        "https://www.facebook.com/facebook/videos/1230923098438186",
        "https://www.facebook.com/reel/987654321",
    ]


def test_content_links_from_anchors_extracts_instagram_reel_and_post_urls():
    links = content_links_from_anchors(
        [
            "https://www.instagram.com/reel/DW9XXwFjDvK/?utm_source=ig_web_copy_link",
            "/p/ABC123def45/",
            "https://www.instagram.com/explore/search/keyword/?q=test",
        ],
        platform=Platform.INSTAGRAM,
        limit=5,
    )

    assert links == [
        "https://www.instagram.com/reel/DW9XXwFjDvK",
        "https://www.instagram.com/p/ABC123def45",
    ]


def test_content_links_from_anchors_extracts_x_status_urls():
    links = content_links_from_anchors(
        [
            "https://x.com/example/status/1810000000000000000?s=20",
            "https://twitter.com/other/status/1810000000000000001/video/1",
            "https://x.com/example",
        ],
        platform=Platform.X,
        limit=5,
    )

    assert links == [
        "https://x.com/example/status/1810000000000000000",
        "https://x.com/other/status/1810000000000000001",
    ]
