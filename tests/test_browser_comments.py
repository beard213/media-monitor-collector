from app.adapters.browser_comments import comments_from_json, comments_from_text_blocks
from app.domain.enums import Platform


def test_comments_from_json_finds_nested_comment_records():
    payload = {
        "data": {
            "comments": [
                {
                    "cid": "comment-1",
                    "text": "hello world",
                    "user": {"uid": "user-1", "nickname": "Viewer"},
                    "create_time": 1_700_000_000,
                    "digg_count": 7,
                }
            ]
        }
    }

    comments = comments_from_json(payload, platform=Platform.TIKTOK, content_id="video-1", limit=5)

    assert len(comments) == 1
    assert comments[0].external_id == "comment-1"
    assert comments[0].content_external_id == "video-1"
    assert comments[0].author_external_id == "user-1"
    assert comments[0].text == "hello world"
    assert comments[0].raw["author"] == "Viewer"
    assert comments[0].raw["like_count"] == 7


def test_comments_from_text_blocks_extracts_visible_comment_like_blocks():
    blocks = [
        "Viewer One\nhello from the browser\n2 likes\n1h",
        "not enough",
        "Viewer Two\nsecond comment\nReply",
    ]

    comments = comments_from_text_blocks(blocks, platform=Platform.INSTAGRAM, content_id="reel-1", limit=5)

    assert [comment.author_external_id for comment in comments] == ["Viewer One", "Viewer Two"]
    assert [comment.text for comment in comments] == ["hello from the browser", "second comment"]
