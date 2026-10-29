from app.adapters.browser_comments import (
    BrowserCommentCollector,
    comments_from_json,
    comments_from_text_blocks,
    youtube_author_profile_url,
    youtube_comments_from_dom_nodes,
)
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
        "0:28\nrecommended video title\nFacebook\n2.3M views\n5d ago",
    ]

    comments = comments_from_text_blocks(blocks, platform=Platform.INSTAGRAM, content_id="reel-1", limit=5)

    # The visible generic block contains display names only, not stable IDs.
    assert [comment.author_external_id for comment in comments] == ["", ""]
    assert [comment.raw["author"] for comment in comments] == ["Viewer One", "Viewer Two"]
    assert [comment.text for comment in comments] == ["hello from the browser", "second comment"]


def test_youtube_dom_comments_preserve_permalink_id_and_author():
    comments = youtube_comments_from_dom_nodes(
        [{
            "id": "UgzExampleComment",
            "author": "Viewer One",
            "author_url": "https://www.youtube.com/@viewerone",
            "text": "This was collected from YouTube's visible comment DOM.",
            "like_count": "12",
            "published_text": "2 days ago",
        }],
        content_id="video-1",
        limit=5,
    )

    assert len(comments) == 1
    assert comments[0].external_id == "UgzExampleComment"
    assert comments[0].author_external_id == "viewerone"
    assert comments[0].author_profile_url == "https://www.youtube.com/@viewerone"
    assert comments[0].raw["source"] == "youtube_browser_dom"
    assert comments[0].raw["like_count"] == 12


def test_youtube_channel_uid_becomes_a_routable_profile_url():
    assert youtube_author_profile_url("UCabcDef_123", "") == "https://www.youtube.com/channel/UCabcDef_123"
    comments = youtube_comments_from_dom_nodes(
        [{"id": "comment-1", "author": "Viewer", "author_id": "UCabcDef_123", "text": "A real comment."}],
        content_id="video-1",
        limit=5,
    )
    assert comments[0].author_external_id == "UCabcDef_123"
    assert comments[0].author_profile_url == "https://www.youtube.com/channel/UCabcDef_123"


def test_x_numeric_uid_and_post_url_never_become_profile_targets():
    comments = comments_from_json(
        {"id": "reply-1", "text": "a real reply", "author": {"id": "123456", "url": "https://x.com/123456/status/99"}},
        platform=Platform.X,
        content_id="source-post",
        limit=5,
    )
    assert len(comments) == 1
    assert comments[0].author_external_id == "123456"
    assert comments[0].author_profile_url == ""
    assert comments[0].raw["author_url"] == ""


def test_comments_from_text_blocks_extracts_instagram_flat_comment_stream():
    body = """Log In
Sign Up
stellawebb
•
Follow
stellawebb
Edited
•
14w
@nasa This reel goes out to the Artemis II crew
chrd___
\xa0
2w
😂😂😂😂
Like
Reply
orlandomarosini
\xa0
13w
well thats genius!😂🤩🙌🏽💫
Like
Reply
"""

    comments = comments_from_text_blocks([body], platform=Platform.INSTAGRAM, content_id="reel-1", limit=5)

    assert [comment.author_external_id for comment in comments] == ["chrd___", "orlandomarosini"]
    assert [comment.text for comment in comments] == ["😂😂😂😂", "well thats genius!😂🤩🙌🏽💫"]


def test_comments_from_text_blocks_extracts_tiktok_flat_comment_stream():
    body = """TikTok
Search
For You
Comments
You may like
283 comments
Rachel DeMita
I’m so sad it’s over
10h ago
Reply
10
View 4 replies
coolskyranterlol
CHANGE THE NEW PROFILE THING CHANGE IT BACK
1h ago
Reply
1
"""

    comments = comments_from_text_blocks([body], platform=Platform.TIKTOK, content_id="video-1", limit=5)

    # TikTok's flat text stream has no verified profile identity.  Keep the
    # display name in raw data, but do not submit it as an account ID.
    assert [comment.author_external_id for comment in comments] == ["", ""]
    assert [comment.raw["author"] for comment in comments] == ["Rachel DeMita", "coolskyranterlol"]
    assert [comment.text for comment in comments] == ["I’m so sad it’s over", "CHANGE THE NEW PROFILE THING CHANGE IT BACK"]


def test_comments_from_json_ignores_facebook_ui_text_without_author():
    payload = {"data": [{"text": "全部"}, {"text": "real comment", "author": {"id": "viewer-1", "name": "Viewer One"}}]}

    comments = comments_from_json(payload, platform=Platform.FACEBOOK, content_id="video-1", limit=5)

    assert len(comments) == 1
    assert comments[0].author_external_id == "viewer-1"
    assert comments[0].text == "real comment"


def test_comments_from_json_ignores_unhashable_candidate_values():
    payload = {
        "data": [
            {"id": "bad-comment", "text": {"runs": [{"text": "nested text"}]}, "author": {"id": "viewer-1"}},
            {"id": "good-comment", "text": "real comment", "author": {"id": "viewer-2"}},
        ]
    }

    comments = comments_from_json(payload, platform=Platform.X, content_id="post-1", limit=5)

    assert any(comment.external_id == "good-comment" and comment.text == "real comment" for comment in comments)


def test_x_comments_from_json_ignores_cursors_and_extracts_tweet_replies():
    payload = {
        "data": {
            "threaded_conversation_with_injections_v2": {
                "instructions": [
                    {
                        "entries": [
                            {
                                "__typename": "TimelineTimelineCursor",
                                "cursorType": "Top",
                                "entryType": "TimelineTimelineCursor",
                                "value": "DAAJAAA",
                            },
                            {
                                "content": {
                                    "itemContent": {
                                        "tweet_results": {
                                            "result": {
                                                "__typename": "Tweet",
                                                "rest_id": "reply-1",
                                                "core": {
                                                    "user_results": {
                                                        "result": {
                                                            "rest_id": "user-1",
                                                            "legacy": {
                                                                "screen_name": "reply_user",
                                                                "name": "Reply User",
                                                                "profile_image_url_https": "https://pbs.twimg.com/profile_images/reply_user.jpg",
                                                            },
                                                        }
                                                    }
                                                },
                                                "legacy": {
                                                    "full_text": "这才是真正的评论",
                                                    "created_at": "Tue Nov 14 22:13:20 +0000 2023",
                                                    "favorite_count": 7,
                                                },
                                            }
                                        }
                                    }
                                }
                            },
                        ]
                    }
                ]
            }
        }
    }

    comments = comments_from_json(payload, platform=Platform.X, content_id="source-post", limit=5)

    assert [comment.external_id for comment in comments] == ["reply-1"]
    assert comments[0].author_external_id == "reply_user"
    assert comments[0].text == "这才是真正的评论"
    assert comments[0].raw["author"] == "Reply User"
    assert comments[0].author_avatar_url == "https://pbs.twimg.com/profile_images/reply_user.jpg"
    assert comments[0].raw["author_avatar_url"] == "https://pbs.twimg.com/profile_images/reply_user.jpg"


def test_x_comments_from_text_blocks_require_handle_and_real_comment_text():
    body = """To view keyboard shortcuts, press question mark
查看键盘快捷键
乌克兰战争・最新进展
@HXR001
牛牛
@JosephYeung11
匿名用户
显示更多
Reply User
@reply_user
·
2h
这是一条真实评论
Reply
Repost
Like
"""

    comments = comments_from_text_blocks([body], platform=Platform.X, content_id="post-1", limit=5)

    assert [comment.author_external_id for comment in comments] == ["reply_user"]
    assert [comment.text for comment in comments] == ["这是一条真实评论"]


def test_browser_comment_collector_falls_back_when_cdp_browser_is_unavailable(tmp_path, monkeypatch):
    collector = BrowserCommentCollector()
    monkeypatch.setattr(
        collector,
        "_collect_from_cdp",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("CDP browser is unavailable")),
    )

    comments = collector.collect(
        url="https://example.com/post-1",
        platform=Platform.INSTAGRAM,
        content_id="post-1",
        profile=tmp_path / "missing-profile",
        browser="chromium",
        limit=10,
        timeout_seconds=5,
        cdp_url="http://127.0.0.1:9225",
    )

    assert comments == []
