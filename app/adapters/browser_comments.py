from __future__ import annotations

import hashlib
import shutil
import tempfile
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable

from app.domain.enums import Platform
from app.domain.models import CollectedComment


COMMENT_TEXT_KEYS = {"text", "comment_text", "content", "commentText"}
COMMENT_ID_KEYS = {"cid", "id", "comment_id", "commentId", "pk"}
AUTHOR_ID_KEYS = {"uid", "user_id", "userId", "author_id", "authorId", "id", "username", "unique_id"}
AUTHOR_NAME_KEYS = {"nickname", "username", "unique_id", "name", "full_name", "author"}
LIKE_KEYS = {"digg_count", "like_count", "likeCount", "likes"}
TIME_KEYS = {"create_time", "created_at", "timestamp", "time"}


def comments_from_json(payload: Any, *, platform: Platform, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    for node in _walk_json(payload):
        if len(comments) >= limit:
            break
        comment = _comment_from_json_node(node, platform=platform, content_id=content_id)
        if not comment or comment.external_id in seen:
            continue
        seen.add(comment.external_id)
        comments.append(comment)
    return comments


def comments_from_text_blocks(blocks: Iterable[str], *, platform: Platform, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    for block in blocks:
        if len(comments) >= limit:
            break
        lines = [line.strip() for line in str(block).splitlines() if line.strip()]
        if platform == Platform.X:
            for comment in _x_comments_from_lines(lines, content_id=content_id, limit=limit):
                if comment.external_id in seen:
                    continue
                seen.add(comment.external_id)
                comments.append(comment)
                if len(comments) >= limit:
                    break
            continue
        if platform == Platform.TIKTOK and len(lines) > 8:
            for comment in _tiktok_comments_from_lines(lines, platform=platform, content_id=content_id, limit=limit):
                if comment.external_id in seen:
                    continue
                seen.add(comment.external_id)
                comments.append(comment)
                if len(comments) >= limit:
                    break
            if comments:
                continue
            continue
        if platform == Platform.INSTAGRAM and len(lines) > 8:
            for comment in _instagram_comments_from_lines(lines, platform=platform, content_id=content_id, limit=limit):
                if comment.external_id in seen:
                    continue
                seen.add(comment.external_id)
                comments.append(comment)
                if len(comments) >= limit:
                    break
            if comments:
                continue
        if len(lines) < 2:
            continue
        if platform == Platform.TIKTOK and _looks_like_tiktok_shell_block(lines):
            continue
        author = lines[0]
        if _looks_like_media_duration(author) or any(" views" in line.lower() for line in lines):
            continue
        text = _first_comment_text_line(lines[1:])
        if not text or _looks_like_non_comment_text(text):
            continue
        external_id = _stable_comment_id(platform, content_id, author, text)
        if external_id in seen:
            continue
        seen.add(external_id)
        comments.append(
            CollectedComment(
                platform=platform,
                external_id=external_id,
                content_external_id=content_id,
                author_external_id=author,
                text=text,
                raw={"author": author, "source": "browser_dom", "text_block": block},
            )
        )
    return comments


def _tiktok_comments_from_lines(lines: list[str], *, platform: Platform, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    start = 0
    for index, line in enumerate(lines):
        parts = line.lower().split()
        if len(parts) == 2 and parts[0].isdigit() and parts[1] == "comments":
            start = index + 1
            break
    for index in range(start, len(lines) - 3):
        if len(comments) >= limit:
            break
        author = lines[index].strip()
        text = lines[index + 1].strip()
        time_value = lines[index + 2].strip()
        reply_value = lines[index + 3].strip().lower()
        if author.lower() in {"comments", "you may like", "reply", "like", "tiktok"}:
            continue
        if not text or not _looks_like_tiktok_time(time_value) or reply_value != "reply":
            continue
        external_id = _stable_comment_id(platform, content_id, author, text)
        if external_id in seen:
            continue
        seen.add(external_id)
        comments.append(
            CollectedComment(
                platform=platform,
                external_id=external_id,
                content_external_id=content_id,
                author_external_id=author,
                text=text,
                raw={"author": author, "source": "browser_dom_tiktok", "relative_time": time_value},
            )
        )
    return comments


def _instagram_comments_from_lines(lines: list[str], *, platform: Platform, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    for index, line in enumerate(lines[:-2]):
        if len(comments) >= limit:
            break
        if not _looks_like_instagram_username(line):
            continue
        cursor = index + 1
        while cursor < len(lines) and lines[cursor] in {"\xa0", "•", "Edited"}:
            cursor += 1
        if cursor >= len(lines) or not _looks_like_relative_time(lines[cursor]):
            continue
        cursor += 1
        while cursor < len(lines) and lines[cursor] in {"\xa0", "•"}:
            cursor += 1
        if cursor >= len(lines):
            continue
        text = lines[cursor].strip()
        if not text or text.lower() in {"like", "reply", "follow"}:
            continue
        if "reply" not in {item.lower() for item in lines[cursor + 1 : cursor + 4]}:
            continue
        external_id = _stable_comment_id(platform, content_id, line, text)
        if external_id in seen:
            continue
        seen.add(external_id)
        comments.append(
            CollectedComment(
                platform=platform,
                external_id=external_id,
                content_external_id=content_id,
                author_external_id=line,
                text=text,
                raw={"author": line, "source": "browser_dom_instagram"},
            )
        )
    return comments


def _looks_like_instagram_username(value: str) -> bool:
    if not 2 <= len(value) <= 30:
        return False
    return all(ch.isalnum() or ch in {"_", "."} for ch in value)


def _looks_like_relative_time(value: str) -> bool:
    value = value.strip().lower()
    if not value:
        return False
    return value[-1:] in {"s", "m", "h", "d", "w", "y"} and value[:-1].isdigit()


def _looks_like_tiktok_time(value: str) -> bool:
    value = value.strip().lower()
    if _looks_like_relative_time(value):
        return True
    if value.endswith(" ago") and _looks_like_relative_time(value[:-4].strip()):
        return True
    parts = value.split("-")
    return len(parts) in {2, 3} and all(part.isdigit() for part in parts)


def _looks_like_media_duration(value: str) -> bool:
    parts = value.split(":")
    return len(parts) in {2, 3} and all(part.isdigit() for part in parts)


def _looks_like_tiktok_shell_block(lines: list[str]) -> bool:
    lowered = {line.lower() for line in lines}
    return "log in" in lowered and ("comments" in lowered or "for you" in lowered or "you may like" in lowered)


def _x_comments_from_lines(lines: list[str], *, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    for index in range(0, max(0, len(lines) - 2)):
        if len(comments) >= limit:
            break
        author = lines[index].strip()
        handle = lines[index + 1].strip()
        if not author or not _looks_like_x_handle(handle):
            continue
        cursor = index + 2
        while cursor < len(lines) and _looks_like_x_metadata_line(lines[cursor]):
            cursor += 1
        if cursor >= len(lines):
            continue
        text = lines[cursor].strip()
        if cursor + 1 < len(lines) and _looks_like_x_handle(lines[cursor + 1]):
            continue
        if _looks_like_non_comment_text(author) or _looks_like_non_comment_text(text):
            continue
        author_id = handle.lstrip("@")
        external_id = _stable_comment_id(Platform.X, content_id, author_id, text)
        if external_id in seen:
            continue
        seen.add(external_id)
        comments.append(
            CollectedComment(
                platform=Platform.X,
                external_id=external_id,
                content_external_id=content_id,
                author_external_id=author_id,
                text=text,
                raw={"author": author, "source": "browser_dom_x", "handle": handle},
            )
        )
    return comments


def _looks_like_x_handle(value: str) -> bool:
    value = value.strip()
    if not value.startswith("@") or not 2 <= len(value) <= 31:
        return False
    return all(ch.isalnum() or ch == "_" for ch in value[1:])


def _looks_like_x_metadata_line(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if lowered in {"·", "•", "follow", "following", "subscribed", "reply", "repost", "like", "likes", "share", "show more"}:
        return True
    if lowered in {"显示更多", "回复", "转帖", "喜欢", "关注", "正在关注"}:
        return True
    if _looks_like_relative_time(lowered):
        return True
    if lowered.endswith(" views") or lowered.endswith(" view"):
        return True
    return False


class BrowserCommentCollector:
    def collect(
        self,
        *,
        url: str,
        platform: Platform,
        content_id: str,
        profile: Path,
        browser: str,
        limit: int,
        timeout_seconds: int,
        cdp_url: str | None = None,
    ) -> list[CollectedComment]:
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception:
            return []

        if cdp_url:
            try:
                return self._collect_from_cdp(
                    cdp_url=cdp_url,
                    url=url,
                    platform=platform,
                    content_id=content_id,
                    limit=limit,
                    timeout_seconds=timeout_seconds,
                )
            except Exception:
                # A login browser may be intentionally stopped between jobs.
                # Continue with the copied profile when it is available.
                pass

        if not profile.exists():
            return []

        with tempfile.TemporaryDirectory(prefix=f"browser-comments-{platform.value.lower()}-") as tmpdir:
            profile_copy = Path(tmpdir) / "profile"
            _copy_profile(profile, profile_copy)
            response_payloads: list[Any] = []

            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(profile_copy),
                    executable_path=_chromium_executable(browser),
                    headless=True,
                    viewport={"width": 1366, "height": 900},
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                    timeout=timeout_seconds * 1000,
                )
                page = context.new_page()

                def on_response(response):
                    response_url = response.url.lower()
                    if not _looks_like_comment_response(response_url):
                        return
                    try:
                        response_payloads.append(response.json())
                    except Exception:
                        return

                page.on("response", on_response)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
                    _interact_for_comments(page, platform, timeout_seconds=timeout_seconds)
                    comments = _comments_from_payloads(response_payloads, platform=platform, content_id=content_id, limit=limit)
                    if len(comments) < limit:
                        blocks = _visible_comment_blocks(page)
                        comments.extend(
                            comment
                            for comment in comments_from_text_blocks(blocks, platform=platform, content_id=content_id, limit=limit)
                            if comment.external_id not in {item.external_id for item in comments}
                        )
                    return comments[:limit]
                except PlaywrightTimeoutError:
                    return _comments_from_payloads(response_payloads, platform=platform, content_id=content_id, limit=limit)
                finally:
                    context.close()

    def _collect_from_cdp(
        self,
        *,
        cdp_url: str,
        url: str,
        platform: Platform,
        content_id: str,
        limit: int,
        timeout_seconds: int,
    ) -> list[CollectedComment]:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return []

        response_payloads: list[Any] = []
        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=timeout_seconds * 1000)
            try:
                context = browser.contexts[0] if browser.contexts else browser.new_context(viewport={"width": 1366, "height": 900})
                page = context.pages[0] if context.pages else context.new_page()

                def on_response(response):
                    response_url = response.url.lower()
                    if not _looks_like_comment_response(response_url):
                        return
                    try:
                        response_payloads.append(response.json())
                    except Exception:
                        return

                page.on("response", on_response)
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
                _interact_for_comments(page, platform, timeout_seconds=timeout_seconds)
                comments = _comments_from_payloads(response_payloads, platform=platform, content_id=content_id, limit=limit)
                if len(comments) < limit:
                    comments.extend(
                        comment
                        for comment in comments_from_text_blocks(_visible_comment_blocks(page), platform=platform, content_id=content_id, limit=limit)
                        if comment.external_id not in {item.external_id for item in comments}
                    )
                return comments[:limit]
            finally:
                browser.close()


def _walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def _comment_from_json_node(node: dict[str, Any], *, platform: Platform, content_id: str) -> CollectedComment | None:
    if platform == Platform.X:
        x_comment = _x_comment_from_json_node(node, content_id=content_id)
        if x_comment:
            return x_comment
        if not _x_allows_generic_json_comment(node):
            return None
    text = _first_string(node, COMMENT_TEXT_KEYS)
    if not text or len(text) > 2000 or _looks_like_non_comment_text(text):
        return None
    raw_author = node.get("user") or node.get("author") or node.get("owner") or {}
    author = raw_author if isinstance(raw_author, dict) else {}
    author_id = _first_string(author, AUTHOR_ID_KEYS) or _first_string(node, AUTHOR_ID_KEYS) or _first_string(author, AUTHOR_NAME_KEYS)
    author_name = _first_string(author, AUTHOR_NAME_KEYS) or _first_string(node, AUTHOR_NAME_KEYS) or author_id
    if platform == Platform.FACEBOOK and not (author_id or author_name):
        return None
    comment_id = _first_string(node, COMMENT_ID_KEYS) or _stable_comment_id(platform, content_id, author_id, text)
    return CollectedComment(
        platform=platform,
        external_id=comment_id,
        content_external_id=content_id,
        author_external_id=author_id,
        text=text,
        published_at=_timestamp_to_datetime(_first_value(node, TIME_KEYS)),
        raw={
            "author": author_name,
            "author_id": author_id,
            "like_count": _optional_int(_first_value(node, LIKE_KEYS)),
            "source": "browser_json",
        },
    )


def _x_allows_generic_json_comment(node: dict[str, Any]) -> bool:
    typename = str(node.get("__typename") or node.get("entryType") or "")
    if "cursor" in typename.lower():
        return False
    if not _first_string(node, COMMENT_TEXT_KEYS):
        return False
    raw_author = node.get("user") or node.get("author") or node.get("owner") or {}
    if isinstance(raw_author, dict) and (_first_string(raw_author, AUTHOR_ID_KEYS) or _first_string(raw_author, AUTHOR_NAME_KEYS)):
        return True
    return bool(_first_string(node, AUTHOR_ID_KEYS) or _first_string(node, AUTHOR_NAME_KEYS))


def _x_comment_from_json_node(node: dict[str, Any], *, content_id: str) -> CollectedComment | None:
    legacy = node.get("legacy")
    if not isinstance(legacy, dict):
        return None
    text = _first_string(legacy, {"full_text", "text"})
    if not text:
        note_tweet = node.get("note_tweet")
        if isinstance(note_tweet, dict):
            note_results = note_tweet.get("note_tweet_results")
            if isinstance(note_results, dict):
                result = note_results.get("result")
                if isinstance(result, dict):
                    text = _first_string(result, {"text"})
    if not text or _looks_like_non_comment_text(text):
        return None
    external_id = _first_string(node, {"rest_id", "id"}) or _first_string(legacy, {"id_str", "conversation_id_str"})
    if not external_id or external_id == content_id:
        return None
    user = _x_user_from_node(node)
    author_id = _first_string_in_order(user, ("screen_name", "id_str", "rest_id", "id"))
    author_name = _first_string(user, {"name"}) or author_id
    if not author_id:
        return None
    return CollectedComment(
        platform=Platform.X,
        external_id=external_id,
        content_external_id=content_id,
        author_external_id=author_id,
        text=text,
        published_at=_x_created_at_to_datetime(_first_string(legacy, {"created_at"})),
        raw={
            "author": author_name,
            "author_id": author_id,
            "like_count": _optional_int(_first_value(legacy, {"favorite_count", "like_count"})),
            "source": "browser_json_x",
        },
    )


def _x_user_from_node(node: dict[str, Any]) -> dict[str, Any]:
    core = node.get("core")
    if not isinstance(core, dict):
        return {}
    user_results = core.get("user_results")
    if not isinstance(user_results, dict):
        return {}
    result = user_results.get("result")
    if not isinstance(result, dict):
        return {}
    legacy = result.get("legacy")
    if isinstance(legacy, dict):
        merged = dict(legacy)
        for key in ("rest_id", "id"):
            if key in result and key not in merged:
                merged[key] = result[key]
        return merged
    return result


def _comments_from_payloads(payloads: list[Any], *, platform: Platform, content_id: str, limit: int) -> list[CollectedComment]:
    comments: list[CollectedComment] = []
    seen: set[str] = set()
    for payload in payloads:
        for comment in comments_from_json(payload, platform=platform, content_id=content_id, limit=limit):
            if comment.external_id in seen:
                continue
            seen.add(comment.external_id)
            comments.append(comment)
            if len(comments) >= limit:
                return comments
    return comments


def _first_string(source: dict[str, Any], keys: set[str]) -> str:
    value = _first_value(source, keys)
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        return ""
    return str(value).strip()


def _first_string_in_order(source: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key not in source:
            continue
        value = source[key]
        if value is None or value == "" or isinstance(value, (dict, list, tuple, set)):
            continue
        return str(value).strip()
    return ""


def _first_value(source: dict[str, Any], keys: set[str]) -> Any:
    for key in keys:
        if key not in source:
            continue
        value = source[key]
        if value is None or value == "":
            continue
        return value
    return None


def _first_comment_text_line(lines: list[str]) -> str:
    ignored = {"reply", "replies", "like", "likes", "view replies", "see translation"}
    for line in lines:
        lowered = line.lower()
        if lowered in ignored or lowered.endswith(" likes") or lowered.endswith(" like"):
            continue
        if _looks_like_non_comment_text(line):
            continue
        if len(line) > 1:
            return line
    return ""


def _looks_like_non_comment_text(value: str) -> bool:
    text = value.strip()
    lowered = text.lower()
    if not text:
        return True
    exact = {
        "to view keyboard shortcuts, press question mark",
        "查看键盘快捷键",
        "显示更多",
        "发生了什么",
        "what's happening",
        "show more",
        "匿名用户",
        "reply",
        "repost",
        "like",
        "share",
        "follow",
    }
    if lowered in exact or text in exact:
        return True
    if "__typename" in text or "TimelineTimelineCursor" in text or "cursorType" in text:
        return True
    return False


def _stable_comment_id(platform: Platform, content_id: str, author: str, text: str) -> str:
    digest = hashlib.sha1(f"{platform}:{content_id}:{author}:{text}".encode("utf-8")).hexdigest()[:20]
    return f"browser-{digest}"


def _timestamp_to_datetime(value: Any):
    if value is None:
        return None


def _x_created_at_to_datetime(value: str):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            from datetime import timezone

            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return _timestamp_to_datetime(value)
    try:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(float(value), timezone.utc)
    except Exception:
        return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _looks_like_comment_response(url: str) -> bool:
    return any(token in url for token in ("comment", "reply", "replies", "graphql", "api/graphql", "aweme/v1/comment"))


def _interact_for_comments(page, platform: Platform, *, timeout_seconds: int) -> None:
    if platform == Platform.TIKTOK:
        _interact_for_tiktok_comments(page)
        return
    for text in ("comments", "comment", "view comments", "view all comments", "more comments", "view replies", "see more"):
        try:
            page.get_by_text(text, exact=False).first.click(timeout=1500)
        except Exception:
            pass
    for _ in range(5):
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(min(1000, max(250, timeout_seconds * 100)))


def _interact_for_tiktok_comments(page) -> None:
    try:
        page.locator('[aria-label*="Read or add comments"]').first.click(timeout=2000)
        page.wait_for_timeout(1000)
    except Exception:
        pass
    for _ in range(4):
        try:
            box = page.evaluate(
                """() => {
                    const nodes = [...document.querySelectorAll('button, div, span')];
                    const target = nodes.find((node) => {
                        const text = (node.innerText || node.textContent || '').trim();
                        const rect = node.getBoundingClientRect();
                        return text === 'Comments' && rect.x > window.innerWidth * 0.55 && rect.width > 20 && rect.height > 10;
                    });
                    if (!target) return null;
                    const rect = target.getBoundingClientRect();
                    return {x: rect.x + rect.width / 2, y: rect.y + rect.height / 2};
                }"""
            )
            if box:
                page.mouse.click(box["x"], box["y"])
            else:
                page.mouse.click(1090, 96)
            page.wait_for_timeout(3500)
            body_text = page.evaluate("() => (document.body && document.body.innerText || '').toLowerCase()")
            if " comments" in body_text and "reply" in body_text:
                return
        except Exception:
            pass
        try:
            page.mouse.click(1090, 96)
            page.wait_for_timeout(2500)
        except Exception:
            pass


def _visible_comment_blocks(page) -> list[str]:
    return page.evaluate(
        """() => {
            const selectors = [
                '[data-e2e*="comment"]',
                '[aria-label*="comment" i]',
                '[role="listitem"]',
                'article',
                'ul li'
            ];
            const blocks = [];
            const seen = new Set();
            for (const selector of selectors) {
                for (const node of document.querySelectorAll(selector)) {
                    const text = (node.innerText || '').trim();
                    if (text.length < 8 || text.length > 2000 || seen.has(text)) continue;
                    seen.add(text);
                    blocks.push(text);
                }
            }
            const bodyText = (document.body && document.body.innerText || '').trim();
            if (bodyText && !seen.has(bodyText)) blocks.push(bodyText);
            return blocks.slice(0, 200);
        }"""
    )


def _copy_profile(source: Path, destination: Path) -> None:
    def ignore(_dir: str, names: list[str]) -> set[str]:
        ignored = {"SingletonLock", "SingletonSocket", "SingletonCookie", "Crashpad", "ShaderCache", "GrShaderCache"}
        return {name for name in names if name in ignored or name.endswith("-journal")}

    shutil.copytree(source, destination, ignore=ignore)


def _chromium_executable(browser: str) -> str:
    for candidate in (browser, "/usr/bin/chromium-browser", "/usr/bin/chromium", "/usr/bin/google-chrome"):
        if candidate and Path(candidate).exists():
            return candidate
    return "/usr/bin/chromium-browser"
