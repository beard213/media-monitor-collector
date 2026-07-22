from __future__ import annotations

import hashlib
import shutil
import tempfile
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
        if len(lines) < 2:
            continue
        author = lines[0]
        text = _first_comment_text_line(lines[1:])
        if not text:
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
    ) -> list[CollectedComment]:
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception:
            return []

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


def _walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def _comment_from_json_node(node: dict[str, Any], *, platform: Platform, content_id: str) -> CollectedComment | None:
    text = _first_string(node, COMMENT_TEXT_KEYS)
    if not text or len(text) > 2000:
        return None
    raw_author = node.get("user") or node.get("author") or node.get("owner") or {}
    author = raw_author if isinstance(raw_author, dict) else {}
    author_id = _first_string(author, AUTHOR_ID_KEYS) or _first_string(node, AUTHOR_ID_KEYS) or _first_string(author, AUTHOR_NAME_KEYS)
    author_name = _first_string(author, AUTHOR_NAME_KEYS) or _first_string(node, AUTHOR_NAME_KEYS) or author_id
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
    return str(value).strip()


def _first_value(source: dict[str, Any], keys: set[str]) -> Any:
    for key in keys:
        if key in source and source[key] not in {None, ""}:
            return source[key]
    return None


def _first_comment_text_line(lines: list[str]) -> str:
    ignored = {"reply", "replies", "like", "likes", "view replies", "see translation"}
    for line in lines:
        lowered = line.lower()
        if lowered in ignored or lowered.endswith(" likes") or lowered.endswith(" like"):
            continue
        if len(line) > 1:
            return line
    return ""


def _stable_comment_id(platform: Platform, content_id: str, author: str, text: str) -> str:
    digest = hashlib.sha1(f"{platform}:{content_id}:{author}:{text}".encode("utf-8")).hexdigest()[:20]
    return f"browser-{digest}"


def _timestamp_to_datetime(value: Any):
    if value is None:
        return None
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
    for text in ("comments", "comment", "view comments", "view all comments", "more comments", "view replies", "see more"):
        try:
            page.get_by_text(text, exact=False).first.click(timeout=1500)
        except Exception:
            pass
    for _ in range(5):
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(min(1000, max(250, timeout_seconds * 100)))


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
