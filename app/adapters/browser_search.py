from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse

from app.domain.enums import Platform


def content_links_from_anchors(anchors: list[str], *, platform: Platform, limit: int) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for href in anchors:
        normalized = _content_link(href, platform=platform)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        links.append(normalized)
        if len(links) >= limit:
            break
    return links


class BrowserSearchCollector:
    def collect(
        self,
        *,
        url: str,
        platform: Platform,
        profile: Path,
        browser: str,
        limit: int,
        timeout_seconds: int,
        cdp_url: str | None = None,
    ) -> list[str]:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return []

        if cdp_url:
            with sync_playwright() as playwright:
                try:
                    browser_obj = playwright.chromium.connect_over_cdp(cdp_url, timeout=timeout_seconds * 1000)
                    try:
                        context = browser_obj.contexts[0] if browser_obj.contexts else browser_obj.new_context(viewport={"width": 1366, "height": 900})
                        page = context.pages[0] if context.pages else context.new_page()
                        return _collect_from_page(page, url=url, platform=platform, limit=limit, timeout_seconds=timeout_seconds)
                    finally:
                        browser_obj.close()
                except Exception:
                    pass

        if not profile.exists():
            return []
        with tempfile.TemporaryDirectory(prefix=f"browser-search-{platform.value.lower()}-") as tmpdir:
            profile_copy = Path(tmpdir) / "profile"
            _copy_profile(profile, profile_copy)
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(profile_copy),
                    executable_path=_chromium_executable(browser),
                    headless=True,
                    viewport={"width": 1366, "height": 900},
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                    timeout=timeout_seconds * 1000,
                )
                try:
                    page = context.new_page()
                    return _collect_from_page(page, url=url, platform=platform, limit=limit, timeout_seconds=timeout_seconds)
                finally:
                    context.close()


def _collect_from_page(page, *, url: str, platform: Platform, limit: int, timeout_seconds: int) -> list[str]:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
    links: list[str] = []
    for _ in range(8):
        page.wait_for_timeout(1500)
        anchors = page.evaluate("""() => [...document.querySelectorAll('a[href]')].map((node) => node.href || node.getAttribute('href') || '')""")
        links = content_links_from_anchors(anchors, platform=platform, limit=limit)
        if len(links) >= limit:
            return links
        page.mouse.wheel(0, 1100)
    return links


def _content_link(href: str, *, platform: Platform) -> str:
    value = str(href or "").strip()
    if not value:
        return ""
    if platform == Platform.TIKTOK:
        return _tiktok_content_link(value)
    if platform == Platform.FACEBOOK:
        return _facebook_content_link(value)
    if platform == Platform.INSTAGRAM:
        return _instagram_content_link(value)
    if platform == Platform.X:
        return _x_content_link(value)
    return ""


def _tiktok_content_link(href: str) -> str:
    parsed = urlparse(urljoin("https://www.tiktok.com", href))
    match = re.match(r"^/@[^/]+/video/\d+", parsed.path)
    if not match:
        return ""
    return urlunparse(("https", "www.tiktok.com", match.group(0), "", "", ""))


def _facebook_content_link(href: str) -> str:
    parsed = urlparse(urljoin("https://www.facebook.com", href))
    if parsed.netloc and "facebook.com" not in parsed.netloc:
        return ""
    if parsed.path == "/watch/":
        video_id = parse_qs(parsed.query).get("v", [""])[0]
        if video_id:
            return f"https://www.facebook.com/watch/?{urlencode({'v': video_id})}"
    match = re.match(r"^/[^/]+/videos/[^/?#]+/?", parsed.path)
    if match:
        return urlunparse(("https", "www.facebook.com", match.group(0).rstrip("/"), "", "", ""))
    match = re.match(r"^/reel/[^/?#]+/?", parsed.path)
    if match:
        return urlunparse(("https", "www.facebook.com", match.group(0).rstrip("/"), "", "", ""))
    return ""


def _instagram_content_link(href: str) -> str:
    parsed = urlparse(urljoin("https://www.instagram.com", href))
    if parsed.netloc and "instagram.com" not in parsed.netloc:
        return ""
    match = re.match(r"^/(reel|p)/[^/?#]+/?", parsed.path)
    if not match:
        return ""
    return urlunparse(("https", "www.instagram.com", match.group(0).rstrip("/"), "", "", ""))


def _x_content_link(href: str) -> str:
    parsed = urlparse(urljoin("https://x.com", href))
    if parsed.netloc and not any(host in parsed.netloc for host in ("x.com", "twitter.com")):
        return ""
    match = re.match(r"^/([^/]+)/status/(\d+)", parsed.path)
    if not match:
        return ""
    username, status_id = match.groups()
    return f"https://x.com/{username}/status/{status_id}"


def search_url_for_keyword(platform: Platform, keyword: str) -> str:
    encoded = quote(keyword.strip())
    if platform == Platform.TIKTOK:
        return f"https://www.tiktok.com/search/video?q={encoded}"
    if platform == Platform.FACEBOOK:
        return f"https://www.facebook.com/search/videos/?q={encoded}"
    if platform == Platform.INSTAGRAM:
        return f"https://www.instagram.com/explore/tags/{encoded}/"
    if platform == Platform.X:
        return f"https://x.com/search?q={encoded}&f=live"
    return keyword


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
