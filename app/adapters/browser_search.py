from __future__ import annotations

import asyncio
import json
import re
import shutil
import tempfile
import time
import urllib.request
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse

from app.domain.enums import Platform


@dataclass(frozen=True)
class BrowserSearchResult:
    links: list[str]
    page_url: str = ""
    canonical_url: str = ""
    # YouTube's handle is mutable and its canonical URL commonly remains an
    # @handle URL.  The stable UC channel ID is required by the business
    # ingestion API, especially when browser fallback supplies metadata.
    platform_account_id: str = ""
    title: str = ""
    body_text: str = ""


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
        refresh: bool = False,
    ) -> list[str]:
        return self.collect_with_context(
            url=url,
            platform=platform,
            profile=profile,
            browser=browser,
            limit=limit,
            timeout_seconds=timeout_seconds,
            cdp_url=cdp_url,
            refresh=refresh,
        ).links

    def collect_with_context(
        self,
        *,
        url: str,
        platform: Platform,
        profile: Path,
        browser: str,
        limit: int,
        timeout_seconds: int,
        cdp_url: str | None = None,
        refresh: bool = False,
    ) -> BrowserSearchResult:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return BrowserSearchResult(links=[])

        if cdp_url:
            # The persistent-browser watchdog starts an idle profile on its
            # next short polling tick.  A just-admitted collection job can
            # otherwise reach CDP a few seconds before Chromium is listening
            # and be incorrectly recorded as an empty platform search.
            # Retry only that explicit CDP-startup failure; a rendered empty
            # search page still returns immediately and remains observable.
            startup_deadline = time.monotonic() + min(25, max(8, timeout_seconds))
            while True:
                result = _collect_from_cdp(
                    cdp_url=cdp_url,
                    url=url,
                    platform=platform,
                    limit=limit,
                    timeout_seconds=timeout_seconds,
                    refresh=refresh,
                )
                if result.links or not result.body_text.startswith("CDP_X_COLLECT_FAILED"):
                    break
                if time.monotonic() >= startup_deadline:
                    break
                time.sleep(2)
            if platform == Platform.TIKTOK and not result.links:
                return _collect_tiktok_main_page(
                    cdp_url=cdp_url,
                    url=url,
                    limit=limit,
                    timeout_seconds=timeout_seconds,
                )
            return result

        if not profile.exists():
            return BrowserSearchResult(links=[])
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
                    return _collect_from_page(page, url=url, platform=platform, limit=limit, timeout_seconds=timeout_seconds, refresh=refresh)
                finally:
                    context.close()



def _collect_from_cdp(*, cdp_url: str, url: str, platform: Platform, limit: int, timeout_seconds: int, refresh: bool = False) -> BrowserSearchResult:
    try:
        return asyncio.run(_collect_from_cdp_async(cdp_url=cdp_url, url=url, platform=platform, limit=limit, timeout_seconds=timeout_seconds, refresh=refresh))
    except Exception as exc:  # noqa: BLE001 - return context to caller instead of blocking the job.
        return BrowserSearchResult(links=[], page_url=url, body_text=f"CDP_X_COLLECT_FAILED: {type(exc).__name__}: {exc}")


async def _collect_from_cdp_async(*, cdp_url: str, url: str, platform: Platform, limit: int, timeout_seconds: int, refresh: bool = False) -> BrowserSearchResult:
    """Collect in a disposable CDP target, never in the login tab.

    The platform browsers are persistent solely to retain their authenticated
    profile.  Reusing their first page for media/search navigation made a
    long-lived SPA renderer retain video buffers and application caches across
    many collection rounds.  A standalone target has the same browser
    profile/cookies, but can be closed deterministically after one request.
    """
    try:
        page_info = await asyncio.to_thread(_cdp_new_page, cdp_url, url, min(8, max(2, timeout_seconds)))
    except Exception as exc:  # noqa: BLE001 - keep browser fallback non-fatal.
        return BrowserSearchResult(links=[], page_url=url, body_text=f"CDP_X_COLLECT_FAILED: temporary page: {type(exc).__name__}: {exc}")

    target_id = str(page_info.get("id") or "")
    if not target_id or not page_info.get("webSocketDebuggerUrl"):
        return BrowserSearchResult(links=[], page_url=url, body_text="CDP_X_COLLECT_FAILED: temporary page has no websocket")
    try:
        result = await _collect_from_cdp_page_async(
            page_info=page_info,
            url=url,
            platform=platform,
            limit=limit,
            timeout_seconds=timeout_seconds,
            refresh=refresh,
        )
        # Never reuse a previous-query result as the requested search term.
        if platform == Platform.TIKTOK and _page_url_key(result.page_url) != _page_url_key(url):
            result = BrowserSearchResult(links=[], page_url=result.page_url, title=result.title, body_text=result.body_text)
        return result
    finally:
        # Closing the page target releases the renderer even if navigation,
        # extraction, or a platform challenge timed out.
        try:
            await asyncio.to_thread(_cdp_close_page, cdp_url, target_id, 5)
        except Exception:
            pass


async def _collect_from_cdp_page_async(
    *,
    page_info: dict,
    url: str,
    platform: Platform,
    limit: int,
    timeout_seconds: int,
    refresh: bool = False,
) -> BrowserSearchResult:
    try:
        import websockets
    except Exception as exc:  # noqa: BLE001
        return BrowserSearchResult(links=[], page_url=url, body_text=f"CDP_X_COLLECT_FAILED: websockets unavailable: {exc}")

    deadline = asyncio.get_running_loop().time() + max(8, timeout_seconds)
    async with websockets.connect(page_info["webSocketDebuggerUrl"], max_size=20_000_000, open_timeout=min(8, max(1, timeout_seconds))) as ws:
        counter = 0

        async def call(method: str, params: dict | None = None, *, timeout: float = 8) -> dict:
            nonlocal counter
            counter += 1
            message_id = counter
            await ws.send(json.dumps({"id": message_id, "method": method, "params": params or {}}))
            while True:
                remaining = max(0.5, min(timeout, deadline - asyncio.get_running_loop().time()))
                message = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
                if message.get("id") == message_id:
                    return message

        await call("Runtime.enable", timeout=5)
        current = str(page_info.get("url") or "")
        # A CDP browser is often already on the platform homepage.  Matching
        # only the domain leaves it there and silently collects the wrong page.
        # A platform page can keep the navigation request open while it is
        # waiting on challenge/API requests.  Treat that as a soft timeout:
        # the renderer is still usable and the next Runtime.evaluate can
        # extract whatever the page has already rendered.  Previously one
        # slow navigation aborted the whole browser fallback and was exposed
        # to the job as "no collectable content".
        if url and _page_url_key(current) != _page_url_key(url):
            try:
                await call("Page.navigate", {"url": url}, timeout=5)
            except (asyncio.TimeoutError, TimeoutError):
                # Stop an endless platform navigation before evaluating the
                # DOM.  TikTok/Meta frequently leave requests open forever;
                # Chromium will otherwise stop answering Runtime commands.
                try:
                    await call("Page.stopLoading", timeout=3)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
        elif refresh:
            try:
                await call("Runtime.evaluate", {"expression": "location.reload()", "returnByValue": True}, timeout=5)
            except (asyncio.TimeoutError, TimeoutError):
                pass
        links: list[str] = []
        page_url = current or url
        canonical_url = ""
        platform_account_id = ""
        title = str(page_info.get("title") or "")
        body_text = ""
        rounds = 20 if platform == Platform.TIKTOK and limit > 10 else 10
        for _ in range(rounds):
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(1.5)
            try:
                result = await call(
                    "Runtime.evaluate",
                    {"expression": _cdp_extract_script(platform), "returnByValue": True, "awaitPromise": True},
                    timeout=8,
                )
            except (asyncio.TimeoutError, TimeoutError):
                # Keep polling the same target.  Chromium can temporarily
                # stop answering while a SPA swaps its document or a bot
                # challenge is being resolved.
                try:
                    await call("Page.stopLoading", timeout=3)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                continue
            value = result.get("result", {}).get("result", {}).get("value") or {}
            page_url = str(value.get("url") or page_url)
            canonical_url = str(value.get("canonicalUrl") or canonical_url)
            platform_account_id = str(value.get("platformAccountId") or platform_account_id)
            title = str(value.get("title") or title)
            body_text = str(value.get("body") or body_text)
            raw_links = value.get("links") if isinstance(value, dict) else []
            if isinstance(raw_links, list):
                links = content_links_from_anchors(links + [str(item) for item in raw_links], platform=platform, limit=limit)
            if len(links) >= limit:
                break
        return BrowserSearchResult(
            links=links,
            page_url=page_url,
            canonical_url=canonical_url,
            platform_account_id=platform_account_id,
            title=title,
            body_text=body_text,
        )



def _platform_url_matches(platform: Platform, page_url: str) -> bool:
    value = page_url.lower()
    if platform == Platform.YOUTUBE:
        return "youtube.com" in value
    if platform == Platform.FACEBOOK:
        return "facebook.com" in value
    if platform == Platform.INSTAGRAM:
        return "instagram.com" in value
    if platform == Platform.TIKTOK:
        return "tiktok.com" in value
    if platform == Platform.X:
        return "x.com" in value or "twitter.com" in value
    return False


def _page_url_key(value: str) -> str:
    parsed = urlparse(str(value or ""))
    if not parsed.scheme or not parsed.netloc:
        return str(value or "").rstrip("/")
    query = urlencode(sorted(parse_qs(parsed.query, keep_blank_values=True).items()), doseq=True)
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/") or "/", "", query, ""))


def _cdp_extract_script(platform: Platform) -> str:
    if platform == Platform.X:
        return _X_HOME_EXTRACT_SCRIPT
    return _GENERIC_HOME_EXTRACT_SCRIPT


_GENERIC_HOME_EXTRACT_SCRIPT = r"""
(() => {
  window.scrollBy(0, 900);
  const currentBody = document.body?.innerText || '';
  // TikTok sometimes renders the profile shell but leaves the video grid in
  // a transient "Something went wrong" state.  Its own Refresh action
  // retries the signed API request and commonly restores the video anchors;
  // click it once per page load instead of returning an empty account.
  if (/Something went wrong|Sorry about that/i.test(currentBody)
      && (!window.__mediaMonitorRefreshClicked || Date.now() - window.__mediaMonitorRefreshClicked > 5000)) {
    const refresh = [...document.querySelectorAll('button, a, [role="button"]')]
      .find((node) => /refresh|重试/i.test(node.innerText || node.getAttribute('aria-label') || ''));
    if (refresh) {
      window.__mediaMonitorRefreshClicked = Date.now();
      refresh.click();
    }
  }
  const links = [...document.querySelectorAll('a[href]')]
    .map((node) => node.href || node.getAttribute('href') || '')
    .filter(Boolean);
  return {
    url: location.href,
    canonicalUrl: document.querySelector('link[rel="canonical"]')?.href || '',
    platformAccountId: document.querySelector('meta[itemprop="channelId"]')?.content
      || window.ytInitialData?.metadata?.channelMetadataRenderer?.externalId
      || '',
    title: document.title,
    links: [...new Set(links)],
    articleCount: document.querySelectorAll('article').length,
    videoCount: document.querySelectorAll('video').length,
    body: (document.body?.innerText || '').slice(0, 4000)
  };
})()
"""

def _cdp_json(url: str, timeout_seconds: int):
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _cdp_new_page(cdp_url: str, url: str, timeout_seconds: int) -> dict:
    """Create an isolated Chrome DevTools page backed by the login profile."""
    target_url = f"{cdp_url.rstrip('/')}/json/new?{quote(url, safe='')}"
    request = urllib.request.Request(target_url, method="PUT")
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _cdp_close_page(cdp_url: str, target_id: str, timeout_seconds: int) -> None:
    request = urllib.request.Request(f"{cdp_url.rstrip('/')}/json/close/{quote(target_id, safe='')}")
    with urllib.request.urlopen(request, timeout=timeout_seconds):
        pass


def _cdp_existing_platform_page(cdp_url: str, platform: Platform, timeout_seconds: int) -> dict:
    """Return the persistent login tab for the narrow TikTok search fallback."""
    pages = _cdp_json(f"{cdp_url.rstrip('/')}/json/list", timeout_seconds)
    candidates = [item for item in pages if item.get("type") == "page" and _platform_url_matches(platform, str(item.get("url") or ""))]
    # Prefer the homepage rather than a previously opened detail page.
    candidates.sort(key=lambda item: ("/video/" in str(item.get("url") or ""), len(str(item.get("url") or ""))))
    return candidates[0] if candidates else {}


def _collect_tiktok_main_page(*, cdp_url: str, url: str, limit: int, timeout_seconds: int) -> BrowserSearchResult:
    """Use Playwright to navigate TikTok's authenticated main tab exactly."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=min(15_000, max(2_000, timeout_seconds * 1000)))
            context = browser.contexts[0] if browser.contexts else None
            if not context:
                return BrowserSearchResult(links=[], page_url=url, body_text="TikTok main tab unavailable")
            page = next((item for item in context.pages if _platform_url_matches(Platform.TIKTOK, item.url)), None)
            if not page:
                return BrowserSearchResult(links=[], page_url=url, body_text="TikTok main tab unavailable")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=min(20_000, max(8_000, timeout_seconds * 1000)))
            except Exception:
                pass
            if _page_url_key(page.url) != _page_url_key(url):
                return BrowserSearchResult(links=[], page_url=page.url, body_text="TikTok search navigation did not reach requested query")
            return _collect_from_page(page, url=url, platform=Platform.TIKTOK, limit=limit, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - browser fallback must not block collection.
        return BrowserSearchResult(links=[], page_url=url, body_text=f"TikTok main-tab search failed: {type(exc).__name__}: {exc}")


_X_HOME_EXTRACT_SCRIPT = r"""
(() => {
  window.scrollBy(0, 900);
  const normalizeLinks = (nodes) => [...nodes]
    .map((node) => node.href || node.getAttribute('href') || '')
    .filter((href) => /https?:\/\/(x|twitter)\.com\/[^/]+\/status\/[0-9]+/.test(href));
  const videoHints = [
    'video',
    '[data-testid="videoPlayer"]',
    '[data-testid="videoComponent"]',
    '[data-testid="playButton"]',
    '[aria-label*="Play"]',
    '[aria-label*="Video"]',
    '[aria-label*="播放"]',
    '[aria-label*="影片"]'
  ];
  const links = [];
  for (const article of [...document.querySelectorAll('article')]) {
    const hasVideo = videoHints.some((selector) => article.querySelector(selector));
    if (!hasVideo) continue;
    for (const href of normalizeLinks(article.querySelectorAll('a[href]'))) links.push(href);
  }
  return {
    url: location.href,
    title: document.title,
    links: [...new Set(links)],
    articleCount: document.querySelectorAll('article').length,
    videoCount: document.querySelectorAll('video').length,
    body: (document.body?.innerText || '').slice(0, 4000)
  };
})()
"""

def _collect_from_page(page, *, url: str, platform: Platform, limit: int, timeout_seconds: int, refresh: bool = False) -> BrowserSearchResult:
    if url:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
    if refresh:
        page.reload(wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
    links: list[str] = []
    body_text = ""
    rounds = 20 if platform == Platform.TIKTOK and limit > 10 else 8
    for _ in range(rounds):
        page.wait_for_timeout(1500)
        links = content_links_from_anchors(
            links + _content_links_from_page(page, platform=platform, limit=limit),
            platform=platform,
            limit=limit,
        )
        if len(links) >= limit:
            return BrowserSearchResult(
                links=links,
                page_url=page.url,
                canonical_url=page.locator('link[rel="canonical"]').get_attribute('href') or '',
                platform_account_id=_page_platform_account_id(page, platform),
                title=page.title(),
                body_text=body_text,
            )
        body_text = page.evaluate("""() => document.body ? document.body.innerText.slice(0, 4000) : ''""")
        page.mouse.wheel(0, 1100)
    return BrowserSearchResult(
        links=links,
        page_url=page.url,
        canonical_url=page.locator('link[rel="canonical"]').get_attribute('href') or '',
        platform_account_id=_page_platform_account_id(page, platform),
        title=page.title(),
        body_text=body_text,
    )


def _page_platform_account_id(page, platform: Platform) -> str:
    if platform != Platform.YOUTUBE:
        return ""
    try:
        value = page.evaluate(
            """() => document.querySelector('meta[itemprop=\"channelId\"]')?.content
                || window.ytInitialData?.metadata?.channelMetadataRenderer?.externalId || ''"""
        )
        return str(value or "")
    except Exception:
        return ""



def _content_links_from_page(page, *, platform: Platform, limit: int) -> list[str]:
    if platform == Platform.X:
        article_links = page.evaluate(
            r"""
            () => {
              const articles = [...document.querySelectorAll('article')];
              const videoHints = [
                'video',
                '[data-testid="videoPlayer"]',
                '[data-testid="videoComponent"]',
                '[data-testid="playButton"]',
                '[aria-label*="Play"]',
                '[aria-label*="Video"]',
                '[aria-label*="播放"]',
                '[aria-label*="影片"]'
              ];
              const links = [];
              for (const article of articles) {
                const hasVideo = videoHints.some((selector) => article.querySelector(selector));
                if (!hasVideo) continue;
                const statusLinks = [...article.querySelectorAll('a[href]')]
                  .map((node) => node.href || node.getAttribute('href') || '')
                  .filter((href) => /\/(status)\/[0-9]+/.test(href));
                for (const href of statusLinks) links.push(href);
              }
              return links;
            }
            """
        )
        links = content_links_from_anchors(article_links, platform=platform, limit=limit)
        if links:
            return links
    anchors = page.evaluate("""() => [...document.querySelectorAll('a[href]')].map((node) => node.href || node.getAttribute('href') || '')""")
    return content_links_from_anchors(anchors, platform=platform, limit=limit)

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
    if platform == Platform.YOUTUBE:
        return _youtube_content_link(value)
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
    # Profile grids link to /<username>/(reel|p)/<id>, while shared links
    # use /reel/<id> or /p/<id>.  The old pattern only handled the latter,
    # so a perfectly rendered account page produced zero collectable links.
    match = re.match(r"^/(?:[^/]+/)?(reel|p)/[^/?#]+/?", parsed.path)
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


def _youtube_content_link(href: str) -> str:
    parsed = urlparse(urljoin("https://www.youtube.com", href))
    if parsed.netloc and "youtube.com" not in parsed.netloc and "youtu.be" not in parsed.netloc:
        return ""
    if "youtu.be" in parsed.netloc:
        video_id = parsed.path.strip("/").split("/", 1)[0]
        return f"https://www.youtube.com/watch?{urlencode({'v': video_id})}" if video_id else ""
    if parsed.path == "/watch":
        video_id = parse_qs(parsed.query).get("v", [""])[0]
        return f"https://www.youtube.com/watch?{urlencode({'v': video_id})}" if video_id else ""
    match = re.match(r"^/shorts/([^/?#]+)", parsed.path)
    if match:
        return f"https://www.youtube.com/shorts/{match.group(1)}"
    return ""


def search_url_for_keyword(platform: Platform, keyword: str) -> str:
    encoded = quote(keyword.strip())
    if platform == Platform.TIKTOK:
        return f"https://www.tiktok.com/search/video?q={encoded}"
    if platform == Platform.FACEBOOK:
        return f"https://www.facebook.com/search/videos/?q={encoded}"
    if platform == Platform.INSTAGRAM:
        # ``/explore/tags/`` accepts one hashtag token, not a phrase.  The
        # former implementation encoded spaces as ``%20`` and sent invalid
        # tag routes such as ``/explore/tags/chinese%20communist%20party/``.
        # Keep CJK letters, digits and Latin letters while removing whitespace
        # and punctuation so both ``Xi Jinping`` and ``习近平`` map to a real
        # Instagram tag path.
        tag = re.sub(r"[\s\W_]+", "", unicodedata.normalize("NFKC", keyword), flags=re.UNICODE)
        return f"https://www.instagram.com/explore/tags/{quote(tag)}/" if tag else "https://www.instagram.com/explore/tags/"
    if platform == Platform.X:
        # ``f=live`` restricts the result set to active live broadcasts and
        # made ordinary keyword-video replenishment look empty.  Ask X for
        # normal top results and constrain the query itself to media posts.
        # The downstream extractor still keeps only resolvable status/video
        # links, so this does not turn a video task into a text-post task.
        return f"https://x.com/search?q={quote(keyword.strip() + ' filter:videos')}&f=top"
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
