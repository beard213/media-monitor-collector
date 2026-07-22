# Facebook and TikTok Basic Adapter Design

Date: 2026-07-22

## Scope

Implement basic Facebook and TikTok platform adapters using the same yt-dlp plus persistent Chromium browser profile pattern already used by Instagram and X. The first usable flow is platform health reporting plus small sample collection from a public content URL or account page using the manually logged-in browser profiles.

## Architecture

- Add Facebook and TikTok settings for enabled flag, browser name, browser profile path, and output root.
- Replace the current StubAdapter registrations for Facebook and TikTok with real adapters.
- Keep adapter behavior local to app/adapters/facebook.py and app/adapters/tiktok.py, following the Instagram/X adapter conventions.
- Use yt-dlp cookiesfrombrowser with the configured profile. Do not export cookies or automate login challenges.

## Capabilities

- TikTok: ACCOUNT_TIMELINE, CONTENT_DETAIL, PROFILE, MEDIA_DOWNLOAD.
- Facebook: ACCOUNT_TIMELINE, CONTENT_DETAIL, PROFILE, MEDIA_DOWNLOAD.
- Comments and keyword search remain unimplemented unless proven supported and stable later.

## Data Flow

1. /platforms calls each adapter healthcheck.
2. Healthcheck confirms yt-dlp is importable, platform is enabled, and profile path exists.
3. /collections/run creates a CollectionRequest.
4. Adapter converts account/content values to platform URLs, runs yt-dlp with cookiesfrombrowser, normalizes accounts, contents, and media assets into CollectionBatch.
5. Backend sync remains unchanged and only syncs platforms currently supported by ingestion backend.

## Error Handling

- Missing profile maps to AUTH_EXPIRED with REAUTH action.
- Login/cookie/auth messages map to AUTH_EXPIRED.
- 429/rate-limit messages map to RATE_LIMITED and cooldown.
- Network/timeouts map to retryable NETWORK_TEMPORARY.
- Other extractor failures map to PLATFORM_CHANGED.

## Testing

- Add unit tests for /platforms status showing configured Facebook/TikTok profiles.
- Add adapter tests with monkeypatched yt-dlp to verify target URL conversion, cookiesfrombrowser parameters, batch normalization, and missing-profile auth errors.
- Run the existing test suite after implementation.
