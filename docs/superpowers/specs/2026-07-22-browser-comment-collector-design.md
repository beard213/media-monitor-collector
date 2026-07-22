# Browser Comment Collector Design

Date: 2026-07-22

## Scope

Add a cookie/profile-based browser comment collector for platforms where `yt-dlp` returns video and user metadata but not comment lists. The collector uses the manually logged-in Chromium profile as the session source and gathers only comments/user details visible to that logged-in account.

## Constraints

- No official platform API tokens are available.
- Do not automate CAPTCHA, 2FA, account creation, ban evasion, stealth fingerprint spoofing, or access-control circumvention.
- Use the persisted Chromium profile/cookies already configured for each platform.
- Keep `yt-dlp` as the media and baseline metadata collector.

## Architecture

- Add a browser comment collector module that launches Chromium with a temporary copy of the configured profile to avoid locking the interactive login profile.
- Use Playwright with the system Chromium executable.
- Capture comments from normal page-rendered data: visible DOM text and JSON responses loaded by the page.
- Normalize discovered comment records to `CollectedComment`.
- Attach browser comments to the existing `CollectionBatch` after `yt-dlp` collection.

## Platform Order

1. TikTok and Instagram first because their comment areas are frontend loaded and are the strongest fit for browser collection.
2. Facebook next for specific video pages.
3. X after that for post replies.

## Testing

- Unit-test JSON walkers and DOM comment normalization without live network.
- Test adapter integration using a fake browser collector.
- Run live smoke tests against the existing logged-in profiles and known public content URLs.
