from __future__ import annotations

from pathlib import Path

from app.adapters.browser_ytdlp import BrowserYtDlpAdapter
from app.domain.enums import Platform


class FacebookAdapter(BrowserYtDlpAdapter):
    platform = Platform.FACEBOOK
    content_type = "VIDEO"

    @property
    def enabled(self) -> bool:
        return bool(self.settings.facebook_enabled)

    @property
    def browser_name(self) -> str:
        return self.settings.facebook_browser or "chromium"

    @property
    def browser_profile_path(self) -> Path | None:
        return self.settings.facebook_browser_profile

    @property
    def output_root(self) -> Path:
        return self.settings.facebook_output_root

    def _account_url(self, value: str) -> str:
        return f"https://www.facebook.com/{value.lstrip('@').strip('/')}"

    def _content_url(self, value: str) -> str:
        cleaned = value.strip("/")
        if cleaned.startswith("watch") or cleaned.startswith("videos/"):
            return f"https://www.facebook.com/{cleaned}"
        return f"https://www.facebook.com/watch/?v={cleaned}"
