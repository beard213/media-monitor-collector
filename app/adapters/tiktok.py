from __future__ import annotations

from pathlib import Path

from app.adapters.browser_ytdlp import BrowserYtDlpAdapter
from app.domain.enums import Platform


class TikTokAdapter(BrowserYtDlpAdapter):
    platform = Platform.TIKTOK
    content_type = "SHORT_VIDEO"

    @property
    def enabled(self) -> bool:
        return bool(self.settings.tiktok_enabled)

    @property
    def browser_name(self) -> str:
        return self.settings.tiktok_browser or "chromium"

    @property
    def browser_profile_path(self) -> Path | None:
        return self.settings.tiktok_browser_profile

    @property
    def output_root(self) -> Path:
        return self.settings.tiktok_output_root

    def _account_url(self, value: str) -> str:
        return f"https://www.tiktok.com/@{value.lstrip('@')}"

    def _content_url(self, value: str) -> str:
        if value.startswith("@"):
            return self._account_url(value)
        return f"https://www.tiktok.com/t/{value.lstrip('/')}"
