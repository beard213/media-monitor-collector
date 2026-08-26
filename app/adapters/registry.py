from app.adapters.base import PlatformAdapter
from app.adapters.facebook import FacebookAdapter
from app.adapters.instagram import InstagramAdapter
from app.adapters.tiktok import TikTokAdapter
from app.adapters.x import XAdapter
from app.adapters.youtube import YouTubeAdapter
from app.core.config import get_settings
from app.domain.enums import Platform


def build_registry() -> dict[Platform, PlatformAdapter]:
    settings = get_settings()
    return {
        Platform.YOUTUBE: YouTubeAdapter(),
        Platform.TIKTOK: TikTokAdapter(),
        Platform.FACEBOOK: FacebookAdapter(),
        Platform.X: XAdapter(),
        Platform.INSTAGRAM: InstagramAdapter(),
    }


def get_adapter(platform: Platform) -> PlatformAdapter:
    return build_registry()[platform]


def platform_statuses() -> list[dict]:
    rows = []
    for adapter in build_registry().values():
        health = adapter.healthcheck()
        rows.append(
            {
                "platform": adapter.platform,
                "enabled": health.ok,
                "mode": health.mode,
                "message": health.message,
                "capabilities": sorted(adapter.capabilities),
                "checked_at": health.checked_at,
            }
        )
    return rows
