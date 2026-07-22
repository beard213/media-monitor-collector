from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "foreign-media-collector"
    environment: str = "development"
    database_url: str = "sqlite:///./data/media_monitor.db"
    storage_root: Path = Path("./data/storage")
    admin_api_key: str = ""

    account_lease_ttl_seconds: int = 1800
    default_collection_interval_seconds: int = 3600
    worker_id: str = "local-worker-1"

    youtube_enabled: bool = True
    youtube_cookie_file: Path | None = None
    youtube_cookie_dir: Path = Path("./data/storage/private/youtube_cookies")
    youtube_browser: str = "chrome"
    youtube_browser_profile: Path | None = None
    youtube_proxy: str = ""
    youtube_output_root: Path = Path("./data/storage/youtube")
    youtube_default_comments: int = 100
    youtube_js_runtime: str = "/home/ubuntu/.deno/bin/deno"
    youtube_remote_components: str = "ejs:github"
    youtube_disable_plugins: bool = True
    tiktok_enabled: bool = False
    tiktok_browser: str = "chromium"
    tiktok_browser_profile: Path | None = None
    tiktok_output_root: Path = Path("./data/storage/tiktok")
    facebook_enabled: bool = False
    facebook_browser: str = "chromium"
    facebook_browser_profile: Path | None = None
    facebook_output_root: Path = Path("./data/storage/facebook")
    x_enabled: bool = False
    x_browser: str = "chromium"
    x_browser_profile: Path | None = None
    x_output_root: Path = Path("./data/storage/x")
    instagram_enabled: bool = False
    instagram_browser: str = "chromium"
    instagram_browser_profile: Path | None = None
    instagram_output_root: Path = Path("./data/storage/instagram")

    collector_sync_enabled: bool = False
    collector_backend_base_url: str = ""
    collector_access_key: str = "collector-default"
    collector_access_token: str = "12345678"
    collector_backend_timeout_seconds: int = 300
    browser_comment_collection_enabled: bool = True
    browser_comment_limit: int = 50
    browser_comment_timeout_seconds: int = 45

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.storage_root.mkdir(parents=True, exist_ok=True)
    (settings.storage_root / "private").mkdir(parents=True, exist_ok=True)
    settings.youtube_output_root.mkdir(parents=True, exist_ok=True)
    settings.youtube_cookie_dir.mkdir(parents=True, exist_ok=True)
    settings.tiktok_output_root.mkdir(parents=True, exist_ok=True)
    settings.facebook_output_root.mkdir(parents=True, exist_ok=True)
    settings.x_output_root.mkdir(parents=True, exist_ok=True)
    settings.instagram_output_root.mkdir(parents=True, exist_ok=True)
    return settings
