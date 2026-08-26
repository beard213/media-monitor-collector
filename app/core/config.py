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
    youtube_js_runtime: str = "deno"
    youtube_remote_components: str = "ejs:github"
    youtube_disable_plugins: bool = True
    tiktok_enabled: bool = False
    tiktok_browser: str = "chromium"
    tiktok_browser_profile: Path | None = None
    tiktok_browser_cdp_url: str = ""
    tiktok_output_root: Path = Path("./data/storage/tiktok")
    facebook_enabled: bool = False
    facebook_browser: str = "chromium"
    facebook_browser_profile: Path | None = None
    facebook_browser_cdp_url: str = ""
    facebook_output_root: Path = Path("./data/storage/facebook")
    x_enabled: bool = False
    x_browser: str = "chromium"
    x_browser_profile: Path | None = None
    x_browser_cdp_url: str = ""
    x_output_root: Path = Path("./data/storage/x")
    instagram_enabled: bool = False
    instagram_browser: str = "chromium"
    instagram_browser_profile: Path | None = None
    instagram_browser_cdp_url: str = ""
    instagram_output_root: Path = Path("./data/storage/instagram")

    collector_sync_enabled: bool = False
    collector_backend_base_url: str = ""
    collector_access_key: str = "collector-default"
    collector_access_token: str = "change-me-token"
    collector_backend_timeout_seconds: int = 300
    agent_ingest_enabled: bool = True
    agent_base_url: str = "http://127.0.0.1:8088"
    agent_ingest_url: str = "http://127.0.0.1:8088/api/agent/ingest"
    agent_ingest_strategy: str = "rag_repair"
    agent_ingest_timeout_seconds: int = 5
    agent_ingest_max_attempts: int = 2
    agent_ingest_retry_interval_seconds: float = 0.2
    keyword_db_fallback_enabled: bool = False
    keyword_db_host: str = ""
    keyword_db_port: int = 3306
    keyword_db_name: str = ""
    keyword_db_user: str = ""
    keyword_db_password: str = ""
    browser_comment_collection_enabled: bool = True
    browser_comment_limit: int = 50
    browser_comment_timeout_seconds: int = 45
    collector_cleanup_enabled: bool = True
    collector_cleanup_retention_hours: int = 24
    collector_cleanup_empty_dirs: bool = True
    collector_memory_gc_enabled: bool = True

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
