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
    # Cookie files in youtube_cookie_dir form a sequential, single-worker
    # fallback pool.  This is deliberately not a concurrency multiplier:
    # one request uses one session and only auth/rate failures cool it down.
    youtube_cookie_pool_cooldown_seconds: int = 900
    # When a managed browser profile is actively authenticated, prefer
    # extracting its current cookies over an older exported Netscape file.
    # Kept opt-in because some headless deployments intentionally have no
    # browser profile.
    youtube_prefer_browser_cookies: bool = False
    # After an account/API bot challenge, do not retry every account in the
    # next scheduler tick.  This protects the signed-in session and gives the
    # operator a deterministic re-authentication/PO-token recovery window.
    youtube_auth_circuit_breaker_seconds: int = 3600
    # A single authenticated YouTube profile must never service high-volume
    # account refreshes while it is also keeping a live room open.  Sharing
    # it across those patterns is the fastest way to trigger a bot challenge.
    youtube_defer_batch_while_live: bool = True
    youtube_live_priority_wait_seconds: int = 15
    youtube_po_token_enabled: bool = False
    youtube_po_token_provider_url: str = "http://127.0.0.1:4416"
    youtube_browser_fallback_enabled: bool = True
    # Browser comment crawling is a targeted repair path, not a bulk
    # collection strategy. It runs only when an explicitly opted-in YouTube
    # request asked for comments but yt-dlp returned none.
    youtube_browser_comment_fallback_enabled: bool = True
    youtube_browser_comment_fallback_max_videos: int = 3
    youtube_browser_comment_fallback_timeout_seconds: int = 45
    # Account media collection is bounded and paced. Keep the per-account
    # ceiling aligned with the public account-job limit so a one-year refresh
    # can actually return the requested batch without creating an unbounded
    # download burst.
    collector_account_media_items_per_account: int = 10
    youtube_download_pacing_seconds: float = 2.0
    # Scheduler-originated bulk refreshes are a different workload from an
    # operator repairing one account.  Keep them deliberately small and
    # paced: a 1,000-account submission must not translate into 10,000 media
    # downloads and comment crawls on one signed-in YouTube session.
    collector_scheduled_bulk_media_items_per_account: int = 3
    collector_scheduled_bulk_collect_comments: bool = False
    collector_scheduled_bulk_account_pause_seconds: float = 1.0
    collector_scheduled_bulk_chunk_size: int = 20
    collector_scheduled_bulk_chunk_pause_seconds: float = 15.0
    # During keyword-expansion mode, do not admit another large external
    # account sweep. Priority accounts remain eligible through their own
    # watcher/single-account path.
    collector_scheduled_bulk_enabled: bool = False
    # Priority accounts are watched independently from the large external
    # scheduler batch.  The probe is metadata-only; video and comments are
    # downloaded only after a new platform content ID is observed.
    priority_account_watch_enabled: bool = False
    priority_account_watch_interval_seconds: int = 300
    # Capacity is intentionally larger than the normal analyst shortlist:
    # all manually maintained overseas accounts can use the independent
    # watcher, while discovery accounts remain on bulk polling.
    priority_account_watch_max_accounts: int = 150
    priority_account_watch_retry_seconds: int = 60
    youtube_browser: str = "chrome"
    youtube_browser_profile: Path | None = None
    # Comma-separated dedicated profiles. This is a failover pool, not a
    # concurrency multiplier: one collection keeps one profile throughout.
    youtube_browser_profiles: str = ""
    youtube_browser_cdp_url: str = ""
    youtube_proxy: str = ""
    youtube_output_root: Path = Path("./data/storage/youtube")
    youtube_default_comments: int = 100
    youtube_js_runtime: str = "/home/ubuntu/.deno/bin/deno"
    youtube_remote_components: str = "ejs:github"
    youtube_disable_plugins: bool = True
    youtube_live_clip_seconds: int = 60
    tiktok_enabled: bool = False
    tiktok_browser: str = "chromium"
    tiktok_browser_profile: Path | None = None
    tiktok_browser_cdp_url: str = ""
    tiktok_output_root: Path = Path("./data/storage/tiktok")
    hls_output_root: Path = Path("./data/live-relay/hls")
    collector_public_base_url: str = "http://127.0.0.1:18083"
    # Optional media-only pilot. Keep disabled by default; the platform
    # adapters continue using their existing yt-dlp/browser path unless a
    # platform and (optionally) a target are explicitly allow-listed.
    streamlink_enabled: bool = False
    streamlink_platforms: str = ""
    streamlink_pilot_targets: str = ""
    streamlink_quality: str = "best"
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
    # Enable only after the business backend accepts metadata-only content
    # upserts (null videoUrl/thumbnailUrl plus mediaStatus).  Keeping this
    # explicit makes rollout safe against older backend releases.
    collector_metadata_only_sync_enabled: bool = False
    # High-volume Facebook keyword expansion receives MP4 files whose video
    # stream is often AV1 or VP9.  Modern web players can play those streams
    # directly, while an unconditional H.264 re-encode serializes the whole
    # batch and turns discovery into a CPU bottleneck.  This remains opt-in
    # and deliberately applies only to that bounded collection profile.
    collector_facebook_keyword_fast_media_enabled: bool = False
    collector_backend_base_url: str = ""
    collector_access_key: str = "collector-default"
    collector_access_token: str = "12345678"
    collector_backend_timeout_seconds: int = 300
    collector_media_timeout_seconds: int = 30
    # Liveness is a separate control-plane loop. Keep its cadence explicit so
    # the UI can distinguish a real heartbeat ACK from media/chat activity.
    collector_heartbeat_interval_seconds: int = 5
    collector_heartbeat_timeout_seconds: int = 10
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
    collector_cleanup_partial_retention_hours: int = 1
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
    settings.hls_output_root.mkdir(parents=True, exist_ok=True)
    settings.facebook_output_root.mkdir(parents=True, exist_ok=True)
    settings.x_output_root.mkdir(parents=True, exist_ok=True)
    settings.instagram_output_root.mkdir(parents=True, exist_ok=True)
    return settings
