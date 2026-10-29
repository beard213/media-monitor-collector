from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.domain.enums import Capability, Platform


class CollectionJobCreate(BaseModel):
    platform: Platform
    capability: Capability
    target_type: str = Field(pattern="^(ACCOUNT|KEYWORD|CONTENT|LIVE_ROOM)$")
    target_value: str = Field(min_length=1, max_length=4000)
    source_url: str = ""
    limit: int = Field(default=10, ge=1, le=10)
    collect_comments: bool = True
    download_media: bool = False
    dry_run: bool = False
    timeout_seconds: int = Field(default=1800, ge=30, le=14400)
    metadata: dict[str, Any] = {}


class KeywordMonitorRunCreate(BaseModel):
    platforms: list[Platform] = [Platform.YOUTUBE]
    limit_per_keyword: int = Field(default=10, ge=1, le=10)
    collect_comments: bool = True
    download_media: bool = True
    dry_run: bool = False
    metadata: dict[str, Any] = {}


class CollectionJobView(CollectionJobCreate):
    job_id: str
    status: str
    created_at: datetime
    updated_at: datetime
