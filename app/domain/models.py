from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from app.domain.enums import Capability, CredentialAction, CollectorErrorCode, CredentialType, Platform


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CollectionTarget(BaseModel):
    platform: Platform
    target_type: str = Field(pattern="^(ACCOUNT|KEYWORD|CONTENT|LIVE_ROOM)$")
    target_value: str = Field(min_length=1, max_length=4000)
    source_url: str = ""
    interval_seconds: int = Field(default=3600, ge=60)
    capabilities: list[Capability] = []
    metadata: dict[str, Any] = {}


class CollectionRequest(BaseModel):
    job_id: str
    platform: Platform
    capability: Capability
    target: CollectionTarget
    limit: int = Field(default=20, ge=1, le=1000)
    collect_comments: bool = True
    download_media: bool = False
    cursor: dict[str, Any] = {}


class CredentialRef(BaseModel):
    credential_id: str
    platform: Platform
    credential_type: CredentialType
    secret_ref: str
    display_name: str = ""
    allowed_capabilities: list[Capability] = []
    metadata: dict[str, Any] = {}


class CredentialLease(BaseModel):
    lease_id: str
    credential: CredentialRef
    worker_id: str
    acquired_at: datetime = Field(default_factory=utcnow)
    expires_at: datetime


class CollectedAccount(BaseModel):
    platform: Platform
    external_id: str
    username: str = ""
    nickname: str = ""
    profile_url: str = ""
    raw: dict[str, Any] = {}


class CollectedContent(BaseModel):
    platform: Platform
    external_id: str
    url: str = ""
    content_type: str = "VIDEO"
    title: str = ""
    description: str = ""
    author_external_id: str = ""
    published_at: datetime | None = None
    collected_at: datetime = Field(default_factory=utcnow)
    raw: dict[str, Any] = {}


class CollectedComment(BaseModel):
    platform: Platform
    external_id: str
    content_external_id: str
    author_external_id: str = ""
    text: str = ""
    published_at: datetime | None = None
    collected_at: datetime = Field(default_factory=utcnow)
    raw: dict[str, Any] = {}


class MediaAsset(BaseModel):
    platform: Platform
    content_external_id: str
    asset_type: str
    local_path: str = ""
    object_key: str = ""
    url: str = ""
    raw: dict[str, Any] = {}


class CollectionBatch(BaseModel):
    accounts: list[CollectedAccount] = []
    contents: list[CollectedContent] = []
    comments: list[CollectedComment] = []
    assets: list[MediaAsset] = []
    next_cursor: dict[str, Any] = {}
    raw: dict[str, Any] = {}


class AdapterHealth(BaseModel):
    platform: Platform
    ok: bool
    mode: str
    message: str = ""
    checked_at: datetime = Field(default_factory=utcnow)


class CollectorError(Exception):
    def __init__(
        self,
        code: CollectorErrorCode,
        message: str,
        *,
        retryable: bool = False,
        credential_action: CredentialAction = CredentialAction.NONE,
    ) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        self.credential_action = credential_action
        super().__init__(message)
