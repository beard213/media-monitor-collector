from app.adapters.base import PlatformAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import AdapterHealth, CollectionBatch, CollectionRequest, CollectorError, CredentialLease


class StubAdapter(PlatformAdapter):
    def __init__(self, platform: Platform, capabilities: set[Capability], enabled: bool = False) -> None:
        self.platform = platform
        self.capabilities = capabilities
        self.enabled = enabled

    def healthcheck(self) -> AdapterHealth:
        return AdapterHealth(
            platform=self.platform,
            ok=self.enabled,
            mode="stub",
            message="adapter contract registered; real collector not implemented",
        )

    def _not_ready(self) -> None:
        raise CollectorError(
            CollectorErrorCode.PLATFORM_CHANGED,
            f"{self.platform} adapter is not implemented",
            retryable=False,
            credential_action=CredentialAction.NONE,
        )

    def discover(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        self._not_ready()

    def collect_comments(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        self._not_ready()

    def collect_profile(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        self._not_ready()

    def resolve_media(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        self._not_ready()
