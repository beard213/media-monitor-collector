from typing import Protocol

from app.domain.enums import Capability, Platform
from app.domain.models import AdapterHealth, CollectionBatch, CollectionRequest, CredentialLease


class PlatformAdapter(Protocol):
    platform: Platform
    capabilities: set[Capability]

    def healthcheck(self) -> AdapterHealth:
        """Return adapter readiness without exposing secrets."""

    def discover(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        """Collect keyword search, account timeline, or content detail results."""

    def collect_comments(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        """Collect comments for a content target."""

    def collect_profile(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        """Collect public profile/account data."""

    def resolve_media(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        """Resolve or download media assets when the platform supports it."""
