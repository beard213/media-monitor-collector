from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.domain.enums import CollectorErrorCode, CredentialAction, CredentialStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class CredentialHealthDecision:
    next_status: CredentialStatus
    cooldown_until: datetime | None
    action: CredentialAction
    reason: str


def decide_credential_health(
    *,
    current_status: CredentialStatus,
    error_code: CollectorErrorCode | None,
    consecutive_failures: int,
    now: datetime | None = None,
) -> CredentialHealthDecision:
    checked_at = now or utcnow()
    if error_code is None:
        return CredentialHealthDecision(CredentialStatus.ACTIVE, None, CredentialAction.NONE, "success")
    if error_code == CollectorErrorCode.AUTH_EXPIRED:
        return CredentialHealthDecision(CredentialStatus.NEEDS_REAUTH, None, CredentialAction.REAUTH, "auth expired")
    if error_code == CollectorErrorCode.RATE_LIMITED:
        return CredentialHealthDecision(
            CredentialStatus.COOLDOWN,
            checked_at + timedelta(minutes=30),
            CredentialAction.COOLDOWN,
            "rate limited",
        )
    if error_code == CollectorErrorCode.ACCESS_DENIED:
        return CredentialHealthDecision(current_status, None, CredentialAction.NONE, "target access denied")
    if consecutive_failures >= 5:
        return CredentialHealthDecision(CredentialStatus.DISABLED, None, CredentialAction.DISABLE, "too many failures")
    if error_code == CollectorErrorCode.NETWORK_TEMPORARY:
        return CredentialHealthDecision(
            CredentialStatus.COOLDOWN,
            checked_at + timedelta(minutes=5),
            CredentialAction.COOLDOWN,
            "temporary network failure",
        )
    return CredentialHealthDecision(
        CredentialStatus.COOLDOWN,
        checked_at + timedelta(minutes=10),
        CredentialAction.COOLDOWN,
        "collector failure",
    )
