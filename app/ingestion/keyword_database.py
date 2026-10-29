from __future__ import annotations

from typing import Callable

from app.core.config import Settings, get_settings
from app.domain.enums import CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectorError
from app.ingestion.backend import ActiveKeyword


class KeywordDatabaseClient:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connection_factory: Callable | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._connection_factory = connection_factory

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> "KeywordDatabaseClient":
        resolved = settings or get_settings()
        return cls(
            host=resolved.keyword_db_host,
            port=resolved.keyword_db_port,
            user=resolved.keyword_db_user,
            password=resolved.keyword_db_password,
            database=resolved.keyword_db_name,
        )

    def active_keywords(self, *, platforms: list[Platform | str]) -> list[ActiveKeyword]:
        platform_values = _platform_values_with_aliases(platforms)
        params = {f"platform_{index}": value for index, value in enumerate(platform_values)}
        placeholders = ", ".join(f"%({key})s" for key in params) or "'__none__'"
        sql = f"""
            select id, platform, keyword, match_type, risk_level
            from monitor_keyword
            where status = 'ACTIVE'
              and (platform in ({placeholders}) or platform = 'ALL')
            order by id asc
        """
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except Exception as exc:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"keyword database active keyword query failed: {exc}",
                retryable=True,
                credential_action=CredentialAction.NONE,
            ) from exc
        finally:
            connection.close()
        return [
            ActiveKeyword(
                id=int(row["id"]),
                platform=_normalize_platform(str(row["platform"])),
                keyword=str(row["keyword"]),
                match_type=str(row.get("match_type") or ""),
                risk_level=str(row.get("risk_level") or ""),
            )
            for row in rows
            if row.get("id") is not None and str(row.get("keyword") or "").strip()
        ]

    def _connect(self):
        try:
            if self._connection_factory:
                return self._connection_factory(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    charset="utf8mb4",
                )
            import pymysql

            return pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
            )
        except Exception as exc:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"keyword database connection failed: {exc}",
                retryable=True,
                credential_action=CredentialAction.NONE,
            ) from exc


def _platform_values_with_aliases(platforms: list[Platform | str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for platform in platforms:
        value = platform.value if isinstance(platform, Platform) else str(platform)
        for candidate in (value, *_platform_aliases(value)):
            if candidate not in seen:
                seen.add(candidate)
                values.append(candidate)
    return values


def _platform_aliases(value: str) -> tuple[str, ...]:
    if value.upper() == Platform.X.value:
        return ("TWITTER",)
    return ()


def _normalize_platform(value: str) -> str:
    if value.upper() == "TWITTER":
        return Platform.X.value
    return value
