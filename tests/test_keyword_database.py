from app.domain.enums import Platform
from app.ingestion.keyword_database import KeywordDatabaseClient


class FakeCursor:
    description = [("id",), ("platform",), ("keyword",), ("match_type",), ("risk_level",)]

    def __init__(self):
        self.sql = ""
        self.params = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [
            {"id": 1, "platform": "YOUTUBE", "keyword": "北戴河", "match_type": "CONTAINS", "risk_level": "MEDIUM"},
            {"id": 2, "platform": "ALL", "keyword": "秦皇岛", "match_type": "CONTAINS", "risk_level": "HIGH"},
        ]


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_keyword_database_client_reads_active_monitor_keywords_for_platforms():
    connection = FakeConnection()
    client = KeywordDatabaseClient(
        host="db",
        port=3307,
        user="media_monitor",
        password="secret",
        database="media_monitor",
        connection_factory=lambda **kwargs: connection,
    )

    keywords = client.active_keywords(platforms=[Platform.YOUTUBE])

    assert "from monitor_keyword" in connection.cursor_obj.sql
    assert "status = 'ACTIVE'" in connection.cursor_obj.sql
    assert connection.cursor_obj.params == {"platform_0": "YOUTUBE"}
    assert connection.closed is True
    assert [item.id for item in keywords] == [1, 2]
    assert [item.platform for item in keywords] == ["YOUTUBE", "ALL"]
    assert [item.keyword for item in keywords] == ["北戴河", "秦皇岛"]


def test_keyword_database_client_queries_twitter_alias_for_x_platform():
    connection = FakeConnection()
    connection.cursor_obj.fetchall = lambda: [
        {"id": 20, "platform": "TWITTER", "keyword": "秦皇岛", "match_type": "CONTAINS", "risk_level": "MEDIUM"}
    ]
    client = KeywordDatabaseClient(
        host="db",
        port=3307,
        user="media_monitor",
        password="secret",
        database="media_monitor",
        connection_factory=lambda **kwargs: connection,
    )

    keywords = client.active_keywords(platforms=[Platform.X])

    assert set(connection.cursor_obj.params.values()) == {"X", "TWITTER"}
    assert keywords[0].platform == "X"
    assert keywords[0].keyword == "秦皇岛"
