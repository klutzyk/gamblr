from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def to_sync_db_url(database_url: str) -> str:
    """Convert async SQLAlchemy URL into a psycopg2-compatible sync URL."""
    sync_url = database_url.replace("+asyncpg", "", 1)
    parsed = urlparse(sync_url)
    if not parsed.query:
        return sync_url

    normalized_query: list[tuple[str, str]] = []
    has_sslmode = False
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered_key = key.lower()
        lowered_value = (value or "").lower()
        if lowered_key == "ssl":
            if not has_sslmode:
                sslmode_value = (
                    "disable" if lowered_value in {"0", "false", "disable", "no", "off"} else "require"
                )
                normalized_query.append(("sslmode", sslmode_value))
                has_sslmode = True
            continue
        if lowered_key == "sslmode":
            has_sslmode = True
        normalized_query.append((key, value))

    return urlunparse(parsed._replace(query=urlencode(normalized_query)))
