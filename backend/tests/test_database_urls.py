from app.db.url_utils import to_async_db_url, to_sync_db_url


def test_postgresql_driver_urls_are_normalized():
    assert to_sync_db_url("postgresql+asyncpg://u:p@localhost/db") == (
        "postgresql+psycopg2://u:p@localhost/db"
    )
    assert to_async_db_url("postgresql+psycopg2://u:p@localhost/db") == (
        "postgresql+asyncpg://u:p@localhost/db"
    )


def test_ssl_parameters_are_translated_for_each_driver():
    assert to_sync_db_url("postgresql+asyncpg://u:p@host/db?ssl=require").endswith(
        "?sslmode=require"
    )
    assert to_async_db_url("postgresql://u:p@host/db?sslmode=require").endswith(
        "?ssl=require"
    )
