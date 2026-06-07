from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text


def ensure_mlb_prop_odds_side_schema(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                ALTER TABLE mlb_prop_odds_snapshots
                ADD COLUMN IF NOT EXISTS side text NOT NULL DEFAULT 'Over'
                """
            )
        )
        conn.execute(
            text(
                """
                DO $$
                DECLARE
                    constraint_columns text;
                BEGIN
                    SELECT string_agg(att.attname, ',' ORDER BY keys.ordinality)
                    INTO constraint_columns
                    FROM pg_constraint con
                    JOIN unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality) ON true
                    JOIN pg_attribute att
                      ON att.attrelid = con.conrelid
                     AND att.attnum = keys.attnum
                    WHERE con.conname = 'uq_mlb_prop_odds_snapshot_lookup'
                      AND con.conrelid = 'mlb_prop_odds_snapshots'::regclass;

                    IF constraint_columns IS DISTINCT FROM (
                        'provider,bookmaker,market,event_id,normalized_player_name,side,line'
                    ) THEN
                        ALTER TABLE mlb_prop_odds_snapshots
                        DROP CONSTRAINT IF EXISTS uq_mlb_prop_odds_snapshot_lookup;

                        ALTER TABLE mlb_prop_odds_snapshots
                        ADD CONSTRAINT uq_mlb_prop_odds_snapshot_lookup
                        UNIQUE (
                            provider,
                            bookmaker,
                            market,
                            event_id,
                            normalized_player_name,
                            side,
                            line
                        );
                    END IF;
                END $$;
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE mlb_prop_odds_snapshots
                ALTER COLUMN side DROP DEFAULT
                """
            )
        )


def _clean(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def _float_or_none(value: Any) -> float | None:
    value = _clean(value)
    if value is None:
        return None
    return float(value)


def _int_or_none(value: Any) -> int | None:
    value = _clean(value)
    if value is None:
        return None
    return int(value)


def _datetime_or_none(value: Any) -> datetime | None:
    value = _clean(value)
    if value is None:
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def upsert_mlb_prop_odds(
    engine,
    props: list[dict[str, Any]],
    *,
    provider: str,
    sport: str,
    market: str,
    bookmaker: str,
    game_date,
) -> int:
    if not props:
        return 0

    ensure_mlb_prop_odds_side_schema(engine)
    game_date_value = pd.to_datetime(game_date).date()
    stmt = text(
        """
        INSERT INTO mlb_prop_odds_snapshots (
            provider,
            sport,
            market,
            bookmaker,
            event_id,
            game_date,
            commence_time,
            home_team,
            away_team,
            player_name,
            normalized_player_name,
            side,
            line,
            american_odds,
            decimal_odds,
            implied_probability,
            payload,
            fetched_at,
            updated_at
        )
        VALUES (
            :provider,
            :sport,
            :market,
            :bookmaker,
            :event_id,
            :game_date,
            :commence_time,
            :home_team,
            :away_team,
            :player_name,
            :normalized_player_name,
            :side,
            :line,
            :american_odds,
            :decimal_odds,
            :implied_probability,
            CAST(:payload AS json),
            now(),
            now()
        )
        ON CONFLICT (
            provider,
            bookmaker,
            market,
            event_id,
            normalized_player_name,
            side,
            line
        )
        DO UPDATE SET
            sport = EXCLUDED.sport,
            game_date = EXCLUDED.game_date,
            commence_time = EXCLUDED.commence_time,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            player_name = EXCLUDED.player_name,
            american_odds = EXCLUDED.american_odds,
            decimal_odds = EXCLUDED.decimal_odds,
            implied_probability = EXCLUDED.implied_probability,
            payload = EXCLUDED.payload,
            fetched_at = now(),
            updated_at = now()
        """
    )
    rows = 0
    with engine.begin() as conn:
        for prop in props:
            conn.execute(
                stmt,
                {
                    "provider": provider,
                    "sport": sport,
                    "market": market,
                    "bookmaker": bookmaker,
                    "event_id": str(prop.get("event_id") or ""),
                    "game_date": game_date_value,
                    "commence_time": _datetime_or_none(prop.get("commence_time")),
                    "home_team": _clean(prop.get("home_team")),
                    "away_team": _clean(prop.get("away_team")),
                    "player_name": _clean(prop.get("player_name")),
                    "normalized_player_name": _clean(prop.get("normalized_player_name")),
                    "side": _clean(prop.get("side")) or "Over",
                    "line": _float_or_none(prop.get("line")),
                    "american_odds": _int_or_none(prop.get("american_odds")),
                    "decimal_odds": _float_or_none(prop.get("decimal_odds")),
                    "implied_probability": _float_or_none(prop.get("implied_probability")),
                    "payload": json.dumps(prop, default=str),
                },
            )
            rows += 1
    return rows


def record_mlb_prop_odds_fetch(
    engine,
    *,
    provider: str,
    sport: str,
    market: str,
    bookmaker: str,
    game_date,
    props_count: int,
    events_count: int,
    status: str = "completed",
    notes: str | None = None,
) -> None:
    stmt = text(
        """
        INSERT INTO mlb_prop_odds_fetch_logs (
            provider,
            sport,
            market,
            bookmaker,
            game_date,
            status,
            props_count,
            events_count,
            notes,
            fetched_at,
            updated_at
        )
        VALUES (
            :provider,
            :sport,
            :market,
            :bookmaker,
            :game_date,
            :status,
            :props_count,
            :events_count,
            :notes,
            now(),
            now()
        )
        ON CONFLICT (provider, bookmaker, market, game_date)
        DO UPDATE SET
            sport = EXCLUDED.sport,
            status = EXCLUDED.status,
            props_count = EXCLUDED.props_count,
            events_count = EXCLUDED.events_count,
            notes = EXCLUDED.notes,
            fetched_at = now(),
            updated_at = now()
        """
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "provider": provider,
                "sport": sport,
                "market": market,
                "bookmaker": bookmaker,
                "game_date": pd.to_datetime(game_date).date(),
                "status": status,
                "props_count": int(props_count),
                "events_count": int(events_count),
                "notes": notes,
            },
        )


def load_fresh_mlb_prop_odds_fetch_log(
    engine,
    *,
    provider: str,
    market: str,
    bookmaker: str,
    game_date,
    max_age_minutes: int,
) -> dict[str, Any] | None:
    stmt = text(
        """
        SELECT
            provider,
            sport,
            market,
            bookmaker,
            game_date,
            status,
            props_count,
            events_count,
            notes,
            fetched_at
        FROM mlb_prop_odds_fetch_logs
        WHERE provider = :provider
          AND market = :market
          AND bookmaker = :bookmaker
          AND game_date = :game_date
          AND fetched_at >= :fresh_after
        ORDER BY fetched_at DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(
            stmt,
            {
                "provider": provider,
                "market": market,
                "bookmaker": bookmaker,
                "game_date": pd.to_datetime(game_date).date(),
                "fresh_after": datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes),
            },
        ).mappings().first()
    if not row:
        return None
    result = dict(row)
    fetched_at = result.get("fetched_at")
    if fetched_at is not None:
        result["fetched_at"] = pd.to_datetime(fetched_at, utc=True).isoformat()
    if result.get("game_date") is not None:
        result["game_date"] = str(result["game_date"])
    return result


def load_mlb_prop_odds(
    engine,
    *,
    provider: str,
    market: str,
    bookmaker: str,
    game_date,
    max_age_minutes: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ensure_mlb_prop_odds_side_schema(engine)
    sql = """
        SELECT
            event_id,
            commence_time,
            home_team,
            away_team,
            bookmaker,
            market,
            player_name,
            normalized_player_name,
            side,
            line,
            american_odds,
            decimal_odds,
            implied_probability,
            fetched_at
        FROM mlb_prop_odds_snapshots
        WHERE provider = :provider
          AND market = :market
          AND bookmaker = :bookmaker
          AND game_date = :game_date
    """
    params: dict[str, Any] = {
        "provider": provider,
        "market": market,
        "bookmaker": bookmaker,
        "game_date": pd.to_datetime(game_date).date(),
    }
    if max_age_minutes is not None:
        sql += " AND fetched_at >= :fresh_after"
        params["fresh_after"] = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    sql += " ORDER BY fetched_at DESC, event_id, normalized_player_name, side"

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    if df.empty:
        return [], {"rows": 0, "latest_fetched_at": None, "oldest_fetched_at": None}

    if "commence_time" in df.columns:
        df["commence_time"] = pd.to_datetime(df["commence_time"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce").max()
    oldest = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce").min()
    df = df.drop(columns=["fetched_at"])
    df = df.where(pd.notna(df), None)
    return df.to_dict("records"), {
        "rows": len(df),
        "latest_fetched_at": latest.isoformat() if pd.notna(latest) else None,
        "oldest_fetched_at": oldest.isoformat() if pd.notna(oldest) else None,
    }
