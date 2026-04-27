from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text


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
                    "line": _float_or_none(prop.get("line")),
                    "american_odds": _int_or_none(prop.get("american_odds")),
                    "decimal_odds": _float_or_none(prop.get("decimal_odds")),
                    "implied_probability": _float_or_none(prop.get("implied_probability")),
                    "payload": json.dumps(prop, default=str),
                },
            )
            rows += 1
    return rows


def load_mlb_prop_odds(
    engine,
    *,
    provider: str,
    market: str,
    bookmaker: str,
    game_date,
    max_age_minutes: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
    sql += " ORDER BY fetched_at DESC, event_id, normalized_player_name"

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
