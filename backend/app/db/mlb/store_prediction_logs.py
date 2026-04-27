from __future__ import annotations

from datetime import datetime
import json
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text


STORED_MARKETS = (
    "batter_home_runs",
    "batter_hits",
    "batter_total_bases",
    "pitcher_strikeouts",
)


def _clean(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return value
    if isinstance(value, np.generic):
        return value.item()
    return value


def _bool_or_none(value: Any) -> bool | None:
    value = _clean(value)
    if value is None:
        return None
    return bool(value)


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


def _row_payload(row: pd.Series) -> dict[str, Any]:
    payload_cols = [
        "is_home",
        "batting_order",
        "has_posted_lineup",
        "starter_pitcher_id",
        "venue_name",
        "venue_city",
        "venue_state",
        "roof_type",
        "turf_type",
        "weather_condition",
        "temperature_f",
        "wind_text",
        "temperature_2m_c",
        "wind_speed_10m_kph",
        "wind_gusts_10m_kph",
        "park_factor_hr",
        "recent_games",
    ]
    payload = {}
    for col in payload_cols:
        if col not in row.index:
            continue
        value = _clean(row.get(col))
        if isinstance(value, str) and col == "recent_games":
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        payload[col] = value
    return payload


def upsert_mlb_prediction_logs(
    engine,
    market: str,
    scored,
    *,
    model_path: str | None = None,
    prediction_date=None,
) -> int:
    if scored is None or scored.empty:
        return 0

    df = scored.copy()
    if prediction_date is None:
        prediction_date = datetime.utcnow().date()
    else:
        prediction_date = pd.to_datetime(prediction_date).date()

    stmt = text(
        """
        INSERT INTO mlb_prediction_logs (
            market,
            game_pk,
            game_date,
            prediction_date,
            player_id,
            player_name,
            team_id,
            team_abbreviation,
            opponent_team_id,
            opponent_team_abbreviation,
            is_home,
            batting_order,
            has_posted_lineup,
            starter_pitcher_id,
            probability,
            prediction,
            model_path,
            payload,
            updated_at
        )
        VALUES (
            :market,
            :game_pk,
            :game_date,
            :prediction_date,
            :player_id,
            :player_name,
            :team_id,
            :team_abbreviation,
            :opponent_team_id,
            :opponent_team_abbreviation,
            :is_home,
            :batting_order,
            :has_posted_lineup,
            :starter_pitcher_id,
            :probability,
            :prediction,
            :model_path,
            CAST(:payload AS json),
            now()
        )
        ON CONFLICT (market, game_pk, player_id)
        DO UPDATE SET
            game_date = EXCLUDED.game_date,
            prediction_date = EXCLUDED.prediction_date,
            player_name = EXCLUDED.player_name,
            team_id = EXCLUDED.team_id,
            team_abbreviation = EXCLUDED.team_abbreviation,
            opponent_team_id = EXCLUDED.opponent_team_id,
            opponent_team_abbreviation = EXCLUDED.opponent_team_abbreviation,
            is_home = EXCLUDED.is_home,
            batting_order = EXCLUDED.batting_order,
            has_posted_lineup = EXCLUDED.has_posted_lineup,
            starter_pitcher_id = EXCLUDED.starter_pitcher_id,
            probability = EXCLUDED.probability,
            prediction = EXCLUDED.prediction,
            model_path = EXCLUDED.model_path,
            payload = EXCLUDED.payload,
            updated_at = now(),
            abs_error = CASE
                WHEN mlb_prediction_logs.actual_value IS NOT NULL
                THEN ABS(mlb_prediction_logs.actual_value - COALESCE(EXCLUDED.prediction, EXCLUDED.probability))
                ELSE mlb_prediction_logs.abs_error
            END
        """
    )

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                stmt,
                {
                    "market": market,
                    "game_pk": _int_or_none(row.get("game_pk")),
                    "game_date": pd.to_datetime(row.get("game_date")).date(),
                    "prediction_date": prediction_date,
                    "player_id": _int_or_none(row.get("player_id")),
                    "player_name": _clean(row.get("player_name")),
                    "team_id": _int_or_none(row.get("team_id")),
                    "team_abbreviation": _clean(row.get("team_abbreviation")),
                    "opponent_team_id": _int_or_none(row.get("opponent_team_id")),
                    "opponent_team_abbreviation": _clean(row.get("opponent_team_abbreviation")),
                    "is_home": _bool_or_none(row.get("is_home")),
                    "batting_order": _float_or_none(row.get("batting_order")),
                    "has_posted_lineup": _bool_or_none(row.get("has_posted_lineup")),
                    "starter_pitcher_id": _int_or_none(row.get("starter_pitcher_id")),
                    "probability": _float_or_none(row.get("probability")),
                    "prediction": _float_or_none(row.get("prediction")),
                    "model_path": model_path,
                    "payload": pd.Series(_row_payload(row)).to_json(),
                },
            )
            inserted += 1
    return inserted


def load_mlb_prediction_logs(
    engine,
    *,
    market: str,
    game_date,
    limit: int | None = None,
) -> pd.DataFrame:
    sql = """
        SELECT
            p.game_date,
            g.start_time_utc,
            p.game_pk,
            p.player_id,
            p.player_name,
            p.team_id,
            p.team_abbreviation,
            p.opponent_team_id,
            p.opponent_team_abbreviation,
            v.name as venue_name,
            v.city as venue_city,
            v.state as venue_state,
            v.roof_type,
            v.turf_type,
            g.weather_condition,
            g.temperature_f,
            g.wind_text,
            weather.temperature_2m_c,
            weather.wind_speed_10m_kph,
            weather.wind_gusts_10m_kph,
            park.park_factor_hr,
            COALESCE(recent_batter.recent_games, recent_pitcher.recent_games) as recent_games,
            p.is_home,
            p.batting_order,
            p.has_posted_lineup,
            p.starter_pitcher_id,
            p.probability,
            p.prediction,
            p.model_path,
            p.prediction_date,
            p.updated_at
        FROM mlb_prediction_logs p
        LEFT JOIN mlb_games g ON g.game_pk = p.game_pk
        LEFT JOIN mlb_venues v ON v.id = g.venue_id
        LEFT JOIN LATERAL (
            SELECT
                temperature_2m_c,
                wind_speed_10m_kph,
                wind_gusts_10m_kph
            FROM mlb_weather_snapshots ws
            WHERE ws.game_pk = p.game_pk
            ORDER BY abs(coalesce(ws.game_time_offset_hours, 9999))
            LIMIT 1
        ) weather ON true
        LEFT JOIN LATERAL (
            SELECT
                max(stat_value) FILTER (WHERE stat_key = 'index_HR') as park_factor_hr
            FROM mlb_park_factors pf
            WHERE pf.venue_id = g.venue_id
              AND pf.season = EXTRACT(YEAR FROM p.game_date)::integer
        ) park ON true
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'game_date', game_date,
                    'matchup', matchup,
                    'hits', hits,
                    'home_runs', home_runs,
                    'total_bases', total_bases,
                    'strikeouts', strikeouts,
                    'plate_appearances', plate_appearances
                )
                ORDER BY game_date DESC, game_pk DESC
            ) as recent_games
            FROM (
                SELECT
                    b.game_pk,
                    bg.official_date as game_date,
                    at.abbreviation || ' @ ' || ht.abbreviation as matchup,
                    b.hits,
                    b.home_runs,
                    b.total_bases,
                    b.strikeouts,
                    b.plate_appearances
                FROM mlb_player_game_batting b
                JOIN mlb_games bg ON bg.game_pk = b.game_pk
                LEFT JOIN mlb_teams ht ON ht.id = bg.home_team_id
                LEFT JOIN mlb_teams at ON at.id = bg.away_team_id
                WHERE b.player_id = p.player_id
                  AND bg.official_date < p.game_date
                  AND b.plate_appearances > 0
                ORDER BY bg.official_date DESC, b.game_pk DESC
                LIMIT 5
            ) recent_rows
        ) recent_batter ON p.market <> 'pitcher_strikeouts'
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'game_date', game_date,
                    'matchup', matchup,
                    'strikeouts', strikeouts,
                    'innings_pitched', innings_pitched,
                    'pitches_thrown', pitches_thrown,
                    'earned_runs', earned_runs
                )
                ORDER BY game_date DESC, game_pk DESC
            ) as recent_games
            FROM (
                SELECT
                    pg.game_pk,
                    bg.official_date as game_date,
                    at.abbreviation || ' @ ' || ht.abbreviation as matchup,
                    pg.strikeouts,
                    pg.innings_pitched,
                    pg.pitches_thrown,
                    pg.earned_runs
                FROM mlb_player_game_pitching pg
                JOIN mlb_games bg ON bg.game_pk = pg.game_pk
                LEFT JOIN mlb_teams ht ON ht.id = bg.home_team_id
                LEFT JOIN mlb_teams at ON at.id = bg.away_team_id
                WHERE pg.player_id = p.player_id
                  AND bg.official_date < p.game_date
                  AND pg.is_starter = true
                ORDER BY bg.official_date DESC, pg.game_pk DESC
                LIMIT 5
            ) recent_rows
        ) recent_pitcher ON p.market = 'pitcher_strikeouts'
        WHERE p.market = :market
          AND p.game_date = :game_date
        ORDER BY COALESCE(probability, prediction) DESC NULLS LAST
    """
    params = {"market": market, "game_date": pd.to_datetime(game_date).date()}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def load_mlb_prediction_slate_logs(
    engine,
    *,
    game_date,
    limit_per_market: int | None = None,
) -> dict[str, pd.DataFrame]:
    return {
        market: load_mlb_prediction_logs(
            engine,
            market=market,
            game_date=game_date,
            limit=limit_per_market,
        )
        for market in STORED_MARKETS
    }
