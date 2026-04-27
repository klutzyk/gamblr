from __future__ import annotations

from datetime import datetime
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
    if pd.isna(value):
        return None
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
    ]
    return {col: _clean(row.get(col)) for col in payload_cols if col in row.index}


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
