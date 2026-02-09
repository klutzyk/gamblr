from fastapi import APIRouter, Query
from ..services.nba_client import NBAClient
from sqlalchemy import create_engine, text
from app.core.config import settings
from fastapi.concurrency import run_in_threadpool
from app.services.rotowire_lineups_client import RotoWireLineupsClient
from app.services.jedibets_first_basket_client import JediBetsFirstBasketClient
from app.services.lineup_resolver import LineupResolver
from app.services.lineup_context import fetch_lineups_payload, build_expected_lineup_sets
from app.db.store_first_basket import upsert_first_basket_prediction_logs
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import math
from datetime import datetime
from datetime import timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

# get teh path to the ml folder
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.predict import (
    predict_points,
    predict_assists,
    predict_rebounds,
    predict_threept,
    predict_threepa,
)
from ml.first_basket_model import predict_first_basket_with_models
from app.db.store_prediction_logs import log_predictions

router = APIRouter()
client = NBAClient(timeout=15)
sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))
rotowire_lineups_client = RotoWireLineupsClient(timeout=20)
jedi_client = JediBetsFirstBasketClient(timeout=20)
lineup_resolver = LineupResolver(sync_engine)


# Helper function to convert DataFrame to list of dicts for API response
def df_to_dict(df):
    df = df.replace({pd.NA: None, np.inf: None, -np.inf: None})
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    for row in records:
        for key, value in row.items():
            if isinstance(value, (float, np.floating)):
                if not math.isfinite(value):
                    row[key] = None
    return records


def fetch_under_risk(engine, stat_type: str, player_ids: list[int]):
    if not player_ids:
        return {}
    ids = ",".join(str(int(pid)) for pid in set(player_ids))
    query = text(
        f"""
        SELECT player_id, under_rate, sample_size
        FROM player_under_risk
        WHERE stat_type = :stat_type
          AND player_id IN ({ids})
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"stat_type": stat_type}).fetchall()
    return {int(r[0]): {"under_rate": r[1], "sample_size": r[2]} for r in rows}


def fetch_last_under(engine, stat_type: str, player_ids: list[int]):
    if not player_ids:
        return {}
    threshold_type = "midpoint" if stat_type == "points" else "pred_p10"
    ids = ",".join(str(int(pid)) for pid in set(player_ids))
    query = text(
        f"""
        WITH ranked AS (
            SELECT player_id, game_date, actual_value, pred_value, pred_p10, game_id,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
            FROM prediction_logs
            WHERE stat_type = :stat_type
              AND actual_value IS NOT NULL
              AND game_date IS NOT NULL
              AND (
                    (:threshold_type = 'midpoint' AND pred_value IS NOT NULL AND pred_p10 IS NOT NULL)
                 OR (:threshold_type = 'pred_p10' AND pred_p10 IS NOT NULL)
              )
              AND player_id IN ({ids})
        ),
        undered AS (
            SELECT player_id, game_date, actual_value, rn, game_id,
                   CASE
                       WHEN actual_value < (
                           CASE
                               WHEN :threshold_type = 'midpoint' THEN (pred_p10 + pred_value) / 2.0
                               ELSE pred_p10
                           END
                       ) THEN 1
                       ELSE 0
                   END AS is_under
            FROM ranked
        ),
        last_under AS (
            SELECT player_id, game_date, actual_value, rn, game_id,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY rn ASC) AS rn_under
            FROM undered
            WHERE is_under = 1
        )
        SELECT lu.player_id, lu.game_date, lu.actual_value, lu.rn AS last_under_rn,
               pgs.matchup, pgs.minutes,
               (
                 SELECT COUNT(*)
                 FROM player_game_stats pgs2
                 WHERE pgs2.player_id = lu.player_id
                   AND pgs2.game_date > lu.game_date
               ) AS games_after
        FROM last_under lu
        LEFT JOIN player_game_stats pgs
          ON pgs.player_id = lu.player_id AND pgs.game_id = lu.game_id
        WHERE rn_under = 1
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query, {"stat_type": stat_type, "threshold_type": threshold_type}
        ).fetchall()
    result = {}
    for r in rows:
        player_id = int(r[0])
        last_under_rn = int(r[3]) if r[3] is not None else None
        games_after = int(r[6]) if r[6] is not None else None
        result[player_id] = {
            "last_under_date": r[1],
            "last_under_value": r[2],
            "last_under_games_ago": (
                games_after if games_after is not None else (last_under_rn - 1)
            ),
            "last_under_matchup": r[4],
            "last_under_minutes": r[5],
        }
    return result


def fetch_player_games_for_stat(engine, stat_type: str, player_ids: list[int]):
    stat_column = {
        "points": "points",
        "assists": "assists",
        "rebounds": "rebounds",
        "threept": "fg3m",
        "threepa": "fg3a",
    }.get(stat_type)
    if not stat_column or not player_ids:
        return pd.DataFrame()

    ids = ",".join(str(int(pid)) for pid in set(player_ids))
    query = f"""
    SELECT player_id, game_date, matchup, minutes, {stat_column} AS stat_value
    FROM player_game_stats
    WHERE player_id IN ({ids})
      AND game_date IS NOT NULL
      AND {stat_column} IS NOT NULL
    """
    return pd.read_sql(query, engine)


def compute_last_under_by_threshold(
    engine,
    df_preds: pd.DataFrame,
    stat_type: str,
):
    if df_preds.empty:
        return {}

    if stat_type == "points":
        thresholds = df_preds.apply(
            lambda r: (r["pred_p10"] + r["pred_value"]) / 2.0
            if pd.notnull(r.get("pred_p10")) and pd.notnull(r.get("pred_value"))
            else np.nan,
            axis=1,
        )
    else:
        thresholds = df_preds["pred_p10"]

    player_ids = df_preds["player_id"].tolist()
    games = fetch_player_games_for_stat(engine, stat_type, player_ids)
    if games.empty:
        return {}

    games["game_date"] = pd.to_datetime(games["game_date"])
    games = games.sort_values(["player_id", "game_date"], ascending=[True, False])

    threshold_map = {
        int(pid): float(thresholds.iloc[idx])
        for idx, pid in enumerate(df_preds["player_id"].tolist())
        if pd.notnull(thresholds.iloc[idx])
    }

    result = {}
    for pid, group in games.groupby("player_id"):
        threshold = threshold_map.get(int(pid))
        if threshold is None:
            continue
        group = group.reset_index(drop=True)
        under_rows = group[group["stat_value"] < threshold]
        if under_rows.empty:
            continue
        row = under_rows.iloc[0]
        result[int(pid)] = {
            "last_under_date": row["game_date"].date(),
            "last_under_value": row["stat_value"],
            "last_under_games_ago": int(under_rows.index[0]),
            "last_under_matchup": row["matchup"],
            "last_under_minutes": row["minutes"],
        }

    return result


def fetch_good_player_ids(engine, stat_type: str):
    stat_thresholds = {
        "points": 20,
        "assists": 6,
        "rebounds": 6,
        "threept": 2,
        "threepa": 4.0,
    }
    threshold = stat_thresholds.get(stat_type)
    if threshold is None:
        return set()

    stat_query = text(
        """
        WITH ranked AS (
            SELECT player_id, actual_value, game_date,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
            FROM prediction_logs
            WHERE stat_type = :stat_type
              AND actual_value IS NOT NULL
              AND game_date IS NOT NULL
        ),
        recent AS (
            SELECT player_id, actual_value
            FROM ranked
            WHERE rn <= 20
        )
        SELECT player_id, AVG(actual_value) AS avg_actual
        FROM recent
        GROUP BY player_id
        HAVING AVG(actual_value) >= :threshold
        """
    )

    minutes_query = text(
        """
        WITH ranked AS (
            SELECT player_id, minutes, game_date,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
            FROM player_game_stats
            WHERE minutes IS NOT NULL
              AND game_date IS NOT NULL
        ),
        recent AS (
            SELECT player_id, minutes
            FROM ranked
            WHERE rn <= 20
        )
        SELECT player_id, AVG(minutes) AS avg_minutes
        FROM recent
        GROUP BY player_id
        HAVING AVG(minutes) >= 20
        """
    )

    with engine.connect() as conn:
        stat_rows = conn.execute(
            stat_query, {"stat_type": stat_type, "threshold": threshold}
        ).fetchall()
        min_rows = conn.execute(minutes_query).fetchall()

    stat_ids = {int(r[0]) for r in stat_rows}
    min_ids = {int(r[0]) for r in min_rows}
    return stat_ids.intersection(min_ids)


def apply_under_risk_boost(df, stat_type: str, good_ids: set[int]):
    boosts = {
        "points": 0.026,
        "assists": 0.055,
        "rebounds": 0.023,
        "threept": 0.013,
        "threepa": 0.01,
    }
    delta = boosts.get(stat_type, 0)
    if not delta:
        return

    def adjust(row):
        base = row.get("under_risk")
        if base is None:
            return base
        if row.get("last_under_games_ago") == 0 and row.get("player_id") in good_ids:
            return max(0.0, float(base) - delta)
        return base

    df["under_risk"] = df.apply(adjust, axis=1)

@router.get("/top_scorers")
def top_scorers(season: str = "2025-26", top_n: int = 10):
    df = client.fetch_player_stats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
    )
    df_sorted = df.sort_values("PTS", ascending=False).head(top_n)
    return df_to_dict(df_sorted)


@router.get("/top_assists")
def top_assists(season: str = "2025-26", top_n: int = 10):
    df = client.fetch_player_stats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
    )
    df_sorted = df.sort_values("AST", ascending=False).head(top_n)
    return df_to_dict(df_sorted)


@router.get("/top_rebounders")
def top_rebounders(season: str = "2025-26", top_n: int = 10):
    df = client.fetch_player_stats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
    )
    df_sorted = df.sort_values("REB", ascending=False).head(top_n)
    return df_to_dict(df_sorted)


@router.get("/guards_stats")
def guards_stats(season: str = "2025-26", top_n: int = 10):
    df = client.fetch_player_stats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
        player_position_abbreviation_nullable="G",
    )
    df_filtered = df[["PLAYER_NAME", "PTS", "AST", "REB", "NBA_FANTASY_PTS"]].head(
        top_n
    )
    return df_to_dict(df_filtered)


@router.get("/recent_performers")
def recent_performers(season: str = "2025-26", last_n_games: int = 5, top_n: int = 10):
    df = client.fetch_player_stats(
        season=season,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
        last_n_games=last_n_games,
    )
    df_sorted = df.sort_values("NBA_FANTASY_PTS", ascending=False).head(top_n)
    return df_to_dict(df_sorted)


## Predivtion routes
@router.get("/predictions/points")
async def predict_points_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    """
    Predict player points for NBA games.
    day = today | tomorrow | yesterday
    """
    lineups_payload = await run_in_threadpool(fetch_lineups_payload, sync_engine, day)
    expected_map, excluded_map = build_expected_lineup_sets(lineups_payload)
    df_preds = await run_in_threadpool(
        predict_points,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

    df_preds = _apply_lineup_filters(df_preds, day, lineups_payload=lineups_payload)

    under_risk = fetch_under_risk(
        sync_engine, "points", df_preds["player_id"].tolist()
    )
    last_under = compute_last_under_by_threshold(sync_engine, df_preds, "points")
    good_ids = fetch_good_player_ids(sync_engine, "points")
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )
    df_preds["last_under_date"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_date")
    )
    df_preds["last_under_value"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_value")
    )
    df_preds["last_under_games_ago"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_games_ago")
    )
    df_preds["last_under_matchup"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_matchup")
    )
    df_preds["last_under_minutes"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_minutes")
    )
    apply_under_risk_boost(df_preds, "points", good_ids)

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "points",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)


@router.get("/predictions/assists")
async def predict_assists_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    """
    Predict player assists for NBA games.
    day = today | tomorrow | yesterday
    """
    lineups_payload = await run_in_threadpool(fetch_lineups_payload, sync_engine, day)
    expected_map, excluded_map = build_expected_lineup_sets(lineups_payload)
    df_preds = await run_in_threadpool(
        predict_assists,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

    df_preds = _apply_lineup_filters(df_preds, day, lineups_payload=lineups_payload)

    under_risk = fetch_under_risk(
        sync_engine, "assists", df_preds["player_id"].tolist()
    )
    last_under = compute_last_under_by_threshold(sync_engine, df_preds, "assists")
    good_ids = fetch_good_player_ids(sync_engine, "assists")
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )
    df_preds["last_under_date"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_date")
    )
    df_preds["last_under_value"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_value")
    )
    df_preds["last_under_games_ago"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_games_ago")
    )
    df_preds["last_under_matchup"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_matchup")
    )
    df_preds["last_under_minutes"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_minutes")
    )
    apply_under_risk_boost(df_preds, "assists", good_ids)

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "assists",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)


@router.get("/predictions/rebounds")
async def predict_rebounds_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    """
    Predict player rebounds for NBA games.
    day = today | tomorrow | yesterday
    """
    lineups_payload = await run_in_threadpool(fetch_lineups_payload, sync_engine, day)
    expected_map, excluded_map = build_expected_lineup_sets(lineups_payload)
    df_preds = await run_in_threadpool(
        predict_rebounds,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

    df_preds = _apply_lineup_filters(df_preds, day, lineups_payload=lineups_payload)

    under_risk = fetch_under_risk(
        sync_engine, "rebounds", df_preds["player_id"].tolist()
    )
    last_under = compute_last_under_by_threshold(sync_engine, df_preds, "rebounds")
    good_ids = fetch_good_player_ids(sync_engine, "rebounds")
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )
    df_preds["last_under_date"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_date")
    )
    df_preds["last_under_value"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_value")
    )
    df_preds["last_under_games_ago"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_games_ago")
    )
    df_preds["last_under_matchup"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_matchup")
    )
    df_preds["last_under_minutes"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_minutes")
    )
    apply_under_risk_boost(df_preds, "rebounds", good_ids)

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "rebounds",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)


@router.get("/predictions/threept")
async def predict_threept_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    """
    Predict player made 3-pointers for NBA games.
    day = today | tomorrow | yesterday
    """
    lineups_payload = await run_in_threadpool(fetch_lineups_payload, sync_engine, day)
    expected_map, excluded_map = build_expected_lineup_sets(lineups_payload)
    df_preds = await run_in_threadpool(
        predict_threept,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

    df_preds = _apply_lineup_filters(df_preds, day, lineups_payload=lineups_payload)

    under_risk = fetch_under_risk(
        sync_engine, "threept", df_preds["player_id"].tolist()
    )
    last_under = compute_last_under_by_threshold(sync_engine, df_preds, "threept")
    good_ids = fetch_good_player_ids(sync_engine, "threept")
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )
    df_preds["last_under_date"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_date")
    )
    df_preds["last_under_value"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_value")
    )
    df_preds["last_under_games_ago"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_games_ago")
    )
    df_preds["last_under_matchup"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_matchup")
    )
    df_preds["last_under_minutes"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_minutes")
    )
    apply_under_risk_boost(df_preds, "threept", good_ids)

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "threept",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)


@router.get("/predictions/threepa")
async def predict_threepa_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    """
    Predict player 3-point attempts for NBA games.
    day = today | tomorrow | yesterday
    """
    lineups_payload = await run_in_threadpool(fetch_lineups_payload, sync_engine, day)
    expected_map, excluded_map = build_expected_lineup_sets(lineups_payload)
    df_preds = await run_in_threadpool(
        predict_threepa,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

    df_preds = _apply_lineup_filters(df_preds, day, lineups_payload=lineups_payload)

    under_risk = fetch_under_risk(
        sync_engine, "threepa", df_preds["player_id"].tolist()
    )
    last_under = compute_last_under_by_threshold(sync_engine, df_preds, "threepa")
    good_ids = fetch_good_player_ids(sync_engine, "threepa")
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )
    df_preds["last_under_date"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_date")
    )
    df_preds["last_under_value"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_value")
    )
    df_preds["last_under_games_ago"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_games_ago")
    )
    df_preds["last_under_matchup"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_matchup")
    )
    df_preds["last_under_minutes"] = df_preds["player_id"].map(
        lambda pid: last_under.get(int(pid), {}).get("last_under_minutes")
    )
    apply_under_risk_boost(df_preds, "threepa", good_ids)

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "threepa",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)


def fetch_recent_points_avgs(engine, player_ids: list[int], n_games: int = 10):
    if not player_ids:
        return {}
    ids = ",".join(str(int(pid)) for pid in set(player_ids))
    query = text(
        f"""
        WITH ranked AS (
            SELECT player_id, points, game_date,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
            FROM player_game_stats
            WHERE player_id IN ({ids})
              AND points IS NOT NULL
              AND game_date IS NOT NULL
        )
        SELECT player_id, AVG(points) AS avg_points
        FROM ranked
        WHERE rn <= :n_games
        GROUP BY player_id
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"n_games": n_games}).fetchall()
    return {int(r[0]): float(r[1]) for r in rows if r[1] is not None}


def _injury_multiplier(injury_tag: str | None):
    if not injury_tag:
        return 1.0
    tag = injury_tag.lower()
    if tag == "prob":
        return 0.9
    if tag == "ques":
        return 0.75
    if tag == "doubt":
        return 0.4
    if tag in {"out", "ofs"}:
        return 0.0
    return 1.0


def _build_lineup_injury_index(
    day: str,
    df_preds: pd.DataFrame,
    lineups_payload: dict | None = None,
):
    if df_preds.empty:
        return {}
    lineups = lineups_payload
    if not lineups:
        rotowire_day = _rotowire_day_for_prediction_day(day)
        try:
            raw_lineups = rotowire_lineups_client.fetch_lineups(day=rotowire_day)
        except Exception:
            return {}
        if not raw_lineups or (raw_lineups.get("games_count") or 0) == 0:
            return {}

        lineups = lineup_resolver.enrich_rotowire_payload(raw_lineups)
        lineups = attach_schedule_metadata(lineups)
    if not lineups or not lineups.get("games"):
        return {}

    def severity(tag: str | None) -> int:
        t = (tag or "").lower()
        if t in {"out", "ofs"}:
            return 4
        if t == "doubt":
            return 3
        if t == "ques":
            return 2
        if t == "prob":
            return 1
        return 0

    game_index: dict[str, dict[int, dict]] = {}
    for game in lineups.get("games", []):
        game_id = game.get("game_id")
        if not game_id:
            continue
        game_key = str(game_id)
        per_game = game_index.setdefault(game_key, {})
        for team_key in ["away", "home"]:
            team = game.get(team_key, {})
            team_status = team.get("status")
            for group in ("starters", "may_not_play"):
                for player in team.get(group, []) or []:
                    pid = player.get("resolved_player_id")
                    if pid is None:
                        continue
                    try:
                        pid = int(pid)
                    except (TypeError, ValueError):
                        continue
                    injury_tag = player.get("injury_tag")
                    play_pct = player.get("play_pct")
                    existing = per_game.get(pid)
                    if existing is None or severity(injury_tag) > severity(
                        existing.get("injury_tag")
                    ):
                        per_game[pid] = {
                            "injury_tag": injury_tag,
                            "play_pct": play_pct,
                            "lineup_status": team_status,
                        }
                    elif existing and existing.get("play_pct") is None and play_pct is not None:
                        existing["play_pct"] = play_pct

    pred_game_ids = {
        str(gid)
        for gid in df_preds["game_id"].tolist()
        if pd.notnull(gid)
    }
    return {gid: info for gid, info in game_index.items() if gid in pred_game_ids}


def _rotowire_day_for_prediction_day(day: str) -> str | None:
    if ZoneInfo:
        base_date = datetime.now(ZoneInfo("America/New_York")).date()
    else:
        base_date = datetime.now().date()

    if day == "auto":
        if ZoneInfo:
            aus_hour = datetime.now(ZoneInfo("Australia/Sydney")).hour
            target_date = base_date if aus_hour >= 17 else base_date - timedelta(days=1)
        else:
            target_date = base_date
    elif day in {"today", "tomorrow", "yesterday"}:
        target_date = _target_et_date_for_day(day)
    else:
        target_date = base_date

    diff_days = (target_date - base_date).days
    if diff_days == -1:
        return "yesterday"
    if diff_days == 0:
        return "today"
    if diff_days == 1:
        return "tomorrow"
    return None


def _apply_lineup_filters(
    df_preds: pd.DataFrame, day: str, lineups_payload: dict | None = None
):
    if df_preds.empty:
        return df_preds
    injury_index = _build_lineup_injury_index(day, df_preds, lineups_payload)
    if not injury_index:
        return df_preds

    adjusted_rows = []
    for _, row in df_preds.iterrows():
        game_id = row.get("game_id")
        pid = row.get("player_id")
        if pd.isnull(game_id) or pd.isnull(pid):
            adjusted_rows.append(row)
            continue
        game_info = injury_index.get(str(game_id))
        if not game_info:
            adjusted_rows.append(row)
            continue
        info = game_info.get(int(pid))
        if not info:
            adjusted_rows.append(row)
            continue

        injury_tag = info.get("injury_tag")
        play_pct = info.get("play_pct")
        mult = _injury_multiplier(injury_tag)
        if play_pct is not None:
            try:
                mult *= float(play_pct) / 100.0
            except (TypeError, ValueError):
                pass

        if mult <= 0:
            continue

        for col in ["pred_value", "pred_p10", "pred_p50", "pred_p90"]:
            val = row.get(col)
            if pd.notnull(val):
                row[col] = float(val) * mult

        row["lineup_injury_tag"] = injury_tag
        row["lineup_play_pct"] = play_pct
        row["lineup_status"] = info.get("lineup_status")
        adjusted_rows.append(row)

    return pd.DataFrame(adjusted_rows)


TEAM_ABBR_ALIAS = {
    "NY": "NYK",
    "GS": "GSW",
    "WSH": "WAS",
    "NO": "NOP",
    "SA": "SAS",
    "UTAH": "UTA",
}


def _canon_team_abbr(team_abbr: str | None):
    if not team_abbr:
        return ""
    t = team_abbr.upper().strip()
    return TEAM_ABBR_ALIAS.get(t, t)


def _norm_name(name: str | None):
    if not name:
        return ""
    return "".join(ch for ch in name.lower() if ch.isalnum())


def build_jedibets_priors(jedibets_stats: dict | None):
    if not jedibets_stats:
        return {}, {}

    player_counts: dict[tuple[str, str], int] = {}
    team_fg_pct: dict[str, float] = {}

    for row in jedibets_stats.get("players", []):
        name = _norm_name(row.get("player"))
        team = _canon_team_abbr(row.get("team"))
        count = row.get("first_baskets")
        if name and team and isinstance(count, int):
            player_counts[(name, team)] = count

    for row in jedibets_stats.get("teams", []):
        team = _canon_team_abbr(row.get("team"))
        fg_pct = row.get("first_fg_pct")
        if team and isinstance(fg_pct, (float, int)):
            team_fg_pct[team] = float(fg_pct)

    return player_counts, team_fg_pct


def build_first_basket_predictions(
    lineups_payload: dict,
    df_points: pd.DataFrame,
    top_n_per_game: int,
    jedibets_stats: dict | None = None,
):
    points_map = {}
    if not df_points.empty and "player_id" in df_points and "pred_value" in df_points:
        points_map = {
            int(pid): float(val)
            for pid, val in zip(df_points["player_id"], df_points["pred_value"])
            if pd.notnull(pid) and pd.notnull(val)
        }

    starter_ids = []
    for game in lineups_payload.get("games", []):
        for team_key in ["away", "home"]:
            for starter in game.get(team_key, {}).get("starters", []):
                pid = starter.get("resolved_player_id")
                if pid is not None:
                    starter_ids.append(int(pid))
    recent_points_map = fetch_recent_points_avgs(sync_engine, starter_ids, n_games=10)
    player_fb_counts, team_fg_pct = build_jedibets_priors(jedibets_stats)

    output_rows = []
    for game in lineups_payload.get("games", []):
        team_rows = {"away": [], "home": []}
        team_scores = {"away": 0.0, "home": 0.0}

        for team_key, team_abbr_key in [("away", "away_team_abbr"), ("home", "home_team_abbr")]:
            team = game.get(team_key, {})
            team_status = (team.get("status") or "expected").lower()
            status_multiplier = 1.0 if team_status == "confirmed" else 0.9

            for starter in team.get("starters", []):
                pid = starter.get("resolved_player_id")
                if pid is None:
                    continue

                pred_points = points_map.get(int(pid))
                if pred_points is None:
                    pred_points = recent_points_map.get(int(pid), 5.0)

                play_pct = starter.get("play_pct") or 100
                pos = (starter.get("position") or "").upper()
                injury_mult = _injury_multiplier(starter.get("injury_tag"))
                pos_mult = 1.18 if pos == "C" else 1.0
                team_abbr = _canon_team_abbr(game.get(team_abbr_key))
                player_name_key = _norm_name(starter.get("resolved_full_name") or starter.get("name"))
                fb_count = player_fb_counts.get((player_name_key, team_abbr), 0)
                fg_team_rate = team_fg_pct.get(team_abbr, 0.5)
                # Blend priors from JediBets into baseline score.
                player_prior_mult = 1.0 + min(0.8, fb_count / 20.0)
                team_prior_mult = 0.8 + fg_team_rate

                base_score = max(0.1, (pred_points + 2.0))
                score = (
                    base_score
                    * (play_pct / 100.0)
                    * pos_mult
                    * status_multiplier
                    * injury_mult
                    * player_prior_mult
                    * team_prior_mult
                )
                if score <= 0:
                    continue

                row = {
                    "matchup": game.get("matchup"),
                    "tipoff_et": game.get("tipoff_et"),
                    "team_abbreviation": game.get(team_abbr_key),
                    "team_side": team_key,
                    "lineup_status": team.get("status"),
                    "player_id": int(pid),
                    "full_name": starter.get("resolved_full_name") or starter.get("name"),
                    "position": starter.get("position"),
                    "pred_points": round(float(pred_points), 2),
                    "lineup_play_pct": play_pct,
                    "jedibets_first_baskets": fb_count,
                    "jedibets_team_first_fg_pct": round(float(fg_team_rate), 4),
                    "raw_score": float(score),
                }
                team_rows[team_key].append(row)
                team_scores[team_key] += float(score)

        game_total = team_scores["away"] + team_scores["home"]
        if game_total <= 0:
            continue

        game_results = []
        for team_key in ["away", "home"]:
            team_total = team_scores[team_key]
            if team_total <= 0:
                continue
            team_first_score_prob = team_total / game_total
            for row in team_rows[team_key]:
                player_share = row["raw_score"] / team_total
                first_basket_prob = team_first_score_prob * player_share
                row["team_scores_first_prob"] = round(float(team_first_score_prob), 4)
                row["player_share_on_team"] = round(float(player_share), 4)
                row["first_basket_prob"] = round(float(first_basket_prob), 4)
                game_results.append(row)

        game_results.sort(key=lambda r: r["first_basket_prob"], reverse=True)
        output_rows.extend(game_results[:top_n_per_game])

    output_rows.sort(key=lambda r: (r["tipoff_et"] or "", -r["first_basket_prob"]))
    return output_rows


def attach_schedule_metadata(lineups_payload: dict):
    games = lineups_payload.get("games", [])
    if not games:
        return lineups_payload

    # Use team abbreviations to attach local schedule game_id/date.
    pair_keys = {
        f"{(g.get('away_team_abbr') or '').upper()}@{(g.get('home_team_abbr') or '').upper()}"
        for g in games
        if g.get("away_team_abbr") and g.get("home_team_abbr")
    }
    if not pair_keys:
        return lineups_payload

    with sync_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT game_id, game_date, matchup, home_team_id, away_team_id, home_team_abbr, away_team_abbr
                FROM game_schedule
                WHERE game_date >= (CURRENT_DATE - INTERVAL '7 day')
                  AND game_date <= (CURRENT_DATE + INTERVAL '7 day')
                ORDER BY game_date DESC
                """
            ),
            {},
        ).fetchall()

    by_pair = {}
    for r in rows:
        pair = f"{(r[6] or '').upper()}@{(r[5] or '').upper()}"
        if pair in pair_keys and pair not in by_pair:
            by_pair[pair] = r

    for game in lineups_payload.get("games", []):
        key = f"{(game.get('away_team_abbr') or '').upper()}@{(game.get('home_team_abbr') or '').upper()}"
        row = by_pair.get(key)
        if not row:
            continue
        game["game_id"] = row[0]
        game["game_date"] = row[1]
        if game.get("home_team_id") is None and row[3] is not None:
            game["home_team_id"] = int(row[3])
        if game.get("away_team_id") is None and row[4] is not None:
            game["away_team_id"] = int(row[4])
    return lineups_payload


def _tipoff_et_to_au_text(tipoff_et: str | None, game_date):
    if not tipoff_et or game_date is None or ZoneInfo is None:
        return None
    try:
        parsed = datetime.strptime(tipoff_et.replace(" ET", "").strip(), "%I:%M %p")
        d = pd.to_datetime(game_date).date()
        et_dt = datetime(
            d.year,
            d.month,
            d.day,
            parsed.hour,
            parsed.minute,
            tzinfo=ZoneInfo("America/New_York"),
        )
        au_dt = et_dt.astimezone(ZoneInfo("Australia/Sydney"))
        return au_dt.strftime("%b %d, %I:%M %p %Z")
    except Exception:
        return None


def _target_et_date_for_day(day: str):
    if ZoneInfo:
        base_date = datetime.now(ZoneInfo("America/New_York")).date()
    else:
        base_date = datetime.now().date()
    if day == "today":
        return base_date - timedelta(days=1)
    if day == "tomorrow":
        return base_date
    if day == "yesterday":
        return base_date - timedelta(days=2)
    return base_date


def _infer_team_starters(engine, team_abbr: str, target_date, top_n: int = 5):
    df = pd.read_sql(
        text(
            """
            SELECT p.id AS player_id, p.full_name, p.team_abbreviation,
                   pg.minutes, pg.game_date
            FROM players p
            JOIN player_game_stats pg ON pg.player_id = p.id
            WHERE p.team_abbreviation = :team_abbr
              AND pg.game_date < :target_date
              AND pg.game_date >= (:target_date - INTERVAL '40 day')
              AND pg.minutes IS NOT NULL
            ORDER BY pg.game_date DESC
            """
        ),
        engine,
        params={"team_abbr": team_abbr, "target_date": target_date},
    )
    if df.empty:
        return []
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0.0)
    df = df.sort_values(["player_id", "game_date"], ascending=[True, False])
    recent = df.groupby("player_id").head(5)
    rank = (
        recent.groupby(["player_id", "full_name", "team_abbreviation"], as_index=False)["minutes"]
        .mean()
        .sort_values("minutes", ascending=False)
        .head(top_n)
    )
    rows = []
    for _, r in rank.iterrows():
        rows.append(
            {
                "name": r["full_name"],
                "resolved_full_name": r["full_name"],
                "position": None,
                "injury_tag": None,
                "play_pct": 70,
                "resolved_player_id": int(r["player_id"]),
                "team_id": None,
                "name_match_type": "inferred",
                "name_match_score": 0.0,
            }
        )
    return rows


def build_inferred_lineups_from_schedule(engine, day: str):
    target_date = _target_et_date_for_day(day)
    df_schedule = pd.read_sql(
        text(
            """
            SELECT game_id, game_date, matchup, home_team_id, away_team_id,
                   home_team_abbr, away_team_abbr
            FROM game_schedule
            WHERE game_date = :target_date
            ORDER BY game_id
            """
        ),
        engine,
        params={"target_date": target_date},
    )
    if df_schedule.empty:
        return {
            "source": "schedule_inferred",
            "games_count": 0,
            "games": [],
        }

    games = []
    for _, game in df_schedule.iterrows():
        away_abbr = game["away_team_abbr"]
        home_abbr = game["home_team_abbr"]
        away_starters = _infer_team_starters(engine, away_abbr, target_date)
        home_starters = _infer_team_starters(engine, home_abbr, target_date)
        for s in away_starters:
            s["team_id"] = int(game["away_team_id"]) if pd.notnull(game["away_team_id"]) else None
        for s in home_starters:
            s["team_id"] = int(game["home_team_id"]) if pd.notnull(game["home_team_id"]) else None
        games.append(
            {
                "game_id": game["game_id"],
                "game_date": game["game_date"],
                "matchup": game["matchup"],
                "tipoff_et": None,
                "away_team_abbr": away_abbr,
                "home_team_abbr": home_abbr,
                "away_team_id": int(game["away_team_id"]) if pd.notnull(game["away_team_id"]) else None,
                "home_team_id": int(game["home_team_id"]) if pd.notnull(game["home_team_id"]) else None,
                "away": {
                    "status": "inferred",
                    "status_text": "Inferred from recent starters/minutes",
                    "team_id": int(game["away_team_id"]) if pd.notnull(game["away_team_id"]) else None,
                    "starters": away_starters,
                    "all_listed_players": away_starters,
                    "resolved_starters": len([s for s in away_starters if s.get("resolved_player_id")]),
                    "all_starters_resolved": len(away_starters) >= 5,
                },
                "home": {
                    "status": "inferred",
                    "status_text": "Inferred from recent starters/minutes",
                    "team_id": int(game["home_team_id"]) if pd.notnull(game["home_team_id"]) else None,
                    "starters": home_starters,
                    "all_listed_players": home_starters,
                    "resolved_starters": len([s for s in home_starters if s.get("resolved_player_id")]),
                    "all_starters_resolved": len(home_starters) >= 5,
                },
                "starter_count_ok": len(away_starters) >= 5 and len(home_starters) >= 5,
            }
        )

    total_starters = sum(len(g["away"]["starters"]) + len(g["home"]["starters"]) for g in games)
    resolved = sum(
        g["away"]["resolved_starters"] + g["home"]["resolved_starters"]
        for g in games
    )
    return {
        "source": "schedule_inferred",
        "games_count": len(games),
        "games": games,
        "resolution": {
            "resolved_starters": resolved,
            "total_starters": total_starters,
            "resolution_rate": round((resolved / total_starters), 4) if total_starters else 0.0,
        },
    }


@router.get("/predictions/first_basket")
async def predict_first_basket_api(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
    top_n_per_game: int = Query(6, ge=1, le=10),
):
    """
    Generate first-basket probabilities using projected starters and point projections.
    """
    rotowire_day = day if day in {"today", "tomorrow", "yesterday"} else None
    raw_lineups = await run_in_threadpool(rotowire_lineups_client.fetch_lineups, rotowire_day)
    lineups = await run_in_threadpool(lineup_resolver.enrich_rotowire_payload, raw_lineups)
    lineups = await run_in_threadpool(attach_schedule_metadata, lineups)
    lineups_source = "rotowire"
    if (lineups.get("games_count") or 0) == 0:
        inferred = await run_in_threadpool(build_inferred_lineups_from_schedule, sync_engine, day)
        if (inferred.get("games_count") or 0) > 0:
            lineups = inferred
            lineups_source = "schedule_inferred"
    jedibets_stats = None
    try:
        jedibets_stats = await run_in_threadpool(jedi_client.fetch_stats)
    except Exception:
        jedibets_stats = None

    expected_map, excluded_map = build_expected_lineup_sets(lineups)
    df_points = await run_in_threadpool(
        predict_points,
        sync_engine,
        day,
        expected_players_by_team=expected_map,
        excluded_players_by_team=excluded_map,
    )
    source = f"heuristic[{lineups_source}]"
    model_version = None
    try:
        model_result = await run_in_threadpool(
            predict_first_basket_with_models,
            sync_engine,
            lineups,
            df_points,
        )
        model_version = model_result.get("model_version")
        predictions = model_result.get("data", [])
        source = f"two_stage_ml[{lineups_source}]"
    except FileNotFoundError:
        predictions = await run_in_threadpool(
            build_first_basket_predictions,
            lineups,
            df_points,
            top_n_per_game,
            jedibets_stats,
        )
        if jedibets_stats:
            source = f"heuristic+jedibets[{lineups_source}]"

    grouped = {}
    for row in predictions:
        grouped.setdefault(row.get("matchup"), []).append(row)
    top_rows = []
    for matchup, rows in grouped.items():
        rows.sort(key=lambda x: x.get("first_basket_prob", 0), reverse=True)
        top_rows.extend(rows[:top_n_per_game])

    fallback_date = _target_et_date_for_day(day)
    for row in top_rows:
        base_date = row.get("game_date") or fallback_date
        row["tipoff_au"] = _tipoff_et_to_au_text(row.get("tipoff_et"), base_date)

    loggable_rows = [
        r
        for r in top_rows
        if r.get("game_id") and r.get("player_id") is not None
    ]
    if loggable_rows:
        await run_in_threadpool(
            upsert_first_basket_prediction_logs,
            sync_engine,
            loggable_rows,
            model_version,
        )

    return {
        "source": source,
        "games_count": lineups.get("games_count"),
        "lineup_resolution": lineups.get("resolution"),
        "day": day,
        "top_n_per_game": top_n_per_game,
        "model_version": model_version,
        "used_jedibets": bool(jedibets_stats),
        "data": top_rows,
    }
