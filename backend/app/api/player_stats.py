from fastapi import APIRouter, Query
from ..services.nba_client import NBAClient
from sqlalchemy import create_engine, text
from app.core.config import settings
from fastapi.concurrency import run_in_threadpool
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import math

# get teh path to the ml folder
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.predict import predict_points, predict_assists, predict_rebounds, predict_threept
from app.db.store_prediction_logs import log_predictions

router = APIRouter()
client = NBAClient(timeout=15)
sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))


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
    df_preds = await run_in_threadpool(predict_points, sync_engine, day)

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

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
    df_preds = await run_in_threadpool(predict_assists, sync_engine, day)

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

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
    df_preds = await run_in_threadpool(predict_rebounds, sync_engine, day)

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

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
    df_preds = await run_in_threadpool(predict_threept, sync_engine, day)

    if df_preds.empty:
        return {"message": f"No games found for {day}", "data": []}

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
