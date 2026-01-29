from fastapi import APIRouter, Query
from ..services.nba_client import NBAClient
from sqlalchemy import create_engine, text
from app.core.config import settings
from fastapi.concurrency import run_in_threadpool
import sys
from pathlib import Path

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
    return df.to_dict(orient="records")


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
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )

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
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )

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
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )

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
    df_preds["under_risk"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("under_rate")
    )
    df_preds["under_risk_n"] = df_preds["player_id"].map(
        lambda pid: under_risk.get(int(pid), {}).get("sample_size")
    )

    df_preds = df_preds.sort_values("pred_value", ascending=False)

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        "threept",
        df_preds["model_version"].iloc[0] if "model_version" in df_preds else None,
    )

    return df_to_dict(df_preds)
