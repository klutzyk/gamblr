import sys
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, text

from app.core.config import settings
from app.db.mlb.store_prediction_logs import (
    load_mlb_prediction_logs,
    load_mlb_prediction_slate_logs,
    upsert_mlb_prediction_logs,
)
from app.db.mlb.session import MlbAsyncSessionLocal
from app.db.mlb.store_ingestion import ingest_active_rosters, ingest_schedule
from app.db.url_utils import to_sync_db_url
from app.services.cache import cached


ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.mlb.artifacts import (  # noqa: E402
    all_market_statuses,
    latest_market_report,
    list_market_reports,
    market_names,
    market_status,
)
from ml.mlb.evaluate import score_completed_games  # noqa: E402
from ml.mlb.pregame import (  # noqa: E402
    resolve_prediction_date,
    score_market_pregame,
    score_pregame_slate,
    scored_rows_for_api,
)
from ml.mlb.training import train_all as train_all_mlb_markets  # noqa: E402
from ml.mlb.training import train_market  # noqa: E402

router = APIRouter()

PLANNED_MARKETS = market_names()


def _sync_engine():
    return create_engine(to_sync_db_url(settings.ML_DATABASE_URL))


def _count_sync(sql: str, params: dict) -> int:
    engine = create_engine(to_sync_db_url(settings.ML_DATABASE_URL))
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar() or 0)


async def _ensure_mlb_slate_data(
    *,
    target_date,
    refresh: bool = False,
) -> dict[str, object]:
    season = target_date.year
    games_count = _count_sync(
        "select count(*) from mlb_games where official_date = :target_date",
        {"target_date": target_date},
    )

    rosters_count = _count_sync(
        """
        select count(*)
        from mlb_roster_snapshots
        where roster_type = 'active'
          and roster_date <= :target_date
          and roster_date >= :min_roster_date
        """,
        {
            "target_date": target_date,
            "min_roster_date": target_date - timedelta(days=7),
        },
    )
    loaded = {"schedule": False, "rosters": False}

    if refresh or games_count == 0 or rosters_count == 0:
        async with MlbAsyncSessionLocal() as db:
            if refresh or games_count == 0:
                await ingest_schedule(
                    db,
                    season=season,
                    start_date=target_date.isoformat(),
                    end_date=target_date.isoformat(),
                    load_teams_first=games_count == 0,
                )
                loaded["schedule"] = True

            if refresh or rosters_count == 0:
                await ingest_active_rosters(
                    db,
                    season=season,
                    roster_date=target_date,
                    roster_type="active",
                )
                loaded["rosters"] = True

    return {
        "target_date": target_date.isoformat(),
        "games_before": games_count,
        "rosters_before": rosters_count,
        "loaded": loaded,
        "changed": loaded["schedule"] or loaded["rosters"],
    }


def _payload_for_scored_market(market: str, scored, *, limit: int) -> dict:
    return {
        "market": market,
        "count": len(scored),
        "model_status": market_status(market),
        "missing_model_feature_count": len(scored.attrs.get("missing_model_features", [])),
        "missing_model_features_sample": scored.attrs.get("missing_model_features", [])[:10],
        "data": scored_rows_for_api(scored, limit=limit),
    }


def _stored_market_payload(market: str, stored, *, limit: int) -> dict:
    return {
        "market": market,
        "count": len(stored),
        "model_status": market_status(market),
        "missing_model_feature_count": 0,
        "missing_model_features_sample": [],
        "data": scored_rows_for_api(stored, limit=limit),
    }


def _stored_market_is_complete(market: str, stored, *, requested_limit: int) -> bool:
    if stored.empty:
        return False
    if market in {"batter_home_runs", "batter_hits", "batter_total_bases"}:
        return len(stored) >= min(requested_limit, 30)
    return True


@cached(ttl_seconds=300)
def _build_mlb_prediction_slate_payload(
    *,
    database_url: str,
    day: str,
    target_date: str | None,
    limit_per_market: int,
    cache_bust: str | None = None,
) -> dict:
    engine = create_engine(database_url)
    resolved_date = resolve_prediction_date(day=day, target_date=target_date)
    force_compute = cache_bust is not None

    if not force_compute:
        stored_by_market = load_mlb_prediction_slate_logs(
            engine,
            game_date=resolved_date,
            limit_per_market=None,
        )
        if all(
            _stored_market_is_complete(market, stored, requested_limit=limit_per_market)
            for market, stored in stored_by_market.items()
        ):
            return {
                "sport": "mlb",
                "status": "scored",
                "source": "stored",
                "day": day,
                "date": resolved_date.isoformat(),
                "cache_bust": cache_bust,
                "markets": {
                    market: _stored_market_payload(market, stored, limit=limit_per_market)
                    for market, stored in stored_by_market.items()
                },
            }

    scored_by_market = score_pregame_slate(
        database_url=database_url,
        day=day,
        target_date=target_date,
        limit_per_market=None,
    )
    markets = {}
    prediction_date = target_date
    for market, scored in scored_by_market.items():
        prediction_date = scored.attrs.get("prediction_date") or prediction_date
        upsert_mlb_prediction_logs(
            engine,
            market,
            scored,
            model_path=scored.attrs.get("artifact_path"),
            prediction_date=prediction_date,
        )
        markets[market] = _payload_for_scored_market(
            market,
            scored,
            limit=limit_per_market,
        )
    return {
        "sport": "mlb",
        "status": "scored",
        "source": "computed",
        "day": day,
        "date": prediction_date,
        "cache_bust": cache_bust,
        "markets": markets,
    }


@cached(ttl_seconds=300)
def _build_mlb_market_prediction_payload(
    *,
    market: str,
    database_url: str,
    day: str,
    target_date: str | None,
    limit: int,
    cache_bust: str | None = None,
) -> dict:
    engine = create_engine(database_url)
    resolved_date = resolve_prediction_date(day=day, target_date=target_date)
    if cache_bust is None:
        stored = load_mlb_prediction_logs(
            engine,
            market=market,
            game_date=resolved_date,
            limit=None,
        )
        if _stored_market_is_complete(market, stored, requested_limit=limit):
            return {
                "sport": "mlb",
                "status": "scored",
                "source": "stored",
                "market": market,
                "day": day,
                "date": resolved_date.isoformat(),
                "cache_bust": cache_bust,
                **_stored_market_payload(market, stored, limit=limit),
            }

    scored = score_market_pregame(
        market,
        database_url=database_url,
        day=day,
        target_date=target_date,
        limit=None,
    )
    upsert_mlb_prediction_logs(
        engine,
        market,
        scored,
        model_path=scored.attrs.get("artifact_path"),
        prediction_date=scored.attrs.get("prediction_date"),
    )
    return {
        "sport": "mlb",
        "status": "scored",
        "source": "computed",
        "market": market,
        "day": day,
        "date": scored.attrs.get("prediction_date"),
        "cache_bust": cache_bust,
        **_payload_for_scored_market(market, scored, limit=limit),
    }


@router.get("/markets")
def get_mlb_markets():
    return {
        "sport": "mlb",
        "status": "training_ready",
        "markets": all_market_statuses(),
    }


@router.get("/models/{market}/latest")
def get_latest_mlb_model(market: str):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    return {
        "sport": "mlb",
        "status": market_status(market),
    }


@router.get("/reports/{market}")
def get_mlb_model_reports(
    market: str,
    limit: int = Query(5, ge=1, le=50),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    return {
        "sport": "mlb",
        "market": market,
        "reports": list_market_reports(market, limit=limit),
    }


@router.post("/train/all")
async def train_all_mlb(
    min_player_games: int = Query(3, ge=0, le=100),
):
    try:
        reports = await run_in_threadpool(
            train_all_mlb_markets,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            min_player_games=min_player_games,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "trained", "reports": reports}


@router.post("/train/{market}")
async def train_mlb_market(
    market: str,
    min_player_games: int = Query(3, ge=0, le=100),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    try:
        report = await run_in_threadpool(
            train_market,
            market,
            engine=_sync_engine(),
            min_player_games=min_player_games,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "trained", "market": market, "report": report}


@router.get("/evaluate/{market}")
async def evaluate_mlb_market(
    market: str,
    since: str | None = Query(None, description="Inclusive YYYY-MM-DD lower bound."),
    until: str | None = Query(None, description="Inclusive YYYY-MM-DD upper bound."),
    last_days: int | None = Query(30, ge=1, le=365),
    limit: int = Query(25, ge=1, le=200),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    try:
        result = await run_in_threadpool(
            score_completed_games,
            market,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            since=since,
            until=until,
            last_days=last_days,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "evaluated", **result}


@router.get("/slate")
async def get_mlb_prediction_slate(
    day: str = Query("tomorrow", enum=["today", "tomorrow", "yesterday", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    limit_per_market: int = Query(60, ge=1, le=200),
    ensure_data: bool = Query(True, description="Auto-load missing schedule/roster rows before scoring."),
    refresh: bool = Query(False, description="Force-refresh schedule/rosters and bypass prediction cache."),
):
    target_date = resolve_prediction_date(day=day, target_date=date)
    ensure_result = {"changed": False}
    if ensure_data:
        try:
            ensure_result = await _ensure_mlb_slate_data(target_date=target_date, refresh=refresh)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"MLB slate data load failed: {exc}") from exc

    cache_bust = datetime.utcnow().isoformat() if refresh or ensure_result.get("changed") else None
    try:
        payload = await run_in_threadpool(
            _build_mlb_prediction_slate_payload,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            day=day,
            target_date=target_date.isoformat(),
            limit_per_market=limit_per_market,
            cache_bust=cache_bust,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {**payload, "data_load": ensure_result}


@router.get("/{market}")
async def get_mlb_predictions(
    market: str,
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    limit: int = Query(100, ge=1, le=500),
    ensure_data: bool = Query(True, description="Auto-load missing schedule/roster rows before scoring."),
    refresh: bool = Query(False, description="Force-refresh schedule/rosters and bypass prediction cache."),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")

    target_date = resolve_prediction_date(day=day, target_date=date)
    ensure_result = {"changed": False}
    if ensure_data:
        try:
            ensure_result = await _ensure_mlb_slate_data(target_date=target_date, refresh=refresh)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"MLB slate data load failed: {exc}") from exc

    cache_bust = datetime.utcnow().isoformat() if refresh or ensure_result.get("changed") else None
    try:
        payload = await run_in_threadpool(
            _build_mlb_market_prediction_payload,
            market=market,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            day=day,
            target_date=target_date.isoformat(),
            limit=limit,
            cache_bust=cache_bust,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {**payload, "data_load": ensure_result}
