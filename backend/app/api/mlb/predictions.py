import asyncio
import gc
import json
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

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
prediction_precompute_jobs: dict[str, dict] = {}
training_jobs: dict[str, dict] = {}


def _sync_engine():
    return create_engine(
        to_sync_db_url(settings.ML_DATABASE_URL),
        poolclass=NullPool,
        pool_pre_ping=True,
    )


def _count_sync(sql: str, params: dict) -> int:
    engine = _sync_engine()
    try:
        with engine.connect() as conn:
            return int(conn.execute(text(sql), params).scalar() or 0)
    finally:
        engine.dispose()


def _ensure_precompute_job_table(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS mlb_prediction_precompute_jobs (
                job_id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                days JSONB NOT NULL,
                limit_per_market INTEGER NOT NULL,
                refresh BOOLEAN NOT NULL,
                ensure_data BOOLEAN NOT NULL,
                steps_done INTEGER NOT NULL DEFAULT 0,
                steps_total INTEGER NOT NULL DEFAULT 0,
                current_day TEXT NULL,
                current_date_value TEXT NULL,
                result JSONB NULL,
                error TEXT NULL,
                created_at TIMESTAMPTZ NULL,
                started_at TIMESTAMPTZ NULL,
                finished_at TIMESTAMPTZ NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )


def _persist_precompute_job(job: dict) -> None:
    engine = _sync_engine()
    try:
        with engine.begin() as conn:
            _ensure_precompute_job_table(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO mlb_prediction_precompute_jobs (
                        job_id,
                        type,
                        status,
                        days,
                        limit_per_market,
                        refresh,
                        ensure_data,
                        steps_done,
                        steps_total,
                        current_day,
                        current_date_value,
                        result,
                        error,
                        created_at,
                        started_at,
                        finished_at,
                        updated_at
                    )
                    VALUES (
                        :job_id,
                        :type,
                        :status,
                        CAST(:days AS jsonb),
                        :limit_per_market,
                        :refresh,
                        :ensure_data,
                        :steps_done,
                        :steps_total,
                        :current_day,
                        :current_date_value,
                        CAST(:result AS jsonb),
                        :error,
                        :created_at,
                        :started_at,
                        :finished_at,
                        now()
                    )
                    ON CONFLICT (job_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        days = EXCLUDED.days,
                        limit_per_market = EXCLUDED.limit_per_market,
                        refresh = EXCLUDED.refresh,
                        ensure_data = EXCLUDED.ensure_data,
                        steps_done = EXCLUDED.steps_done,
                        steps_total = EXCLUDED.steps_total,
                        current_day = EXCLUDED.current_day,
                        current_date_value = EXCLUDED.current_date_value,
                        result = EXCLUDED.result,
                        error = EXCLUDED.error,
                        started_at = EXCLUDED.started_at,
                        finished_at = EXCLUDED.finished_at,
                        updated_at = now()
                    """
                ),
                {
                    "job_id": job.get("job_id"),
                    "type": job.get("type") or "mlb_prediction_precompute",
                    "status": job.get("status") or "queued",
                    "days": json.dumps(job.get("days") or []),
                    "limit_per_market": int(job.get("limit_per_market") or 0),
                    "refresh": bool(job.get("refresh")),
                    "ensure_data": bool(job.get("ensure_data")),
                    "steps_done": int(job.get("steps_done") or 0),
                    "steps_total": int(job.get("steps_total") or 0),
                    "current_day": job.get("current_day"),
                    "current_date_value": job.get("current_date"),
                    "result": json.dumps(job.get("result")) if job.get("result") is not None else None,
                    "error": job.get("error"),
                    "created_at": job.get("created_at"),
                    "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"),
                },
            )
    finally:
        engine.dispose()


def _load_precompute_job(job_id: str) -> dict | None:
    engine = _sync_engine()
    try:
        with engine.begin() as conn:
            _ensure_precompute_job_table(conn)
            row = conn.execute(
                text(
                    """
                    SELECT
                        job_id,
                        type,
                        status,
                        days,
                        limit_per_market,
                        refresh,
                        ensure_data,
                        steps_done,
                        steps_total,
                        current_day,
                        current_date_value,
                        result,
                        error,
                        created_at,
                        started_at,
                        finished_at,
                        updated_at
                    FROM mlb_prediction_precompute_jobs
                    WHERE job_id = :job_id
                    """
                ),
                {"job_id": job_id},
            ).mappings().first()
    finally:
        engine.dispose()

    if not row:
        return None

    job = dict(row)
    job["current_date"] = job.pop("current_date_value")
    for key in ("created_at", "started_at", "finished_at", "updated_at"):
        if job.get(key) is not None:
            job[key] = job[key].isoformat()
    return job


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
    compute_if_missing: bool = True,
) -> dict:
    engine = create_engine(database_url, poolclass=NullPool, pool_pre_ping=True)
    try:
        resolved_date = resolve_prediction_date(day=day, target_date=target_date)
        force_compute = cache_bust is not None

        stored_by_market = None
        if not force_compute:
            stored_by_market = load_mlb_prediction_slate_logs(engine, game_date=resolved_date, limit_per_market=None)
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
            if not compute_if_missing:
                return {
                    "sport": "mlb",
                    "status": "stored_partial",
                    "source": "stored_partial",
                    "day": day,
                    "date": resolved_date.isoformat(),
                    "cache_bust": cache_bust,
                    "needs_precompute": True,
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
    finally:
        gc.collect()
        engine.dispose()


@cached(ttl_seconds=300)
def _build_mlb_market_prediction_payload(
    *,
    market: str,
    database_url: str,
    day: str,
    target_date: str | None,
    limit: int,
    cache_bust: str | None = None,
    compute_if_missing: bool = True,
) -> dict:
    engine = create_engine(database_url, poolclass=NullPool, pool_pre_ping=True)
    try:
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
            if not compute_if_missing:
                return {
                    "sport": "mlb",
                    "status": "stored_partial",
                    "source": "stored_partial",
                    "market": market,
                    "day": day,
                    "date": resolved_date.isoformat(),
                    "cache_bust": cache_bust,
                    "needs_precompute": True,
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
    finally:
        gc.collect()
        engine.dispose()


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
    search_models: bool = Query(False, description="Train/evaluate every candidate model. False trains XGBoost only."),
):
    try:
        reports = await run_in_threadpool(
            train_all_mlb_markets,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            min_player_games=min_player_games,
            search_models=search_models,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    selected_models = {
        market: report.get("model_name")
        for market, report in reports.items()
    }
    return {
        "sport": "mlb",
        "status": "trained",
        "search_models": search_models,
        "selected_models": selected_models,
        "reports": reports,
    }


def _run_mlb_training_job(
    job_id: str,
    *,
    markets: list[str],
    min_player_games: int,
    search_models: bool,
) -> None:
    job = training_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.utcnow().isoformat()
    job["steps_total"] = len(markets)
    reports: dict[str, dict] = {}

    try:
        engine = _sync_engine()
        try:
            for step, market in enumerate(markets, start=1):
                job["current_market"] = market
                job["steps_done"] = step - 1
                report = train_market(
                    market,
                    engine=engine,
                    min_player_games=min_player_games,
                    search_models=search_models,
                )
                reports[market] = report
                job["selected_models"] = {
                    report_market: report_payload.get("model_name")
                    for report_market, report_payload in reports.items()
                }
                job["reports"] = reports
                job["steps_done"] = step
        finally:
            engine.dispose()

        job["status"] = "completed"
        job["finished_at"] = datetime.utcnow().isoformat()
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = datetime.utcnow().isoformat()


def _create_mlb_training_job(*, markets: list[str], min_player_games: int, search_models: bool) -> str:
    job_id = f"mlb-train-{uuid.uuid4()}"
    training_jobs[job_id] = {
        "job_id": job_id,
        "type": "mlb_training",
        "status": "queued",
        "markets": markets,
        "min_player_games": min_player_games,
        "search_models": search_models,
        "steps_done": 0,
        "steps_total": len(markets),
        "current_market": None,
        "selected_models": {},
        "reports": {},
        "error": None,
        "created_at": datetime.utcnow().isoformat(),
        "started_at": None,
        "finished_at": None,
    }
    return job_id


@router.post("/train/all/start")
async def start_train_all_mlb(
    min_player_games: int = Query(3, ge=0, le=100),
    search_models: bool = Query(False, description="Train/evaluate every candidate model. False trains XGBoost only."),
):
    markets = list(PLANNED_MARKETS)
    job_id = _create_mlb_training_job(
        markets=markets,
        min_player_games=min_player_games,
        search_models=search_models,
    )
    asyncio.create_task(
        run_in_threadpool(
            _run_mlb_training_job,
            job_id,
            markets=markets,
            min_player_games=min_player_games,
            search_models=search_models,
        )
    )
    return {
        "sport": "mlb",
        "status": "queued",
        "job_id": job_id,
        "markets": markets,
        "search_models": search_models,
    }


@router.post("/train/{market}/start")
async def start_train_mlb_market(
    market: str,
    min_player_games: int = Query(3, ge=0, le=100),
    search_models: bool = Query(False, description="Train/evaluate every candidate model. False trains XGBoost only."),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    job_id = _create_mlb_training_job(
        markets=[market],
        min_player_games=min_player_games,
        search_models=search_models,
    )
    asyncio.create_task(
        run_in_threadpool(
            _run_mlb_training_job,
            job_id,
            markets=[market],
            min_player_games=min_player_games,
            search_models=search_models,
        )
    )
    return {
        "sport": "mlb",
        "status": "queued",
        "job_id": job_id,
        "markets": [market],
        "search_models": search_models,
    }


@router.get("/train/jobs/{job_id}")
async def get_mlb_training_job(job_id: str):
    job = training_jobs.get(job_id)
    if not job:
        return {"status": "not_found", "job_id": job_id}
    return job


@router.post("/train/{market}")
async def train_mlb_market(
    market: str,
    min_player_games: int = Query(3, ge=0, le=100),
    search_models: bool = Query(False, description="Train/evaluate every candidate model. False trains XGBoost only."),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    engine = _sync_engine()
    try:
        report = await run_in_threadpool(
            train_market,
            market,
            engine=engine,
            min_player_games=min_player_games,
            search_models=search_models,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        engine.dispose()
    return {
        "sport": "mlb",
        "status": "trained",
        "market": market,
        "search_models": search_models,
        "selected_model": report.get("model_name"),
        "report": report,
    }


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
    day: str = Query("tomorrow", enum=["today", "tomorrow", "yesterday", "two_days_ago", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    limit_per_market: int = Query(60, ge=1, le=200),
    ensure_data: bool = Query(True, description="Auto-load missing schedule/roster rows before scoring."),
    refresh: bool = Query(False, description="Force-refresh schedule/rosters and bypass prediction cache."),
    compute_if_missing: bool = Query(False, description="Compute synchronously if stored prediction logs are missing."),
):
    target_date = resolve_prediction_date(day=day, target_date=date)
    ensure_result = {"changed": False}
    if ensure_data:
        try:
            ensure_result = await _ensure_mlb_slate_data(target_date=target_date, refresh=refresh)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"MLB slate data load failed: {exc}") from exc

    cache_bust = datetime.utcnow().isoformat() if refresh else None
    try:
        payload = await run_in_threadpool(
            _build_mlb_prediction_slate_payload,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            day=day,
            target_date=target_date.isoformat(),
            limit_per_market=limit_per_market,
            cache_bust=cache_bust,
            compute_if_missing=compute_if_missing,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {**payload, "data_load": ensure_result}


def _parse_precompute_days(days: str) -> list[str]:
    day_values = [value.strip() for value in days.split(",") if value.strip()]
    if not day_values:
        raise HTTPException(status_code=400, detail="days must include at least one value.")
    valid_days = {"today", "tomorrow", "yesterday", "two_days_ago", "auto"}
    invalid_days = []
    for day in day_values:
        if day in valid_days:
            continue
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except (TypeError, ValueError):
            invalid_days.append(day)
    if invalid_days:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day/date value(s): {', '.join(invalid_days)}",
        )
    return day_values


def _slate_market_counts(payload: dict) -> dict[str, int]:
    markets = payload.get("markets") or {}
    return {
        market: int((market_payload or {}).get("count") or 0)
        for market, market_payload in markets.items()
    }


async def _run_mlb_prediction_precompute_job(
    job_id: str,
    *,
    day_values: list[str],
    limit_per_market: int,
    refresh: bool,
    ensure_data: bool,
) -> None:
    job = prediction_precompute_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.utcnow().isoformat()
    job["steps_total"] = len(day_values)
    _persist_precompute_job(job)
    results: dict[str, dict] = {}

    try:
        for step, day in enumerate(day_values, start=1):
            target_date = resolve_prediction_date(day=day)
            job["current_day"] = day
            job["current_date"] = target_date.isoformat()
            job["steps_done"] = step - 1
            _persist_precompute_job(job)

            ensure_result = {"changed": False}
            if ensure_data:
                ensure_result = await _ensure_mlb_slate_data(
                    target_date=target_date,
                    refresh=refresh,
                )

            cache_bust = datetime.utcnow().isoformat() if refresh or ensure_result.get("changed") else None
            payload = await run_in_threadpool(
                _build_mlb_prediction_slate_payload,
                database_url=to_sync_db_url(settings.ML_DATABASE_URL),
                day=day,
                target_date=target_date.isoformat(),
                limit_per_market=limit_per_market,
                cache_bust=cache_bust,
                compute_if_missing=True,
            )
            results[day] = {
                "date": payload.get("date"),
                "source": payload.get("source"),
                "market_counts": _slate_market_counts(payload),
                "data_load": ensure_result,
            }
            job["steps_done"] = step
            _persist_precompute_job(job)

        job["status"] = "completed"
        job["result"] = results
        job["finished_at"] = datetime.utcnow().isoformat()
        _persist_precompute_job(job)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = datetime.utcnow().isoformat()
        _persist_precompute_job(job)


def _create_mlb_prediction_precompute_job(
    *,
    day_values: list[str],
    limit_per_market: int,
    refresh: bool,
    ensure_data: bool,
) -> str:
    job_id = f"mlb-pred-{uuid.uuid4()}"
    prediction_precompute_jobs[job_id] = {
        "job_id": job_id,
        "type": "mlb_prediction_precompute",
        "status": "queued",
        "days": day_values,
        "limit_per_market": limit_per_market,
        "refresh": refresh,
        "ensure_data": ensure_data,
        "steps_done": 0,
        "steps_total": len(day_values),
        "current_day": None,
        "current_date": None,
        "result": None,
        "error": None,
        "created_at": datetime.utcnow().isoformat(),
    }
    _persist_precompute_job(prediction_precompute_jobs[job_id])
    return job_id


@router.post("/precompute/start")
async def start_mlb_prediction_precompute(
    days: str = Query(
        "today,tomorrow",
        description="Comma-separated relative days or YYYY-MM-DD dates: today,tomorrow,yesterday,two_days_ago,auto,2026-06-01",
    ),
    limit_per_market: int = Query(200, ge=1, le=500),
    refresh: bool = Query(False, description="Recompute even if prediction logs already exist."),
    ensure_data: bool = Query(True, description="Load missing schedule/roster data before scoring."),
):
    day_values = _parse_precompute_days(days)
    job_id = _create_mlb_prediction_precompute_job(
        day_values=day_values,
        limit_per_market=limit_per_market,
        refresh=refresh,
        ensure_data=ensure_data,
    )
    asyncio.create_task(
        _run_mlb_prediction_precompute_job(
            job_id,
            day_values=day_values,
            limit_per_market=limit_per_market,
            refresh=refresh,
            ensure_data=ensure_data,
        )
    )
    return {"status": "queued", "job_id": job_id, "days": day_values}


@router.post("/precompute/run")
async def run_mlb_prediction_precompute(
    days: str = Query(
        "today,tomorrow",
        description="Comma-separated relative days or YYYY-MM-DD dates: today,tomorrow,yesterday,two_days_ago,auto,2026-06-01",
    ),
    limit_per_market: int = Query(200, ge=1, le=500),
    refresh: bool = Query(False, description="Recompute even if prediction logs already exist."),
    ensure_data: bool = Query(True, description="Load missing schedule/roster data before scoring."),
):
    day_values = _parse_precompute_days(days)
    job_id = _create_mlb_prediction_precompute_job(
        day_values=day_values,
        limit_per_market=limit_per_market,
        refresh=refresh,
        ensure_data=ensure_data,
    )
    await _run_mlb_prediction_precompute_job(
        job_id,
        day_values=day_values,
        limit_per_market=limit_per_market,
        refresh=refresh,
        ensure_data=ensure_data,
    )
    return prediction_precompute_jobs[job_id]


@router.get("/precompute/jobs/{job_id}")
async def get_mlb_prediction_precompute_job(job_id: str):
    job = prediction_precompute_jobs.get(job_id)
    if not job:
        job = await run_in_threadpool(_load_precompute_job, job_id)
    if not job:
        return {"status": "not_found", "job_id": job_id}
    return job


@router.get("/{market}")
async def get_mlb_predictions(
    market: str,
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "two_days_ago", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    limit: int = Query(100, ge=1, le=500),
    ensure_data: bool = Query(True, description="Auto-load missing schedule/roster rows before scoring."),
    refresh: bool = Query(False, description="Force-refresh schedule/rosters and bypass prediction cache."),
    compute_if_missing: bool = Query(False, description="Compute synchronously if stored prediction logs are missing."),
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

    cache_bust = datetime.utcnow().isoformat() if refresh else None
    try:
        payload = await run_in_threadpool(
            _build_mlb_market_prediction_payload,
            market=market,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            day=day,
            target_date=target_date.isoformat(),
            limit=limit,
            cache_bust=cache_bust,
            compute_if_missing=compute_if_missing,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {**payload, "data_load": ensure_result}
