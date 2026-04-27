from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any
import math

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

from app.core.config import settings
from app.db.mlb.store_prop_odds import (
    load_fresh_mlb_prop_odds_fetch_log,
    load_mlb_prop_odds,
    record_mlb_prop_odds_fetch,
    upsert_mlb_prop_odds,
)
from app.db.url_utils import to_sync_db_url
from app.services.propline_client import PropLineClient

ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.mlb.pregame import resolve_prediction_date, score_batter_home_run_pregame  # noqa: E402


router = APIRouter()

MLB_SPORT = "baseball_mlb"
HR_MARKET = "batter_home_runs"
PROVIDER = "propline"


class PlayerProbability(BaseModel):
    player_name: str
    model_probability: float = Field(..., ge=0, le=1)
    player_id: int | None = None


class HREvRequest(BaseModel):
    predictions: list[PlayerProbability]
    bookmaker: str = "fanduel"
    max_events: int | None = Field(None, ge=1, le=30)


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    text = value.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _sync_engine():
    return create_engine(to_sync_db_url(settings.ML_DATABASE_URL))


def _american_to_decimal(price: float | int) -> float:
    price = float(price)
    if price > 0:
        return 1.0 + price / 100.0
    if price < 0:
        return 1.0 + 100.0 / abs(price)
    raise ValueError("American odds cannot be zero.")


def _implied_probability_from_american(price: float | int) -> float:
    return 1.0 / _american_to_decimal(price)


def _ev_per_dollar(model_probability: float, american_price: float | int) -> float:
    decimal_price = _american_to_decimal(american_price)
    return model_probability * (decimal_price - 1.0) - (1.0 - model_probability)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, float) and not math.isfinite(value):
            return None
    except TypeError:
        return value
    return value


def _bookmaker_matches(book_key: str | None, requested: str) -> bool:
    if not book_key:
        return False
    return _normalize_name(book_key).replace(" ", "") == _normalize_name(requested).replace(" ", "")


def _extract_player_name(outcome: dict[str, Any]) -> str | None:
    description = outcome.get("description")
    if description:
        return str(description)
    player = outcome.get("player") or outcome.get("participant") or outcome.get("name")
    if isinstance(player, dict):
        return player.get("name") or player.get("full_name") or player.get("fullName")
    if player and str(player).lower() not in {"over", "under", "yes", "no"}:
        return str(player)
    return None


def _is_hr_over_outcome(outcome: dict[str, Any], *, market_key: str | None = None) -> bool:
    if market_key == HR_MARKET:
        return True
    name = str(outcome.get("name") or outcome.get("label") or "").lower()
    side = str(outcome.get("side") or outcome.get("type") or "").lower()
    point = outcome.get("point")
    return (
        name in {"over", "yes"}
        or side in {"over", "yes"}
        or point in (0.5, "0.5")
    )


def _iter_bookmakers(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("bookmakers"), list):
            return payload["bookmakers"]
        if isinstance(payload.get("data"), dict):
            return _iter_bookmakers(payload["data"])
    return []


def _normalize_hr_props(event_odds_payloads: list[dict[str, Any]], *, bookmaker: str) -> list[dict[str, Any]]:
    props: list[dict[str, Any]] = []
    for item in event_odds_payloads:
        event = item.get("event") or {}
        odds = item.get("odds")
        for book in _iter_bookmakers(odds):
            book_key = str(book.get("key") or book.get("title") or book.get("name") or "")
            if bookmaker and not _bookmaker_matches(book_key, bookmaker):
                continue
            for market in book.get("markets") or []:
                market_key = str(market.get("key") or market.get("market") or "")
                if market_key and market_key != HR_MARKET:
                    continue
                for outcome in market.get("outcomes") or []:
                    price = outcome.get("price") or outcome.get("odds")
                    player_name = _extract_player_name(outcome)
                    if price is None or not player_name or not _is_hr_over_outcome(outcome, market_key=market_key):
                        continue
                    try:
                        american_price = int(float(price))
                        implied_probability = _implied_probability_from_american(american_price)
                    except (TypeError, ValueError, ZeroDivisionError):
                        continue
                    props.append(
                        {
                            "event_id": event.get("id"),
                            "commence_time": event.get("commence_time") or event.get("commenceTime"),
                            "home_team": event.get("home_team") or event.get("homeTeam"),
                            "away_team": event.get("away_team") or event.get("awayTeam"),
                            "bookmaker": bookmaker,
                            "market": HR_MARKET,
                            "player_name": player_name,
                            "normalized_player_name": _normalize_name(player_name),
                            "line": outcome.get("point") or 0.5,
                            "american_odds": american_price,
                            "decimal_odds": _american_to_decimal(american_price),
                            "implied_probability": implied_probability,
                        }
                    )
    return props


async def _load_or_fetch_hr_props(
    *,
    engine,
    target_date,
    bookmaker: str,
    max_events: int | None,
    max_age_minutes: int,
    refresh: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not refresh:
        props, meta = await run_in_threadpool(
            load_mlb_prop_odds,
            engine,
            provider=PROVIDER,
            market=HR_MARKET,
            bookmaker=bookmaker,
            game_date=target_date,
            max_age_minutes=max_age_minutes,
        )
        if props:
            return props, {"source": "stored", **meta}
        fetch_log = await run_in_threadpool(
            load_fresh_mlb_prop_odds_fetch_log,
            engine,
            provider=PROVIDER,
            market=HR_MARKET,
            bookmaker=bookmaker,
            game_date=target_date,
            max_age_minutes=max_age_minutes,
        )
        if fetch_log:
            return [], {
                "source": "stored_empty",
                "rows": 0,
                "latest_fetched_at": fetch_log.get("fetched_at"),
                "props_count": fetch_log.get("props_count"),
                "events_count": fetch_log.get("events_count"),
                "max_age_minutes": max_age_minutes,
            }

    payloads = await PropLineClient().get_market_odds_for_events(
        sport=MLB_SPORT,
        markets=HR_MARKET,
        bookmakers=bookmaker,
        odds_format="american",
        event_date=target_date,
        max_events=max_events,
    )
    props = _normalize_hr_props(payloads, bookmaker=bookmaker)
    stored_count = await run_in_threadpool(
        upsert_mlb_prop_odds,
        engine,
        props,
        provider=PROVIDER,
        sport=MLB_SPORT,
        market=HR_MARKET,
        bookmaker=bookmaker,
        game_date=target_date,
    )
    await run_in_threadpool(
        record_mlb_prop_odds_fetch,
        engine,
        provider=PROVIDER,
        sport=MLB_SPORT,
        market=HR_MARKET,
        bookmaker=bookmaker,
        game_date=target_date,
        props_count=len(props),
        events_count=len(payloads),
    )
    return props, {
        "source": "fetched",
        "rows": len(props),
        "stored_count": stored_count,
        "events_count": len(payloads),
        "max_age_minutes": max_age_minutes,
    }


@router.get("/propline/events")
async def get_propline_mlb_events():
    try:
        return {"sport": "mlb", "provider": "propline", "events": await PropLineClient().get_events(MLB_SPORT)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/propline/hr-props")
async def get_propline_hr_props(
    bookmaker: str = Query("fanduel"),
    date: str | None = Query(None, description="Optional YYYY-MM-DD event date filter."),
    max_events: int | None = Query(None, ge=1, le=30),
    max_age_minutes: int = Query(30, ge=1, le=240),
    refresh: bool = Query(False, description="Bypass stored odds and fetch fresh PropLine data."),
):
    try:
        target_date = resolve_prediction_date(day="today", target_date=date)
        props, odds_cache = await _load_or_fetch_hr_props(
            engine=_sync_engine(),
            target_date=target_date,
            bookmaker=bookmaker,
            max_events=max_events,
            max_age_minutes=max_age_minutes,
            refresh=refresh,
        )
        return {
            "sport": "mlb",
            "provider": PROVIDER,
            "bookmaker": bookmaker,
            "market": HR_MARKET,
            "date": target_date.isoformat(),
            "odds_cache": odds_cache,
            "count": len(props),
            "props": props,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/propline/hr-ev")
async def get_propline_hr_ev(request: HREvRequest):
    try:
        payloads = await PropLineClient().get_market_odds_for_events(
            sport=MLB_SPORT,
            markets=HR_MARKET,
            bookmakers=request.bookmaker,
            odds_format="american",
            max_events=request.max_events,
        )
        props = _normalize_hr_props(payloads, bookmaker=request.bookmaker)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    props_by_name: dict[str, list[dict[str, Any]]] = {}
    for prop in props:
        props_by_name.setdefault(prop["normalized_player_name"], []).append(prop)

    rows = []
    unmatched = []
    for prediction in request.predictions:
        normalized = _normalize_name(prediction.player_name)
        candidates = props_by_name.get(normalized) or []
        if not candidates:
            unmatched.append(prediction.model_dump())
            continue
        best = max(candidates, key=lambda item: item["american_odds"])
        edge = prediction.model_probability - best["implied_probability"]
        ev = _ev_per_dollar(prediction.model_probability, best["american_odds"])
        rows.append(
            {
                **best,
                "player_id": prediction.player_id,
                "model_probability": prediction.model_probability,
                "edge": edge,
                "ev_per_dollar": ev,
            }
        )

    rows.sort(key=lambda item: (item["ev_per_dollar"], item["edge"], item["model_probability"]), reverse=True)
    return {
        "sport": "mlb",
        "provider": "propline",
        "bookmaker": request.bookmaker,
        "market": HR_MARKET,
        "matched": len(rows),
        "unmatched": unmatched,
        "positive_ev": [row for row in rows if row["ev_per_dollar"] > 0 and row["edge"] > 0],
        "all": rows,
    }


def _join_predictions_to_props(scored, props: list[dict[str, Any]], *, limit: int) -> dict[str, Any]:
    props_by_name: dict[str, list[dict[str, Any]]] = {}
    for prop in props:
        props_by_name.setdefault(prop["normalized_player_name"], []).append(prop)

    rows = []
    unmatched = []
    for prediction in scored.to_dict("records"):
        player_name = prediction.get("player_name")
        probability = prediction.get("probability")
        if not player_name or probability is None:
            continue
        normalized = _normalize_name(str(player_name))
        candidates = props_by_name.get(normalized) or []
        if not candidates:
            unmatched.append(
                {
                    "player_id": prediction.get("player_id"),
                    "player_name": player_name,
                    "model_probability": probability,
                }
            )
            continue
        best = max(candidates, key=lambda item: item["american_odds"])
        edge = float(probability) - best["implied_probability"]
        ev = _ev_per_dollar(float(probability), best["american_odds"])
        rows.append(
            {
                **{key: _json_safe(value) for key, value in best.items()},
                "game_pk": _json_safe(prediction.get("game_pk")),
                "game_date": str(prediction.get("game_date"))[:10],
                "player_id": _json_safe(prediction.get("player_id")),
                "player_name": player_name,
                "team_id": _json_safe(prediction.get("team_id")),
                "team_abbreviation": _json_safe(prediction.get("team_abbreviation")),
                "opponent_team_id": _json_safe(prediction.get("opponent_team_id")),
                "opponent_team_abbreviation": _json_safe(prediction.get("opponent_team_abbreviation")),
                "batting_order": _json_safe(prediction.get("batting_order")),
                "has_posted_lineup": _json_safe(prediction.get("has_posted_lineup")),
                "starter_pitcher_id": _json_safe(prediction.get("starter_pitcher_id")),
                "model_probability": float(probability),
                "edge": edge,
                "ev_per_dollar": ev,
            }
        )

    rows.sort(key=lambda item: (item["ev_per_dollar"], item["edge"], item["model_probability"]), reverse=True)
    return {
        "matched": len(rows),
        "unmatched_count": len(unmatched),
        "unmatched_sample": unmatched[:10],
        "positive_ev": [row for row in rows if row["ev_per_dollar"] > 0 and row["edge"] > 0][:limit],
        "all": rows[:limit],
    }


@router.get("/propline/hr-ev-board")
async def get_propline_hr_ev_board(
    day: str = Query("tomorrow", enum=["today", "tomorrow", "yesterday", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    bookmaker: str = Query("fanduel"),
    max_events: int | None = Query(30, ge=1, le=30),
    max_age_minutes: int = Query(30, ge=1, le=240),
    refresh: bool = Query(False, description="Bypass stored odds and fetch fresh PropLine data."),
    prediction_limit: int = Query(300, ge=10, le=600),
    limit: int = Query(50, ge=1, le=200),
):
    target_date = resolve_prediction_date(day=day, target_date=date)
    try:
        engine = _sync_engine()
        scored = await run_in_threadpool(
            score_batter_home_run_pregame,
            engine=engine,
            day=day,
            target_date=target_date,
            limit=prediction_limit,
        )
        props, odds_cache = await _load_or_fetch_hr_props(
            engine=engine,
            target_date=target_date,
            bookmaker=bookmaker,
            max_events=max_events,
            max_age_minutes=max_age_minutes,
            refresh=refresh,
        )
        joined = _join_predictions_to_props(scored, props, limit=limit)
        return {
            "sport": "mlb",
            "status": "scored",
            "provider": PROVIDER,
            "bookmaker": bookmaker,
            "market": HR_MARKET,
            "day": day,
            "date": target_date.isoformat(),
            "odds_cache": odds_cache,
            "model_path": scored.attrs.get("artifact_path") if hasattr(scored, "attrs") else None,
            "scored_players": len(scored),
            "props_count": len(props),
            "missing_model_feature_count": len(scored.attrs.get("missing_model_features", [])),
            "missing_model_features_sample": scored.attrs.get("missing_model_features", [])[:10],
            **joined,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
