from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any
import math
from datetime import datetime
from itertools import combinations
from zoneinfo import ZoneInfo

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
from app.db.mlb.store_prediction_logs import load_mlb_prediction_logs, upsert_mlb_prediction_logs
from app.db.url_utils import to_sync_db_url
from app.services.propline_client import PropLineClient
from app.services.theodds_client import TheOddsClient

ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.mlb.betting_grade import add_hr_betting_grades  # noqa: E402
from ml.mlb.pregame import resolve_prediction_date, score_batter_home_run_pregame  # noqa: E402


router = APIRouter()

MLB_SPORT = "baseball_mlb"
HR_MARKET = "batter_home_runs"
PROVIDER = "propline"
THEODDS_PROVIDER = "theodds"
BEST_BET_MARKETS = (
    "batter_home_runs",
    "batter_hits",
    "batter_total_bases",
    "pitcher_strikeouts",
)
BEST_BET_MARKET_LABELS = {
    "batter_home_runs": "Home Run",
    "batter_hits": "Hit",
    "batter_total_bases": "Total Bases",
    "pitcher_strikeouts": "Pitcher Strikeouts",
}


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


def _extract_bet_side(outcome: dict[str, Any]) -> str | None:
    name = str(outcome.get("name") or outcome.get("label") or "").lower()
    side = str(outcome.get("side") or outcome.get("type") or "").lower()
    if name in {"over", "yes"} or side in {"over", "yes"}:
        return "Over"
    if name in {"under", "no"} or side in {"under", "no"}:
        return "Under"
    return None


def _event_date_et(value: str | None) -> Any:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(ZoneInfo("America/New_York")).date()


def _normalize_theodds_props(
    event_odds_payloads: list[dict[str, Any]],
    *,
    bookmaker: str,
    markets: tuple[str, ...],
) -> list[dict[str, Any]]:
    allowed_markets = set(markets)
    props: list[dict[str, Any]] = []
    for item in event_odds_payloads:
        event = item.get("event") or {}
        odds = item.get("odds")
        odds_data = odds.get("data") if isinstance(odds, dict) and isinstance(odds.get("data"), dict) else {}
        event_id = event.get("id") or odds_data.get("id")
        commence_time = event.get("commence_time") or event.get("commenceTime") or odds_data.get("commence_time")
        home_team = event.get("home_team") or event.get("homeTeam") or odds_data.get("home_team")
        away_team = event.get("away_team") or event.get("awayTeam") or odds_data.get("away_team")
        for book in _iter_bookmakers(odds):
            book_key = str(book.get("key") or book.get("title") or book.get("name") or "")
            if bookmaker and not _bookmaker_matches(book_key, bookmaker):
                continue
            for market in book.get("markets") or []:
                market_key = str(market.get("key") or market.get("market") or "")
                if market_key not in allowed_markets:
                    continue
                for outcome in market.get("outcomes") or []:
                    side = _extract_bet_side(outcome)
                    if side is None:
                        continue
                    price = outcome.get("price") or outcome.get("odds")
                    player_name = _extract_player_name(outcome)
                    if price is None or not player_name:
                        continue
                    try:
                        american_price = int(float(price))
                        implied_probability = _implied_probability_from_american(american_price)
                    except (TypeError, ValueError, ZeroDivisionError):
                        continue
                    props.append(
                        {
                            "event_id": event_id,
                            "commence_time": commence_time,
                            "home_team": home_team,
                            "away_team": away_team,
                            "bookmaker": bookmaker,
                            "market": market_key,
                            "side": side,
                            "player_name": player_name,
                            "normalized_player_name": _normalize_name(player_name),
                            "line": outcome.get("point") or (0.5 if market_key == HR_MARKET else None),
                            "american_odds": american_price,
                            "decimal_odds": _american_to_decimal(american_price),
                            "implied_probability": implied_probability,
                        }
                    )
    return props


async def _load_or_fetch_theodds_props(
    *,
    engine,
    target_date,
    bookmaker: str,
    markets: tuple[str, ...],
    max_events: int | None,
    max_age_minutes: int,
    refresh: bool = False,
    fetch_if_missing: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not refresh:
        stored_props: list[dict[str, Any]] = []
        market_meta: dict[str, Any] = {}
        empty_logs = 0
        for market in markets:
            props, meta = await run_in_threadpool(
                load_mlb_prop_odds,
                engine,
                provider=THEODDS_PROVIDER,
                market=market,
                bookmaker=bookmaker,
                game_date=target_date,
                max_age_minutes=max_age_minutes,
            )
            market_meta[market] = meta
            stored_props.extend(props)
            if props:
                continue
            fetch_log = await run_in_threadpool(
                load_fresh_mlb_prop_odds_fetch_log,
                engine,
                provider=THEODDS_PROVIDER,
                market=market,
                bookmaker=bookmaker,
                game_date=target_date,
                max_age_minutes=max_age_minutes,
            )
            if fetch_log:
                empty_logs += 1
        if stored_props or empty_logs == len(markets):
            return stored_props, {
                "source": "stored",
                "rows": len(stored_props),
                "markets": market_meta,
                "empty_market_logs": empty_logs,
                "max_age_minutes": max_age_minutes,
            }
        if not fetch_if_missing:
            return [], {
                "source": "stored_missing",
                "rows": 0,
                "markets": market_meta,
                "empty_market_logs": empty_logs,
                "max_age_minutes": max_age_minutes,
            }

    client = TheOddsClient()
    events = await client.get_events(MLB_SPORT)
    event_rows = [
        event
        for event in events
        if _event_date_et(event.get("commence_time") or event.get("commenceTime")) == target_date
    ]
    if max_events is not None:
        event_rows = event_rows[:max_events]

    market_param = ",".join(markets)
    payloads: list[dict[str, Any]] = []
    for event in event_rows:
        event_id = event.get("id")
        if not event_id:
            continue
        odds = await client.get_event_odds(
            MLB_SPORT,
            str(event_id),
            bookmakers=bookmaker,
            markets=market_param,
            odds_format="american",
        )
        payloads.append({"event": event, "odds": odds})

    props = _normalize_theodds_props(payloads, bookmaker=bookmaker, markets=markets)
    stored_count = 0
    for market in markets:
        market_props = [prop for prop in props if prop.get("market") == market]
        stored_count += await run_in_threadpool(
            upsert_mlb_prop_odds,
            engine,
            market_props,
            provider=THEODDS_PROVIDER,
            sport=MLB_SPORT,
            market=market,
            bookmaker=bookmaker,
            game_date=target_date,
        )
        await run_in_threadpool(
            record_mlb_prop_odds_fetch,
            engine,
            provider=THEODDS_PROVIDER,
            sport=MLB_SPORT,
            market=market,
            bookmaker=bookmaker,
            game_date=target_date,
            props_count=len(market_props),
            events_count=len(payloads),
        )
    return props, {
        "source": "fetched",
        "rows": len(props),
        "stored_count": stored_count,
        "events_count": len(payloads),
        "markets": list(markets),
        "usage": client.latest_usage(),
        "max_age_minutes": max_age_minutes,
    }


def _poisson_cdf(k: int, mu: float) -> float:
    if k < 0:
        return 0.0
    mu = max(float(mu), 0.001)
    term = math.exp(-mu)
    total = term
    for value in range(1, k + 1):
        term *= mu / value
        total += term
    return max(0.0, min(1.0, total))


def _side_probability_for_prediction(
    market: str,
    prediction: dict[str, Any],
    line: float | None,
    side: str,
) -> float | None:
    if market == HR_MARKET:
        probability = prediction.get("probability")
        if probability is None:
            return None
        hr_probability = float(probability)
        return 1.0 - hr_probability if side == "Under" else hr_probability
    projected = prediction.get("prediction")
    if projected is None or line is None:
        return None
    threshold = math.floor(float(line))
    under_probability = _poisson_cdf(threshold, max(float(projected), 0.001))
    return under_probability if side == "Under" else 1.0 - under_probability


def _grade_best_bet(row: dict[str, Any]) -> dict[str, Any]:
    ev = float(row.get("ev_per_dollar") or 0.0)
    edge = float(row.get("edge") or 0.0)
    probability = float(row.get("model_probability") or 0.0)
    score = 50.0
    score += max(-1.0, min(1.0, ev / 0.22)) * 34.0
    score += max(-1.0, min(1.0, edge / 0.07)) * 28.0
    score += max(-0.4, min(1.0, (probability - 0.50) / 0.25)) * 10.0
    if ev <= 0 or edge <= 0:
        score -= 16.0
    score = round(max(0.0, min(100.0, score)), 1)
    if score >= 90:
        grade = "A+"
    elif score >= 82:
        grade = "A"
    elif score >= 72:
        grade = "B"
    elif score >= 62:
        grade = "C"
    elif score >= 50:
        grade = "D"
    else:
        grade = "F"
    row["betting_score"] = score
    row["betting_grade"] = grade
    row["bet_recommendation"] = "Bet" if grade in {"A+", "A"} and ev > 0 and edge > 0 else "Lean" if grade == "B" and ev > 0 and edge > 0 else "Pass"
    row["kelly_fraction"] = round(max(0.0, min(1.0, (row["decimal_odds"] * probability - 1.0) / (row["decimal_odds"] - 1.0))), 4)
    return row


def _prediction_index_by_market(engine, *, target_date, markets: tuple[str, ...], limit: int) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, int]]:
    index: dict[str, dict[str, dict[str, Any]]] = {}
    counts: dict[str, int] = {}
    for market in markets:
        df = load_mlb_prediction_logs(engine, market=market, game_date=target_date, limit=limit)
        counts[market] = len(df)
        market_index: dict[str, dict[str, Any]] = {}
        for row in df.to_dict("records"):
            name = row.get("player_name")
            if name:
                market_index[_normalize_name(str(name))] = row
        index[market] = market_index
    return index, counts


def _join_best_bet_predictions(
    *,
    props: list[dict[str, Any]],
    prediction_index: dict[str, dict[str, dict[str, Any]]],
    min_edge: float,
    min_prob: float,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    candidates: list[dict[str, Any]] = []
    skipped = {"no_prediction": 0, "no_probability": 0, "low_edge": 0}
    seen: set[tuple[str, str, str, float | None]] = set()
    for prop in props:
        market = str(prop.get("market") or "")
        normalized = _normalize_name(str(prop.get("player_name") or ""))
        prediction = prediction_index.get(market, {}).get(normalized)
        if not prediction:
            skipped["no_prediction"] += 1
            continue
        line = prop.get("line")
        try:
            line_float = float(line) if line is not None else None
        except (TypeError, ValueError):
            line_float = None
        side = str(prop.get("side") or "Over")
        if side not in {"Over", "Under"}:
            continue
        model_prob = _side_probability_for_prediction(market, prediction, line_float, side)
        if model_prob is None:
            skipped["no_probability"] += 1
            continue
        implied = float(prop["implied_probability"])
        edge = float(model_prob) - implied
        ev = _ev_per_dollar(float(model_prob), int(prop["american_odds"]))
        if model_prob < min_prob or edge < min_edge:
            skipped["low_edge"] += 1
            continue
        key = (str(prop.get("event_id") or ""), market, normalized, line_float)
        if key in seen:
            continue
        seen.add(key)
        row = {
            **{k: _json_safe(v) for k, v in prop.items()},
            "game_pk": _json_safe(prediction.get("game_pk")),
            "game_date": str(prediction.get("game_date"))[:10],
            "player_id": _json_safe(prediction.get("player_id")),
            "player_name": prediction.get("player_name") or prop.get("player_name"),
            "team_abbreviation": _json_safe(prediction.get("team_abbreviation")),
            "opponent_team_abbreviation": _json_safe(prediction.get("opponent_team_abbreviation")),
            "market_label": BEST_BET_MARKET_LABELS.get(market, market),
            "matchup": f"{prop.get('away_team') or prediction.get('team_abbreviation') or '-'} @ {prop.get('home_team') or prediction.get('opponent_team_abbreviation') or '-'}",
            "prediction": _json_safe(prediction.get("prediction")),
            "probability": _json_safe(prediction.get("probability")),
            "model_probability": round(float(model_prob), 4),
            "edge": round(edge, 4),
            "ev_per_dollar": round(ev, 4),
            "probability_method": "direct_model_probability" if market == HR_MARKET else "poisson_count_approximation",
        }
        candidates.append(_grade_best_bet(row))
    candidates.sort(key=lambda item: (item["ev_per_dollar"], item["edge"], item["model_probability"]), reverse=True)
    return candidates[:limit], skipped


def _diversify_best_bet_candidates(candidates: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    market_order = list(BEST_BET_MARKETS)
    by_market: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        by_market.setdefault(str(candidate.get("market") or ""), []).append(candidate)

    diversified: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, float | None]] = set()
    while len(diversified) < limit:
        added = False
        for market in market_order:
            rows = by_market.get(market) or []
            while rows:
                row = rows.pop(0)
                key = (
                    str(row.get("event_id") or ""),
                    str(row.get("market") or ""),
                    str(row.get("player_name") or "").lower(),
                    row.get("line"),
                )
                if key in seen:
                    continue
                seen.add(key)
                diversified.append(row)
                added = True
                break
            if len(diversified) >= limit:
                break
        if not added:
            break
    return diversified


def _build_best_bet_parlays(candidates: list[dict[str, Any]], *, target_multiplier: float, leg_count: int) -> list[dict[str, Any]]:
    if leg_count < 2 or not candidates:
        return []
    parlays: list[dict[str, Any]] = []
    pool = _diversify_best_bet_candidates(candidates, limit=min(len(candidates), 24))
    for combo in combinations(pool, leg_count):
        if len({leg.get("event_id") for leg in combo}) < min(2, leg_count):
            continue
        if len({(leg.get("event_id"), leg.get("player_name")) for leg in combo}) != len(combo):
            continue
        if len({leg.get("market") for leg in combo}) < min(2, leg_count):
            continue
        combined_odds = 1.0
        combined_prob = 1.0
        for leg in combo:
            combined_odds *= float(leg["decimal_odds"])
            combined_prob *= float(leg["model_probability"])
        ev = combined_prob * (combined_odds - 1.0) - (1.0 - combined_prob)
        parlays.append(
            {
                "legs": list(combo),
                "leg_count": leg_count,
                "combined_odds": round(combined_odds, 4),
                "combined_probability": round(combined_prob, 4),
                "expected_value_per_unit": round(ev, 4),
                "meets_target": combined_odds >= target_multiplier,
            }
        )
    return sorted(
        parlays,
        key=lambda item: (
            item["meets_target"] is not True,
            abs(item["combined_odds"] - target_multiplier),
            -item["expected_value_per_unit"],
        ),
    )[:5]


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


async def _load_or_score_hr_predictions(
    *,
    engine,
    day: str,
    target_date,
    limit: int,
    refresh: bool,
) -> tuple[Any, dict[str, Any]]:
    if not refresh:
        stored = await run_in_threadpool(
            load_mlb_prediction_logs,
            engine,
            market=HR_MARKET,
            game_date=target_date,
            limit=limit,
        )
        if not stored.empty:
            return stored, {
                "source": "stored",
                "rows": len(stored),
                "model_path": stored["model_path"].dropna().iloc[0] if stored["model_path"].notna().any() else None,
            }

    scored = await run_in_threadpool(
        score_batter_home_run_pregame,
        engine=engine,
        day=day,
        target_date=target_date,
        limit=limit,
    )
    stored_count = await run_in_threadpool(
        upsert_mlb_prediction_logs,
        engine,
        HR_MARKET,
        scored,
        model_path=scored.attrs.get("artifact_path"),
        prediction_date=scored.attrs.get("prediction_date"),
    )
    return scored, {
        "source": "computed",
        "rows": len(scored),
        "stored_count": stored_count,
        "model_path": scored.attrs.get("artifact_path") if hasattr(scored, "attrs") else None,
    }


@router.get("/propline/events")
async def get_propline_mlb_events():
    try:
        return {"sport": "mlb", "provider": "propline", "events": await PropLineClient().get_events(MLB_SPORT)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/best-bets")
async def get_mlb_best_bets(
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
    date: str | None = Query(None, description="Optional YYYY-MM-DD override."),
    bookmaker: str = Query("fanduel"),
    markets: str = Query(",".join(BEST_BET_MARKETS)),
    max_events: int | None = Query(30, ge=1, le=30),
    max_age_minutes: int = Query(30, ge=1, le=240),
    refresh: bool = Query(False, description="Bypass stored odds and fetch fresh The Odds API data."),
    fetch_if_missing: bool = Query(True, description="Fetch fresh odds when no stored odds are available."),
    prediction_limit: int = Query(600, ge=10, le=1000),
    limit: int = Query(60, ge=1, le=200),
    min_edge: float = Query(0.02, ge=-0.5, le=0.5),
    min_prob: float = Query(0.35, ge=0.01, le=0.99),
    target_multiplier: float = Query(2.0, ge=1.01, le=100),
    leg_count: int = Query(2, ge=1, le=4),
):
    target_date = resolve_prediction_date(day=day, target_date=date)
    requested_markets = tuple(
        market.strip()
        for market in markets.split(",")
        if market.strip() in BEST_BET_MARKETS
    )
    if not requested_markets:
        raise HTTPException(status_code=400, detail="No supported MLB best bet markets requested.")

    try:
        engine = _sync_engine()
        prediction_index, prediction_counts = await run_in_threadpool(
            _prediction_index_by_market,
            engine,
            target_date=target_date,
            markets=requested_markets,
            limit=prediction_limit,
        )
        if sum(prediction_counts.values()) == 0:
            return {
                "sport": "mlb",
                "status": "no_predictions",
                "message": "No stored MLB predictions found for this slate. Run MLB precompute first.",
                "provider": THEODDS_PROVIDER,
                "bookmaker": bookmaker,
                "day": day,
                "date": target_date.isoformat(),
                "prediction_market_counts": prediction_counts,
            }

        props, odds_cache = await _load_or_fetch_theodds_props(
            engine=engine,
            target_date=target_date,
            bookmaker=bookmaker,
            markets=requested_markets,
            max_events=max_events,
            max_age_minutes=max_age_minutes,
            refresh=refresh,
            fetch_if_missing=fetch_if_missing,
        )
        if not props:
            return {
                "sport": "mlb",
                "status": "no_props",
                "message": "No MLB prop odds found for the selected bookmaker/date.",
                "provider": THEODDS_PROVIDER,
                "bookmaker": bookmaker,
                "day": day,
                "date": target_date.isoformat(),
                "prediction_market_counts": prediction_counts,
                "odds_cache": odds_cache,
            }

        candidates, skipped = await run_in_threadpool(
            _join_best_bet_predictions,
            props=props,
            prediction_index=prediction_index,
            min_edge=min_edge,
            min_prob=min_prob,
            limit=max(limit * 3, 120),
        )
        props_by_market: dict[str, int] = {}
        for prop in props:
            market = str(prop.get("market") or "")
            props_by_market[market] = props_by_market.get(market, 0) + 1
        candidates_by_market: dict[str, int] = {}
        for candidate in candidates:
            market = str(candidate.get("market") or "")
            candidates_by_market[market] = candidates_by_market.get(market, 0) + 1
        candidates = _diversify_best_bet_candidates(candidates, limit=limit)
        parlays = await run_in_threadpool(
            _build_best_bet_parlays,
            candidates,
            target_multiplier=target_multiplier,
            leg_count=leg_count,
        )
        return {
            "sport": "mlb",
            "status": "ok" if candidates else "no_candidates",
            "message": None if candidates else "No MLB bets met the selected edge/probability filters.",
            "provider": THEODDS_PROVIDER,
            "bookmaker": bookmaker,
            "day": day,
            "date": target_date.isoformat(),
            "markets": list(requested_markets),
            "target_multiplier": target_multiplier,
            "leg_count": leg_count,
            "filters": {
                "min_edge": min_edge,
                "min_prob": min_prob,
            },
            "prediction_market_counts": prediction_counts,
            "odds_cache": odds_cache,
            "props_count": len(props),
            "pool_size": len(candidates),
            "top_single_legs": candidates,
            "recommended_parlays": parlays,
            "debug": {
                "skipped": skipped,
                "sides_supported": ["Over", "Under"],
                "props_by_market": props_by_market,
                "candidates_by_market_before_diversity": candidates_by_market,
            },
        }
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

    add_hr_betting_grades(rows)
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

    add_hr_betting_grades(rows)
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
        scored, prediction_cache = await _load_or_score_hr_predictions(
            engine=engine,
            day=day,
            target_date=target_date,
            limit=prediction_limit,
            refresh=refresh,
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
            "prediction_cache": prediction_cache,
            "odds_cache": odds_cache,
            "model_path": prediction_cache.get("model_path"),
            "scored_players": len(scored),
            "props_count": len(props),
            "missing_model_feature_count": len(scored.attrs.get("missing_model_features", [])),
            "missing_model_features_sample": scored.attrs.get("missing_model_features", [])[:10],
            **joined,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
