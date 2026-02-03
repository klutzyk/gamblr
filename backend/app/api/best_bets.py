import math
import re
from datetime import datetime, timedelta
from itertools import combinations
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db
from app.models.bookmaker import Bookmaker
from app.models.event import Event
from app.models.market import Market
from app.models.player_prop import PlayerProp
from ml.predict import predict_assists, predict_points, predict_rebounds

router = APIRouter()
sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))

MARKET_TO_STATS = {
    "player_points": ("points",),
    "player_assists": ("assists",),
    "player_rebounds": ("rebounds",),
    "player_points_rebounds_assists": ("points", "rebounds", "assists"),
    "player_points_rebounds": ("points", "rebounds"),
    "player_points_assists": ("points", "assists"),
    "player_rebounds_assists": ("rebounds", "assists"),
}


def _normalize_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", "", (name or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _build_prediction_index(day: str) -> dict[str, dict[str, dict]]:
    frames = {
        "points": predict_points(sync_engine, day),
        "assists": predict_assists(sync_engine, day),
        "rebounds": predict_rebounds(sync_engine, day),
    }
    index: dict[str, dict[str, dict]] = {"points": {}, "assists": {}, "rebounds": {}}

    for stat_type, df in frames.items():
        if df.empty:
            continue
        for row in df.to_dict(orient="records"):
            key = _normalize_name(str(row.get("full_name") or ""))
            if not key:
                continue
            index[stat_type][key] = row
    return index


def _compose_prediction_row(
    stat_components: tuple[str, ...], player_name: str, index: dict[str, dict[str, dict]]
) -> dict | None:
    rows = []
    for stat in stat_components:
        row = index.get(stat, {}).get(player_name)
        if not row:
            return None
        rows.append(row)

    def _sum_or_none(field: str):
        vals = [r.get(field) for r in rows]
        if any(v is None for v in vals):
            return None
        return float(sum(float(v) for v in vals))

    confidence_values = [
        float(r.get("confidence"))
        for r in rows
        if r.get("confidence") is not None
    ]
    confidence = min(confidence_values) if confidence_values else None

    return {
        "pred_value": _sum_or_none("pred_value"),
        "pred_p10": _sum_or_none("pred_p10"),
        "pred_p50": _sum_or_none("pred_p50"),
        "pred_p90": _sum_or_none("pred_p90"),
        "confidence": confidence,
        "team_abbreviation": rows[0].get("team_abbreviation"),
    }


def _model_probability(row: dict, line: float, side: str) -> tuple[float, float]:
    center = row.get("pred_p50")
    if center is None:
        center = row.get("pred_value")
    if center is None:
        raise ValueError("Missing prediction center")

    p10 = row.get("pred_p10")
    p90 = row.get("pred_p90")
    if isinstance(p10, (int, float)) and isinstance(p90, (int, float)) and p90 > p10:
        sigma = (float(p90) - float(p10)) / 2.563
    else:
        sigma = max(1.0, abs(float(center)) * 0.18)
    sigma = max(sigma, 0.35)

    z = (float(line) - float(center)) / sigma
    over_raw = 1.0 - _norm_cdf(z)
    under_raw = 1.0 - over_raw

    raw_prob = over_raw if side.lower() == "over" else under_raw

    conf = row.get("confidence")
    conf_scale = _clamp((float(conf) if conf is not None else 65.0) / 100.0, 0.4, 1.0)
    adj_prob = 0.5 + (raw_prob - 0.5) * conf_scale
    return _clamp(raw_prob, 0.01, 0.99), _clamp(adj_prob, 0.01, 0.99)


def _combo_product(values: list[float]) -> float:
    result = 1.0
    for value in values:
        result *= value
    return result


@router.get("/best")
async def get_best_bets(
    target_multiplier: float = 2.0,
    leg_count: int = 2,
    bookmaker: str = "sportsbet",
    day: str = "auto",
    include_combos: bool = False,
    min_confidence: int = 55,
    min_edge: float = 0.02,
    min_prob: float = 0.52,
    max_candidates: int = 30,
    db: AsyncSession = Depends(get_db),
):
    if leg_count < 1 or leg_count > 6:
        raise HTTPException(status_code=400, detail="leg_count must be between 1 and 6")
    if target_multiplier < 1.01:
        raise HTTPException(
            status_code=400, detail="target_multiplier must be greater than 1.0"
        )
    if day not in {"today", "tomorrow", "yesterday", "auto"}:
        raise HTTPException(
            status_code=400, detail="day must be one of today, tomorrow, yesterday, auto"
        )

    prediction_index = await run_in_threadpool(_build_prediction_index, day)

    now_utc = datetime.now(ZoneInfo("UTC")) - timedelta(hours=2)
    markets_to_use = [
        "player_points",
        "player_assists",
        "player_rebounds",
    ]
    if include_combos:
        markets_to_use.extend(
            [
                "player_points_rebounds_assists",
                "player_points_rebounds",
                "player_points_assists",
                "player_rebounds_assists",
            ]
        )

    stmt = (
        select(
            Event.id,
            Event.commence_time,
            Event.home_team,
            Event.away_team,
            Bookmaker.key,
            Market.key,
            PlayerProp.player_name,
            PlayerProp.side,
            PlayerProp.line,
            PlayerProp.price,
        )
        .join(Bookmaker, Bookmaker.event_id == Event.id)
        .join(Market, Market.bookmaker_id == Bookmaker.id)
        .join(PlayerProp, PlayerProp.market_id == Market.id)
        .where(
            Bookmaker.key == bookmaker,
            Market.key.in_(tuple(markets_to_use)),
            Event.commence_time >= now_utc,
        )
        .order_by(Event.commence_time.asc())
    )

    rows = (await db.execute(stmt)).all()
    if not rows:
        return {
            "status": "no_props",
            "message": "No stored props found for the selected bookmaker.",
            "bookmaker": bookmaker,
        }

    candidates = []
    skipped_no_prediction = 0
    skipped_low_confidence = 0
    skipped_low_edge = 0

    for row in rows:
        (
            event_id,
            commence_time,
            home_team,
            away_team,
            book_key,
            market_key,
            player_name,
            side,
            line,
            price,
        ) = row
        if line is None or price is None or price <= 1.0:
            continue
        stat_components = MARKET_TO_STATS.get(market_key)
        if not stat_components:
            continue

        normalized = _normalize_name(player_name or "")
        if len(stat_components) == 1:
            pred = prediction_index.get(stat_components[0], {}).get(normalized)
        else:
            pred = _compose_prediction_row(stat_components, normalized, prediction_index)
        if not pred:
            skipped_no_prediction += 1
            continue

        confidence = pred.get("confidence")
        if confidence is not None and float(confidence) < min_confidence:
            skipped_low_confidence += 1
            continue

        raw_prob, model_prob = _model_probability(pred, float(line), str(side))
        implied_prob = 1.0 / float(price)
        edge = model_prob - implied_prob
        if model_prob < min_prob or edge < min_edge:
            skipped_low_edge += 1
            continue

        expected_value = model_prob * (float(price) - 1.0) - (1.0 - model_prob)
        stat_type = "+".join(stat_components)
        candidates.append(
            {
                "event_id": event_id,
                "commence_time": commence_time.isoformat(),
                "matchup": f"{away_team} @ {home_team}",
                "bookmaker": book_key,
                "market": market_key,
                "stat_type": stat_type,
                "player_name": player_name,
                "side": side,
                "line": float(line),
                "price_decimal": float(price),
                "implied_prob": round(implied_prob, 4),
                "model_prob_raw": round(raw_prob, 4),
                "model_prob": round(model_prob, 4),
                "edge": round(edge, 4),
                "ev_per_unit": round(expected_value, 4),
                "prediction": {
                    "pred_value": pred.get("pred_value"),
                    "pred_p10": pred.get("pred_p10"),
                    "pred_p50": pred.get("pred_p50"),
                    "pred_p90": pred.get("pred_p90"),
                    "confidence": pred.get("confidence"),
                    "team_abbreviation": pred.get("team_abbreviation"),
                },
            }
        )

    if not candidates:
        return {
            "status": "no_candidates",
            "message": "No bets met confidence/edge thresholds. Try lower filters.",
            "bookmaker": bookmaker,
            "filters": {
                "target_multiplier": target_multiplier,
                "leg_count": leg_count,
                "min_confidence": min_confidence,
                "min_edge": min_edge,
                "min_prob": min_prob,
            },
            "debug": {
                "rows_loaded": len(rows),
                "skipped_no_prediction": skipped_no_prediction,
                "skipped_low_confidence": skipped_low_confidence,
                "skipped_low_edge": skipped_low_edge,
            },
        }

    ranked = sorted(
        candidates,
        key=lambda c: (c["edge"], c["model_prob"], c["ev_per_unit"]),
        reverse=True,
    )[: max(leg_count, max_candidates)]

    parlay_options = []
    if leg_count == 1:
        for leg in ranked[: min(8, len(ranked))]:
            payout = leg["price_decimal"]
            parlay_options.append(
                {
                    "legs": [leg],
                    "combined_odds": round(payout, 4),
                    "combined_probability": leg["model_prob"],
                    "expected_value_per_unit": leg["ev_per_unit"],
                    "meets_target": payout >= target_multiplier,
                }
            )
    else:
        pool = ranked[: min(len(ranked), 18)]
        for combo in combinations(pool, leg_count):
            combo_keys = {
                (leg["event_id"], leg["player_name"], leg["market"], leg["side"], leg["line"])
                for leg in combo
            }
            if len(combo_keys) != len(combo):
                continue

            combined_odds = _combo_product([leg["price_decimal"] for leg in combo])
            if combined_odds < target_multiplier:
                continue

            combined_prob = _combo_product([leg["model_prob"] for leg in combo])
            expected_value = combined_prob * (combined_odds - 1.0) - (1.0 - combined_prob)
            parlay_options.append(
                {
                    "legs": list(combo),
                    "combined_odds": round(combined_odds, 4),
                    "combined_probability": round(combined_prob, 4),
                    "expected_value_per_unit": round(expected_value, 4),
                    "meets_target": True,
                }
            )

    parlay_options = sorted(
        parlay_options,
        key=lambda p: (p["combined_probability"], p["expected_value_per_unit"]),
        reverse=True,
    )[:5]

    top_single_legs = sorted(
        candidates, key=lambda c: (c["model_prob"], c["edge"]), reverse=True
    )[:10]

    return {
        "status": "ok",
        "generated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "bookmaker": bookmaker,
        "target_multiplier": target_multiplier,
        "leg_count": leg_count,
        "day": day,
        "include_combos": include_combos,
        "filters": {
            "min_confidence": min_confidence,
            "min_edge": min_edge,
            "min_prob": min_prob,
        },
        "pool_size": len(candidates),
        "top_single_legs": top_single_legs,
        "recommended_parlays": parlay_options,
        "debug": {
            "rows_loaded": len(rows),
            "skipped_no_prediction": skipped_no_prediction,
            "skipped_low_confidence": skipped_low_confidence,
            "skipped_low_edge": skipped_low_edge,
        },
    }
