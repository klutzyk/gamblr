import math
import re
from datetime import datetime, timedelta
from itertools import combinations
from zoneinfo import ZoneInfo

import numpy as np
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db
from app.models.bookmaker import Bookmaker
from app.models.event import Event
from app.models.market import Market
from app.models.player_prop import PlayerProp
from ml.predict import predict_assists, predict_points, predict_rebounds
from ml.under_side_model import load_latest_under_side_model, predict_under_probability

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


def _collect_prediction_player_ids(index: dict[str, dict[str, dict]]) -> list[int]:
    player_ids: set[int] = set()
    for stat_rows in index.values():
        for row in stat_rows.values():
            pid = row.get("player_id")
            if pid is None:
                continue
            try:
                player_ids.add(int(pid))
            except (TypeError, ValueError):
                continue
    return sorted(player_ids)


def _fetch_under_risk_index(engine, player_ids: list[int]) -> dict[int, dict[str, dict]]:
    if not player_ids:
        return {}
    ids = ",".join(str(int(pid)) for pid in sorted(set(player_ids)))
    query = text(
        f"""
        SELECT player_id, stat_type, under_rate, sample_size
        FROM player_under_risk
        WHERE stat_type IN ('points', 'assists', 'rebounds')
          AND player_id IN ({ids})
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    out: dict[int, dict[str, dict]] = {}
    for player_id, stat_type, under_rate, sample_size in rows:
        pid = int(player_id)
        out.setdefault(pid, {})[str(stat_type)] = {
            "under_rate": float(under_rate) if under_rate is not None else None,
            "sample_size": int(sample_size) if sample_size is not None else 0,
        }
    return out


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
        "player_id": rows[0].get("player_id"),
        "full_name": rows[0].get("full_name"),
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


def _apply_under_overlay(
    model_prob: float,
    side: str,
    stat_components: tuple[str, ...],
    pred: dict,
    under_risk_index: dict[int, dict[str, dict]],
) -> tuple[float, dict | None]:
    player_id = pred.get("player_id")
    if player_id is None:
        return model_prob, None
    try:
        pid = int(player_id)
    except (TypeError, ValueError):
        return model_prob, None

    stat_map = under_risk_index.get(pid, {})
    if not stat_map:
        return model_prob, None

    numer = 0.0
    denom = 0.0
    stats_used = 0
    total_sample = 0
    for stat in stat_components:
        profile = stat_map.get(stat)
        if not profile:
            continue
        under_rate = profile.get("under_rate")
        sample = int(profile.get("sample_size") or 0)
        if under_rate is None or sample <= 0:
            continue
        weight = min(sample, 30) / 30.0
        numer += (float(under_rate) - 0.5) * weight
        denom += weight
        stats_used += 1
        total_sample += sample

    if denom <= 0:
        return model_prob, None

    under_signal = numer / denom
    conf = pred.get("confidence")
    conf_scale = _clamp((float(conf) if conf is not None else 65.0) / 100.0, 0.55, 1.0)
    max_shift = 0.06
    shift = max_shift * (under_signal * 2.0) * conf_scale
    side_l = str(side).lower()
    adjusted = model_prob + shift if side_l == "under" else model_prob - shift
    adjusted = _clamp(adjusted, 0.01, 0.99)
    return adjusted, {
        "under_signal": round(float(under_signal), 4),
        "prob_shift": round(float(adjusted - model_prob), 4),
        "stats_used": stats_used,
        "sample_size_sum": total_sample,
    }


def _compose_under_profile(
    stat_components: tuple[str, ...],
    pred: dict,
    under_risk_index: dict[int, dict[str, dict]],
) -> dict | None:
    player_id = pred.get("player_id")
    if player_id is None:
        return None
    try:
        pid = int(player_id)
    except (TypeError, ValueError):
        return None
    stat_map = under_risk_index.get(pid, {})
    if not stat_map:
        return None

    numer = 0.0
    denom = 0.0
    total_sample = 0
    stats_used = 0
    for stat in stat_components:
        profile = stat_map.get(stat)
        if not profile:
            continue
        under_rate = profile.get("under_rate")
        sample = int(profile.get("sample_size") or 0)
        if under_rate is None or sample <= 0:
            continue
        weight = min(sample, 30) / 30.0
        numer += float(under_rate) * weight
        denom += weight
        total_sample += sample
        stats_used += 1

    if denom <= 0:
        return None
    return {
        "under_rate": max(0.0, min(1.0, numer / denom)),
        "sample_size": total_sample,
        "stats_used": stats_used,
    }


def _apply_under_side_model(
    model_prob: float,
    side: str,
    stat_components: tuple[str, ...],
    pred: dict,
    under_risk_index: dict[int, dict[str, dict]],
    side_model_payload: dict | None,
) -> tuple[float, dict | None]:
    if side_model_payload is None:
        return model_prob, None
    if not stat_components:
        return model_prob, None

    under_profile = _compose_under_profile(stat_components, pred, under_risk_index)

    under_probs = []
    model_stat_keys = side_model_payload.get("models", {}).keys()
    for stat in stat_components:
        if stat not in model_stat_keys:
            continue
        p_under = predict_under_probability(side_model_payload, stat, pred, under_profile)
        if p_under is not None:
            under_probs.append(float(p_under))

    if not under_probs:
        return model_prob, None

    calibrator_under = float(np.mean(under_probs))
    calibrator_side = calibrator_under if str(side).lower() == "under" else (1.0 - calibrator_under)

    confidence = pred.get("confidence")
    conf_scale = _clamp((float(confidence) if confidence is not None else 65.0) / 100.0, 0.4, 1.0)
    sample_size = int(under_profile.get("sample_size", 0)) if under_profile else 0
    sample_scale = _clamp(min(sample_size, 30) / 30.0, 0.2, 1.0)
    blend_weight = 0.38 * conf_scale * sample_scale

    adjusted = (1.0 - blend_weight) * float(model_prob) + blend_weight * float(calibrator_side)
    adjusted = _clamp(adjusted, 0.01, 0.99)
    return adjusted, {
        "blend_weight": round(float(blend_weight), 4),
        "calibrator_under_prob": round(float(calibrator_under), 4),
        "calibrator_side_prob": round(float(calibrator_side), 4),
        "sample_size": sample_size,
        "stats_used": int(under_profile.get("stats_used", 0)) if under_profile else 0,
        "model_version": side_model_payload.get("model_version"),
    }


def _combo_product(values: list[float]) -> float:
    result = 1.0
    for value in values:
        result *= value
    return result


@router.get("/best")
async def get_best_bets(
    target_multiplier: float = 2.0,
    leg_count: int = 2,
    leg_mode: str = "exact",
    max_legs: int | None = None,
    bookmaker: str = "sportsbet",
    day: str = "auto",
    include_combos: bool = False,
    event_ids: str | None = None,
    min_confidence: int = 55,
    min_edge: float = 0.02,
    min_prob: float = 0.52,
    use_under_model: bool = False,
    use_under_overlay: bool = True,
    max_candidates: int = 30,
    db: AsyncSession = Depends(get_db),
):
    if leg_count < 1 or leg_count > 6:
        raise HTTPException(status_code=400, detail="leg_count must be between 1 and 6")
    if leg_mode not in {"exact", "up_to"}:
        raise HTTPException(status_code=400, detail="leg_mode must be exact or up_to")
    if max_legs is not None and (max_legs < 1 or max_legs > 6):
        raise HTTPException(status_code=400, detail="max_legs must be between 1 and 6")
    if target_multiplier < 1.01:
        raise HTTPException(
            status_code=400, detail="target_multiplier must be greater than 1.0"
        )
    if day not in {"today", "tomorrow", "yesterday", "auto"}:
        raise HTTPException(
            status_code=400, detail="day must be one of today, tomorrow, yesterday, auto"
        )

    prediction_index = await run_in_threadpool(_build_prediction_index, day)
    under_risk_index: dict[int, dict[str, dict]] = {}
    under_side_model_payload: dict | None = None
    if use_under_overlay or use_under_model:
        player_ids = await run_in_threadpool(_collect_prediction_player_ids, prediction_index)
        under_risk_index = await run_in_threadpool(
            _fetch_under_risk_index, sync_engine, player_ids
        )
    if use_under_model:
        try:
            under_side_model_payload, _ = await run_in_threadpool(load_latest_under_side_model)
        except FileNotFoundError:
            under_side_model_payload = None

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

    selected_ids = [eid.strip() for eid in (event_ids or "").split(",") if eid.strip()]
    if selected_ids:
        stmt = stmt.where(Event.id.in_(tuple(selected_ids)))

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
    overlay_applied = 0
    under_model_applied = 0

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
        overlay_meta = None
        if use_under_overlay:
            model_prob, overlay_meta = _apply_under_overlay(
                model_prob=model_prob,
                side=str(side),
                stat_components=stat_components,
                pred=pred,
                under_risk_index=under_risk_index,
            )
            if overlay_meta is not None:
                overlay_applied += 1
        under_model_meta = None
        if use_under_model and under_side_model_payload is not None:
            model_prob, under_model_meta = _apply_under_side_model(
                model_prob=model_prob,
                side=str(side),
                stat_components=stat_components,
                pred=pred,
                under_risk_index=under_risk_index,
                side_model_payload=under_side_model_payload,
            )
            if under_model_meta is not None:
                under_model_applied += 1
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
                "under_overlay": overlay_meta,
                "under_side_model": under_model_meta,
                "prediction": {
                    "pred_value": pred.get("pred_value"),
                    "pred_p10": pred.get("pred_p10"),
                    "pred_p50": pred.get("pred_p50"),
                    "pred_p90": pred.get("pred_p90"),
                    "confidence": pred.get("confidence"),
                    "player_id": pred.get("player_id"),
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
                "under_overlay_enabled": use_under_overlay,
                "under_overlay_applied": overlay_applied,
                "under_model_enabled": use_under_model,
                "under_model_loaded": under_side_model_payload is not None,
                "under_model_applied": under_model_applied,
            },
        }

    ranked = sorted(
        candidates,
        key=lambda c: (c["edge"], c["model_prob"], c["ev_per_unit"]),
        reverse=True,
    )[: max(leg_count, max_candidates)]

    parlay_options = []
    all_combo_candidates = []
    max_target_overshoot = 1.8
    target_leg_max = max_legs if max_legs is not None else leg_count
    if leg_mode == "up_to":
        leg_counts = [n for n in range(2, target_leg_max + 1)]
        if not leg_counts:
            leg_counts = [1]
    else:
        leg_counts = [leg_count]

    for active_leg_count in leg_counts:
        if active_leg_count == 1:
            for leg in ranked[: min(8, len(ranked))]:
                payout = leg["price_decimal"]
                parlay_options.append(
                    {
                        "legs": [leg],
                        "leg_count": 1,
                        "combined_odds": round(payout, 4),
                        "combined_probability": leg["model_prob"],
                        "expected_value_per_unit": leg["ev_per_unit"],
                        "meets_target": payout >= target_multiplier,
                    }
                )
            continue

        pool = ranked[: min(len(ranked), 18)]
        min_distinct_events = 2 if len(selected_ids) >= 2 and active_leg_count >= 2 else 1
        for combo in combinations(pool, active_leg_count):
            combo_keys = {
                (leg["event_id"], leg["player_name"], leg["market"], leg["side"], leg["line"])
                for leg in combo
            }
            if len(combo_keys) != len(combo):
                continue

            # If user selected multiple matchups, force event diversity in parlays.
            distinct_events = {leg["event_id"] for leg in combo}
            if len(distinct_events) < min_distinct_events:
                continue

            combined_odds = _combo_product([leg["price_decimal"] for leg in combo])
            combined_prob = _combo_product([leg["model_prob"] for leg in combo])
            expected_value = combined_prob * (combined_odds - 1.0) - (1.0 - combined_prob)
            parlay_payload = {
                "legs": list(combo),
                "leg_count": active_leg_count,
                "combined_odds": round(combined_odds, 4),
                "combined_probability": round(combined_prob, 4),
                "expected_value_per_unit": round(expected_value, 4),
                "meets_target": combined_odds >= target_multiplier,
            }
            all_combo_candidates.append(parlay_payload)

            if combined_odds < target_multiplier:
                continue
            # Keep recommendations near the requested payout target.
            if combined_odds > target_multiplier * max_target_overshoot:
                continue
            parlay_options.append(parlay_payload)

    ranked_parlays = sorted(
        parlay_options,
        key=lambda p: (
            abs(p["combined_odds"] - target_multiplier),
            -p["combined_probability"],
            -p["expected_value_per_unit"],
        ),
    )

    # Fallback path: if no near-target parlays, show best available combos.
    # Preference order:
    # 1) combos above target with more "Over" legs
    # 2) if none above target, combos below target with more "Over" legs
    if not ranked_parlays and max(leg_counts) > 1 and all_combo_candidates:
        above_target = [p for p in all_combo_candidates if p["combined_odds"] >= target_multiplier]
        below_target = [p for p in all_combo_candidates if p["combined_odds"] < target_multiplier]

        def over_count(parlay: dict) -> int:
            return sum(1 for leg in parlay["legs"] if str(leg.get("side", "")).lower() == "over")

        if above_target:
            ranked_parlays = sorted(
                above_target,
                key=lambda p: (
                    -over_count(p),
                    abs(p["combined_odds"] - target_multiplier),
                    -p["combined_probability"],
                ),
            )
        elif below_target:
            ranked_parlays = sorted(
                below_target,
                key=lambda p: (
                    -over_count(p),
                    abs(p["combined_odds"] - target_multiplier),
                    -p["combined_probability"],
                ),
            )

    # Diversify recommendations so the same player does not repeat across parlays.
    parlay_options = []
    used_players: set[tuple[str, str]] = set()
    for parlay in ranked_parlays:
        parlay_players = {
            (leg["event_id"], str(leg["player_name"]).lower()) for leg in parlay["legs"]
        }
        if used_players.intersection(parlay_players):
            continue
        parlay_options.append(parlay)
        used_players.update(parlay_players)
        if len(parlay_options) >= 5:
            break

    top_single_legs = sorted(
        candidates, key=lambda c: (c["model_prob"], c["edge"]), reverse=True
    )[:10]

    return {
        "status": "ok",
        "generated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "bookmaker": bookmaker,
        "target_multiplier": target_multiplier,
        "leg_count": leg_count,
        "leg_mode": leg_mode,
        "max_legs": target_leg_max,
        "day": day,
        "include_combos": include_combos,
        "selected_event_count": len(selected_ids),
        "filters": {
            "min_confidence": min_confidence,
            "min_edge": min_edge,
            "min_prob": min_prob,
            "use_under_model": use_under_model,
            "use_under_overlay": use_under_overlay,
        },
        "pool_size": len(candidates),
        "top_single_legs": top_single_legs,
        "recommended_parlays": parlay_options,
        "debug": {
            "rows_loaded": len(rows),
            "skipped_no_prediction": skipped_no_prediction,
            "skipped_low_confidence": skipped_low_confidence,
            "skipped_low_edge": skipped_low_edge,
            "under_overlay_enabled": use_under_overlay,
            "under_overlay_applied": overlay_applied,
            "under_model_enabled": use_under_model,
            "under_model_loaded": under_side_model_payload is not None,
            "under_model_applied": under_model_applied,
        },
    }
