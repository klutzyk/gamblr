from __future__ import annotations

import math
from typing import Any


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def probability_to_american(probability: float | None) -> int | None:
    p = _as_float(probability)
    if p is None or p <= 0 or p >= 1:
        return None
    if p >= 0.5:
        return int(round(-100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def kelly_fraction(model_probability: float | None, decimal_odds: float | None) -> float:
    p = _as_float(model_probability)
    decimal = _as_float(decimal_odds)
    if p is None or decimal is None or p <= 0 or p >= 1 or decimal <= 1:
        return 0.0
    payout = decimal - 1.0
    fraction = (payout * p - (1.0 - p)) / payout
    return _clamp(fraction, 0.0, 1.0)


def grade_hr_bet(row: dict[str, Any]) -> dict[str, Any]:
    probability = _as_float(row.get("model_probability"), 0.0) or 0.0
    implied = _as_float(row.get("implied_probability"), 0.0) or 0.0
    decimal_odds = _as_float(row.get("decimal_odds"))
    edge = _as_float(row.get("edge"), probability - implied) or 0.0
    ev = _as_float(row.get("ev_per_dollar"), 0.0) or 0.0
    order = _as_float(row.get("batting_order"))
    has_posted_lineup = row.get("has_posted_lineup") is True

    score = 50.0
    score += _clamp(ev / 0.30, -1.0, 1.0) * 34.0
    score += _clamp(edge / 0.06, -1.0, 1.0) * 26.0
    score += _clamp((probability - 0.07) / 0.11, -0.35, 1.0) * 12.0

    if has_posted_lineup:
        score += 6.0
    elif row.get("has_posted_lineup") is False:
        score -= 5.0

    if order is not None:
        if order <= 4:
            score += 7.0
        elif order <= 6:
            score += 4.0
        elif order >= 8:
            score -= 5.0

    if probability < 0.035:
        score -= 10.0
    if ev <= 0 or edge <= 0:
        score -= 16.0

    score = round(_clamp(score, 0.0, 100.0), 1)

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

    if grade in {"A+", "A"} and ev > 0 and edge > 0:
        recommendation = "Bet"
    elif grade == "B" and ev > 0 and edge > 0:
        recommendation = "Lean"
    else:
        recommendation = "Pass"

    reasons: list[str] = []
    if ev > 0:
        reasons.append(f"+${ev:.2f} EV per $1")
    else:
        reasons.append("Negative EV")
    if edge > 0:
        reasons.append(f"{edge * 100:.1f}% model edge")
    else:
        reasons.append("No model edge")
    if has_posted_lineup:
        reasons.append("Posted lineup")
    elif row.get("has_posted_lineup") is False:
        reasons.append("Projected lineup")
    if order is not None:
        reasons.append(f"Batting {int(round(order))}")
    if probability < 0.035:
        reasons.append("Low base HR probability")

    return {
        "betting_grade": grade,
        "betting_score": score,
        "bet_recommendation": recommendation,
        "kelly_fraction": round(kelly_fraction(probability, decimal_odds), 4),
        "fair_american_odds": probability_to_american(probability),
        "grade_reasons": reasons[:5],
    }


def add_hr_betting_grades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row.update(grade_hr_bet(row))
    return rows
