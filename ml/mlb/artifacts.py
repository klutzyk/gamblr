from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from .training import MARKETS, MODELS_DIR, REPORTS_DIR


def market_names() -> list[str]:
    return sorted(MARKETS.keys())


def _market_prefix(market: str) -> str:
    if market not in MARKETS:
        raise ValueError(f"Unknown MLB market '{market}'. Choose from: {', '.join(market_names())}")
    return str(MARKETS[market]["model_prefix"])


def list_market_reports(market: str, *, limit: int | None = None) -> list[dict[str, Any]]:
    prefix = _market_prefix(market)
    paths = sorted(REPORTS_DIR.glob(f"{prefix}*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if limit is not None:
        paths = paths[:limit]
    reports = []
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        data["report_path"] = str(path)
        reports.append(data)
    return reports


def latest_market_report(market: str) -> dict[str, Any] | None:
    reports = list_market_reports(market, limit=1)
    return reports[0] if reports else None


def latest_model_path(market: str) -> Path | None:
    prefix = _market_prefix(market)
    paths = sorted(MODELS_DIR.glob(f"{prefix}*.pkl"), key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0] if paths else None


def load_latest_model(market: str) -> dict[str, Any]:
    path = latest_model_path(market)
    if path is None:
        raise FileNotFoundError(f"No trained MLB artifact found for {market}.")
    artifact = joblib.load(path)
    artifact["artifact_path"] = str(path)
    return artifact


def market_status(market: str) -> dict[str, Any]:
    report = latest_market_report(market)
    model_path = latest_model_path(market)
    config = MARKETS[market]
    return {
        "market": market,
        "kind": config["kind"],
        "target": config["target"],
        "trained": model_path is not None,
        "model_path": str(model_path) if model_path else None,
        "latest_report_path": report.get("report_path") if report else None,
        "trained_at": report.get("trained_at") if report else None,
        "rows_total": report.get("rows_total") if report else None,
        "rows_train": report.get("rows_train") if report else None,
        "rows_valid": report.get("rows_valid") if report else None,
        "split_date": report.get("split_date") if report else None,
        "date_min": report.get("date_min") if report else None,
        "date_max": report.get("date_max") if report else None,
        "best_metrics": report.get("best_metrics") if report else None,
    }


def all_market_statuses() -> list[dict[str, Any]]:
    return [market_status(market) for market in market_names()]


def score_frame(
    market: str,
    frame: pd.DataFrame,
    *,
    limit: int | None = None,
    strict_features: bool = False,
) -> pd.DataFrame:
    artifact = load_latest_model(market)
    feature_columns = artifact["feature_columns"]
    scored = frame.copy()
    missing = [column for column in feature_columns if column not in frame.columns]
    if missing:
        if strict_features:
            raise ValueError(
                f"Scoring frame for {market} is missing {len(missing)} model features: "
                f"{', '.join(missing[:10])}"
            )
        for column in missing:
            scored[column] = np.nan
    scored.attrs["missing_model_features"] = missing

    model = artifact["model"]
    predictions = (
        model.predict_proba(scored[feature_columns])[:, 1]
        if artifact["kind"] == "classification"
        else model.predict(scored[feature_columns])
    )
    prediction_col = "probability" if artifact["kind"] == "classification" else "prediction"
    scored[prediction_col] = np.clip(predictions, 0, None)
    if prediction_col == "probability":
        scored[prediction_col] = np.clip(scored[prediction_col], 0, 1)

    sort_cols = [prediction_col]
    scored = scored.sort_values(sort_cols, ascending=False)
    if limit is not None:
        scored = scored.head(limit)
    return scored
