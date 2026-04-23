from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sqlalchemy import text


BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models"

SUPPORTED_STATS = ("points", "assists", "rebounds")
MODEL_PREFIX = "under_side_calibrator_"

FEATURES = [
    "center",
    "band_width",
    "confidence",
    "under_rate",
    "under_rate_sample",
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_feature_row_from_prediction(
    pred_row: dict,
    under_profile: dict | None,
) -> list[float]:
    center = pred_row.get("pred_p50")
    if center is None:
        center = pred_row.get("pred_value")
    center = _safe_float(center, 0.0)

    p10 = pred_row.get("pred_p10")
    p90 = pred_row.get("pred_p90")
    if p10 is not None and p90 is not None and _safe_float(p90) > _safe_float(p10):
        band_width = _safe_float(p90) - _safe_float(p10)
    else:
        band_width = max(1.0, abs(center) * 0.36)

    confidence = _safe_float(pred_row.get("confidence"), 65.0)

    under_rate = 0.5
    under_rate_sample = 0.0
    if under_profile:
        under_rate = _safe_float(under_profile.get("under_rate"), 0.5)
        sample_size = max(0, int(_safe_float(under_profile.get("sample_size"), 0)))
        under_rate_sample = min(sample_size, 30) / 30.0

    return [center, band_width, confidence, under_rate, under_rate_sample]


def _load_training_frame(engine, lookback_days: int | None = None) -> pd.DataFrame:
    where_clause = ""
    params: dict[str, Any] = {}
    if lookback_days is not None and lookback_days > 0:
        where_clause = "AND pl.game_date >= (CURRENT_DATE - (:lookback_days || ' day')::interval)"
        params["lookback_days"] = int(lookback_days)

    query = text(
        f"""
        SELECT
            pl.player_id,
            pl.stat_type,
            pl.game_date,
            pl.pred_value,
            pl.pred_p10,
            pl.pred_p50,
            pl.pred_p90,
            pl.confidence,
            pl.actual_value,
            pur.under_rate,
            pur.sample_size
        FROM prediction_logs pl
        LEFT JOIN player_under_risk pur
          ON pur.player_id = pl.player_id
         AND pur.stat_type = pl.stat_type
        WHERE pl.stat_type IN ('points', 'assists', 'rebounds')
          AND pl.actual_value IS NOT NULL
          AND pl.game_date IS NOT NULL
          AND pl.pred_value IS NOT NULL
          AND pl.pred_p10 IS NOT NULL
          {where_clause}
        ORDER BY pl.game_date ASC
        """
    )
    return pd.read_sql(query, engine, params=params)


def _build_frame_for_stat(df: pd.DataFrame, stat_type: str) -> tuple[pd.DataFrame, np.ndarray]:
    stat_df = df[df["stat_type"] == stat_type].copy()
    if stat_df.empty:
        return pd.DataFrame(), np.array([])

    if stat_type == "points":
        # For points, midpoint between p10 and p50 tracks practical under behavior better.
        threshold = (stat_df["pred_p10"].astype(float) + stat_df["pred_value"].astype(float)) / 2.0
    else:
        threshold = stat_df["pred_p10"].astype(float)
    y_under = (stat_df["actual_value"].astype(float) < threshold).astype(int).to_numpy()

    center = stat_df["pred_p50"].fillna(stat_df["pred_value"]).astype(float)
    band_width = (stat_df["pred_p90"].astype(float) - stat_df["pred_p10"].astype(float)).clip(lower=0.25)
    confidence = stat_df["confidence"].fillna(65.0).astype(float)
    under_rate = stat_df["under_rate"].fillna(0.5).astype(float).clip(lower=0.0, upper=1.0)
    sample_norm = stat_df["sample_size"].fillna(0).astype(float).clip(lower=0.0, upper=30.0) / 30.0

    X = pd.DataFrame(
        {
            "center": center,
            "band_width": band_width,
            "confidence": confidence,
            "under_rate": under_rate,
            "under_rate_sample": sample_norm,
        }
    )
    return X, y_under


def _time_split_indices(df_stat: pd.DataFrame, ratio: float = 0.8) -> tuple[np.ndarray, np.ndarray]:
    dates = pd.to_datetime(df_stat["game_date"]).dt.date
    unique_dates = sorted(dates.unique().tolist())
    if len(unique_dates) <= 1:
        mask = np.ones(len(df_stat), dtype=bool)
        return mask, ~mask
    split_idx = max(1, int(len(unique_dates) * ratio))
    split_idx = min(split_idx, len(unique_dates) - 1)
    split_date = unique_dates[split_idx]
    train_mask = (dates <= split_date).to_numpy()
    valid_mask = ~train_mask
    if valid_mask.sum() == 0:
        train_mask = np.ones(len(df_stat), dtype=bool)
        valid_mask = ~train_mask
    return train_mask, valid_mask


def train_under_side_model(
    engine,
    lookback_days: int | None = 180,
    min_rows_per_stat: int = 300,
) -> dict[str, Any]:
    df = _load_training_frame(engine, lookback_days=lookback_days)
    if df.empty:
        raise ValueError("No prediction_logs rows with actuals were found for under-side training.")

    models: dict[str, Any] = {}
    metrics: dict[str, dict[str, Any]] = {}

    for stat in SUPPORTED_STATS:
        stat_df = df[df["stat_type"] == stat].copy()
        if len(stat_df) < min_rows_per_stat:
            metrics[stat] = {
                "status": "skipped",
                "reason": f"insufficient rows ({len(stat_df)} < {min_rows_per_stat})",
                "rows_total": int(len(stat_df)),
            }
            continue

        X, y = _build_frame_for_stat(df, stat)
        if len(X) < min_rows_per_stat or len(np.unique(y)) < 2:
            metrics[stat] = {
                "status": "skipped",
                "reason": "not enough class diversity",
                "rows_total": int(len(X)),
            }
            continue

        train_mask, valid_mask = _time_split_indices(stat_df)
        X_train = X.loc[train_mask]
        y_train = y[train_mask]
        X_valid = X.loc[valid_mask]
        y_valid = y[valid_mask]

        if len(np.unique(y_train)) < 2:
            metrics[stat] = {
                "status": "skipped",
                "reason": "training split has one class only",
                "rows_total": int(len(X)),
            }
            continue

        model = LogisticRegression(max_iter=800, solver="lbfgs")
        model.fit(X_train, y_train)

        valid_probs = model.predict_proba(X_valid)[:, 1] if len(X_valid) else np.array([])
        valid_auc = float(roc_auc_score(y_valid, valid_probs)) if len(X_valid) and len(np.unique(y_valid)) > 1 else None
        valid_brier = float(brier_score_loss(y_valid, valid_probs)) if len(X_valid) else None
        valid_logloss = float(log_loss(y_valid, valid_probs, labels=[0, 1])) if len(X_valid) else None

        models[stat] = model
        metrics[stat] = {
            "status": "trained",
            "rows_total": int(len(X)),
            "rows_train": int(len(X_train)),
            "rows_valid": int(len(X_valid)),
            "base_under_rate": float(np.mean(y)),
            "valid_auc": valid_auc,
            "valid_brier": valid_brier,
            "valid_log_loss": valid_logloss,
        }

    if not models:
        raise ValueError("No under-side model trained. Try lower min_rows_per_stat or larger lookback.")

    MODELS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    model_path = MODELS_DIR / f"{MODEL_PREFIX}{stamp}.pkl"
    payload = {
        "model_version": stamp,
        "trained_at_utc": datetime.utcnow().isoformat(),
        "features": FEATURES,
        "lookback_days": lookback_days,
        "min_rows_per_stat": min_rows_per_stat,
        "metrics": metrics,
        "models": models,
    }
    joblib.dump(payload, model_path)

    return {
        "model_path": str(model_path),
        "model_version": stamp,
        "lookback_days": lookback_days,
        "min_rows_per_stat": min_rows_per_stat,
        "metrics": metrics,
        "rows_source": int(len(df)),
        "stats_trained": sorted(models.keys()),
    }


def load_latest_under_side_model(models_dir: Path = MODELS_DIR) -> tuple[dict[str, Any], Path]:
    paths = sorted(models_dir.glob(f"{MODEL_PREFIX}*.pkl"))
    if not paths:
        raise FileNotFoundError("No under-side calibrator model found.")
    path = paths[-1]
    payload = joblib.load(path)
    return payload, path


def predict_under_probability(
    payload: dict[str, Any],
    stat_type: str,
    pred_row: dict,
    under_profile: dict | None = None,
) -> float | None:
    models = payload.get("models", {})
    model = models.get(stat_type)
    if model is None:
        return None
    features = _extract_feature_row_from_prediction(pred_row, under_profile)
    X = np.array([features], dtype=float)
    prob = float(model.predict_proba(X)[:, 1][0])
    return max(0.01, min(0.99, prob))
