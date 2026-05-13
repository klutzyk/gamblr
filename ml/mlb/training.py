from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline

try:
    from xgboost import XGBClassifier, XGBRegressor
except Exception:  # pragma: no cover - optional dependency at runtime
    XGBClassifier = None
    XGBRegressor = None

from .features import (
    build_batter_training_frame,
    build_pitcher_training_frame,
    get_engine,
    model_feature_columns,
)
from .hr_physics import fit_hr_physics_models


logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models" / "mlb"
REPORTS_DIR = BASE_DIR / "reports" / "mlb"


MARKETS = {
    "batter_home_runs": {
        "frame": "batter",
        "target": "target_home_run",
        "kind": "classification",
        "model_prefix": "mlb_batter_home_runs_",
    },
    "batter_hits": {
        "frame": "batter",
        "target": "target_hits",
        "kind": "regression",
        "model_prefix": "mlb_batter_hits_",
    },
    "batter_total_bases": {
        "frame": "batter",
        "target": "target_total_bases",
        "kind": "regression",
        "model_prefix": "mlb_batter_total_bases_",
    },
    "pitcher_strikeouts": {
        "frame": "pitcher",
        "target": "target_strikeouts",
        "kind": "regression",
        "model_prefix": "mlb_pitcher_strikeouts_",
    },
}


def _time_split(df: pd.DataFrame, valid_fraction: float = 0.2) -> tuple[pd.Series, pd.Series, str]:
    dates = sorted(pd.to_datetime(df["game_date"]).dt.date.unique())
    if len(dates) < 5:
        raise ValueError("Not enough distinct game dates for time-based validation.")
    split_idx = max(1, min(len(dates) - 1, int(len(dates) * (1 - valid_fraction))))
    split_date = dates[split_idx]
    train_mask = pd.to_datetime(df["game_date"]).dt.date < split_date
    valid_mask = ~train_mask
    if train_mask.sum() == 0 or valid_mask.sum() == 0:
        raise ValueError("Time split produced an empty train or validation set.")
    return train_mask, valid_mask, split_date.isoformat()


def _candidate_models(kind: str, y_train: pd.Series, *, search_models: bool = True) -> dict[str, Any]:
    models: dict[str, Any] = {}
    if kind == "classification":
        positives = max(float((y_train == 1).sum()), 1.0)
        negatives = max(float((y_train == 0).sum()), 1.0)
        scale_pos_weight = negatives / positives
        if XGBClassifier is not None:
            models["xgboost"] = XGBClassifier(
                n_estimators=500,
                learning_rate=0.03,
                max_depth=4,
                subsample=0.85,
                colsample_bytree=0.85,
                min_child_weight=3,
                reg_alpha=0.1,
                reg_lambda=2.0,
                objective="binary:logistic",
                eval_metric="logloss",
                random_state=42,
                n_jobs=4,
            )
            if not search_models:
                return models
            models["xgboost_balanced"] = XGBClassifier(
                n_estimators=500,
                learning_rate=0.03,
                max_depth=4,
                subsample=0.85,
                colsample_bytree=0.85,
                min_child_weight=3,
                reg_alpha=0.1,
                reg_lambda=2.0,
                objective="binary:logistic",
                eval_metric="logloss",
                scale_pos_weight=scale_pos_weight,
                random_state=42,
                n_jobs=4,
            )
        if not search_models and models:
            return models
        models["random_forest"] = RandomForestClassifier(
            n_estimators=250,
            min_samples_leaf=10,
            random_state=42,
            n_jobs=-1,
        )
        models["random_forest_balanced"] = RandomForestClassifier(
            n_estimators=250,
            min_samples_leaf=10,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        )
    else:
        if XGBRegressor is not None:
            models["xgboost"] = XGBRegressor(
                n_estimators=500,
                learning_rate=0.03,
                max_depth=4,
                subsample=0.85,
                colsample_bytree=0.85,
                min_child_weight=3,
                reg_alpha=0.1,
                reg_lambda=2.0,
                objective="reg:squarederror",
                random_state=42,
                n_jobs=4,
            )
            if not search_models:
                return models
        if not search_models and models:
            return models
        models["random_forest"] = RandomForestRegressor(
            n_estimators=250,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1,
        )
    return models


def _evaluate(kind: str, y_valid: pd.Series, prediction: np.ndarray) -> dict[str, float]:
    y_array = y_valid.to_numpy()
    if kind == "classification":
        clipped = np.clip(prediction, 1e-6, 1 - 1e-6)
        metrics = {
            "brier": float(brier_score_loss(y_array, clipped)),
            "log_loss": float(log_loss(y_array, clipped, labels=[0, 1])),
            "positive_rate": float(np.mean(y_array)),
            "predicted_positive_rate": float(np.mean(clipped)),
        }
        if len(np.unique(y_array)) > 1:
            metrics["roc_auc"] = float(roc_auc_score(y_array, clipped))
            metrics["average_precision"] = float(average_precision_score(y_array, clipped))
        else:
            metrics["roc_auc"] = float("nan")
            metrics["average_precision"] = float("nan")
        return metrics

    clipped = np.clip(prediction, 0, None)
    return {
        "mae": float(mean_absolute_error(y_array, clipped)),
        "rmse": float(mean_squared_error(y_array, clipped) ** 0.5),
        "target_mean": float(np.mean(y_array)),
        "prediction_mean": float(np.mean(clipped)),
    }


def _score_for_selection(kind: str, metrics: dict[str, float]) -> float:
    if kind == "classification":
        return metrics["brier"]
    return metrics["mae"]


def _baseline_metrics(kind: str, y_train: pd.Series, y_valid: pd.Series) -> dict[str, float]:
    if kind == "classification":
        prediction = np.full(len(y_valid), float(y_train.mean()))
    else:
        prediction = np.full(len(y_valid), float(y_train.mean()))
    return _evaluate(kind, y_valid, prediction)


def _emit_progress(message: str) -> None:
    logger.info(message)
    print(message, flush=True)


def _training_sample_weights(df: pd.DataFrame, train_mask: pd.Series) -> tuple[pd.Series, dict[str, float]]:
    train_dates = pd.to_datetime(df.loc[train_mask, "game_date"])
    max_date = train_dates.max()
    age_days = (max_date - train_dates).dt.days.clip(lower=0)

    weights = pd.Series(
        np.power(0.5, age_days / 90.0),
        index=train_dates.index,
        dtype=float,
    )
    if "season" in df.columns and df.loc[train_mask, "season"].notna().any():
        max_season = df.loc[train_mask, "season"].max()
        weights *= np.where(df.loc[train_mask, "season"] == max_season, 1.65, 1.0)

    weights *= np.where(age_days <= 30, 1.35, 1.0)
    weights *= np.where(age_days <= 14, 1.25, 1.0)
    weights = weights.clip(lower=0.15, upper=5.0)
    weights = weights / max(float(weights.mean()), 1e-9)

    summary = {
        "half_life_days": 90.0,
        "current_season_multiplier": 1.65,
        "last_30_days_multiplier": 1.35,
        "last_14_days_multiplier": 1.25,
        "min": float(weights.min()),
        "mean": float(weights.mean()),
        "max": float(weights.max()),
    }
    return weights, summary


def _load_frame(market: str, engine) -> pd.DataFrame:
    frame_kind = MARKETS[market]["frame"]
    if frame_kind == "batter":
        return build_batter_training_frame(engine=engine)
    if frame_kind == "pitcher":
        return build_pitcher_training_frame(engine=engine)
    raise ValueError(f"Unknown frame kind: {frame_kind}")


def train_market(
    market: str,
    *,
    engine=None,
    database_url: str | None = None,
    min_player_games: int = 3,
    search_models: bool = True,
) -> dict[str, Any]:
    if market not in MARKETS:
        raise ValueError(f"Unknown MLB market '{market}'. Choose from: {', '.join(MARKETS)}")

    engine = engine or get_engine(database_url)
    config = MARKETS[market]
    target_col = config["target"]
    kind = config["kind"]
    _emit_progress(f"[mlb-training] {market}: loading {config['frame']} training frame")
    df = _load_frame(market, engine)
    if df.empty:
        raise ValueError(f"No training rows found for {market}.")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=[target_col, "game_date"]).copy()
    if "player_games_played_season" in df.columns:
        df = df[df["player_games_played_season"].fillna(0) >= min_player_games].copy()
    if df.empty:
        raise ValueError(f"No rows remain for {market} after minimum history filtering.")

    train_mask, valid_mask, split_date = _time_split(df)
    physics_summary = None
    if market == "batter_home_runs":
        _emit_progress(f"[mlb-training] {market}: fitting HR physics/contact ensemble")
        df, physics_summary = fit_hr_physics_models(
            df,
            train_mask=train_mask,
            valid_mask=valid_mask,
        )

    feature_cols = model_feature_columns(df, target_col)
    if not feature_cols:
        raise ValueError(f"No usable numeric features found for {market}.")

    _emit_progress(
        "[mlb-training] "
        f"{market}: rows={len(df)} train={int(train_mask.sum())} "
        f"valid={int(valid_mask.sum())} features={len(feature_cols)} split={split_date}"
    )
    X_train = df.loc[train_mask, feature_cols]
    y_train = df.loc[train_mask, target_col].astype(float if kind == "regression" else int)
    X_valid = df.loc[valid_mask, feature_cols]
    y_valid = df.loc[valid_mask, target_col].astype(float if kind == "regression" else int)
    sample_weight, sample_weight_summary = _training_sample_weights(df, train_mask)

    baseline = _baseline_metrics(kind, y_train, y_valid)
    candidates = _candidate_models(kind, y_train, search_models=search_models)
    model_results: dict[str, dict[str, Any]] = {"baseline_mean": {"metrics": baseline}}
    best_name = None
    best_score = float("inf")
    best_pipeline = None

    for name, model in candidates.items():
        _emit_progress(f"[mlb-training] {market}: fitting {name}")
        pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("model", model),
            ]
        )
        try:
            pipeline.fit(X_train, y_train, model__sample_weight=sample_weight)
        except TypeError:
            pipeline.fit(X_train, y_train)
        if kind == "classification":
            prediction = pipeline.predict_proba(X_valid)[:, 1]
        else:
            prediction = pipeline.predict(X_valid)
        metrics = _evaluate(kind, y_valid, prediction)
        model_results[name] = {"metrics": metrics}
        score = _score_for_selection(kind, metrics)
        metric_text = ", ".join(
            f"{metric}={value:.4f}"
            for metric, value in metrics.items()
            if isinstance(value, float) and not np.isnan(value)
        )
        _emit_progress(f"[mlb-training] {market}: {name} done ({metric_text})")
        if score < best_score:
            best_name = name
            best_score = score
            best_pipeline = pipeline

    if best_pipeline is None or best_name is None:
        raise ValueError(f"No candidate model trained for {market}.")

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / f"{config['model_prefix']}{stamp}.pkl"
    report_path = REPORTS_DIR / f"{config['model_prefix']}{stamp}.json"

    artifact = {
        "market": market,
        "kind": kind,
        "target": target_col,
        "model_name": best_name,
        "model": best_pipeline,
        "feature_columns": feature_cols,
        "trained_at": now.isoformat(),
        "split_date": split_date,
        "min_player_games": min_player_games,
        "search_models": search_models,
        "rows_total": int(len(df)),
        "rows_train": int(len(X_train)),
        "rows_valid": int(len(X_valid)),
        "date_min": pd.to_datetime(df["game_date"]).min().date().isoformat(),
        "date_max": pd.to_datetime(df["game_date"]).max().date().isoformat(),
        "sample_weighting": sample_weight_summary,
        "physics_ensemble": physics_summary,
    }
    joblib.dump(artifact, model_path)

    report = {
        **{key: value for key, value in artifact.items() if key != "model"},
        "model_path": str(model_path),
        "model_results": model_results,
        "best_metrics": model_results[best_name]["metrics"],
        "feature_count": len(feature_cols),
        "top_features": _top_features(best_pipeline, feature_cols),
    }
    report_path.write_text(json.dumps(report, indent=2, allow_nan=True), encoding="utf-8")
    report["report_path"] = str(report_path)
    _emit_progress(
        f"[mlb-training] {market}: selected {best_name}; "
        f"saved model={model_path} report={report_path}"
    )
    return report


def _top_features(pipeline: Pipeline, feature_cols: list[str], limit: int = 30) -> list[dict[str, float | str]]:
    model = pipeline.named_steps["model"]
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return []
    pairs = sorted(zip(feature_cols, values), key=lambda item: float(item[1]), reverse=True)
    return [{"feature": name, "importance": float(value)} for name, value in pairs[:limit]]


def train_all(
    *,
    database_url: str | None = None,
    min_player_games: int = 3,
    search_models: bool = True,
) -> dict[str, Any]:
    engine = get_engine(database_url)
    results = {}
    total = len(MARKETS)
    for idx, market in enumerate(MARKETS, start=1):
        _emit_progress(f"[mlb-training] train_all: starting {market} ({idx}/{total})")
        results[market] = train_market(
            market,
            engine=engine,
            min_player_games=min_player_games,
            search_models=search_models,
        )
        _emit_progress(f"[mlb-training] train_all: finished {market} ({idx}/{total})")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Train MLB market models.")
    parser.add_argument(
        "--market",
        choices=[*MARKETS.keys(), "all"],
        default="all",
        help="Market to train.",
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--min-player-games", type=int, default=3)
    parser.add_argument("--search-models", action="store_true", help="Train/evaluate all candidate model families.")
    args = parser.parse_args()

    if args.market == "all":
        results = train_all(
            database_url=args.database_url,
            min_player_games=args.min_player_games,
            search_models=args.search_models,
        )
    else:
        results = {
            args.market: train_market(
                args.market,
                database_url=args.database_url,
                min_player_games=args.min_player_games,
                search_models=args.search_models,
            )
        }
    print(json.dumps(results, indent=2, allow_nan=True))


if __name__ == "__main__":
    main()
