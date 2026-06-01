from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import brier_score_loss, mean_absolute_error, roc_auc_score
from sklearn.pipeline import Pipeline

try:
    from xgboost import XGBClassifier, XGBRegressor
except Exception:  # pragma: no cover - optional dependency at runtime
    XGBClassifier = None
    XGBRegressor = None


BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models" / "mlb"
PHYSICS_PREFIX = "mlb_hr_physics_"
PHYSICS_OUTPUT_COLUMNS = [
    "hr_physics_barrel_probability",
    "hr_physics_fly_ball_probability",
    "hr_physics_hr_contact_probability",
    "hr_physics_max_ev_p10",
    "hr_physics_max_ev_p50",
    "hr_physics_max_ev_p90",
    "hr_physics_carry_distance_ft",
    "hr_physics_fence_distance_ft",
    "hr_physics_contact_hr_probability",
    "hr_physics_game_hr_probability",
]
PHYSICS_TARGET_COLUMNS = [
    "target_barrel_game",
    "target_fly_ball_game",
    "target_hr_contact_game",
    "target_max_exit_velocity",
]


def _target_columns() -> set[str]:
    return {
        "target_home_run",
        "target_hits",
        "target_total_bases",
        "target_strikeouts",
        *PHYSICS_TARGET_COLUMNS,
    }


def _excluded_columns() -> set[str]:
    return {
        "game_pk",
        "player_id",
        "team_id",
        "home_team_id",
        "away_team_id",
        "opponent_team_id",
        "venue_id",
        "game_date",
        "day_night",
        "plate_appearances",
        "at_bats",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "walks",
        "strikeouts",
        "hit_by_pitch",
        "outs_recorded",
        "batters_faced",
        "pitches_thrown",
        "strikes",
        "balls",
        "hits_allowed",
        "home_runs_allowed",
        "earned_runs",
        *PHYSICS_OUTPUT_COLUMNS,
        *_target_columns(),
    }


def add_physics_targets_from_batted_balls(df: pd.DataFrame) -> pd.DataFrame:
    if "bbe_barrel_rate" not in df.columns:
        return df
    out = df.copy()
    out["target_barrel_game"] = (pd.to_numeric(out.get("bbe_barrel_rate"), errors="coerce").fillna(0) > 0).astype(int)
    out["target_fly_ball_game"] = (pd.to_numeric(out.get("bbe_fly_ball_rate"), errors="coerce").fillna(0) > 0).astype(int)
    out["target_hr_contact_game"] = (
        pd.to_numeric(out.get("bbe_hr_contact_rate"), errors="coerce").fillna(0) > 0
    ).astype(int)
    out["target_max_exit_velocity"] = pd.to_numeric(out.get("bbe_max_launch_speed"), errors="coerce")
    return out


def physics_feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = _excluded_columns()
    cols = []
    for col in df.columns:
        if col in excluded or col.startswith("bbe_"):
            continue
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
            if df[col].notna().mean() >= 0.05:
                cols.append(col)
    return sorted(cols)


def _classifier() -> Any:
    if XGBClassifier is not None:
        return XGBClassifier(
            n_estimators=250,
            learning_rate=0.04,
            max_depth=3,
            subsample=0.85,
            colsample_bytree=0.85,
            min_child_weight=5,
            reg_alpha=0.1,
            reg_lambda=2.5,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=42,
            n_jobs=4,
        )
    return RandomForestClassifier(n_estimators=220, min_samples_leaf=20, random_state=42, n_jobs=-1)


def _regressor() -> Any:
    if XGBRegressor is not None:
        return XGBRegressor(
            n_estimators=250,
            learning_rate=0.04,
            max_depth=3,
            subsample=0.85,
            colsample_bytree=0.85,
            min_child_weight=5,
            reg_alpha=0.1,
            reg_lambda=2.5,
            objective="reg:squarederror",
            random_state=42,
            n_jobs=4,
        )
    return RandomForestRegressor(n_estimators=220, min_samples_leaf=20, random_state=42, n_jobs=-1)


def _pipeline(model: Any) -> Pipeline:
    return Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", model)])


def _predict_model(pipeline: Pipeline, kind: str, frame: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    if kind == "classification":
        return pipeline.predict_proba(frame[feature_cols])[:, 1]
    return np.clip(pipeline.predict(frame[feature_cols]), 0, None)


def _s(frame: pd.DataFrame, col: str, default: float = np.nan) -> pd.Series:
    if col in frame.columns:
        return pd.to_numeric(frame[col], errors="coerce")
    return pd.Series(default, index=frame.index, dtype=float)


def _time_oof_predictions(
    df: pd.DataFrame,
    *,
    target_col: str,
    kind: str,
    feature_cols: list[str],
    train_mask: pd.Series,
) -> np.ndarray:
    predictions = np.full(len(df), np.nan, dtype=float)
    train_dates = sorted(pd.to_datetime(df.loc[train_mask, "game_date"]).dt.date.unique())
    if len(train_dates) < 5:
        return predictions

    fold_count = min(5, max(2, len(train_dates) // 20))
    boundaries = np.array_split(train_dates, fold_count)
    for fold_dates in boundaries[1:]:
        fold_start = fold_dates[0]
        fit_mask = train_mask & (pd.to_datetime(df["game_date"]).dt.date < fold_start)
        pred_mask = train_mask & pd.to_datetime(df["game_date"]).dt.date.isin(set(fold_dates))
        fit_rows = df.loc[fit_mask & df[target_col].notna()]
        if fit_rows.empty or pred_mask.sum() == 0:
            continue
        if kind == "classification" and fit_rows[target_col].nunique() < 2:
            continue
        pipe = _pipeline(_classifier() if kind == "classification" else _regressor())
        pipe.fit(fit_rows[feature_cols], fit_rows[target_col].astype(int if kind == "classification" else float))
        predictions[pred_mask.to_numpy()] = _predict_model(pipe, kind, df.loc[pred_mask], feature_cols)

    fallback = float(df.loc[train_mask, target_col].mean()) if df.loc[train_mask, target_col].notna().any() else 0.0
    predictions[train_mask.to_numpy() & np.isnan(predictions)] = fallback
    return predictions


def _game_hr_probability(frame: pd.DataFrame) -> pd.Series:
    barrel = _s(frame, "hr_physics_barrel_probability").fillna(0)
    fly = _s(frame, "hr_physics_fly_ball_probability").fillna(0)
    hr_contact = _s(frame, "hr_physics_hr_contact_probability").fillna(0)
    max_ev = _s(frame, "hr_physics_max_ev_p50").fillna(98)
    angle = _s(frame, "batter_launch_angle_avg").fillna(_s(frame, "batter_bbe_bbe_avg_launch_angle_avg_last20")).fillna(24)
    angle_score = np.exp(-((angle - 27.0) ** 2) / (2 * 7.5**2))
    carry = _s(frame, "hr_weather_carry_index").fillna(1.0)
    park = _s(frame, "park_factor_hr_batter_side").fillna(_s(frame, "park_factor_hr")).fillna(100.0) / 100.0
    fence = _s(frame, "pull_side_distance_avg").fillna(_s(frame, "venue_avg_corner_distance")).fillna(370.0)

    base_distance = 300.0 + (max_ev - 90.0) * 5.3 + angle_score * 42.0
    carry_distance = base_distance * carry.clip(lower=0.82, upper=1.22) * park.clip(lower=0.72, upper=1.35)
    frame["hr_physics_carry_distance_ft"] = carry_distance
    frame["hr_physics_fence_distance_ft"] = fence

    clears_fence = 1.0 / (1.0 + np.exp(-(carry_distance - fence) / 18.0))
    contact_shape = np.maximum(hr_contact, barrel * 0.65 + fly * 0.25)
    contact_hr_probability = np.clip(contact_shape * angle_score * clears_fence, 0, 1)
    expected_pa = _s(frame, "batter_plate_appearances_avg_last20").fillna(4.0)
    batting_order = _s(frame, "batting_order")
    order_boost = np.select(
        [batting_order <= 2, batting_order <= 5, batting_order >= 8],
        [1.08, 1.0, 0.88],
        default=0.94,
    )
    expected_opportunities = (expected_pa * order_boost).clip(lower=2.0, upper=5.2)
    per_pa_probability = np.clip(contact_hr_probability / expected_opportunities.clip(lower=1.0), 0, 0.8)
    frame["hr_physics_contact_hr_probability"] = contact_hr_probability
    return pd.Series(1.0 - np.power(1.0 - per_pa_probability, expected_opportunities), index=frame.index).clip(0, 1)


def _latest_artifact_path() -> Path | None:
    paths = sorted(MODELS_DIR.glob(f"{PHYSICS_PREFIX}*.pkl"), key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0] if paths else None


def _add_output_defaults(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in PHYSICS_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan
    return out


def add_lightweight_hr_physics_outputs(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    expected_contact = _s(out, "hr_expected_contact_score").fillna(0.0)
    batter_power = _s(out, "hr_batter_power_score").fillna(0.0)
    contact_quality = _s(out, "hr_batter_contact_quality_score").fillna(0.0)
    pitcher_contact = _s(out, "hr_pitcher_contact_allowed_score").fillna(0.0)
    bullpen_contact = _s(out, "hr_bullpen_contact_allowed_score").fillna(0.0)

    batter_barrel = _s(out, "batter_barrel_batted_rate").fillna(_s(out, "batter_bbe_bbe_barrel_rate_avg_last20"))
    batter_fly = _s(out, "batter_bbe_bbe_fly_ball_rate_avg_last20")
    batter_hr_contact = _s(out, "batter_bbe_bbe_hr_contact_rate_avg_last20").fillna(batter_barrel)
    max_ev = _s(out, "batter_bbe_bbe_max_launch_speed_avg_last5").fillna(
        _s(out, "batter_bbe_bbe_max_launch_speed_avg_last20")
    )
    avg_ev = _s(out, "batter_exit_velocity_avg").fillna(_s(out, "batter_bbe_bbe_avg_launch_speed_avg_last20"))

    out["hr_physics_barrel_probability"] = np.clip(
        batter_barrel.fillna(0.04) * 0.55 + expected_contact * 0.16 + pitcher_contact * 0.06,
        0.005,
        0.45,
    )
    out["hr_physics_fly_ball_probability"] = np.clip(
        batter_fly.fillna(0.32) * 0.65 + contact_quality * 0.12 + pitcher_contact * 0.05,
        0.08,
        0.70,
    )
    out["hr_physics_hr_contact_probability"] = np.clip(
        batter_hr_contact.fillna(0.045) * 0.55
        + batter_power * 0.11
        + pitcher_contact * 0.07
        + bullpen_contact * 0.04,
        0.003,
        0.40,
    )
    out["hr_physics_max_ev_p50"] = np.clip(
        max_ev.fillna(avg_ev.fillna(89.0) + 7.5) + batter_power * 6.0,
        82.0,
        122.0,
    )
    out["hr_physics_max_ev_p10"] = out["hr_physics_max_ev_p50"] - 7.0
    out["hr_physics_max_ev_p90"] = out["hr_physics_max_ev_p50"] + 7.0
    out = _add_output_defaults(out)
    out["hr_physics_game_hr_probability"] = _game_hr_probability(out)
    return out


def fit_hr_physics_models(
    df: pd.DataFrame,
    *,
    train_mask: pd.Series,
    valid_mask: pd.Series,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    feature_cols = physics_feature_columns(df)
    if not feature_cols:
        return _add_output_defaults(df), {"trained": False, "reason": "no feature columns"}

    targets = {
        "barrel": ("target_barrel_game", "classification"),
        "fly_ball": ("target_fly_ball_game", "classification"),
        "hr_contact": ("target_hr_contact_game", "classification"),
        "max_ev": ("target_max_exit_velocity", "regression"),
    }
    out = df.copy()
    models: dict[str, dict[str, Any]] = {}
    metrics: dict[str, dict[str, float]] = {}

    for name, (target_col, kind) in targets.items():
        if target_col not in out.columns:
            continue
        fit_mask = train_mask & out[target_col].notna()
        if fit_mask.sum() < 200:
            continue
        if kind == "classification" and out.loc[fit_mask, target_col].nunique() < 2:
            continue

        oof = _time_oof_predictions(
            out,
            target_col=target_col,
            kind=kind,
            feature_cols=feature_cols,
            train_mask=train_mask,
        )
        pipe = _pipeline(_classifier() if kind == "classification" else _regressor())
        pipe.fit(out.loc[fit_mask, feature_cols], out.loc[fit_mask, target_col].astype(int if kind == "classification" else float))
        valid_pred = _predict_model(pipe, kind, out.loc[valid_mask], feature_cols)
        all_pred = np.full(len(out), np.nan, dtype=float)
        all_pred[train_mask.to_numpy()] = oof[train_mask.to_numpy()]
        all_pred[valid_mask.to_numpy()] = valid_pred

        output_col = {
            "barrel": "hr_physics_barrel_probability",
            "fly_ball": "hr_physics_fly_ball_probability",
            "hr_contact": "hr_physics_hr_contact_probability",
            "max_ev": "hr_physics_max_ev_p50",
        }[name]
        out[output_col] = all_pred
        valid_eval_mask = valid_mask & out[target_col].notna()
        valid_eval_pred = pd.Series(valid_pred, index=out.loc[valid_mask].index)
        metric_frame = pd.DataFrame(
            {
                "actual": out.loc[valid_eval_mask, target_col],
                "prediction": valid_eval_pred.reindex(out.loc[valid_eval_mask].index),
            }
        ).dropna()
        if name == "max_ev":
            valid_actual = metric_frame["actual"]
            valid_prediction = metric_frame["prediction"]
            valid_error = valid_actual - valid_prediction
            residual_std = float(np.nanstd(valid_error)) if len(valid_error) else 4.0
            residual_std = max(residual_std, 3.0)
            out["hr_physics_max_ev_p10"] = out[output_col] - 1.28 * residual_std
            out["hr_physics_max_ev_p90"] = out[output_col] + 1.28 * residual_std
            metric_payload = {
                "mae": float(mean_absolute_error(valid_actual, valid_prediction)) if len(valid_actual) else float("nan"),
                "residual_std": residual_std,
            }
        else:
            valid_actual = metric_frame["actual"].astype(int)
            clipped = np.clip(metric_frame["prediction"], 1e-6, 1 - 1e-6)
            metric_payload = {"brier": float(brier_score_loss(valid_actual, clipped)) if len(valid_actual) else float("nan")}
            if valid_actual.nunique() > 1:
                metric_payload["roc_auc"] = float(roc_auc_score(valid_actual, clipped))
        metrics[name] = metric_payload
        models[name] = {"kind": kind, "target": target_col, "pipeline": pipe}

    out = _add_output_defaults(out)
    if "hr_physics_max_ev_p50" in out.columns and out["hr_physics_max_ev_p10"].isna().all():
        out["hr_physics_max_ev_p10"] = out["hr_physics_max_ev_p50"] - 5.0
        out["hr_physics_max_ev_p90"] = out["hr_physics_max_ev_p50"] + 5.0
    out["hr_physics_game_hr_probability"] = _game_hr_probability(out)

    now = datetime.now(timezone.utc)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    artifact_path = MODELS_DIR / f"{PHYSICS_PREFIX}{now.strftime('%Y%m%d_%H%M%S')}.pkl"
    artifact = {
        "kind": "hr_physics_ensemble",
        "feature_columns": feature_cols,
        "models": models,
        "metrics": metrics,
        "trained_at": now.isoformat(),
        "output_columns": PHYSICS_OUTPUT_COLUMNS,
    }
    joblib.dump(artifact, artifact_path)
    return out, {
        "trained": True,
        "artifact_path": str(artifact_path),
        "feature_count": len(feature_cols),
        "metrics": metrics,
        "output_columns": PHYSICS_OUTPUT_COLUMNS,
    }


def apply_latest_hr_physics_models(df: pd.DataFrame) -> pd.DataFrame:
    path = _latest_artifact_path()
    if path is None:
        return _add_output_defaults(df)
    artifact = joblib.load(path)
    feature_cols = artifact["feature_columns"]
    out = df.copy()
    missing = [col for col in feature_cols if col not in out.columns]
    for col in missing:
        out[col] = np.nan
    for name, payload in artifact["models"].items():
        pred = _predict_model(payload["pipeline"], payload["kind"], out, feature_cols)
        output_col = {
            "barrel": "hr_physics_barrel_probability",
            "fly_ball": "hr_physics_fly_ball_probability",
            "hr_contact": "hr_physics_hr_contact_probability",
            "max_ev": "hr_physics_max_ev_p50",
        }[name]
        out[output_col] = pred
        if name == "max_ev":
            residual_std = float((artifact.get("metrics", {}).get("max_ev") or {}).get("residual_std") or 5.0)
            out["hr_physics_max_ev_p10"] = out[output_col] - 1.28 * residual_std
            out["hr_physics_max_ev_p90"] = out[output_col] + 1.28 * residual_std
    out = _add_output_defaults(out)
    out["hr_physics_game_hr_probability"] = _game_hr_probability(out)
    return out
