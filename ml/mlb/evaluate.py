from __future__ import annotations

import argparse
import json
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)

from .artifacts import load_latest_model, score_frame
from .features import build_batter_training_frame, build_pitcher_training_frame, get_engine
from .training import MARKETS


def _load_frame_for_market(market: str, engine) -> pd.DataFrame:
    frame_kind = MARKETS[market]["frame"]
    if frame_kind == "batter":
        return build_batter_training_frame(engine=engine)
    if frame_kind == "pitcher":
        return build_pitcher_training_frame(engine=engine)
    raise ValueError(f"Unknown frame kind: {frame_kind}")


def _metrics(kind: str, y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    if kind == "classification":
        clipped = np.clip(y_pred.to_numpy(dtype=float), 1e-6, 1 - 1e-6)
        y_array = y_true.to_numpy(dtype=int)
        result = {
            "rows": int(len(y_true)),
            "brier": float(brier_score_loss(y_array, clipped)),
            "log_loss": float(log_loss(y_array, clipped, labels=[0, 1])),
            "actual_rate": float(np.mean(y_array)),
            "predicted_rate": float(np.mean(clipped)),
        }
        if len(np.unique(y_array)) > 1:
            result["roc_auc"] = float(roc_auc_score(y_array, clipped))
            result["average_precision"] = float(average_precision_score(y_array, clipped))
        return result

    clipped = np.clip(y_pred.to_numpy(dtype=float), 0, None)
    y_array = y_true.to_numpy(dtype=float)
    return {
        "rows": int(len(y_true)),
        "mae": float(mean_absolute_error(y_array, clipped)),
        "rmse": float(mean_squared_error(y_array, clipped) ** 0.5),
        "actual_mean": float(np.mean(y_array)),
        "predicted_mean": float(np.mean(clipped)),
    }


def score_completed_games(
    market: str,
    *,
    database_url: str | None = None,
    since: str | None = None,
    until: str | None = None,
    last_days: int | None = 30,
    limit: int = 25,
) -> dict[str, Any]:
    if market not in MARKETS:
        raise ValueError(f"Unknown MLB market '{market}'. Choose from: {', '.join(MARKETS)}")

    artifact = load_latest_model(market)
    engine = get_engine(database_url)
    df = _load_frame_for_market(market, engine).replace([np.inf, -np.inf], np.nan)
    if df.empty:
        raise ValueError(f"No historical rows found for {market}.")

    df["game_date"] = pd.to_datetime(df["game_date"])
    if since:
        df = df[df["game_date"] >= pd.to_datetime(since)]
    if until:
        df = df[df["game_date"] <= pd.to_datetime(until)]
    if last_days is not None and not since:
        cutoff = df["game_date"].max() - pd.Timedelta(days=last_days)
        df = df[df["game_date"] >= cutoff]

    target = artifact["target"]
    df = df.dropna(subset=[target]).copy()
    if df.empty:
        raise ValueError("No rows remain after date filtering.")

    scored = score_frame(market, df)
    prediction_col = "probability" if artifact["kind"] == "classification" else "prediction"
    metrics = _metrics(artifact["kind"], scored[target], scored[prediction_col])

    identity_cols = [
        "game_date",
        "game_pk",
        "player_id",
        "team_id",
        "opponent_team_id",
        "batting_order",
        target,
        prediction_col,
    ]
    sample_cols = [col for col in identity_cols if col in scored.columns]
    top_rows = scored[sample_cols].head(limit).copy()
    top_rows["game_date"] = pd.to_datetime(top_rows["game_date"]).dt.date.astype(str)

    return {
        "market": market,
        "model_path": artifact["artifact_path"],
        "model_name": artifact["model_name"],
        "kind": artifact["kind"],
        "date_min": scored["game_date"].min().date().isoformat(),
        "date_max": scored["game_date"].max().date().isoformat(),
        "metrics": metrics,
        "top_predictions": top_rows.to_dict(orient="records"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Score trained MLB models on completed games.")
    parser.add_argument("market", choices=MARKETS.keys())
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--since", default=None, help="Inclusive YYYY-MM-DD lower bound.")
    parser.add_argument("--until", default=None, help="Inclusive YYYY-MM-DD upper bound.")
    parser.add_argument("--last-days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    result = score_completed_games(
        args.market,
        database_url=args.database_url,
        since=args.since,
        until=args.until,
        last_days=args.last_days,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, allow_nan=True))


if __name__ == "__main__":
    main()
