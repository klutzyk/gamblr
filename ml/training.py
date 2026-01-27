import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import joblib
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine
from xgboost import XGBRegressor

from .utils import (
    POINTS_FEATURES,
    ASSISTS_FEATURES,
    REBOUNDS_FEATURES,
    add_player_rolling_features,
    build_team_game_features,
)

BASE_DIR = Path(__file__).resolve().parent
MODELS_DIR = BASE_DIR / "models"


def _train_model(
    engine,
    target: str,
    features: list,
    model_prefix: str,
) -> dict:
    query = """
    SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
           pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
    FROM player_game_stats pg
    JOIN players p ON pg.player_id = p.id
    """
    df_raw = pd.read_sql(query, engine)
    df_raw["game_date"] = pd.to_datetime(df_raw["game_date"])

    df_features = add_player_rolling_features(df_raw)
    team_game_features = build_team_game_features(df_features)
    df_features = df_features.merge(
        team_game_features, on=["game_id", "team_abbreviation", "game_date"], how="left"
    )

    df_features[features] = df_features[features].fillna(0)
    df_features = df_features.dropna(subset=[target])

    X = df_features[features]
    y = df_features[target]

    unique_dates = sorted(df_features["game_date"].dt.date.unique())
    split_idx = int(len(unique_dates) * 0.8)
    split_date = unique_dates[split_idx] if unique_dates else None

    if split_date is None:
        raise ValueError("Not enough data to train; no game dates found.")

    train_mask = df_features["game_date"].dt.date <= split_date
    X_train, y_train = X[train_mask], y[train_mask]
    X_valid, y_valid = X[~train_mask], y[~train_mask]

    model = XGBRegressor(
        n_estimators=2000,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=2,
        reg_alpha=0.0,
        reg_lambda=1.0,
        random_state=42,
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_valid, y_valid)],
        verbose=False,
    )

    preds = model.predict(X_valid)
    mae = mean_absolute_error(y_valid, preds)
    rmse = mean_squared_error(y_valid, preds) ** 0.5

    MODELS_DIR.mkdir(exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    model_path = MODELS_DIR / f"{model_prefix}{today_str}.pkl"
    joblib.dump(model, model_path)

    return {
        "model_path": str(model_path),
        "mae": float(mae),
        "rmse": float(rmse),
        "rows_total": int(len(df_features)),
        "rows_train": int(len(X_train)),
        "rows_valid": int(len(X_valid)),
        "target": target,
    }


def _get_engine(engine=None, database_url: Optional[str] = None):
    load_dotenv()
    if engine is None:
        database_url = database_url or os.getenv("SYNC_DATABASE_URL")
        if not database_url:
            raise ValueError("Missing SYNC_DATABASE_URL for training.")
        engine = create_engine(database_url)
    return engine


def train_points_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(engine, "points", POINTS_FEATURES, "xgb_points_model_")


def train_assists_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(engine, "assists", ASSISTS_FEATURES, "xgb_assists_model_")


def train_rebounds_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(engine, "rebounds", REBOUNDS_FEATURES, "xgb_rebounds_model_")


if __name__ == "__main__":
    results = train_points_model()
    print(f"Validation MAE: {results['mae']:.2f}")
    print(f"Validation RMSE: {results['rmse']:.2f}")
    print(f"Model trained and saved as {results['model_path']}")
