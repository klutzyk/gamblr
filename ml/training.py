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
    MINUTES_FEATURES,
    add_player_rolling_features,
    build_team_game_features,
    build_lineup_team_features,
)

BASE_DIR = Path(__file__).resolve().parent
MODELS_DIR = BASE_DIR / "models"


def _train_model(
    engine,
    target: str,
    features: list,
    model_prefix: str,
    use_minutes_model: bool = False,
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

    df_team = None
    try:
        df_team = pd.read_sql(
            """
            SELECT game_id, team_abbreviation, game_date,
                   points AS team_points, assists AS team_assists, rebounds AS team_rebounds
            FROM team_game_stats
            """,
            engine,
        )
        if not df_team.empty:
            df_team["game_date"] = pd.to_datetime(df_team["game_date"])
    except Exception:
        df_team = None

    team_game_features = build_team_game_features(df_features, df_team)
    df_features = df_features.merge(
        team_game_features, on=["game_id", "team_abbreviation", "game_date"], how="left"
    )

    df_lineups = None
    try:
        df_lineups = pd.read_sql(
            """
            SELECT ls.team_id, ls.season, ls.lineup_id, ls.minutes, ls.off_rating,
                   ls.def_rating, ls.net_rating, ls.pace, ls.ast_pct, ls.reb_pct,
                   t.abbreviation AS team_abbreviation
            FROM lineup_stats ls
            JOIN teams t ON ls.team_id = t.id
            """,
            engine,
        )
    except Exception:
        df_lineups = None

    if df_lineups is not None and not df_lineups.empty:
        df_lineups = df_lineups.rename(
            columns={
                "minutes": "minutes",
                "net_rating": "net_rating",
                "pace": "pace",
                "ast_pct": "ast_pct",
                "reb_pct": "reb_pct",
            }
        )
        lineup_team = build_lineup_team_features(df_lineups)
        df_features = df_features.merge(
            lineup_team, on="team_abbreviation", how="left"
        )

    df_features[features] = df_features[features].fillna(0)
    df_features = df_features.dropna(subset=[target])

    unique_dates = sorted(df_features["game_date"].dt.date.unique())
    split_idx = int(len(unique_dates) * 0.8)
    split_date = unique_dates[split_idx] if unique_dates else None

    if split_date is None:
        raise ValueError("Not enough data to train; no game dates found.")

    train_mask = df_features["game_date"].dt.date <= split_date

    minutes_model_path = None
    if use_minutes_model:
        X_minutes = df_features[MINUTES_FEATURES].fillna(0)
        y_minutes = df_features["minutes"]

        X_minutes_train, y_minutes_train = X_minutes[train_mask], y_minutes[train_mask]

        minutes_model = XGBRegressor(
            n_estimators=800,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=2,
            reg_alpha=0.0,
            reg_lambda=1.0,
            random_state=42,
        )
        minutes_model.fit(X_minutes_train, y_minutes_train, verbose=False)

        MODELS_DIR.mkdir(exist_ok=True)
        today_str = datetime.now().strftime("%Y%m%d")
        minutes_model_path = MODELS_DIR / f"xgb_minutes_model_{today_str}.pkl"
        joblib.dump(minutes_model, minutes_model_path)

        df_features["pred_minutes"] = minutes_model.predict(X_minutes)
    else:
        df_features["pred_minutes"] = df_features["avg_minutes_last5"]

    X = df_features[features].fillna(0)
    y = df_features[target]

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
        "minutes_model_path": str(minutes_model_path) if minutes_model_path else None,
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
    return _train_model(
        engine, "points", POINTS_FEATURES, "xgb_points_model_", use_minutes_model=True
    )


def train_assists_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(
        engine, "assists", ASSISTS_FEATURES, "xgb_assists_model_", use_minutes_model=True
    )


def train_rebounds_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(
        engine,
        "rebounds",
        REBOUNDS_FEATURES,
        "xgb_rebounds_model_",
        use_minutes_model=True,
    )


def train_minutes_model(engine=None, database_url: Optional[str] = None) -> dict:
    engine = _get_engine(engine, database_url)
    return _train_model(
        engine,
        "minutes",
        MINUTES_FEATURES,
        "xgb_minutes_model_",
        use_minutes_model=False,
    )


if __name__ == "__main__":
    results = train_points_model()
    print(f"Validation MAE: {results['mae']:.2f}")
    print(f"Validation RMSE: {results['rmse']:.2f}")
    print(f"Model trained and saved as {results['model_path']}")
