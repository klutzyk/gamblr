import os
from dotenv import load_dotenv
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
from sqlalchemy import create_engine
from datetime import datetime
from utils import (
    POINTS_FEATURES,
    add_player_rolling_features,
    build_team_game_features,
)

# load env variables
load_dotenv()
DATABASE_URL = os.getenv("SYNC_DATABASE_URL")

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# get player stats
query = """
SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
       pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
FROM player_game_stats pg
JOIN players p ON pg.player_id = p.id
"""
df_raw = pd.read_sql(query, engine)
df_raw["game_date"] = pd.to_datetime(df_raw["game_date"])

# feature engineering (past-only)
df_features = add_player_rolling_features(df_raw)
team_game_features = build_team_game_features(df_features)
df_features = df_features.merge(
    team_game_features, on=["game_id", "team_abbreviation", "game_date"], how="left"
)

df_features[POINTS_FEATURES] = df_features[POINTS_FEATURES].fillna(0)
df_features = df_features.dropna(subset=["points"])

X = df_features[POINTS_FEATURES]
y = df_features["points"]

# time-based split by date
unique_dates = sorted(df_features["game_date"].dt.date.unique())
split_idx = int(len(unique_dates) * 0.8)
split_date = unique_dates[split_idx] if unique_dates else None

if split_date is None:
    raise ValueError("Not enough data to train; no game dates found.")

train_mask = df_features["game_date"].dt.date <= split_date
X_train, y_train = X[train_mask], y[train_mask]
X_valid, y_valid = X[~train_mask], y[~train_mask]

# train xgboost model
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
    eval_metric="mae",
    verbose=False,
    early_stopping_rounds=50,
)

preds = model.predict(X_valid)
mae = mean_absolute_error(y_valid, preds)
rmse = mean_squared_error(y_valid, preds, squared=False)
print(f"Validation MAE: {mae:.2f}")
print(f"Validation RMSE: {rmse:.2f}")

# save the trained model
today_str = datetime.now().strftime("%Y%m%d")
model_filename = f"../models/xgb_points_model_{today_str}.pkl"
joblib.dump(model, model_filename)
print(f"Model trained and saved as {model_filename}")
