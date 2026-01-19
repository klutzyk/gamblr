import joblib
import pandas as pd
from pathlib import Path
from utils import compute_rolling_features

MODEL_PATH = Path(__file__).parent.parent / "models"


def load_latest_model():
    # get latest model file based on date
    model_files = sorted(MODEL_PATH.glob("xgb_points_model_*.pkl"))
    if not model_files:
        raise FileNotFoundError("No model found")
    return joblib.load(model_files[-1])


def predict_points(df_next, df_history):
    """
    df_next_game: DataFrame containing next game's players with
    precomputed features: avg_minutes_last5, is_home, avg_points_last5, etc.
    """
    # Compute all features for prediction
    df_next_features = compute_rolling_features(df_next, df_history)

    model = load_latest_model()
    features = [
        "avg_minutes_last5",
        "is_home",
        "avg_points_last5",
        "avg_assists_last5",
        "avg_rebounds_last5",
        "teammate_avg_assists_last5",
        "opponent_avg_points_allowed_last5",
        "opponent_avg_blocks_last5",
        "opponent_avg_steals_last5",
        "opponent_avg_turnovers_last5",
    ]

    # Ensure is_home is computed
    df_next_features["is_home"] = df_next_features["matchup"].apply(
        lambda x: 1 if "@" not in x else 0
    )

    df_next_features["pred_points"] = model.predict(df_next_features[features])
    return df_next_features
