# ml/predict.py
import pandas as pd
import joblib
from pathlib import Path
from utils import compute_prediction_features
from datetime import datetime, timedelta

# ml folder
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"


def load_latest_model(models_dir: Path = MODELS_DIR):
    models = sorted(models_dir.glob("xgb_points_model_*.pkl"))
    if not models:
        raise FileNotFoundError("No trained models found.")
    return joblib.load(models[-1])


def predict_points(
    engine,
    day: str = "today",  # "today", "tomorrow", "yesterday"
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
    """
    Predict points for upcoming NBA games. sds
    """
    # Load rolling CSV
    df_rolling = pd.read_csv(rolling_path)
    df_rolling["game_date"] = pd.to_datetime(df_rolling["game_date"], dayfirst=True)

    # Load all historical games
    df_history = pd.read_sql(
        """
        SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
               pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
        FROM player_game_stats pg
        JOIN players p ON pg.player_id = p.id
    """,
        engine,
    )
    df_history["game_date"] = pd.to_datetime(df_history["game_date"])

    # Get upcoming games from schedule
    df_schedule = pd.read_sql("SELECT * FROM game_schedule", engine)
    df_schedule["game_date"] = pd.to_datetime(df_schedule["game_date"], dayfirst=True)

    # get the target date (taking us datetime as the base because its nba)
    base_date = datetime.now().date()

    if day == "today":
        target_date = base_date - timedelta(days=1)
    elif day == "tomorrow":
        target_date = base_date
    elif day == "yesterday":
        target_date = base_date - timedelta(days=2)
    else:
        raise ValueError("day must be one of: today, tomorrow, yesterday")

    target_date = pd.to_datetime(target_date)

    # get the games for that  date
    df_next_games = df_schedule[df_schedule["game_date"] == target_date]

    if df_next_games.empty:
        print(f"No games found for NBA date: {target_date.date()}")
        return pd.DataFrame()

    # For each upcoming game, get all players from the two teams using rolling stats
    rows = []
    for _, game in df_next_games.iterrows():
        for team_abbr in [game["home_team_abbr"], game["away_team_abbr"]]:
            players = df_rolling[df_rolling["team_abbreviation"] == team_abbr].copy()
            players["matchup"] = game["matchup"]  # schedule matchup
            players["game_date"] = game["game_date"]
            rows.append(players)

    df_next_players = pd.concat(rows, ignore_index=True)

    # Now compute features using historical stats
    df_next_features = compute_prediction_features(df_next_players, df_history)

    # Features to feed the model
    FEATURES = [
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

    # Ensure all feature columns are numeric for XGBoost
    df_next_features[FEATURES] = (
        df_next_features[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0)
    )

    # Load model
    model = load_latest_model(models_dir)

    # Predict points
    df_next_features["pred_points"] = model.predict(df_next_features[FEATURES])

    # Return only relevant columns
    return df_next_features[
        ["player_id", "team_abbreviation", "matchup", "game_date", "pred_points"]
    ]
