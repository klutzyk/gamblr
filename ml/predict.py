# ml/predict.py
import pandas as pd
import joblib
from pathlib import Path
from utils import compute_prediction_features

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
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
    """
    Predict points for upcoming NBA games.
    """
    # Load rolling CSV
    df_rolling = pd.read_csv(rolling_path)

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

    # Load upcoming games (join players to get team)
    df_next = pd.read_sql(
        """
        SELECT pg.player_id,
            pg.matchup,
            p.team_abbreviation,
            pg.game_date
        FROM player_game_stats pg
        JOIN players p ON pg.player_id = p.id
        WHERE pg.game_date > CURRENT_DATE
        ORDER BY pg.game_date
        """,
        engine,
    )

    # Fpor testing purposes - Using the latest played game as a proxy for "next game"
    # df_next = pd.read_sql(
    #     """
    #     SELECT DISTINCT ON (pg.player_id)
    #         pg.player_id,
    #         pg.matchup,
    #         p.team_abbreviation,
    #         pg.game_date
    #     FROM player_game_stats pg
    #     JOIN players p ON pg.player_id = p.id
    #     ORDER BY pg.player_id, pg.game_date DESC
    #     """,
    #     engine,
    # )

    if df_next.empty:
        print("No upcoming games.")
        return pd.DataFrame()

    # Build features
    df_next = compute_prediction_features(df_next, df_history)

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

    model = load_latest_model(models_dir)

    df_next["pred_points"] = model.predict(df_next[FEATURES])

    return df_next[
        ["player_id", "team_abbreviation", "matchup", "game_date", "pred_points"]
    ]
