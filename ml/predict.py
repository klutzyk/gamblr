# ml/predict.py
import pandas as pd
import joblib
from pathlib import Path
from .utils import (
    POINTS_FEATURES,
    ASSISTS_FEATURES,
    REBOUNDS_FEATURES,
    MINUTES_FEATURES,
    compute_prediction_features,
)
from datetime import datetime, timedelta

# ml folder
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"


def load_latest_model(models_dir: Path, prefix: str):
    models = sorted(models_dir.glob(f"{prefix}*.pkl"))
    if not models:
        raise FileNotFoundError(f"No trained models found for prefix {prefix}.")
    return joblib.load(models[-1])


def _predict_stat(
    engine,
    day: str,
    features: list,
    model_prefix: str,
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
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

    df_next_games = df_schedule[df_schedule["game_date"] == target_date]

    if df_next_games.empty:
        print(f"No games found for NBA date: {target_date.date()}")
        return pd.DataFrame()

    rows = []
    for _, game in df_next_games.iterrows():
        for team_abbr in [game["home_team_abbr"], game["away_team_abbr"]]:
            players = df_rolling[df_rolling["team_abbreviation"] == team_abbr].copy()
            players["matchup"] = game["matchup"]
            players["game_date"] = game["game_date"]
            rows.append(players)

    df_next_players = pd.concat(rows, ignore_index=True)

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

    df_next_features = compute_prediction_features(
        df_next_players, df_history, df_team, df_lineups
    )

    if "pred_minutes" in features:
        minutes_model = load_latest_model(models_dir, "xgb_minutes_model_")
        df_next_features["pred_minutes"] = minutes_model.predict(
            df_next_features[MINUTES_FEATURES]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
        )

    df_next_features[features] = (
        df_next_features[features].apply(pd.to_numeric, errors="coerce").fillna(0)
    )

    model = load_latest_model(models_dir, model_prefix)

    df_next_features["pred_value"] = model.predict(df_next_features[features])

    df_players = pd.read_sql(
        "SELECT id AS player_id, full_name FROM players",
        engine,
    )

    df_next_features = df_next_features.merge(
        df_players,
        on="player_id",
        how="left",
    )

    return df_next_features[
        [
            "player_id",
            "full_name",
            "team_abbreviation",
            "matchup",
            "game_date",
            "pred_value",
        ]
    ]


def predict_points(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
    return _predict_stat(
        engine,
        day,
        POINTS_FEATURES,
        "xgb_points_model_",
        models_dir,
        rolling_path,
    )


def predict_assists(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
    return _predict_stat(
        engine,
        day,
        ASSISTS_FEATURES,
        "xgb_assists_model_",
        models_dir,
        rolling_path,
    )


def predict_rebounds(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
):
    return _predict_stat(
        engine,
        day,
        REBOUNDS_FEATURES,
        "xgb_rebounds_model_",
        models_dir,
        rolling_path,
    )
