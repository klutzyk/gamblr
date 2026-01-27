# ml/predict.py
import pandas as pd
import numpy as np
from app.core.constants import (
    CONFIDENCE_DEFAULT,
    CONFIDENCE_MIN,
    CONFIDENCE_MAX,
    CONFIDENCE_DECAY,
    CONFIDENCE_WINDOW,
    CONFIDENCE_OVER_PENALTY,
    CONFIDENCE_UNDER_PENALTY,
)
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


def load_latest_model(models_dir: Path, prefix: str, return_path: bool = False):
    models = sorted(models_dir.glob(f"{prefix}*.pkl"))
    if not models:
        raise FileNotFoundError(f"No trained models found for prefix {prefix}.")
    path = models[-1]
    model = joblib.load(path)
    return (model, path) if return_path else model


def _predict_stat(
    engine,
    day: str,
    features: list,
    model_prefix: str,
    stat_type: str,
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
            players["game_id"] = game.get("game_id")
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

    model, model_path = load_latest_model(models_dir, model_prefix, return_path=True)

    if isinstance(model, dict) and "models" in model:
        models = model["models"]
        preds_stack = np.column_stack(
            [m.predict(df_next_features[features]) for m in models]
        )
        pred_p50 = np.percentile(preds_stack, 50, axis=1)
        df_next_features["pred_p50"] = pred_p50

        calibration = model.get("calibration")
        if calibration and calibration.get("abs_error_q") is not None:
            q = float(calibration["abs_error_q"])
            recent_errors = _load_recent_player_errors(
                engine, stat_type, df_next_features["player_id"].tolist()
            )

            def adjust_q(pid):
                player_mae = recent_errors.get(pid)
                if player_mae is None or player_mae <= 0:
                    return q, CONFIDENCE_DEFAULT

                band = max(0.5 * q, min(2.0 * q, float(player_mae)))
                confidence = int(
                    CONFIDENCE_MAX
                    * np.exp(-CONFIDENCE_DECAY * float(player_mae))
                )
                confidence = max(CONFIDENCE_MIN, min(CONFIDENCE_MAX, confidence))
                return band, confidence

            qs, confs = zip(
                *[
                    adjust_q(int(pid))
                    for pid in df_next_features["player_id"].fillna(0).tolist()
                ]
            )
            df_next_features["pred_p10"] = np.maximum(pred_p50 - np.array(qs), 0)
            df_next_features["pred_p90"] = np.maximum(pred_p50 + np.array(qs), 0)
            df_next_features["confidence"] = confs
        else:
            df_next_features["pred_p10"] = np.percentile(preds_stack, 10, axis=1)
            df_next_features["pred_p90"] = np.percentile(preds_stack, 90, axis=1)

        df_next_features["pred_value"] = pred_p50
        df_next_features["model_version"] = model_path.name
    else:
        df_next_features["pred_value"] = model.predict(df_next_features[features])
        df_next_features["model_version"] = model_path.name

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
            "game_id",
            "pred_value",
            "pred_p10",
            "pred_p50",
            "pred_p90",
            "confidence",
            "model_version",
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
        "xgb_points_ensemble_",
        "points",
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
        "xgb_assists_ensemble_",
        "assists",
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
        "xgb_rebounds_ensemble_",
        "rebounds",
        models_dir,
        rolling_path,
    )


def _load_recent_player_errors(
    engine, stat_type: str, player_ids: list, n: int = CONFIDENCE_WINDOW
):
    if not player_ids:
        return {}

    ids = ",".join(str(int(pid)) for pid in set(player_ids))
    query = f"""
    SELECT player_id, pred_value, actual_value, abs_error, game_date
    FROM prediction_logs
    WHERE stat_type = '{stat_type}'
      AND actual_value IS NOT NULL
      AND player_id IN ({ids})
    ORDER BY game_date DESC
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return {}

    df = df.groupby("player_id").head(n)
    over_mask = df["pred_value"] > df["actual_value"]
    df["weighted_error"] = df["abs_error"]
    df.loc[over_mask, "weighted_error"] = (
        df.loc[over_mask, "abs_error"] * CONFIDENCE_OVER_PENALTY
    )
    df.loc[~over_mask, "weighted_error"] = (
        df.loc[~over_mask, "abs_error"] * CONFIDENCE_UNDER_PENALTY
    )
    return df.groupby("player_id")["weighted_error"].mean().to_dict()
