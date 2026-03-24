# ml/predict.py
import pandas as pd
import numpy as np
from sqlalchemy import text
import gc
import os
from app.core.constants import (
    CONFIDENCE_DEFAULT,
    CONFIDENCE_MIN,
    CONFIDENCE_MAX,
    CONFIDENCE_DECAY,
    CONFIDENCE_WINDOW,
    CONFIDENCE_OVER_PENALTY,
    # CONFIDENCE_UNDER_PENALTY,
    CONFIDENCE_UNDER_BONUS,
)
import joblib
from pathlib import Path
from .utils import (
    POINTS_FEATURES,
    ASSISTS_FEATURES,
    REBOUNDS_FEATURES,
    MINUTES_FEATURES,
    THREEPT_FEATURES,
    THREEPA_FEATURES,
    compute_prediction_features,
)
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9
    ZoneInfo = None

# ml folder
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
# Keep windows broad by default to preserve model behavior accuracy.
# Memory savings still come from scoping queries to teams on the target slate.
HISTORY_WINDOW_DAYS = int(os.getenv("PRED_HISTORY_WINDOW_DAYS", "1600"))
TEAM_STATS_WINDOW_DAYS = int(os.getenv("PRED_TEAM_STATS_WINDOW_DAYS", "1200"))
_MODEL_CACHE: dict[str, tuple[str, object]] = {}
_PLAYER_NAME_CACHE: dict[int, str] | None = None
_TEAM_ID_CACHE: dict[str, int] | None = None


def load_latest_model(models_dir: Path, prefix: str, return_path: bool = False):
    models = sorted(models_dir.glob(f"{prefix}*.pkl"))
    if not models:
        raise FileNotFoundError(f"No trained models found for prefix {prefix}.")
    path = models[-1]
    cache_key = f"{models_dir.resolve()}::{prefix}"
    cache_entry = _MODEL_CACHE.get(cache_key)
    if cache_entry and cache_entry[0] == path.name:
        model = cache_entry[1]
    else:
        model = joblib.load(path)
        _MODEL_CACHE[cache_key] = (path.name, model)
    return (model, path) if return_path else model


def _load_reference_maps(engine):
    global _PLAYER_NAME_CACHE, _TEAM_ID_CACHE
    if _PLAYER_NAME_CACHE is None:
        df_players = pd.read_sql(
            "SELECT id AS player_id, full_name FROM players",
            engine,
        )
        _PLAYER_NAME_CACHE = {
            int(r.player_id): str(r.full_name)
            for r in df_players.itertuples(index=False)
            if pd.notnull(r.player_id)
        }
    if _TEAM_ID_CACHE is None:
        df_teams = pd.read_sql(
            "SELECT id AS team_id, abbreviation AS team_abbreviation FROM teams",
            engine,
        )
        _TEAM_ID_CACHE = {
            str(r.team_abbreviation).upper(): int(r.team_id)
            for r in df_teams.itertuples(index=False)
            if pd.notnull(r.team_id) and pd.notnull(r.team_abbreviation)
        }
    return _PLAYER_NAME_CACHE, _TEAM_ID_CACHE


def _build_model_input(
    df_features: pd.DataFrame, fallback_features: list[str], model
) -> pd.DataFrame:
    """Align inference columns to the exact feature schema a model was trained with."""
    trained_features = None

    if hasattr(model, "get_booster"):
        try:
            trained_features = model.get_booster().feature_names
        except Exception:
            trained_features = None

    if not trained_features and hasattr(model, "feature_names_in_"):
        try:
            trained_features = list(model.feature_names_in_)
        except Exception:
            trained_features = None

    feature_cols = list(trained_features) if trained_features else list(fallback_features)
    return df_features.reindex(columns=feature_cols, fill_value=0).apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0)


def _predict_stat(
    engine,
    day: str,
    features: list,
    model_prefix: str,
    stat_type: str,
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    # Load rolling CSV
    df_rolling = pd.read_csv(rolling_path)
    df_rolling["game_date"] = pd.to_datetime(
        df_rolling["game_date"], format="%Y-%m-%d", errors="coerce"
    )
    df_rolling["team_abbreviation"] = (
        df_rolling["team_abbreviation"].astype(str).str.upper()
    )

    if ZoneInfo:
        base_date = datetime.now(ZoneInfo("America/New_York")).date()
    else:
        base_date = datetime.now().date()

    if day == "today":
        target_date = base_date
    elif day == "tomorrow":
        target_date = base_date + timedelta(days=1)
    elif day == "yesterday":
        target_date = base_date - timedelta(days=1)
    elif day == "two_days_ago":
        target_date = base_date - timedelta(days=2)
    elif day == "auto":
        # If it's afternoon/evening in Australia, switch to NBA "tomorrow" (ET)
        if ZoneInfo:
            aus_time = datetime.now(ZoneInfo("Australia/Sydney")).hour
            target_date = base_date if aus_time >= 17 else base_date - timedelta(days=1)
        else:
            target_date = base_date
    else:
        raise ValueError("day must be one of: today, tomorrow, yesterday, two_days_ago, auto")

    target_date = pd.to_datetime(target_date)
    target_date_only = target_date.date()

    # Get upcoming games from schedule for just the target day.
    df_next_games = pd.read_sql(
        text(
            """
            SELECT game_id, game_date, matchup, home_team_abbr, away_team_abbr
            FROM game_schedule
            WHERE game_date = :target_date
            """
        ),
        engine,
        params={"target_date": target_date_only},
    )
    df_next_games["game_date"] = pd.to_datetime(df_next_games["game_date"])

    if df_next_games.empty:
        print(f"No games found for NBA date: {target_date.date()}")
        return pd.DataFrame()

    next_team_abbrs = {
        str(team_abbr).upper()
        for col in ("home_team_abbr", "away_team_abbr")
        for team_abbr in df_next_games[col].tolist()
        if pd.notnull(team_abbr)
    }
    if not next_team_abbrs:
        return pd.DataFrame()

    team_params = {f"team_{idx}": team for idx, team in enumerate(sorted(next_team_abbrs))}
    team_placeholders = ", ".join(f":{name}" for name in team_params)
    history_start_date = (target_date - timedelta(days=HISTORY_WINDOW_DAYS)).date()
    team_stats_start_date = (target_date - timedelta(days=TEAM_STATS_WINDOW_DAYS)).date()

    # Load historical games only for target-day teams and recent window.
    df_history = pd.read_sql(
        text(
            f"""
            SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
                   pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers,
                   pg.fgm, pg.fga, pg.fg3m, pg.fg3a
            FROM player_game_stats pg
            JOIN players p ON pg.player_id = p.id
            WHERE pg.game_date < :target_date
              AND pg.game_date >= :history_start_date
              AND p.team_abbreviation IN ({team_placeholders})
            """
        ),
        engine,
        params={
            "target_date": target_date_only,
            "history_start_date": history_start_date,
            **team_params,
        },
    )
    df_history["game_date"] = pd.to_datetime(df_history["game_date"])
    df_history["team_abbreviation"] = (
        df_history["team_abbreviation"].astype(str).str.upper()
    )

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
            text(
                f"""
                SELECT game_id, team_abbreviation, game_date,
                       points AS team_points, assists AS team_assists, rebounds AS team_rebounds,
                       fgm, fga, fg3m, fg3a
                FROM team_game_stats
                WHERE game_date < :target_date
                  AND game_date >= :team_stats_start_date
                  AND team_abbreviation IN ({team_placeholders})
                """
            ),
            engine,
            params={
                "target_date": target_date_only,
                "team_stats_start_date": team_stats_start_date,
                **team_params,
            },
        )
        if not df_team.empty:
            df_team["game_date"] = pd.to_datetime(df_team["game_date"])
            df_team["team_abbreviation"] = df_team["team_abbreviation"].astype(str).str.upper()
    except Exception:
        df_team = None

    df_lineups = None
    try:
        df_lineups = pd.read_sql(
            text(
                f"""
                SELECT ls.team_id, ls.season, ls.lineup_id, ls.minutes, ls.off_rating,
                       ls.def_rating, ls.net_rating, ls.pace, ls.ast_pct, ls.reb_pct,
                       t.abbreviation AS team_abbreviation
                FROM lineup_stats ls
                JOIN teams t ON ls.team_id = t.id
                WHERE t.abbreviation IN ({team_placeholders})
                """
            ),
            engine,
            params=team_params,
        )
    except Exception:
        df_lineups = None

    df_next_features = compute_prediction_features(
        df_next_players,
        df_history,
        df_team,
        df_lineups,
        expected_players_by_team=expected_players_by_team,
        excluded_players_by_team=excluded_players_by_team,
        bench_minutes_threshold=bench_minutes_threshold,
    )

    if "pred_minutes" in features:
        minutes_model = load_latest_model(models_dir, "xgb_minutes_model_")
        minutes_input = _build_model_input(
            df_next_features, MINUTES_FEATURES, minutes_model
        )
        df_next_features["pred_minutes"] = minutes_model.predict(minutes_input)

    df_next_features[features] = (
        df_next_features[features].apply(pd.to_numeric, errors="coerce").fillna(0)
    )

    model, model_path = load_latest_model(models_dir, model_prefix, return_path=True)

    if isinstance(model, dict) and "models" in model:
        models = model["models"]
        preds_stack = np.column_stack(
            [m.predict(_build_model_input(df_next_features, features, m)) for m in models]
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
                stats = recent_errors.get(pid)
                if not stats:
                    return q, CONFIDENCE_DEFAULT

                mean_abs = stats["mean_abs"]
                mean_weighted = stats["mean_weighted"]
                band = max(0.5 * q, min(2.0 * q, float(mean_abs)))
                confidence = int(
                    CONFIDENCE_MAX * np.exp(-CONFIDENCE_DECAY * float(mean_weighted))
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
        model_input = _build_model_input(df_next_features, features, model)
        df_next_features["pred_value"] = model.predict(model_input)
        df_next_features["model_version"] = model_path.name

    player_name_map, team_id_map = _load_reference_maps(engine)
    df_next_features["full_name"] = df_next_features["player_id"].map(player_name_map)
    df_next_features["team_id"] = (
        df_next_features["team_abbreviation"].astype(str).str.upper().map(team_id_map)
    )

    result = df_next_features[
        [
            "player_id",
            "full_name",
            "team_id",
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
    del df_history, df_next_games, df_next_players, df_next_features, df_team, df_lineups
    gc.collect()
    return result


def predict_points(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    return _predict_stat(
        engine,
        day,
        POINTS_FEATURES,
        "xgb_points_ensemble_",
        "points",
        models_dir,
        rolling_path,
        expected_players_by_team,
        excluded_players_by_team,
        bench_minutes_threshold,
    )


def predict_assists(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    return _predict_stat(
        engine,
        day,
        ASSISTS_FEATURES,
        "xgb_assists_ensemble_",
        "assists",
        models_dir,
        rolling_path,
        expected_players_by_team,
        excluded_players_by_team,
        bench_minutes_threshold,
    )


def predict_rebounds(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    return _predict_stat(
        engine,
        day,
        REBOUNDS_FEATURES,
        "xgb_rebounds_ensemble_",
        "rebounds",
        models_dir,
        rolling_path,
        expected_players_by_team,
        excluded_players_by_team,
        bench_minutes_threshold,
    )


def predict_threept(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    return _predict_stat(
        engine,
        day,
        THREEPT_FEATURES,
        "xgb_threes_ensemble_",
        "threept",
        models_dir,
        rolling_path,
        expected_players_by_team,
        excluded_players_by_team,
        bench_minutes_threshold,
    )


def predict_threepa(
    engine,
    day: str = "today",
    models_dir: Path = MODELS_DIR,
    rolling_path: Path = DATA_DIR / "player_stats_rolling.csv",
    expected_players_by_team: dict | None = None,
    excluded_players_by_team: dict | None = None,
    bench_minutes_threshold: float | None = 12.0,
):
    return _predict_stat(
        engine,
        day,
        THREEPA_FEATURES,
        "xgb_threepa_ensemble_",
        "threepa",
        models_dir,
        rolling_path,
        expected_players_by_team,
        excluded_players_by_team,
        bench_minutes_threshold,
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
        df.loc[~over_mask, "abs_error"] * -CONFIDENCE_UNDER_BONUS
    )
    grouped = df.groupby("player_id")
    return {
        int(pid): {
            "mean_abs": float(group["abs_error"].mean()),
            "mean_weighted": float(group["weighted_error"].mean()),
        }
        for pid, group in grouped
    }
