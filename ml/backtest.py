import pandas as pd
import numpy as np
from app.core.constants import (
    CONFIDENCE_DEFAULT,
    CONFIDENCE_MIN,
    CONFIDENCE_MAX,
    CONFIDENCE_DECAY,
    CONFIDENCE_WINDOW,
)
from datetime import date
from xgboost import XGBRegressor
from .utils import (
    POINTS_FEATURES,
    ASSISTS_FEATURES,
    REBOUNDS_FEATURES,
    add_player_rolling_features,
    build_team_game_features,
    build_lineup_team_features,
)
from app.core.constants import WALK_FORWARD_MIN_GAMES


def _get_features_for_stat(stat_type: str):
    if stat_type == "points":
        return POINTS_FEATURES
    if stat_type == "assists":
        return ASSISTS_FEATURES
    if stat_type == "rebounds":
        return REBOUNDS_FEATURES
    raise ValueError("stat_type must be one of: points, assists, rebounds")


def walk_forward_backtest(
    engine,
    stat_type: str,
    min_games: int = WALK_FORWARD_MIN_GAMES,
    max_dates: int | None = None,
):
    features = _get_features_for_stat(stat_type)

    df_raw = pd.read_sql(
        """
        SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
               pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
        FROM player_game_stats pg
        JOIN players p ON pg.player_id = p.id
        """,
        engine,
    )
    if df_raw.empty:
        return pd.DataFrame()

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
        lineup_team = build_lineup_team_features(df_lineups)
        df_features = df_features.merge(lineup_team, on="team_abbreviation", how="left")

    df_features["pred_minutes"] = df_features["avg_minutes_last5"]

    for col in features:
        if col not in df_features.columns:
            df_features[col] = 0

    df_features = df_features.dropna(subset=[stat_type, "game_date"])
    df_features = df_features.sort_values("game_date")

    df_features = df_features[df_features["games_played_season"] >= min_games]

    unique_dates = df_features["game_date"].dt.date.unique().tolist()
    if max_dates:
        unique_dates = unique_dates[:max_dates]

    preds_all = []
    errors_by_player: dict[int, list[float]] = {}
    global_errors: list[float] = []

    for d in unique_dates:
        train_mask = df_features["game_date"].dt.date < d
        test_mask = df_features["game_date"].dt.date == d

        if train_mask.sum() < 50 or test_mask.sum() == 0:
            continue

        X_train = df_features.loc[train_mask, features].fillna(0)
        y_train = df_features.loc[train_mask, stat_type]

        X_test = df_features.loc[test_mask, features].fillna(0)

        model = XGBRegressor(
            n_estimators=600,
            learning_rate=0.05,
            max_depth=5,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=2,
            reg_alpha=0.0,
            reg_lambda=1.0,
            random_state=42,
        )
        model.fit(X_train, y_train, verbose=False)

        pred = model.predict(X_test)
        actual = df_features.loc[test_mask, stat_type].to_numpy()
        abs_error = np.abs(actual - pred)

        # Compute rolling confidence using prior per-player errors
        confidences = []
        bands = []
        for pid, err in zip(df_features.loc[test_mask, "player_id"], abs_error):
            pid = int(pid)
            hist = errors_by_player.get(pid, [])
            if not hist:
                confidences.append(CONFIDENCE_DEFAULT)
                bands.append(None)
            else:
                recent = hist[-CONFIDENCE_WINDOW:]
                player_mae = float(np.mean(recent))
                player_q = float(np.quantile(recent, 0.8))
                band = max(0.5 * player_q, min(2.0 * player_q, player_mae))
                confidence = int(
                    CONFIDENCE_MAX
                    * np.exp(-CONFIDENCE_DECAY * float(player_mae))
                )
                confidence = max(CONFIDENCE_MIN, min(CONFIDENCE_MAX, confidence))
                confidences.append(confidence)
                bands.append(band)

        # Update error history after confidence computed
        for pid, err in zip(df_features.loc[test_mask, "player_id"], abs_error):
            pid = int(pid)
            errors_by_player.setdefault(pid, []).append(float(err))
            global_errors.append(float(err))

        df_pred = df_features.loc[test_mask, [
            "player_id",
            "game_id",
            "game_date",
        ]].copy()
        df_pred["pred_value"] = pred
        df_pred["pred_p50"] = pred
        df_pred["pred_p10"] = None
        df_pred["pred_p90"] = None
        for idx, band in zip(df_pred.index, bands):
            if band is None:
                continue
            df_pred.at[idx, "pred_p10"] = max(float(df_pred.at[idx, "pred_value"]) - band, 0)
            df_pred.at[idx, "pred_p90"] = float(df_pred.at[idx, "pred_value"]) + band
        df_pred["confidence"] = confidences
        df_pred["actual_value"] = actual
        df_pred["abs_error"] = abs_error
        df_pred["stat_type"] = stat_type
        df_pred["prediction_date"] = pd.to_datetime(d)
        df_pred["model_version"] = f"walkforward_{stat_type}"
        preds_all.append(df_pred)

    if not preds_all:
        return pd.DataFrame()

    return pd.concat(preds_all, ignore_index=True)
