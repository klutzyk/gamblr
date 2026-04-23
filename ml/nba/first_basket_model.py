from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss
from sqlalchemy import text
from xgboost import XGBClassifier


BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models"

TEAM_FEATURES = [
    "home_tip_win_rate_20",
    "away_tip_win_rate_20",
    "home_first_score_rate_20",
    "away_first_score_rate_20",
    "home_points_avg_10",
    "away_points_avg_10",
    "home_points_allowed_avg_10",
    "away_points_allowed_avg_10",
    "home_is_home",
]

PLAYER_FEATURES = [
    "is_center",
    "starter_slot",
    "player_points_avg_10",
    "player_minutes_avg_10",
    "player_first_basket_rate_20",
    "player_first_basket_rate_on_team_20",
    "team_first_score_rate_20",
    "opp_points_allowed_avg_10",
    "team_tip_win_rate_20",
]


def _rolling_team_rates(labels: pd.DataFrame, window: int = 20):
    rows = []
    for _, r in labels.sort_values("game_date").iterrows():
        rows.append(
            {
                "game_id": r["game_id"],
                "game_date": r["game_date"],
                "team_id": r["home_team_id"],
                "opponent_team_id": r["away_team_id"],
                "team_won_tip": 1 if r["winning_jump_ball_team_id"] == r["home_team_id"] else 0,
                "team_scored_first": 1 if r["first_scoring_team_id"] == r["home_team_id"] else 0,
            }
        )
        rows.append(
            {
                "game_id": r["game_id"],
                "game_date": r["game_date"],
                "team_id": r["away_team_id"],
                "opponent_team_id": r["home_team_id"],
                "team_won_tip": 1 if r["winning_jump_ball_team_id"] == r["away_team_id"] else 0,
                "team_scored_first": 1 if r["first_scoring_team_id"] == r["away_team_id"] else 0,
            }
        )
    team_df = pd.DataFrame(rows).sort_values(["team_id", "game_date"])
    if team_df.empty:
        return team_df
    team_df["tip_win_rate_20"] = team_df.groupby("team_id")["team_won_tip"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=3).mean()
    )
    team_df["first_score_rate_20"] = team_df.groupby("team_id")["team_scored_first"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=3).mean()
    )
    return team_df


def _load_team_points_metrics(engine):
    query = """
    SELECT game_id, game_date, team_id, points
    FROM team_game_stats
    WHERE game_date IS NOT NULL
      AND points IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["team_id", "game_date"])
    df["team_points_avg_10"] = df.groupby("team_id")["points"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=3).mean()
    )
    opp = df.rename(
        columns={
            "team_id": "opponent_team_id",
            "team_points_avg_10": "opp_points_allowed_avg_10",
            "points": "opp_points",
        }
    )[["game_id", "opponent_team_id", "opp_points_allowed_avg_10"]]
    merged = df.merge(opp, on="game_id", how="left")
    merged = merged[merged["team_id"] != merged["opponent_team_id"]]
    return merged[["game_id", "team_id", "team_points_avg_10", "opp_points_allowed_avg_10"]]


def _load_player_metrics(engine):
    query = """
    SELECT player_id, game_id, game_date, points, minutes
    FROM player_game_stats
    WHERE game_date IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["player_id", "game_date"])
    df["player_points_avg_10"] = df.groupby("player_id")["points"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=3).mean()
    )
    df["player_minutes_avg_10"] = df.groupby("player_id")["minutes"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=3).mean()
    )
    return df[["player_id", "game_id", "player_points_avg_10", "player_minutes_avg_10"]]


def _parse_ids(raw: str | None):
    if not raw:
        return []
    try:
        arr = json.loads(raw)
    except Exception:
        return []
    return [int(x) for x in arr if str(x).isdigit()]


def _build_training_frames(engine):
    labels = pd.read_sql(
        """
        SELECT game_id, game_date, home_team_id, away_team_id,
               first_scoring_team_id, first_scorer_player_id,
               winning_jump_ball_team_id, home_starter_ids_json, away_starter_ids_json
        FROM first_basket_labels
        WHERE is_valid_label = TRUE
          AND first_scoring_team_id IS NOT NULL
          AND first_scorer_player_id IS NOT NULL
        """,
        engine,
    )
    if labels.empty:
        return pd.DataFrame(), pd.DataFrame()
    labels["game_date"] = pd.to_datetime(labels["game_date"])

    team_rates = _rolling_team_rates(labels)
    team_metrics = _load_team_points_metrics(engine)
    player_metrics = _load_player_metrics(engine)

    # Team-level training set
    team_rows = []
    for _, row in labels.iterrows():
        home_id = int(row["home_team_id"])
        away_id = int(row["away_team_id"])
        game_id = row["game_id"]

        home_rate = team_rates[(team_rates["game_id"] == game_id) & (team_rates["team_id"] == home_id)]
        away_rate = team_rates[(team_rates["game_id"] == game_id) & (team_rates["team_id"] == away_id)]

        home_metrics = team_metrics[(team_metrics["game_id"] == game_id) & (team_metrics["team_id"] == home_id)]
        away_metrics = team_metrics[(team_metrics["game_id"] == game_id) & (team_metrics["team_id"] == away_id)]

        team_rows.append(
            {
                "game_id": game_id,
                "game_date": row["game_date"],
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_tip_win_rate_20": float(home_rate["tip_win_rate_20"].iloc[0]) if not home_rate.empty else np.nan,
                "away_tip_win_rate_20": float(away_rate["tip_win_rate_20"].iloc[0]) if not away_rate.empty else np.nan,
                "home_first_score_rate_20": float(home_rate["first_score_rate_20"].iloc[0]) if not home_rate.empty else np.nan,
                "away_first_score_rate_20": float(away_rate["first_score_rate_20"].iloc[0]) if not away_rate.empty else np.nan,
                "home_points_avg_10": float(home_metrics["team_points_avg_10"].iloc[0]) if not home_metrics.empty else np.nan,
                "away_points_avg_10": float(away_metrics["team_points_avg_10"].iloc[0]) if not away_metrics.empty else np.nan,
                "home_points_allowed_avg_10": float(home_metrics["opp_points_allowed_avg_10"].iloc[0]) if not home_metrics.empty else np.nan,
                "away_points_allowed_avg_10": float(away_metrics["opp_points_allowed_avg_10"].iloc[0]) if not away_metrics.empty else np.nan,
                "home_is_home": 1.0,
                "target_home_scores_first": 1 if int(row["first_scoring_team_id"]) == home_id else 0,
            }
        )
    team_train = pd.DataFrame(team_rows)

    # Player-level candidate set
    player_rates = {}
    player_team_rates = {}
    player_rows = []
    sorted_labels = labels.sort_values("game_date")
    for _, row in sorted_labels.iterrows():
        game_id = row["game_id"]
        home_id = int(row["home_team_id"])
        away_id = int(row["away_team_id"])
        first_scorer = int(row["first_scorer_player_id"])

        def add_candidates(starter_ids: list[int], team_id: int):
            for idx, pid in enumerate(starter_ids):
                player_rows.append(
                    {
                        "game_id": game_id,
                        "game_date": row["game_date"],
                        "team_id": team_id,
                        "player_id": pid,
                        "is_center": 1 if idx == 4 else 0,
                        "starter_slot": float(idx + 1),
                        "target_is_first_scorer": 1 if pid == first_scorer else 0,
                        "player_first_basket_rate_20": player_rates.get(pid, np.nan),
                        "player_first_basket_rate_on_team_20": player_team_rates.get((pid, team_id), np.nan),
                    }
                )

        add_candidates(_parse_ids(row["home_starter_ids_json"]), home_id)
        add_candidates(_parse_ids(row["away_starter_ids_json"]), away_id)

        # Update rates after creating current-row features.
        player_rates[first_scorer] = (
            (player_rates.get(first_scorer, 0.0) * 19.0 + 1.0) / 20.0
            if first_scorer in player_rates
            else 1.0
        )
        player_team_rates[(first_scorer, int(row["first_scoring_team_id"]))] = (
            (player_team_rates.get((first_scorer, int(row["first_scoring_team_id"])), 0.0) * 19.0 + 1.0) / 20.0
            if (first_scorer, int(row["first_scoring_team_id"])) in player_team_rates
            else 1.0
        )

    player_train = pd.DataFrame(player_rows)
    if not player_train.empty:
        player_train = player_train.merge(
            player_metrics,
            on=["game_id", "player_id"],
            how="left",
        )
        team_short = team_train[["game_id", "home_team_id", "away_team_id", "home_first_score_rate_20", "away_first_score_rate_20"]]
        player_train = player_train.merge(team_short, on="game_id", how="left")
        player_train["team_first_score_rate_20"] = np.where(
            player_train["team_id"] == player_train["home_team_id"],
            player_train["home_first_score_rate_20"],
            player_train["away_first_score_rate_20"],
        )
        points_metrics = team_metrics.rename(columns={"team_id": "team_id_lookup"})
        player_train = player_train.merge(
            points_metrics[["game_id", "team_id_lookup", "opp_points_allowed_avg_10"]],
            left_on=["game_id", "team_id"],
            right_on=["game_id", "team_id_lookup"],
            how="left",
        )
        team_tip = team_rates[["game_id", "team_id", "tip_win_rate_20"]].rename(columns={"tip_win_rate_20": "team_tip_win_rate_20"})
        player_train = player_train.merge(team_tip, on=["game_id", "team_id"], how="left")

    return team_train, player_train


def _time_split(df: pd.DataFrame, date_col: str = "game_date", ratio: float = 0.8):
    dates = sorted(pd.to_datetime(df[date_col]).dt.date.unique())
    split_idx = max(1, int(len(dates) * ratio))
    split_date = dates[min(split_idx, len(dates) - 1)]
    mask = pd.to_datetime(df[date_col]).dt.date <= split_date
    return mask


def train_first_basket_models(engine):
    team_train, player_train = _build_training_frames(engine)
    if team_train.empty or player_train.empty:
        raise ValueError("Not enough first_basket_labels data to train.")

    team_train[TEAM_FEATURES] = team_train[TEAM_FEATURES].fillna(0.0)
    player_train[PLAYER_FEATURES] = player_train[PLAYER_FEATURES].fillna(0.0)

    team_mask = _time_split(team_train)
    player_mask = _time_split(player_train)

    team_model = XGBClassifier(
        n_estimators=500,
        learning_rate=0.04,
        max_depth=4,
        subsample=0.9,
        colsample_bytree=0.9,
        min_child_weight=1,
        random_state=42,
        objective="binary:logistic",
    )
    team_model.fit(
        team_train.loc[team_mask, TEAM_FEATURES],
        team_train.loc[team_mask, "target_home_scores_first"],
    )

    player_model = XGBClassifier(
        n_estimators=700,
        learning_rate=0.035,
        max_depth=5,
        subsample=0.9,
        colsample_bytree=0.9,
        min_child_weight=1,
        random_state=42,
        objective="binary:logistic",
    )
    player_model.fit(
        player_train.loc[player_mask, PLAYER_FEATURES],
        player_train.loc[player_mask, "target_is_first_scorer"],
    )

    team_valid = team_train.loc[~team_mask]
    player_valid = player_train.loc[~player_mask]
    team_probs = team_model.predict_proba(team_valid[TEAM_FEATURES])[:, 1] if len(team_valid) else np.array([])
    player_probs = (
        player_model.predict_proba(player_valid[PLAYER_FEATURES])[:, 1] if len(player_valid) else np.array([])
    )

    team_metrics = {
        "brier": float(brier_score_loss(team_valid["target_home_scores_first"], team_probs)) if len(team_valid) else None,
        "log_loss": float(log_loss(team_valid["target_home_scores_first"], team_probs, labels=[0, 1])) if len(team_valid) else None,
    }
    player_metrics = {
        "brier": float(brier_score_loss(player_valid["target_is_first_scorer"], player_probs)) if len(player_valid) else None,
        "log_loss": float(log_loss(player_valid["target_is_first_scorer"], player_probs, labels=[0, 1])) if len(player_valid) else None,
    }

    MODELS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    team_path = MODELS_DIR / f"first_basket_team_model_{stamp}.pkl"
    player_path = MODELS_DIR / f"first_basket_player_model_{stamp}.pkl"
    meta_path = MODELS_DIR / f"first_basket_meta_{stamp}.pkl"

    joblib.dump(team_model, team_path)
    joblib.dump(player_model, player_path)
    joblib.dump(
        {
            "team_features": TEAM_FEATURES,
            "player_features": PLAYER_FEATURES,
            "team_metrics": team_metrics,
            "player_metrics": player_metrics,
            "model_version": stamp,
        },
        meta_path,
    )

    return {
        "team_model_path": str(team_path),
        "player_model_path": str(player_path),
        "meta_path": str(meta_path),
        "team_metrics": team_metrics,
        "player_metrics": player_metrics,
        "rows_team_train": int(team_mask.sum()),
        "rows_team_valid": int((~team_mask).sum()),
        "rows_player_train": int(player_mask.sum()),
        "rows_player_valid": int((~player_mask).sum()),
        "model_version": stamp,
    }


def _load_latest_model(prefix: str):
    paths = sorted(MODELS_DIR.glob(f"{prefix}*.pkl"))
    if not paths:
        raise FileNotFoundError(f"No first-basket model found for prefix {prefix}.")
    return joblib.load(paths[-1]), paths[-1]


def predict_first_basket_with_models(
    engine,
    lineups_payload: dict[str, Any],
    points_df: pd.DataFrame,
):
    team_model, team_path = _load_latest_model("first_basket_team_model_")
    player_model, _ = _load_latest_model("first_basket_player_model_")
    meta, meta_path = _load_latest_model("first_basket_meta_")
    team_features = meta["team_features"]
    player_features = meta["player_features"]

    team_rates = pd.read_sql(
        """
        SELECT team_id, AVG(team_won_tip::int) AS tip_win_rate_20, AVG(team_scored_first::int) AS first_score_rate_20
        FROM (
            SELECT home_team_id AS team_id,
                   (winning_jump_ball_team_id = home_team_id) AS team_won_tip,
                   (first_scoring_team_id = home_team_id) AS team_scored_first,
                   game_date,
                   ROW_NUMBER() OVER (PARTITION BY home_team_id ORDER BY game_date DESC) AS rn
            FROM first_basket_labels
            WHERE is_valid_label = TRUE
            UNION ALL
            SELECT away_team_id AS team_id,
                   (winning_jump_ball_team_id = away_team_id) AS team_won_tip,
                   (first_scoring_team_id = away_team_id) AS team_scored_first,
                   game_date,
                   ROW_NUMBER() OVER (PARTITION BY away_team_id ORDER BY game_date DESC) AS rn
            FROM first_basket_labels
            WHERE is_valid_label = TRUE
        ) t
        WHERE rn <= 20
        GROUP BY team_id
        """,
        engine,
    )
    team_rate_map = {
        int(r["team_id"]): {"tip": float(r["tip_win_rate_20"] or 0.5), "first": float(r["first_score_rate_20"] or 0.5)}
        for _, r in team_rates.iterrows()
    }

    team_points = pd.read_sql(
        """
        SELECT team_id, AVG(points) AS avg_points
        FROM (
            SELECT team_id, points, game_date,
                   ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date DESC) AS rn
            FROM team_game_stats
            WHERE points IS NOT NULL
        ) x
        WHERE rn <= 10
        GROUP BY team_id
        """,
        engine,
    )
    team_points_map = {int(r["team_id"]): float(r["avg_points"] or 110.0) for _, r in team_points.iterrows()}

    player_recent = pd.read_sql(
        """
        SELECT player_id, AVG(points) AS avg_points, AVG(minutes) AS avg_minutes
        FROM (
            SELECT player_id, points, minutes, game_date,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
            FROM player_game_stats
            WHERE points IS NOT NULL
        ) y
        WHERE rn <= 10
        GROUP BY player_id
        """,
        engine,
    )
    player_recent_map = {
        int(r["player_id"]): {
            "points": float(r["avg_points"] or 8.0),
            "minutes": float(r["avg_minutes"] or 20.0),
        }
        for _, r in player_recent.iterrows()
    }

    points_map = {}
    if not points_df.empty:
        points_map = {
            int(pid): float(pred)
            for pid, pred in zip(points_df["player_id"], points_df["pred_value"])
            if pd.notnull(pid) and pd.notnull(pred)
        }

    output = []
    for game in lineups_payload.get("games", []):
        home_abbr = game.get("home_team_abbr")
        away_abbr = game.get("away_team_abbr")
        home_team_id = None
        away_team_id = None
        for side, abbr in [("home", home_abbr), ("away", away_abbr)]:
            starters = game.get(side, {}).get("starters", [])
            for s in starters:
                if s.get("resolved_player_id") is not None and s.get("team_id") is not None:
                    if side == "home":
                        home_team_id = s.get("team_id")
                    else:
                        away_team_id = s.get("team_id")

        # fallback from game payload if team ids were attached elsewhere
        home_team_id = home_team_id or game.get("home_team_id")
        away_team_id = away_team_id or game.get("away_team_id")
        if home_team_id is None or away_team_id is None:
            # Can still infer with neutral defaults.
            home_team_id = int(home_team_id or -1)
            away_team_id = int(away_team_id or -2)

        tr_home = team_rate_map.get(int(home_team_id), {"tip": 0.5, "first": 0.5})
        tr_away = team_rate_map.get(int(away_team_id), {"tip": 0.5, "first": 0.5})

        team_row = pd.DataFrame(
            [
                {
                    "home_tip_win_rate_20": tr_home["tip"],
                    "away_tip_win_rate_20": tr_away["tip"],
                    "home_first_score_rate_20": tr_home["first"],
                    "away_first_score_rate_20": tr_away["first"],
                    "home_points_avg_10": team_points_map.get(int(home_team_id), 110.0),
                    "away_points_avg_10": team_points_map.get(int(away_team_id), 110.0),
                    "home_points_allowed_avg_10": team_points_map.get(int(away_team_id), 110.0),
                    "away_points_allowed_avg_10": team_points_map.get(int(home_team_id), 110.0),
                    "home_is_home": 1.0,
                }
            ]
        )
        p_home_first = float(team_model.predict_proba(team_row[team_features])[:, 1][0])
        p_away_first = 1.0 - p_home_first

        per_team = {"home": [], "away": []}
        for side in ["home", "away"]:
            starters = game.get(side, {}).get("starters", [])
            team_id = int(home_team_id if side == "home" else away_team_id)
            team_abbr = home_abbr if side == "home" else away_abbr
            for idx, starter in enumerate(starters):
                pid = starter.get("resolved_player_id")
                if pid is None:
                    continue
                pid = int(pid)
                recent = player_recent_map.get(pid, {"points": 8.0, "minutes": 20.0})
                pred_points = points_map.get(pid, recent["points"])
                row = {
                    "is_center": 1.0 if (starter.get("position") or "").upper() == "C" else 0.0,
                    "starter_slot": float(idx + 1),
                    "player_points_avg_10": float(pred_points),
                    "player_minutes_avg_10": float(recent["minutes"]),
                    "player_first_basket_rate_20": 0.05,
                    "player_first_basket_rate_on_team_20": 0.05,
                    "team_first_score_rate_20": tr_home["first"] if side == "home" else tr_away["first"],
                    "opp_points_allowed_avg_10": team_points_map.get(
                        int(away_team_id if side == "home" else home_team_id), 110.0
                    ),
                    "team_tip_win_rate_20": tr_home["tip"] if side == "home" else tr_away["tip"],
                }
                score = float(
                    player_model.predict_proba(pd.DataFrame([row])[player_features])[:, 1][0]
                )
                play_pct = float(starter.get("play_pct") or 100.0) / 100.0
                status_mult = 1.0 if (game.get(side, {}).get("status") == "confirmed") else 0.92
                adjusted = score * play_pct * status_mult
                per_team[side].append(
                    {
                        "game_id": game.get("game_id"),
                        "game_date": game.get("game_date"),
                        "matchup": game.get("matchup"),
                        "tipoff_et": game.get("tipoff_et"),
                        "team_side": side,
                        "team_id": team_id if team_id > 0 else None,
                        "team_abbreviation": team_abbr,
                        "lineup_status": game.get(side, {}).get("status"),
                        "player_id": pid,
                        "full_name": starter.get("resolved_full_name") or starter.get("name"),
                        "position": starter.get("position"),
                        "raw_player_score": adjusted,
                    }
                )

        for side in ["home", "away"]:
            team_sum = sum(r["raw_player_score"] for r in per_team[side])
            if team_sum <= 0:
                continue
            team_first = p_home_first if side == "home" else p_away_first
            for r in per_team[side]:
                share = r["raw_player_score"] / team_sum
                r["team_scores_first_prob"] = round(float(team_first), 4)
                r["player_share_on_team"] = round(float(share), 4)
                r["first_basket_prob"] = round(float(team_first * share), 4)
                r["model_version"] = meta.get("model_version") or meta_path.name
                output.append(r)

    output.sort(key=lambda x: (x.get("matchup") or "", -x["first_basket_prob"]))
    return {
        "model_version": meta.get("model_version") or meta_path.name,
        "team_model_path": str(team_path),
        "meta_path": str(meta_path),
        "data": output,
    }
