import json
from datetime import datetime, timezone

from sqlalchemy import text


def upsert_first_basket_labels(engine, rows: list[dict]):
    if not rows:
        return 0

    stmt = text(
        """
        INSERT INTO first_basket_labels (
            game_id, game_date, season,
            home_team_id, away_team_id, home_team_abbr, away_team_abbr,
            first_scoring_team_id, first_scoring_team_abbr, first_scorer_player_id, first_scorer_name,
            first_score_event_num, first_score_seconds, first_score_action_type, first_score_description,
            winning_jump_ball_team_id, winning_jump_ball_team_abbr,
            jump_ball_home_player_id, jump_ball_away_player_id, jump_ball_winner_player_id,
            home_starter_ids_json, away_starter_ids_json,
            is_valid_label, source, created_at, updated_at
        )
        VALUES (
            :game_id, :game_date, :season,
            :home_team_id, :away_team_id, :home_team_abbr, :away_team_abbr,
            :first_scoring_team_id, :first_scoring_team_abbr, :first_scorer_player_id, :first_scorer_name,
            :first_score_event_num, :first_score_seconds, :first_score_action_type, :first_score_description,
            :winning_jump_ball_team_id, :winning_jump_ball_team_abbr,
            :jump_ball_home_player_id, :jump_ball_away_player_id, :jump_ball_winner_player_id,
            :home_starter_ids_json, :away_starter_ids_json,
            :is_valid_label, :source, :created_at, :updated_at
        )
        ON CONFLICT (game_id)
        DO UPDATE SET
            game_date = EXCLUDED.game_date,
            season = EXCLUDED.season,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_team_abbr = EXCLUDED.home_team_abbr,
            away_team_abbr = EXCLUDED.away_team_abbr,
            first_scoring_team_id = EXCLUDED.first_scoring_team_id,
            first_scoring_team_abbr = EXCLUDED.first_scoring_team_abbr,
            first_scorer_player_id = EXCLUDED.first_scorer_player_id,
            first_scorer_name = EXCLUDED.first_scorer_name,
            first_score_event_num = EXCLUDED.first_score_event_num,
            first_score_seconds = EXCLUDED.first_score_seconds,
            first_score_action_type = EXCLUDED.first_score_action_type,
            first_score_description = EXCLUDED.first_score_description,
            winning_jump_ball_team_id = EXCLUDED.winning_jump_ball_team_id,
            winning_jump_ball_team_abbr = EXCLUDED.winning_jump_ball_team_abbr,
            jump_ball_home_player_id = EXCLUDED.jump_ball_home_player_id,
            jump_ball_away_player_id = EXCLUDED.jump_ball_away_player_id,
            jump_ball_winner_player_id = EXCLUDED.jump_ball_winner_player_id,
            home_starter_ids_json = EXCLUDED.home_starter_ids_json,
            away_starter_ids_json = EXCLUDED.away_starter_ids_json,
            is_valid_label = EXCLUDED.is_valid_label,
            source = EXCLUDED.source,
            updated_at = EXCLUDED.updated_at
        """
    )

    now = datetime.now(timezone.utc)
    pred_date = now.date()
    with engine.begin() as conn:
        for row in rows:
            payload = {
                **row,
                "home_starter_ids_json": json.dumps(row.get("home_starter_ids") or []),
                "away_starter_ids_json": json.dumps(row.get("away_starter_ids") or []),
                "created_at": now,
                "updated_at": now,
            }
            conn.execute(stmt, payload)
    return len(rows)


def upsert_first_basket_prediction_logs(engine, rows: list[dict], model_version: str | None):
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    pred_date = now.date()
    stmt = text(
        """
        INSERT INTO first_basket_prediction_logs (
            game_id, game_date, prediction_date,
            player_id, team_id, team_abbreviation,
            first_basket_prob, team_scores_first_prob, player_share_on_team,
            model_version, lineup_status, created_at, updated_at
        )
        VALUES (
            :game_id, :game_date, :prediction_date,
            :player_id, :team_id, :team_abbreviation,
            :first_basket_prob, :team_scores_first_prob, :player_share_on_team,
            :model_version, :lineup_status, :created_at, :updated_at
        )
        ON CONFLICT (game_id, player_id)
        DO UPDATE SET
            game_date = EXCLUDED.game_date,
            prediction_date = EXCLUDED.prediction_date,
            team_id = EXCLUDED.team_id,
            team_abbreviation = EXCLUDED.team_abbreviation,
            first_basket_prob = EXCLUDED.first_basket_prob,
            team_scores_first_prob = EXCLUDED.team_scores_first_prob,
            player_share_on_team = EXCLUDED.player_share_on_team,
            model_version = EXCLUDED.model_version,
            lineup_status = EXCLUDED.lineup_status,
            updated_at = EXCLUDED.updated_at,
            abs_error = CASE
                WHEN first_basket_prediction_logs.actual_first_basket IS NOT NULL
                THEN ABS(first_basket_prediction_logs.actual_first_basket - EXCLUDED.first_basket_prob)
                ELSE first_basket_prediction_logs.abs_error
            END
        """
    )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                stmt,
                {
                    "game_id": row.get("game_id"),
                    "game_date": row.get("game_date"),
                    "prediction_date": row.get("prediction_date") or pred_date,
                    "player_id": row.get("player_id"),
                    "team_id": row.get("team_id"),
                    "team_abbreviation": row.get("team_abbreviation"),
                    "first_basket_prob": row.get("first_basket_prob"),
                    "team_scores_first_prob": row.get("team_scores_first_prob"),
                    "player_share_on_team": row.get("player_share_on_team"),
                    "lineup_status": row.get("lineup_status"),
                    "model_version": model_version,
                    "created_at": now,
                    "updated_at": now,
                },
            )
    return len(rows)


def update_first_basket_actuals(engine):
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE first_basket_prediction_logs p
                SET actual_first_basket = CASE
                    WHEN p.player_id = l.first_scorer_player_id THEN 1
                    ELSE 0
                END,
                abs_error = ABS(
                    CASE WHEN p.player_id = l.first_scorer_player_id THEN 1 ELSE 0 END
                    - p.first_basket_prob
                ),
                updated_at = NOW()
                FROM first_basket_labels l
                WHERE p.game_id = l.game_id
                  AND l.first_scorer_player_id IS NOT NULL
                """
            )
        )
    return res.rowcount
