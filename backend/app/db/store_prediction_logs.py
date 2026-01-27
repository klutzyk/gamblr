from datetime import datetime
from sqlalchemy import text
import pandas as pd


def log_predictions(engine, df_preds, stat_type: str, model_version: str | None):
    if df_preds is None or df_preds.empty:
        return 0

    df = df_preds.copy()
    df["prediction_date"] = datetime.utcnow().date()
    df["stat_type"] = stat_type
    df["model_version"] = model_version

    cols = [
        "player_id",
        "stat_type",
        "game_id",
        "game_date",
        "prediction_date",
        "pred_value",
        "pred_p10",
        "pred_p50",
        "pred_p90",
        "confidence",
        "model_version",
    ]

    with engine.begin() as conn:
        inserted = 0
        for _, row in df[cols].iterrows():
            # upsert by unique constraint (player_id, stat_type, game_id)
            conn.execute(
                text(
                    """
                    INSERT INTO prediction_logs
                    (player_id, stat_type, game_id, game_date, prediction_date,
                     pred_value, pred_p10, pred_p50, pred_p90, confidence, model_version)
                    VALUES
                    (:player_id, :stat_type, :game_id, :game_date, :prediction_date,
                     :pred_value, :pred_p10, :pred_p50, :pred_p90, :confidence, :model_version)
                    ON CONFLICT (player_id, stat_type, game_id)
                    DO UPDATE SET
                      pred_value = EXCLUDED.pred_value,
                      pred_p10 = EXCLUDED.pred_p10,
                      pred_p50 = EXCLUDED.pred_p50,
                      pred_p90 = EXCLUDED.pred_p90,
                      confidence = EXCLUDED.confidence,
                      model_version = EXCLUDED.model_version,
                      prediction_date = EXCLUDED.prediction_date
                    """
                ),
                {
                    "player_id": int(row["player_id"]),
                    "stat_type": row["stat_type"],
                    "game_id": row.get("game_id"),
                    "game_date": row.get("game_date"),
                    "prediction_date": row.get("prediction_date"),
                    "pred_value": row.get("pred_value"),
                    "pred_p10": row.get("pred_p10"),
                    "pred_p50": row.get("pred_p50"),
                    "pred_p90": row.get("pred_p90"),
                    "confidence": row.get("confidence"),
                    "model_version": row.get("model_version"),
                },
            )
            inserted += 1

    return inserted


def update_prediction_actuals(engine, stat_type: str):
    stat_column = {
        "points": "points",
        "assists": "assists",
        "rebounds": "rebounds",
        "minutes": "minutes",
    }.get(stat_type)

    if not stat_column:
        raise ValueError("stat_type must be one of: points, assists, rebounds, minutes")

    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
                UPDATE prediction_logs pl
                SET actual_value = pgs.{stat_column},
                    abs_error = ABS(pgs.{stat_column} - pl.pred_value)
                FROM player_game_stats pgs
                WHERE pl.actual_value IS NULL
                  AND pl.stat_type = :stat_type
                  AND pl.player_id = pgs.player_id
                  AND pl.game_id = pgs.game_id
                """
            ),
            {"stat_type": stat_type},
        )
        return result.rowcount
