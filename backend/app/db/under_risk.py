from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


STAT_TYPES = {"points", "assists", "rebounds", "threept"}


def _threshold_type_for_stat(stat_type: str) -> str:
    return "midpoint" if stat_type == "points" else "pred_p10"


async def compute_under_risk(
    db: AsyncSession,
    stat_type: str,
    window_n: int = 20,
):
    if stat_type not in STAT_TYPES:
        raise ValueError("stat_type must be one of: points, assists, rebounds, threept")

    threshold_type = _threshold_type_for_stat(stat_type)

    result = await db.execute(
        text(
            """
            WITH ranked AS (
                SELECT player_id, game_date, actual_value, pred_value, pred_p10,
                       ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
                FROM prediction_logs
                WHERE stat_type = :stat_type
                  AND actual_value IS NOT NULL
                  AND game_date IS NOT NULL
                  AND (
                        (:threshold_type = 'midpoint' AND pred_value IS NOT NULL AND pred_p10 IS NOT NULL)
                     OR (:threshold_type = 'pred_p10' AND pred_p10 IS NOT NULL)
                  )
            ),
            windowed AS (
                SELECT *
                FROM ranked
                WHERE rn <= :window_n
            )
            SELECT player_id,
                   COUNT(*) AS sample_size,
                   SUM(
                       CASE
                           WHEN actual_value < (
                               CASE
                                   WHEN :threshold_type = 'midpoint' THEN (pred_p10 + pred_value) / 2.0
                                   ELSE pred_p10
                               END
                           ) THEN 1
                           ELSE 0
                       END
                   ) AS under_count,
                   MAX(game_date) AS as_of_date
            FROM windowed
            GROUP BY player_id
            """
        ),
        {
            "stat_type": stat_type,
            "window_n": window_n,
            "threshold_type": threshold_type,
        },
    )

    rows = result.mappings().all()
    if not rows:
        return {"status": "no_data", "stat_type": stat_type, "window_n": window_n}

    computed_at = datetime.utcnow()
    upsert_stmt = text(
        """
        INSERT INTO player_under_risk
        (player_id, stat_type, window_n, sample_size, under_count, under_rate,
         threshold_type, as_of_date, computed_at)
        VALUES
        (:player_id, :stat_type, :window_n, :sample_size, :under_count, :under_rate,
         :threshold_type, :as_of_date, :computed_at)
        ON CONFLICT (player_id, stat_type)
        DO UPDATE SET
          window_n = EXCLUDED.window_n,
          sample_size = EXCLUDED.sample_size,
          under_count = EXCLUDED.under_count,
          under_rate = EXCLUDED.under_rate,
          threshold_type = EXCLUDED.threshold_type,
          as_of_date = EXCLUDED.as_of_date,
          computed_at = EXCLUDED.computed_at
        """
    )

    saved = 0
    for row in rows:
        sample_size = int(row["sample_size"])
        under_count = int(row["under_count"] or 0)
        under_rate = float(under_count) / float(sample_size) if sample_size else 0.0
        await db.execute(
            upsert_stmt,
            {
                "player_id": int(row["player_id"]),
                "stat_type": stat_type,
                "window_n": window_n,
                "sample_size": sample_size,
                "under_count": under_count,
                "under_rate": under_rate,
                "threshold_type": threshold_type,
                "as_of_date": row["as_of_date"],
                "computed_at": computed_at,
            },
        )
        saved += 1

    await db.commit()

    return {
        "status": "completed",
        "stat_type": stat_type,
        "window_n": window_n,
        "players_updated": saved,
        "threshold_type": threshold_type,
    }
