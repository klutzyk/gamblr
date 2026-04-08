from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

router = APIRouter()

VALID_STAT_TYPES = {"points", "assists", "rebounds", "threept", "threepa"}
DEFAULT_DAYS = 30
MAX_DAYS = 180
DEFAULT_LIMIT = 50


def _validate_stat_type(stat_type: str) -> str:
    stat_type = (stat_type or "").strip().lower()
    if stat_type not in VALID_STAT_TYPES:
        raise HTTPException(status_code=400, detail="Invalid stat_type.")
    return stat_type


def _validate_days(days: int) -> int:
    if days < 7 or days > MAX_DAYS:
        raise HTTPException(status_code=400, detail=f"days must be between 7 and {MAX_DAYS}.")
    return days


def _close_threshold(stat_type: str) -> float:
    return {
        "points": 4.0,
        "assists": 2.0,
        "rebounds": 2.0,
        "threept": 1.0,
        "threepa": 2.0,
    }[stat_type]


def _stat_column(stat_type: str) -> str:
    return {
        "points": "points",
        "assists": "assists",
        "rebounds": "rebounds",
        "threept": "fg3m",
        "threepa": "fg3a",
    }[stat_type]


def _relevance_threshold(stat_type: str) -> float:
    return {
        "points": 10.0,
        "assists": 3.0,
        "rebounds": 5.0,
        "threept": 1.2,
        "threepa": 3.0,
    }[stat_type]


def _fmt_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _safe_float(value) -> float | None:
    return float(value) if value is not None else None


def _safe_int(value) -> int:
    return int(value or 0)


def _bias_label(bias: float | None) -> str:
    if bias is None:
        return "balanced"
    if bias >= 1.0:
        return "too high"
    if bias <= -1.0:
        return "too low"
    return "balanced"


def _trend_label(current_avg: float | None, prior_avg: float | None) -> str:
    if current_avg is None or prior_avg is None:
        return "Not enough recent data"
    delta = current_avg - prior_avg
    if delta <= -0.35:
        return "Improving recently"
    if delta >= 0.35:
        return "Cooling off lately"
    return "Holding steady"


def _reliability_tag(avg_abs_error: float | None, stat_type: str, sample_size: int) -> str:
    if sample_size < 5 or avg_abs_error is None:
        return "Still building"
    thresholds = {
        "points": (3.5, 5.5),
        "assists": (1.5, 2.4),
        "rebounds": (1.8, 2.8),
        "threept": (0.8, 1.2),
        "threepa": (1.6, 2.4),
    }
    strong, mixed = thresholds[stat_type]
    if avg_abs_error <= strong:
        return "Reliable"
    if avg_abs_error <= mixed:
        return "Mixed"
    return "Volatile"


def _build_overview_story(
    stat_label: str,
    tracked_predictions: int,
    average_miss: float | None,
    bias: float | None,
    recent_label: str,
    close_rate: float | None,
) -> str:
    if tracked_predictions == 0 or average_miss is None:
        return f"We do not have enough completed {stat_label.lower()} results yet to judge how we're doing."

    bias_text = _bias_label(bias)
    close_pct = f"{round(close_rate * 100)}%" if close_rate is not None else "n/a"
    if bias_text == "too high":
        bias_sentence = "The predictions have been leaning a little high."
    elif bias_text == "too low":
        bias_sentence = "The predictions have been leaning a little low."
    else:
        bias_sentence = "The predictions have been fairly balanced overall."

    return (
        f"Our {stat_label.lower()} predictions have been missing the real result by {average_miss:.1f} on average "
        f"across {tracked_predictions} tracked games. {bias_sentence} "
        f"{recent_label}. About {close_pct} of tracked picks finished within the close-call range."
    )


async def _last_fully_updated_date(db: AsyncSession) -> date | None:
    result = await db.execute(
        text(
            """
            SELECT MAX(game_date)
            FROM prediction_logs
            WHERE actual_value IS NOT NULL
            """
        )
    )
    return result.scalar_one_or_none()


@router.get("/overview")
async def get_review_overview(
    stat_type: str = Query("points"),
    days: int = Query(DEFAULT_DAYS),
    db: AsyncSession = Depends(get_db),
):
    stat_type = _validate_stat_type(stat_type)
    days = _validate_days(days)
    close_threshold = _close_threshold(stat_type)
    window_start = date.today() - timedelta(days=days - 1)
    recent_start = date.today() - timedelta(days=6)
    prior_start = date.today() - timedelta(days=13)
    stat_label = {
        "points": "Points",
        "assists": "Assists",
        "rebounds": "Rebounds",
        "threept": "3PT Made",
        "threepa": "3PT Attempts",
    }[stat_type]

    overview_query = text(
        """
        SELECT
            COUNT(*) AS tracked_predictions,
            AVG(abs_error) AS avg_abs_error,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY abs_error) AS median_abs_error,
            AVG(pred_value - actual_value) AS avg_bias,
            AVG(CASE WHEN abs_error <= :close_threshold THEN 1.0 ELSE 0.0 END) AS close_rate
        FROM prediction_logs
        WHERE stat_type = :stat_type
          AND actual_value IS NOT NULL
          AND game_date IS NOT NULL
          AND game_date >= :window_start
        """
    )
    overview_row = (await db.execute(
        overview_query,
        {
            "stat_type": stat_type,
            "window_start": window_start,
            "close_threshold": close_threshold,
        },
    )).one()

    recent_query = text(
        """
        SELECT
            AVG(CASE WHEN game_date >= :recent_start THEN abs_error END) AS recent_abs_error,
            AVG(CASE WHEN game_date >= :prior_start AND game_date < :recent_start THEN abs_error END) AS prior_abs_error
        FROM prediction_logs
        WHERE stat_type = :stat_type
          AND actual_value IS NOT NULL
          AND game_date IS NOT NULL
          AND game_date >= :prior_start
        """
    )
    recent_row = (await db.execute(
        recent_query,
        {
            "stat_type": stat_type,
            "recent_start": recent_start,
            "prior_start": prior_start,
        },
    )).one()

    tracked_predictions = _safe_int(overview_row.tracked_predictions)
    avg_abs_error = _safe_float(overview_row.avg_abs_error)
    median_abs_error = _safe_float(overview_row.median_abs_error)
    avg_bias = _safe_float(overview_row.avg_bias)
    close_rate = _safe_float(overview_row.close_rate)
    recent_abs_error = _safe_float(recent_row.recent_abs_error)
    prior_abs_error = _safe_float(recent_row.prior_abs_error)
    updated_through = await _last_fully_updated_date(db)

    return {
        "stat_type": stat_type,
        "stat_label": stat_label,
        "days": days,
        "close_call_threshold": close_threshold,
        "tracked_predictions": tracked_predictions,
        "average_miss": avg_abs_error,
        "median_miss": median_abs_error,
        "bias": avg_bias,
        "bias_label": _bias_label(avg_bias),
        "close_rate": close_rate,
        "recent_avg_miss": recent_abs_error,
        "prior_avg_miss": prior_abs_error,
        "recent_trend_label": _trend_label(recent_abs_error, prior_abs_error),
        "updated_through": _fmt_date(updated_through),
        "story": _build_overview_story(
            stat_label=stat_label,
            tracked_predictions=tracked_predictions,
            average_miss=avg_abs_error,
            bias=avg_bias,
            recent_label=_trend_label(recent_abs_error, prior_abs_error),
            close_rate=close_rate,
        ),
    }


@router.get("/trend")
async def get_review_trend(
    stat_type: str = Query("points"),
    days: int = Query(DEFAULT_DAYS),
    db: AsyncSession = Depends(get_db),
):
    stat_type = _validate_stat_type(stat_type)
    days = _validate_days(days)
    close_threshold = _close_threshold(stat_type)
    window_start = date.today() - timedelta(days=days - 1)

    rows = (
        await db.execute(
            text(
                """
                SELECT
                    game_date,
                    COUNT(*) AS prediction_count,
                    AVG(abs_error) AS avg_abs_error,
                    AVG(pred_value - actual_value) AS avg_bias,
                    AVG(CASE WHEN abs_error <= :close_threshold THEN 1.0 ELSE 0.0 END) AS close_rate
                FROM prediction_logs
                WHERE stat_type = :stat_type
                  AND actual_value IS NOT NULL
                  AND game_date IS NOT NULL
                  AND game_date >= :window_start
                GROUP BY game_date
                ORDER BY game_date ASC
                """
            ),
            {
                "stat_type": stat_type,
                "window_start": window_start,
                "close_threshold": close_threshold,
            },
        )
    ).all()

    return {
        "stat_type": stat_type,
        "days": days,
        "points": [
            {
                "game_date": _fmt_date(row.game_date),
                "prediction_count": _safe_int(row.prediction_count),
                "average_miss": _safe_float(row.avg_abs_error),
                "bias": _safe_float(row.avg_bias),
                "close_rate": _safe_float(row.close_rate),
            }
            for row in rows
        ],
    }


@router.get("/players")
async def get_review_players(
    stat_type: str = Query("points"),
    days: int = Query(DEFAULT_DAYS),
    limit: int = Query(40),
    search: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stat_type = _validate_stat_type(stat_type)
    days = _validate_days(days)
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100.")
    close_threshold = _close_threshold(stat_type)
    stat_column = _stat_column(stat_type)
    relevance_threshold = _relevance_threshold(stat_type)
    window_start = date.today() - timedelta(days=days - 1)
    search_value = f"%{(search or '').strip().lower()}%"

    rows = (
        await db.execute(
            text(
                """
                WITH relevant_players AS (
                    SELECT
                        player_id,
                        AVG(minutes) AS avg_minutes,
                        AVG(""" + stat_column + """) AS avg_stat_value
                    FROM player_game_stats
                    WHERE game_date IS NOT NULL
                      AND game_date >= :window_start
                    GROUP BY player_id
                    HAVING AVG(minutes) >= 18 OR AVG(""" + stat_column + """) >= :relevance_threshold
                ),
                base AS (
                    SELECT
                        pl.player_id,
                        p.full_name,
                        COALESCE(p.team_abbreviation, '') AS team_abbreviation,
                        pl.game_date,
                        pl.abs_error,
                        (pl.pred_value - pl.actual_value) AS bias,
                        ROW_NUMBER() OVER (PARTITION BY pl.player_id ORDER BY pl.game_date DESC, pl.id DESC) AS rn
                    FROM prediction_logs pl
                    JOIN players p ON p.id = pl.player_id
                    JOIN relevant_players rp ON rp.player_id = pl.player_id
                    WHERE pl.stat_type = :stat_type
                      AND pl.actual_value IS NOT NULL
                      AND pl.game_date IS NOT NULL
                      AND pl.game_date >= :window_start
                      AND (:search = '%%' OR LOWER(p.full_name) LIKE :search)
                )
                SELECT
                    player_id,
                    full_name,
                    team_abbreviation,
                    COUNT(*) AS tracked_predictions,
                    AVG(abs_error) AS avg_abs_error,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY abs_error) AS median_abs_error,
                    AVG(CASE WHEN abs_error <= :close_threshold THEN 1.0 ELSE 0.0 END) AS close_rate,
                    AVG(bias) AS avg_bias,
                    AVG(CASE WHEN rn <= 5 THEN abs_error END) AS recent_abs_error,
                    AVG(CASE WHEN rn > 5 AND rn <= 10 THEN abs_error END) AS prior_abs_error,
                    MAX(game_date) AS last_game_date
                FROM base
                GROUP BY player_id, full_name, team_abbreviation
                HAVING COUNT(*) >= 5
                ORDER BY avg_abs_error ASC, tracked_predictions DESC
                LIMIT :limit
                """
            ),
            {
                "stat_type": stat_type,
                "window_start": window_start,
                "close_threshold": close_threshold,
                "relevance_threshold": relevance_threshold,
                "search": search_value if search_value != "%%" else "%%",
                "limit": limit,
            },
        )
    ).all()

    players = []
    for row in rows:
        avg_abs_error = _safe_float(row.avg_abs_error)
        tracked_predictions = _safe_int(row.tracked_predictions)
        players.append(
            {
                "player_id": row.player_id,
                "full_name": row.full_name,
                "team_abbreviation": row.team_abbreviation,
                "tracked_predictions": tracked_predictions,
                "average_miss": avg_abs_error,
                "median_miss": _safe_float(row.median_abs_error),
                "close_rate": _safe_float(row.close_rate),
                "bias": _safe_float(row.avg_bias),
                "recent_avg_miss": _safe_float(row.recent_abs_error),
                "prior_avg_miss": _safe_float(row.prior_abs_error),
                "trend_label": _trend_label(
                    _safe_float(row.recent_abs_error), _safe_float(row.prior_abs_error)
                ),
                "reliability_tag": _reliability_tag(avg_abs_error, stat_type, tracked_predictions),
                "last_game_date": _fmt_date(row.last_game_date),
            }
        )

    return {
        "stat_type": stat_type,
        "days": days,
        "players": players,
    }


@router.get("/recent")
async def get_review_recent(
    stat_type: str = Query("points"),
    days: int = Query(14),
    limit: int = Query(DEFAULT_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    stat_type = _validate_stat_type(stat_type)
    days = _validate_days(days)
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100.")
    stat_column = _stat_column(stat_type)
    relevance_threshold = _relevance_threshold(stat_type)
    window_start = date.today() - timedelta(days=days - 1)

    rows = (
        await db.execute(
            text(
                """
                WITH relevant_players AS (
                    SELECT
                        player_id
                    FROM player_game_stats
                    WHERE game_date IS NOT NULL
                      AND game_date >= :window_start
                    GROUP BY player_id
                    HAVING AVG(minutes) >= 18 OR AVG(""" + stat_column + """) >= :relevance_threshold
                )
                SELECT
                    pl.player_id,
                    p.full_name,
                    COALESCE(p.team_abbreviation, '') AS team_abbreviation,
                    pl.game_date,
                    COALESCE(pgs.matchup, '') AS matchup,
                    pgs.minutes,
                    pl.pred_value,
                    pl.actual_value,
                    pl.abs_error,
                    pl.confidence,
                    (pl.pred_value - pl.actual_value) AS bias
                FROM prediction_logs pl
                JOIN players p ON p.id = pl.player_id
                JOIN relevant_players rp ON rp.player_id = pl.player_id
                LEFT JOIN player_game_stats pgs
                  ON pgs.player_id = pl.player_id
                 AND pgs.game_id = pl.game_id
                WHERE pl.stat_type = :stat_type
                  AND pl.actual_value IS NOT NULL
                  AND pl.game_date IS NOT NULL
                  AND pl.game_date >= :window_start
                ORDER BY pl.game_date DESC, COALESCE(pgs.minutes, 0) DESC, pl.abs_error DESC, pl.id DESC
                LIMIT :limit
                """
            ),
            {
                "stat_type": stat_type,
                "window_start": window_start,
                "relevance_threshold": relevance_threshold,
                "limit": limit,
            },
        )
    ).all()

    return {
        "stat_type": stat_type,
        "days": days,
        "results": [
            {
                "player_id": row.player_id,
                "full_name": row.full_name,
                "team_abbreviation": row.team_abbreviation,
                "game_date": _fmt_date(row.game_date),
                "matchup": row.matchup,
                "minutes": _safe_float(row.minutes),
                "predicted": _safe_float(row.pred_value),
                "actual": _safe_float(row.actual_value),
                "average_miss": _safe_float(row.abs_error),
                "confidence": _safe_float(row.confidence),
                "bias": _safe_float(row.bias),
                "result_label": "Ran high" if (row.bias or 0) > 0.25 else "Ran low" if (row.bias or 0) < -0.25 else "Close to target",
            }
            for row in rows
        ],
    }


@router.get("/player/{player_id}")
async def get_review_player_detail(
    player_id: int,
    stat_type: str = Query("points"),
    days: int = Query(60),
    db: AsyncSession = Depends(get_db),
):
    stat_type = _validate_stat_type(stat_type)
    days = _validate_days(days)
    close_threshold = _close_threshold(stat_type)
    window_start = date.today() - timedelta(days=days - 1)

    summary_row = (
        await db.execute(
            text(
                """
                SELECT
                    p.id AS player_id,
                    p.full_name,
                    COALESCE(p.team_abbreviation, '') AS team_abbreviation,
                    COUNT(*) AS tracked_predictions,
                    AVG(pl.abs_error) AS avg_abs_error,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY pl.abs_error) AS median_abs_error,
                    AVG(CASE WHEN pl.abs_error <= :close_threshold THEN 1.0 ELSE 0.0 END) AS close_rate,
                    AVG(pl.pred_value - pl.actual_value) AS avg_bias,
                    MAX(pl.game_date) AS last_game_date
                FROM prediction_logs pl
                JOIN players p ON p.id = pl.player_id
                WHERE pl.player_id = :player_id
                  AND pl.stat_type = :stat_type
                  AND pl.actual_value IS NOT NULL
                  AND pl.game_date IS NOT NULL
                  AND pl.game_date >= :window_start
                GROUP BY p.id, p.full_name, p.team_abbreviation
                """
            ),
            {
                "player_id": player_id,
                "stat_type": stat_type,
                "window_start": window_start,
                "close_threshold": close_threshold,
            },
        )
    ).one_or_none()

    if summary_row is None:
        raise HTTPException(status_code=404, detail="No completed prediction history found for that player.")

    game_rows = (
        await db.execute(
            text(
                """
                SELECT
                    pl.game_date,
                    COALESCE(pgs.matchup, '') AS matchup,
                    pl.pred_value,
                    pl.actual_value,
                    pl.abs_error,
                    pl.confidence,
                    (pl.pred_value - pl.actual_value) AS bias
                FROM prediction_logs pl
                LEFT JOIN player_game_stats pgs
                  ON pgs.player_id = pl.player_id
                 AND pgs.game_id = pl.game_id
                WHERE pl.player_id = :player_id
                  AND pl.stat_type = :stat_type
                  AND pl.actual_value IS NOT NULL
                  AND pl.game_date IS NOT NULL
                  AND pl.game_date >= :window_start
                ORDER BY pl.game_date DESC, pl.id DESC
                LIMIT 20
                """
            ),
            {
                "player_id": player_id,
                "stat_type": stat_type,
                "window_start": window_start,
            },
        )
    ).all()

    avg_abs_error = _safe_float(summary_row.avg_abs_error)
    tracked_predictions = _safe_int(summary_row.tracked_predictions)
    bias = _safe_float(summary_row.avg_bias)
    if bias is None:
        story = f"We do not have enough completed {stat_type} results yet for this player."
    else:
        tendency = "a little high" if bias > 0.75 else "a little low" if bias < -0.75 else "fairly balanced"
        story = (
            f"{summary_row.full_name} has {tracked_predictions} tracked {stat_type} predictions in this view. "
            f"The average miss is {avg_abs_error:.1f}, and the model has been {tendency} on this player."
        )

    return {
        "player_id": summary_row.player_id,
        "full_name": summary_row.full_name,
        "team_abbreviation": summary_row.team_abbreviation,
        "stat_type": stat_type,
        "days": days,
        "tracked_predictions": tracked_predictions,
        "average_miss": avg_abs_error,
        "median_miss": _safe_float(summary_row.median_abs_error),
        "close_rate": _safe_float(summary_row.close_rate),
        "bias": bias,
        "reliability_tag": _reliability_tag(avg_abs_error, stat_type, tracked_predictions),
        "last_game_date": _fmt_date(summary_row.last_game_date),
        "story": story,
        "games": [
            {
                "game_date": _fmt_date(row.game_date),
                "matchup": row.matchup,
                "predicted": _safe_float(row.pred_value),
                "actual": _safe_float(row.actual_value),
                "average_miss": _safe_float(row.abs_error),
                "confidence": _safe_float(row.confidence),
                "bias": _safe_float(row.bias),
            }
            for row in game_rows
        ],
    }



