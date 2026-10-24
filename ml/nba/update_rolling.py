# To update player rolling averages

import pandas as pd
from pathlib import Path
from .utils import compute_history_rolling_features


# ml/ dir
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ROLLING_PATH = DATA_DIR / "player_stats_rolling.csv"


def update_rolling_stats(engine):
    """
    Recompute leak-free rolling features from the database and persist the
    latest row per player.

    The output CSV is intentionally a prediction-time cache, not a source of
    history.  Reusing it as historical input loses all but one prior game per
    player and makes incremental refreshes mathematically incorrect.
    """
    query = """
    SELECT pg.player_id, pg.game_id, pg.game_date, pg.season, pg.matchup,
           COALESCE(pg.team_abbreviation, p.team_abbreviation) AS team_abbreviation,
           pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers,
           pg.fgm, pg.fga, pg.fg3m, pg.fg3a
    FROM player_game_stats pg
    JOIN players p ON pg.player_id = p.id
    ORDER BY pg.player_id, pg.game_date, pg.game_id
    """
    df_history = pd.read_sql(query, engine)
    if df_history.empty:
        print("No games available to build rolling averages.")
        return df_history

    df_history["game_date"] = pd.to_datetime(df_history["game_date"])
    df_history = compute_history_rolling_features(df_history)

    # Keep only the latest row per player
    df_latest = df_history.groupby("player_id").tail(1)
    # Historical rows retain their actual team.  The latest game is the most
    # reliable local roster signal and avoids overwriting it with a stale
    # players-table assignment after a trade.
    df_latest.to_csv(ROLLING_PATH, index=False)

    print(f"Updated rolling averages for {len(df_latest)} players.")
    return df_latest
