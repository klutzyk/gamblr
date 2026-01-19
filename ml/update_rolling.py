# To update player rolling averages

import pandas as pd
from pathlib import Path
from utils import compute_history_rolling_features


# ml/ dir
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ROLLING_PATH = DATA_DIR / "player_stats_rolling.csv"


def update_rolling_stats(engine):
    """
    Pull new games from DB, recompute rolling features,
    and persist latest rolling row per player.
    """

    # Load last rolling averages table
    try:
        df_rolling_old = pd.read_csv(ROLLING_PATH)
        last_date = df_rolling_old["game_date"].max()
    except FileNotFoundError:
        df_rolling_old = pd.DataFrame()
        last_date = "2000-01-01"  # for the first time its fetched, fetches all the data (new data)

    # Fetch new games
    query = f"""
    SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
           pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
    FROM player_game_stats pg
    JOIN players p ON pg.player_id = p.id
    WHERE pg.game_date > '{last_date}'
    ORDER BY pg.game_date
    """

    df_new = pd.read_sql(query, engine)

    if df_new.empty:
        print("No new games to update.")
        return df_rolling_old

    # Combine with previous history
    df_history = pd.concat([df_rolling_old, df_new], ignore_index=True)
    df_history = compute_history_rolling_features(df_history)

    # Keep only the latest row per player
    df_latest = df_history.groupby("player_id").tail(1)
    df_latest.to_csv(ROLLING_PATH, index=False)

    print(f"Updated rolling averages for {len(df_latest)} players.")
    return df_latest
