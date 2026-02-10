# To update player rolling averages

from pathlib import Path

import pandas as pd

try:
    from .utils import compute_history_rolling_features
except ImportError:  # pragma: no cover
    from utils import compute_history_rolling_features


# ml/ dir
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ROLLING_PATH = DATA_DIR / "player_stats_rolling.csv"


def update_rolling_stats(engine, full_rebuild: bool = True):
    """
    Rebuild rolling features from full historical game logs and
    persist only the latest row per player.
    """

    # The previous incremental path mixed prior snapshots with raw boxscores,
    # which can corrupt rolling windows. Keep a full rebuild as default.
    if not full_rebuild:
        print("Incremental rolling mode is disabled; running full rebuild instead.")

    query = """
    SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
           pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers,
           pg.fgm, pg.fga, pg.fg3m, pg.fg3a
    FROM player_game_stats pg
    JOIN players p ON pg.player_id = p.id
    ORDER BY pg.player_id, pg.game_date, pg.game_id
    """

    df_history = pd.read_sql(query, engine)

    if df_history.empty:
        try:
            df_existing = pd.read_csv(ROLLING_PATH)
            print("No player game logs in DB. Kept existing rolling CSV unchanged.")
            return df_existing
        except FileNotFoundError:
            print("No player game logs found; rolling CSV not created.")
            return pd.DataFrame()

    df_history["game_date"] = pd.to_datetime(df_history["game_date"])
    df_history = compute_history_rolling_features(df_history)

    # Keep only the latest row per player.
    df_latest = (
        df_history.sort_values(["player_id", "game_date", "game_id"])
        .groupby("player_id", as_index=False)
        .tail(1)
        .copy()
    )

    players_df = pd.read_sql(
        "SELECT id AS player_id, team_abbreviation FROM players", engine
    )
    team_map = dict(zip(players_df["player_id"], players_df["team_abbreviation"]))
    df_latest["team_abbreviation"] = df_latest["player_id"].map(team_map)

    df_latest.to_csv(ROLLING_PATH, index=False)
    print(f"Rebuilt rolling averages for {len(df_latest)} players.")
    return df_latest
