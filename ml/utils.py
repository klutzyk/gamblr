import pandas as pd


def compute_rolling_features(df_next, df_history):
    """
    Compute rolling averages and teammate/opponent features for next-game prediction.

    df_next: DataFrame with next game's players
        columns: player_id, team_abbreviation, matchup, game_date (next game date)
    df_history: DataFrame with all past player games (same format as training)
    """

    df_next = df_next.copy()

    # 1. Rolling averages of player stats (last 5 games)
    rolling_stats = df_history.groupby("player_id").tail(5)
    df_next["avg_points_last5"] = df_next["player_id"].map(
        df_history.groupby("player_id")["points"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_assists_last5"] = df_next["player_id"].map(
        df_history.groupby("player_id")["assists"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_rebounds_last5"] = df_next["player_id"].map(
        df_history.groupby("player_id")["rebounds"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_minutes_last5"] = df_next["player_id"].map(
        df_history.groupby("player_id")["minutes"].apply(lambda x: x.tail(5).mean())
    )

    # 2. Teammate influence: avg assists of teammates (last 5 games)
    teammate_avg = []
    for idx, row in df_next.iterrows():
        team = row["team_abbreviation"]
        player_id = row["player_id"]
        # all past games for this team, excluding this player
        teammates = df_history[
            (df_history["team_abbreviation"] == team)
            & (df_history["player_id"] != player_id)
        ]
        last5 = teammates.groupby("player_id").tail(5)
        teammate_avg.append(last5["assists"].mean() if not last5.empty else 0)
    df_next["teammate_avg_assists_last5"] = teammate_avg

    # 3. Opponent strength
    opponent_avg_points = []
    opponent_avg_blocks = []
    opponent_avg_steals = []
    opponent_avg_turnovers = []

    for idx, row in df_next.iterrows():
        matchup = row["matchup"]
        player_team = row["team_abbreviation"]
        if " vs. " in matchup:
            opponent_team = matchup.split(" vs. ")[1]
        else:
            opponent_team = matchup.split(" @ ")[1]

        opp_games = df_history[df_history["team_abbreviation"] == opponent_team]
        last5 = opp_games.groupby("player_id").tail(5)

        opponent_avg_points.append(last5["points"].mean() if not last5.empty else 0)
        opponent_avg_blocks.append(last5["blocks"].mean() if not last5.empty else 0)
        opponent_avg_steals.append(last5["steals"].mean() if not last5.empty else 0)
        opponent_avg_turnovers.append(
            last5["turnovers"].mean() if not last5.empty else 0
        )

    df_next["opponent_avg_points_allowed_last5"] = opponent_avg_points
    df_next["opponent_avg_blocks_last5"] = opponent_avg_blocks
    df_next["opponent_avg_steals_last5"] = opponent_avg_steals
    df_next["opponent_avg_turnovers_last5"] = opponent_avg_turnovers

    return df_next
