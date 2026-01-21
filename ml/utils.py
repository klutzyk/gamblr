import pandas as pd


def compute_history_rolling_features(df_history: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling averages per player from historical games only.
    Used to build / update the rolling CSV.
    """

    df = df_history.copy()
    df = df.sort_values(["player_id", "game_date"])

    df["avg_points_last5"] = df.groupby("player_id")["points"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["avg_assists_last5"] = df.groupby("player_id")["assists"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["avg_rebounds_last5"] = df.groupby("player_id")["rebounds"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["avg_minutes_last5"] = df.groupby("player_id")["minutes"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    return df


def compute_prediction_features(
    df_next: pd.DataFrame, df_history: pd.DataFrame
) -> pd.DataFrame:
    """
    Build full feature set for upcoming games.
    Matches training-time features.
    """

    df_next = df_next.copy()

    # Player rolling form
    grouped = df_history.groupby("player_id")

    df_next["avg_points_last5"] = df_next["player_id"].map(
        grouped["points"].apply(lambda x: x.tail(5).mean())
    )

    df_next["avg_assists_last5"] = df_next["player_id"].map(
        grouped["assists"].apply(lambda x: x.tail(5).mean())
    )

    df_next["avg_rebounds_last5"] = df_next["player_id"].map(
        grouped["rebounds"].apply(lambda x: x.tail(5).mean())
    )

    df_next["avg_minutes_last5"] = df_next["player_id"].map(
        grouped["minutes"].apply(lambda x: x.tail(5).mean())
    )

    # compute home away flag
    df_next["is_home"] = df_next["matchup"].apply(lambda x: 1 if "@" not in x else 0)

    # teammate influence
    teammate_avg = []
    for _, row in df_next.iterrows():
        team = row["team_abbreviation"]
        pid = row["player_id"]

        teammates = df_history[
            (df_history["team_abbreviation"] == team) & (df_history["player_id"] != pid)
        ]

        last5 = teammates.groupby("player_id").tail(5)
        teammate_avg.append(last5["assists"].mean() if not last5.empty else 0)

    df_next["teammate_avg_assists_last5"] = teammate_avg

    #  Opponent strength
    opp_pts, opp_blk, opp_stl, opp_tov = [], [], [], []

    for _, row in df_next.iterrows():
        matchup = row["matchup"]
        if " vs. " in matchup:
            opp = matchup.split(" vs. ")[1]
        else:
            opp = matchup.split(" @ ")[1]

        opp_games = df_history[df_history["team_abbreviation"] == opp]
        last5 = opp_games.groupby("player_id").tail(5)

        opp_pts.append(last5["points"].mean() if not last5.empty else 0)
        opp_blk.append(last5["blocks"].mean() if not last5.empty else 0)
        opp_stl.append(last5["steals"].mean() if not last5.empty else 0)
        opp_tov.append(last5["turnovers"].mean() if not last5.empty else 0)

    df_next["opponent_avg_points_allowed_last5"] = opp_pts
    df_next["opponent_avg_blocks_last5"] = opp_blk
    df_next["opponent_avg_steals_last5"] = opp_stl
    df_next["opponent_avg_turnovers_last5"] = opp_tov

    return df_next
