from typing import Optional
import pandas as pd

POINTS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "pred_minutes",
    "avg_points_last5",
    "avg_points_last10",
    "std_points_last10",
    "avg_assists_last5",
    "avg_rebounds_last5",
    "avg_points_per_min_last5",
    "days_since_last_game",
    "is_back_to_back",
    "is_home",
    "games_played_season",
    "team_points_avg_last5",
    "team_points_avg_last10",
    "opponent_points_allowed_last5",
    "team_lineup_net_rating",
    "team_lineup_pace",
    "team_lineup_ast_pct",
    "team_lineup_reb_pct",
]

ASSISTS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "pred_minutes",
    "avg_assists_last5",
    "avg_assists_last10",
    "std_assists_last10",
    "avg_points_last5",
    "avg_turnovers_last5",
    "avg_assists_per_min_last5",
    "days_since_last_game",
    "is_back_to_back",
    "is_home",
    "games_played_season",
    "team_assists_avg_last5",
    "team_assists_avg_last10",
    "opponent_assists_allowed_last5",
    "team_fg_pct_avg_last5",
    "team_fg3m_avg_last5",
    "team_fg3a_avg_last5",
    "opponent_fg_pct_allowed_last5",
    "team_lineup_net_rating",
    "team_lineup_pace",
    "team_lineup_ast_pct",
    "team_lineup_reb_pct",
]

REBOUNDS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "pred_minutes",
    "avg_rebounds_last5",
    "avg_rebounds_last10",
    "std_rebounds_last10",
    "avg_points_last5",
    "avg_rebounds_per_min_last5",
    "days_since_last_game",
    "is_back_to_back",
    "is_home",
    "games_played_season",
    "team_rebounds_avg_last5",
    "team_rebounds_avg_last10",
    "opponent_rebounds_allowed_last5",
    "opponent_missed_fg_allowed_last5",
    "opponent_fg_pct_allowed_last5",
    "team_lineup_net_rating",
    "team_lineup_pace",
    "team_lineup_ast_pct",
    "team_lineup_reb_pct",
]

MINUTES_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "avg_points_last5",
    "avg_assists_last5",
    "avg_rebounds_last5",
    "days_since_last_game",
    "is_back_to_back",
    "is_home",
    "games_played_season",
    "team_points_avg_last5",
    "team_points_avg_last10",
    "team_assists_avg_last5",
    "team_assists_avg_last10",
    "team_rebounds_avg_last5",
    "team_rebounds_avg_last10",
    "team_lineup_net_rating",
    "team_lineup_pace",
    "team_lineup_ast_pct",
    "team_lineup_reb_pct",
]

THREEPT_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "pred_minutes",
    "avg_fg3m_last5",
    "avg_fg3m_last10",
    "avg_fg3a_last5",
    "avg_fg3a_last10",
    "fg3_pct_last10",
    "avg_fga_last5",
    "avg_fga_last10",
    "fg3a_rate_last10",
    "avg_points_last5",
    "days_since_last_game",
    "is_back_to_back",
    "is_home",
    "games_played_season",
    "team_points_avg_last5",
    "team_points_avg_last10",
    "team_fga_avg_last5",
    "team_fga_avg_last10",
    "team_fg3a_avg_last5",
    "team_fg3a_avg_last10",
    "team_fg3m_avg_last5",
    "team_fg3m_avg_last10",
    "opponent_fg3a_allowed_last5",
    "opponent_fg3m_allowed_last5",
    "team_lineup_pace",
    "team_lineup_net_rating",
]


def parse_opponent_team(matchup: str) -> Optional[str]:
    if " vs. " in matchup:
        return matchup.split(" vs. ")[1]
    if " @ " in matchup:
        return matchup.split(" @ ")[1]
    return None


def build_team_game_features(
    df_raw: pd.DataFrame, df_team: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    if df_team is not None and not df_team.empty:
        team_game = df_team.copy()
        for col in ["fgm", "fga", "fg3m", "fg3a"]:
            if col not in team_game.columns:
                team_game[col] = 0
    else:
        stat_cols = ["points", "assists", "rebounds", "fgm", "fga", "fg3m", "fg3a"]
        available = [col for col in stat_cols if col in df_raw.columns]
        if not available:
            available = ["points", "assists", "rebounds"]
        team_game = (
            df_raw.groupby(
                ["game_id", "team_abbreviation", "game_date"], as_index=False
            )[available]
            .sum()
            .rename(
                columns={
                    "points": "team_points",
                    "assists": "team_assists",
                    "rebounds": "team_rebounds",
                    "fgm": "team_fgm",
                    "fga": "team_fga",
                    "fg3m": "team_fg3m",
                    "fg3a": "team_fg3a",
                }
            )
        )

    if "team_fgm" not in team_game.columns and "fgm" in team_game.columns:
        team_game = team_game.rename(columns={"fgm": "team_fgm"})
    if "team_fga" not in team_game.columns and "fga" in team_game.columns:
        team_game = team_game.rename(columns={"fga": "team_fga"})
    if "team_fg3m" not in team_game.columns and "fg3m" in team_game.columns:
        team_game = team_game.rename(columns={"fg3m": "team_fg3m"})
    if "team_fg3a" not in team_game.columns and "fg3a" in team_game.columns:
        team_game = team_game.rename(columns={"fg3a": "team_fg3a"})

    for col in ["team_fgm", "team_fga", "team_fg3m", "team_fg3a"]:
        if col not in team_game.columns:
            team_game[col] = 0

    opp = team_game.merge(
        team_game,
        on="game_id",
        suffixes=("_team", "_opp"),
    )
    opp = opp[opp["team_abbreviation_team"] != opp["team_abbreviation_opp"]]
    opp = opp.rename(
        columns={
            "team_abbreviation_team": "team_abbreviation",
            "game_date_team": "game_date",
            "team_points_opp": "opponent_points",
            "team_assists_opp": "opponent_assists",
            "team_rebounds_opp": "opponent_rebounds",
            "team_fgm_opp": "opponent_fgm",
            "team_fga_opp": "opponent_fga",
            "team_fg3m_opp": "opponent_fg3m",
            "team_fg3a_opp": "opponent_fg3a",
        }
    )[[
        "game_id",
        "team_abbreviation",
        "game_date",
        "opponent_points",
        "opponent_assists",
        "opponent_rebounds",
        "opponent_fgm",
        "opponent_fga",
        "opponent_fg3m",
        "opponent_fg3a",
    ]]

    team_game = team_game.merge(
        opp, on=["game_id", "team_abbreviation", "game_date"], how="left"
    )
    team_game = team_game.sort_values(["team_abbreviation", "game_date"])

    numeric_cols = [
        "team_points",
        "team_assists",
        "team_rebounds",
        "team_fgm",
        "team_fga",
        "team_fg3m",
        "team_fg3a",
        "opponent_points",
        "opponent_assists",
        "opponent_rebounds",
        "opponent_fgm",
        "opponent_fga",
        "opponent_fg3m",
        "opponent_fg3a",
    ]
    for col in numeric_cols:
        if col in team_game.columns:
            team_game[col] = pd.to_numeric(team_game[col], errors="coerce").fillna(0)

    team_game["team_fg_pct"] = team_game["team_fgm"] / team_game["team_fga"].replace(
        0, pd.NA
    )
    team_game["team_missed_fg"] = team_game["team_fga"] - team_game["team_fgm"]
    team_game["opponent_fg_pct"] = team_game["opponent_fgm"] / team_game[
        "opponent_fga"
    ].replace(0, pd.NA)
    team_game["opponent_missed_fg"] = (
        team_game["opponent_fga"] - team_game["opponent_fgm"]
    )

    for col in [
        "team_fg_pct",
        "team_missed_fg",
        "opponent_fg_pct",
        "opponent_missed_fg",
    ]:
        team_game[col] = pd.to_numeric(team_game[col], errors="coerce").fillna(0)

    team_game["team_points_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_points"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_points_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_points"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["opponent_points_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_points"].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))

    team_game["team_assists_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_assists"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_assists_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_assists"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["opponent_assists_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_assists"].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))

    team_game["team_rebounds_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_rebounds"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_rebounds_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_rebounds"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["opponent_rebounds_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_rebounds"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )

    team_game["team_fga_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_fga"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_fga_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_fga"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["team_fg3a_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_fg3a"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_fg3a_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_fg3a"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["team_fg3m_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_fg3m"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_fg3m_avg_last10"] = team_game.groupby("team_abbreviation")[
        "team_fg3m"
    ].transform(lambda x: x.rolling(10, min_periods=1).mean().shift(1))
    team_game["team_fg_pct_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_fg_pct"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
    team_game["team_missed_fg_avg_last5"] = team_game.groupby("team_abbreviation")[
        "team_missed_fg"
    ].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))

    team_game["opponent_fg3a_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_fg3a"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    team_game["opponent_fg3m_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_fg3m"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    team_game["opponent_fg_pct_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_fg_pct"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    team_game["opponent_missed_fg_allowed_last5"] = team_game.groupby(
        "team_abbreviation"
    )["opponent_missed_fg"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )

    return team_game[
        [
            "game_id",
            "team_abbreviation",
            "game_date",
            "team_points_avg_last5",
            "team_points_avg_last10",
            "opponent_points_allowed_last5",
            "team_assists_avg_last5",
            "team_assists_avg_last10",
            "opponent_assists_allowed_last5",
            "team_rebounds_avg_last5",
            "team_rebounds_avg_last10",
            "opponent_rebounds_allowed_last5",
            "team_fga_avg_last5",
            "team_fga_avg_last10",
            "team_fg3a_avg_last5",
            "team_fg3a_avg_last10",
            "team_fg3m_avg_last5",
            "team_fg3m_avg_last10",
            "team_fg_pct_avg_last5",
            "team_missed_fg_avg_last5",
            "opponent_fg3a_allowed_last5",
            "opponent_fg3m_allowed_last5",
            "opponent_fg_pct_allowed_last5",
            "opponent_missed_fg_allowed_last5",
        ]
    ]


def add_player_rolling_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    for col in ["fgm", "fga", "fg3m", "fg3a"]:
        if col not in df.columns:
            df[col] = 0
    df = df.sort_values(["player_id", "game_date"])

    grouped = df.groupby("player_id")

    df["avg_points_last5"] = grouped["points"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_points_last10"] = grouped["points"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["std_points_last10"] = grouped["points"].transform(
        lambda x: x.rolling(10, min_periods=2).std().shift(1)
    )

    df["avg_assists_last5"] = grouped["assists"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_assists_last10"] = grouped["assists"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["std_assists_last10"] = grouped["assists"].transform(
        lambda x: x.rolling(10, min_periods=2).std().shift(1)
    )

    df["avg_rebounds_last5"] = grouped["rebounds"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_rebounds_last10"] = grouped["rebounds"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["std_rebounds_last10"] = grouped["rebounds"].transform(
        lambda x: x.rolling(10, min_periods=2).std().shift(1)
    )

    df["avg_minutes_last5"] = grouped["minutes"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_minutes_last10"] = grouped["minutes"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )

    df["avg_turnovers_last5"] = grouped["turnovers"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )

    df["avg_fgm_last5"] = grouped["fgm"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_fgm_last10"] = grouped["fgm"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["avg_fga_last5"] = grouped["fga"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_fga_last10"] = grouped["fga"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["fg_pct_last10"] = df["avg_fgm_last10"] / df["avg_fga_last10"].replace(
        0, pd.NA
    )
    df["avg_fg3m_last5"] = grouped["fg3m"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_fg3m_last10"] = grouped["fg3m"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["avg_fg3a_last5"] = grouped["fg3a"].transform(
        lambda x: x.rolling(5, min_periods=1).mean().shift(1)
    )
    df["avg_fg3a_last10"] = grouped["fg3a"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
    )
    df["fg3_pct_last10"] = df["avg_fg3m_last10"] / df["avg_fg3a_last10"].replace(
        0, pd.NA
    )
    df["fg3a_rate_last10"] = df["avg_fg3a_last10"] / df["avg_fga_last10"].replace(
        0, pd.NA
    )

    df["avg_points_per_min_last5"] = df["avg_points_last5"] / df[
        "avg_minutes_last5"
    ].replace(0, pd.NA)
    df["avg_assists_per_min_last5"] = df["avg_assists_last5"] / df[
        "avg_minutes_last5"
    ].replace(0, pd.NA)
    df["avg_rebounds_per_min_last5"] = df["avg_rebounds_last5"] / df[
        "avg_minutes_last5"
    ].replace(0, pd.NA)

    df["days_since_last_game"] = grouped["game_date"].diff().dt.days
    df["is_back_to_back"] = (df["days_since_last_game"] <= 1).astype(int)
    df["games_played_season"] = grouped.cumcount()
    df["is_home"] = df["matchup"].apply(lambda x: 1 if "@" not in x else 0)

    return df


def build_lineup_team_features(df_lineups: pd.DataFrame) -> pd.DataFrame:
    if df_lineups is None or df_lineups.empty:
        return pd.DataFrame(
            columns=[
                "team_abbreviation",
                "team_lineup_net_rating",
                "team_lineup_pace",
                "team_lineup_ast_pct",
                "team_lineup_reb_pct",
            ]
        )

    df = df_lineups.copy()
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0)

    def wavg(group, col):
        weights = group["minutes"]
        total = weights.sum()
        if total == 0:
            return group[col].mean()
        return (group[col] * weights).sum() / total

    grouped = df.groupby("team_abbreviation", as_index=False)
    rows = []
    for _, g in grouped:
        rows.append(
            {
                "team_abbreviation": g["team_abbreviation"].iloc[0],
                "team_lineup_net_rating": wavg(g, "net_rating"),
                "team_lineup_pace": wavg(g, "pace"),
                "team_lineup_ast_pct": wavg(g, "ast_pct"),
                "team_lineup_reb_pct": wavg(g, "reb_pct"),
            }
        )

    return pd.DataFrame(rows)


def compute_history_rolling_features(df_history: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling averages per player from historical games only.
    Used to build / update the rolling CSV.
    """

    df = df_history.copy()
    for col in ["fgm", "fga", "fg3m", "fg3a"]:
        if col not in df.columns:
            df[col] = 0
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

    df["avg_fg3m_last5"] = df.groupby("player_id")["fg3m"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["avg_fg3a_last5"] = df.groupby("player_id")["fg3a"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["avg_fga_last5"] = df.groupby("player_id")["fga"].transform(
        lambda x: x.rolling(5, min_periods=1).mean()
    )

    df["fg3_pct_last5"] = df["avg_fg3m_last5"] / df["avg_fg3a_last5"].replace(
        0, pd.NA
    )

    return df


def compute_prediction_features(
    df_next: pd.DataFrame,
    df_history: pd.DataFrame,
    df_team_game: Optional[pd.DataFrame] = None,
    df_lineup_team: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Build full feature set for upcoming games.
    Matches training-time features.
    """

    df_next = df_next.copy()
    df_history = df_history.copy()
    for col in ["fgm", "fga", "fg3m", "fg3a"]:
        if col not in df_history.columns:
            df_history[col] = 0
    df_history = df_history.sort_values(["player_id", "game_date"])

    grouped = df_history.groupby("player_id")

    df_next["avg_points_last5"] = df_next["player_id"].map(
        grouped["points"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_points_last10"] = df_next["player_id"].map(
        grouped["points"].apply(lambda x: x.tail(10).mean())
    )
    df_next["std_points_last10"] = df_next["player_id"].map(
        grouped["points"].apply(lambda x: x.tail(10).std())
    )

    df_next["avg_assists_last5"] = df_next["player_id"].map(
        grouped["assists"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_assists_last10"] = df_next["player_id"].map(
        grouped["assists"].apply(lambda x: x.tail(10).mean())
    )
    df_next["std_assists_last10"] = df_next["player_id"].map(
        grouped["assists"].apply(lambda x: x.tail(10).std())
    )

    df_next["avg_rebounds_last5"] = df_next["player_id"].map(
        grouped["rebounds"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_rebounds_last10"] = df_next["player_id"].map(
        grouped["rebounds"].apply(lambda x: x.tail(10).mean())
    )
    df_next["std_rebounds_last10"] = df_next["player_id"].map(
        grouped["rebounds"].apply(lambda x: x.tail(10).std())
    )

    df_next["avg_minutes_last5"] = df_next["player_id"].map(
        grouped["minutes"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_minutes_last10"] = df_next["player_id"].map(
        grouped["minutes"].apply(lambda x: x.tail(10).mean())
    )

    df_next["avg_turnovers_last5"] = df_next["player_id"].map(
        grouped["turnovers"].apply(lambda x: x.tail(5).mean())
    )

    df_next["avg_fgm_last5"] = df_next["player_id"].map(
        grouped["fgm"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_fgm_last10"] = df_next["player_id"].map(
        grouped["fgm"].apply(lambda x: x.tail(10).mean())
    )
    df_next["avg_fga_last5"] = df_next["player_id"].map(
        grouped["fga"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_fga_last10"] = df_next["player_id"].map(
        grouped["fga"].apply(lambda x: x.tail(10).mean())
    )
    df_next["fg_pct_last10"] = df_next["avg_fgm_last10"] / df_next[
        "avg_fga_last10"
    ].replace(0, pd.NA)

    df_next["avg_fg3m_last5"] = df_next["player_id"].map(
        grouped["fg3m"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_fg3m_last10"] = df_next["player_id"].map(
        grouped["fg3m"].apply(lambda x: x.tail(10).mean())
    )
    df_next["avg_fg3a_last5"] = df_next["player_id"].map(
        grouped["fg3a"].apply(lambda x: x.tail(5).mean())
    )
    df_next["avg_fg3a_last10"] = df_next["player_id"].map(
        grouped["fg3a"].apply(lambda x: x.tail(10).mean())
    )
    df_next["fg3_pct_last10"] = df_next["avg_fg3m_last10"] / df_next[
        "avg_fg3a_last10"
    ].replace(0, pd.NA)
    df_next["fg3a_rate_last10"] = df_next["avg_fg3a_last10"] / df_next[
        "avg_fga_last10"
    ].replace(0, pd.NA)

    df_next["avg_points_per_min_last5"] = df_next["avg_points_last5"] / df_next[
        "avg_minutes_last5"
    ].replace(0, pd.NA)
    df_next["avg_assists_per_min_last5"] = df_next["avg_assists_last5"] / df_next[
        "avg_minutes_last5"
    ].replace(0, pd.NA)
    df_next["avg_rebounds_per_min_last5"] = df_next["avg_rebounds_last5"] / df_next[
        "avg_minutes_last5"
    ].replace(0, pd.NA)

    last_game_dates = grouped["game_date"].max()
    df_next["days_since_last_game"] = (
        df_next["game_date"] - df_next["player_id"].map(last_game_dates)
    ).dt.days
    df_next["is_back_to_back"] = (df_next["days_since_last_game"] <= 1).astype(int)
    df_next["games_played_season"] = df_next["player_id"].map(grouped.size())
    df_next["is_home"] = df_next["matchup"].apply(lambda x: 1 if "@" not in x else 0)

    team_game = build_team_game_features(df_history, df_team_game)
    team_game = team_game.sort_values(["team_abbreviation", "game_date"])

    team_features = team_game.groupby("team_abbreviation").tail(1).set_index(
        "team_abbreviation"
    )

    df_next["team_points_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_points_avg_last5"]
    )
    df_next["team_points_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_points_avg_last10"]
    )
    df_next["team_assists_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_assists_avg_last5"]
    )
    df_next["team_assists_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_assists_avg_last10"]
    )
    df_next["team_rebounds_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_rebounds_avg_last5"]
    )
    df_next["team_rebounds_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_rebounds_avg_last10"]
    )
    df_next["team_fga_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_fga_avg_last5"]
    )
    df_next["team_fga_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_fga_avg_last10"]
    )
    df_next["team_fg3a_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_fg3a_avg_last5"]
    )
    df_next["team_fg3a_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_fg3a_avg_last10"]
    )
    df_next["team_fg3m_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_fg3m_avg_last5"]
    )
    df_next["team_fg3m_avg_last10"] = df_next["team_abbreviation"].map(
        team_features["team_fg3m_avg_last10"]
    )
    df_next["team_fg_pct_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_fg_pct_avg_last5"]
    )
    df_next["team_missed_fg_avg_last5"] = df_next["team_abbreviation"].map(
        team_features["team_missed_fg_avg_last5"]
    )

    df_next["opponent_team"] = df_next["matchup"].apply(parse_opponent_team)
    df_next["opponent_points_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_points_allowed_last5"]
    )
    df_next["opponent_assists_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_assists_allowed_last5"]
    )
    df_next["opponent_rebounds_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_rebounds_allowed_last5"]
    )
    df_next["opponent_fg3a_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_fg3a_allowed_last5"]
    )
    df_next["opponent_fg3m_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_fg3m_allowed_last5"]
    )
    df_next["opponent_fg_pct_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_fg_pct_allowed_last5"]
    )
    df_next["opponent_missed_fg_allowed_last5"] = df_next["opponent_team"].map(
        team_features["opponent_missed_fg_allowed_last5"]
    )

    lineup_team = build_lineup_team_features(df_lineup_team)
    if not lineup_team.empty:
        lineup_team = lineup_team.set_index("team_abbreviation")
        df_next["team_lineup_net_rating"] = df_next["team_abbreviation"].map(
            lineup_team["team_lineup_net_rating"]
        )
        df_next["team_lineup_pace"] = df_next["team_abbreviation"].map(
            lineup_team["team_lineup_pace"]
        )
        df_next["team_lineup_ast_pct"] = df_next["team_abbreviation"].map(
            lineup_team["team_lineup_ast_pct"]
        )
        df_next["team_lineup_reb_pct"] = df_next["team_abbreviation"].map(
            lineup_team["team_lineup_reb_pct"]
        )

    return df_next
