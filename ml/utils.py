from typing import Optional
import pandas as pd

POINTS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
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
]

ASSISTS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
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
]

REBOUNDS_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
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
]


def parse_opponent_team(matchup: str) -> Optional[str]:
    if " vs. " in matchup:
        return matchup.split(" vs. ")[1]
    if " @ " in matchup:
        return matchup.split(" @ ")[1]
    return None


def build_team_game_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    team_game = (
        df_raw.groupby(["game_id", "team_abbreviation", "game_date"], as_index=False)[
            ["points", "assists", "rebounds"]
        ]
        .sum()
        .rename(
            columns={
                "points": "team_points",
                "assists": "team_assists",
                "rebounds": "team_rebounds",
            }
        )
    )

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
        }
    )[[
        "game_id",
        "team_abbreviation",
        "game_date",
        "opponent_points",
        "opponent_assists",
        "opponent_rebounds",
    ]]

    team_game = team_game.merge(
        opp, on=["game_id", "team_abbreviation", "game_date"], how="left"
    )
    team_game = team_game.sort_values(["team_abbreviation", "game_date"])

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
        ]
    ]


def add_player_rolling_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
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
    df_history = df_history.copy()
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

    team_game = build_team_game_features(df_history)
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

    return df_next
