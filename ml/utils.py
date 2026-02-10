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
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
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
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
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
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
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
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
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
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
]

THREEPA_FEATURES = [
    "avg_minutes_last5",
    "avg_minutes_last10",
    "pred_minutes",
    "avg_fg3a_last5",
    "avg_fg3a_last10",
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
    "team_lineup_ast_pct",
    "teammate_count",
    "teammate_avg_points_last5_sum",
    "teammate_avg_assists_last5_sum",
    "teammate_avg_rebounds_last5_sum",
    "teammate_avg_fg3a_last5_sum",
    "teammate_avg_fga_last5_sum",
    "teammate_avg_minutes_last5_sum",
    "teammate_usage_sum_last10",
    "teammate_top_usage_sum_last10",
    "team_change_flag",
    "games_since_team_change",
]


def parse_opponent_team(matchup: str) -> Optional[str]:
    if not matchup:
        return None
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
    df["avg_turnovers_last10"] = grouped["turnovers"].transform(
        lambda x: x.rolling(10, min_periods=1).mean().shift(1)
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
    df["usage_proxy_last10"] = df["avg_fga_last10"] + df["avg_turnovers_last10"]

    df = _add_team_change_features(df)

    return df


def _add_team_change_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["player_id", "game_date"])
    grouped = df.groupby("player_id")
    df["prev_team"] = grouped["team_abbreviation"].shift(1)
    df["team_change_flag"] = (
        (df["team_abbreviation"] != df["prev_team"]) & df["prev_team"].notna()
    ).astype(int)

    def _since_change(series: pd.Series) -> pd.Series:
        prev = None
        count = 0
        out = []
        for team in series:
            if prev is None:
                out.append(0)
            else:
                if team != prev:
                    count = 0
                else:
                    count += 1
                out.append(count)
            prev = team
        return pd.Series(out, index=series.index)

    df["games_since_team_change"] = grouped["team_abbreviation"].apply(
        _since_change
    ).reset_index(level=0, drop=True)
    df = df.drop(columns=["prev_team"])
    return df


def add_teammate_context_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build teammate context features from actual game lineups (players who played).
    Assumes rolling features are already shifted (no leakage).
    """
    df = df.copy()
    context_cols = [
        "avg_points_last5",
        "avg_assists_last5",
        "avg_rebounds_last5",
        "avg_fg3a_last5",
        "avg_fga_last5",
        "avg_minutes_last5",
        "usage_proxy_last10",
    ]
    for col in context_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_cols = ["game_id", "team_abbreviation"]
    sums = df.groupby(group_cols)[context_cols].transform("sum")
    for col in context_cols:
        df[f"teammate_{col}_sum"] = sums[col] - df[col]

    df["teammate_count"] = (
        df.groupby(group_cols)["player_id"].transform("count") - 1
    )
    df["teammate_usage_sum_last10"] = (
        sums["usage_proxy_last10"] - df["usage_proxy_last10"]
    )

    df["teammate_top_usage_sum_last10"] = 0.0

    def _top_usage_per_group(group: pd.DataFrame) -> pd.Series:
        usage = group["usage_proxy_last10"].to_numpy()
        if usage.size == 0:
            return pd.Series([0.0] * len(group), index=group.index)
        order = usage.argsort()[::-1]
        top3_idx = order[:3]
        top3_sum = float(usage[top3_idx].sum())
        fourth_val = float(usage[order[3]]) if usage.size > 3 else 0.0
        top3_set = set(group.index[top3_idx])
        out = []
        for idx, val in zip(group.index, usage):
            if idx in top3_set:
                out.append(top3_sum - float(val) + fourth_val)
            else:
                out.append(top3_sum)
        return pd.Series(out, index=group.index)

    df["teammate_top_usage_sum_last10"] = (
        df.groupby(group_cols, group_keys=False).apply(_top_usage_per_group)
    )
    return df


def add_expected_teammate_context_features(
    df_next: pd.DataFrame,
    expected_players_by_team: Optional[dict[str, set[int]]] = None,
    excluded_players_by_team: Optional[dict[str, set[int]]] = None,
    bench_minutes_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    Build teammate context features for prediction time using expected lineups.
    If expected_players_by_team is None/empty, falls back to all players in df_next.
    """
    df = df_next.copy()
    expected_players_by_team = expected_players_by_team or {}
    excluded_players_by_team = excluded_players_by_team or {}
    def _is_expected(row):
        pid = row.get("player_id")
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return 0
        team_key = str(row.get("team_abbreviation") or "").upper()
        if pid in excluded_players_by_team.get(team_key, set()):
            return 0
        if pid in expected_players_by_team.get(team_key, set()):
            return 1
        if bench_minutes_threshold is not None:
            try:
                if float(row.get("avg_minutes_last5") or 0) >= float(
                    bench_minutes_threshold
                ):
                    return 1
            except (TypeError, ValueError):
                return 0
        return 0

    df["expected_active"] = df.apply(_is_expected, axis=1)
    if not expected_players_by_team and bench_minutes_threshold is None:
        df["expected_active"] = 1

    context_cols = [
        "avg_points_last5",
        "avg_assists_last5",
        "avg_rebounds_last5",
        "avg_fg3a_last5",
        "avg_fga_last5",
        "avg_minutes_last5",
        "usage_proxy_last10",
    ]
    for col in context_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    group_cols = ["game_id", "team_abbreviation"]
    active_df = df[df["expected_active"] == 1]
    if active_df.empty:
        df["teammate_count"] = 0
        for col in context_cols:
            df[f"teammate_{col}_sum"] = 0.0
        df["teammate_usage_sum_last10"] = 0.0
        df["teammate_top_usage_sum_last10"] = 0.0
        return df

    active_sums = (
        active_df.groupby(group_cols)[context_cols]
        .sum()
        .rename(columns={c: f"active_sum_{c}" for c in context_cols})
        .reset_index()
    )
    active_counts = (
        active_df.groupby(group_cols)["player_id"]
        .count()
        .reset_index()
        .rename(columns={"player_id": "active_count"})
    )
    df = df.merge(active_sums, on=group_cols, how="left")
    df = df.merge(active_counts, on=group_cols, how="left")

    for col in context_cols:
        sum_col = f"active_sum_{col}"
        df[sum_col] = pd.to_numeric(df[sum_col], errors="coerce").fillna(0)
        df[f"teammate_{col}_sum"] = df[sum_col] - df[col] * df["expected_active"]

    df["active_count"] = pd.to_numeric(df["active_count"], errors="coerce").fillna(0)
    df["teammate_count"] = df["active_count"] - df["expected_active"]
    df["teammate_usage_sum_last10"] = (
        df["active_sum_usage_proxy_last10"]
        - df["usage_proxy_last10"] * df["expected_active"]
    )

    def _top_usage_info(group: pd.DataFrame) -> pd.Series:
        usage = group["usage_proxy_last10"].to_numpy()
        ids = group["player_id"].to_numpy()
        if usage.size == 0:
            return pd.Series(
                {"active_top3_sum": 0.0, "active_fourth_usage": 0.0, "active_top3_ids": tuple()}
            )
        order = usage.argsort()[::-1]
        top3_idx = order[:3]
        top3_sum = float(usage[top3_idx].sum())
        fourth_val = float(usage[order[3]]) if usage.size > 3 else 0.0
        top3_ids = tuple(int(ids[i]) for i in top3_idx)
        return pd.Series(
            {
                "active_top3_sum": top3_sum,
                "active_fourth_usage": fourth_val,
                "active_top3_ids": top3_ids,
            }
        )

    top_info = (
        active_df.groupby(group_cols)
        .apply(_top_usage_info)
        .reset_index()
    )
    df = df.merge(top_info, on=group_cols, how="left")

    def _teammate_top_usage(row):
        base = row.get("active_top3_sum")
        if base is None:
            return 0.0
        if int(row.get("expected_active") or 0) != 1:
            return float(base)
        top_ids = row.get("active_top3_ids") or tuple()
        pid = row.get("player_id")
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return float(base)
        if pid in top_ids:
            return float(base) - float(row.get("usage_proxy_last10") or 0) + float(
                row.get("active_fourth_usage") or 0
            )
        return float(base)

    df["teammate_top_usage_sum_last10"] = df.apply(_teammate_top_usage, axis=1)

    df = df.drop(
        columns=[
            c
            for c in df.columns
            if c.startswith("active_sum_") or c.startswith("active_")
        ],
        errors="ignore",
    )
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
    expected_players_by_team: Optional[dict[str, set[int]]] = None,
    excluded_players_by_team: Optional[dict[str, set[int]]] = None,
    bench_minutes_threshold: Optional[float] = None,
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
    df_next["avg_turnovers_last10"] = df_next["player_id"].map(
        grouped["turnovers"].apply(lambda x: x.tail(10).mean())
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
    df_next["usage_proxy_last10"] = df_next["avg_fga_last10"] + df_next["avg_turnovers_last10"]

    last_team = grouped["team_abbreviation"].apply(lambda x: x.tail(1).iloc[0])
    last_team_map = df_next["player_id"].map(last_team)
    df_next["team_change_flag"] = (
        df_next["team_abbreviation"].ne(last_team_map)
    ).astype(int)
    df_next.loc[last_team_map.isna(), "team_change_flag"] = 0

    def _team_streak(series: pd.Series) -> int:
        if series.empty:
            return 0
        last_team = series.iloc[-1]
        streak = 0
        for team in reversed(series.tolist()):
            if team == last_team:
                streak += 1
            else:
                break
        return max(0, streak - 1)

    streak_map = grouped["team_abbreviation"].apply(_team_streak)
    df_next["games_since_team_change"] = df_next["player_id"].map(streak_map).fillna(0)
    df_next.loc[df_next["team_change_flag"] == 1, "games_since_team_change"] = 0

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

    if "opponent_team" in df_next.columns:
        df_next["opponent_team"] = df_next["opponent_team"].fillna(
            df_next["matchup"].apply(parse_opponent_team)
        )
    else:
        df_next["opponent_team"] = df_next["matchup"].apply(parse_opponent_team)
    df_next["opponent_team"] = df_next["opponent_team"].astype(str).str.upper()
    df_next.loc[
        df_next["opponent_team"].isin(["", "NAN", "NONE"]), "opponent_team"
    ] = pd.NA
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

    df_next = add_expected_teammate_context_features(
        df_next,
        expected_players_by_team=expected_players_by_team,
        excluded_players_by_team=excluded_players_by_team,
        bench_minutes_threshold=bench_minutes_threshold,
    )

    return df_next
