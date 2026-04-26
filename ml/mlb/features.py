from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


ROOT_DIR = Path(__file__).resolve().parents[2]


def get_engine(database_url: str | None = None):
    load_dotenv(ROOT_DIR / ".env")
    url = database_url or os.getenv("ML_DATABASE_URL") or os.getenv("SYNC_DATABASE_URL")
    if not url:
        raise ValueError("Missing ML_DATABASE_URL or SYNC_DATABASE_URL for MLB training.")
    return create_engine(url)


def _read_sql(sql: str, engine) -> pd.DataFrame:
    return pd.read_sql(sql, engine)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, np.nan)
    return numerator / denominator


def _add_group_rolling(
    df: pd.DataFrame,
    *,
    group_cols: str | list[str],
    value_cols: Iterable[str],
    windows: Iterable[int] = (5, 10, 20),
    prefix: str,
) -> pd.DataFrame:
    group_cols = [group_cols] if isinstance(group_cols, str) else group_cols
    df = df.sort_values(group_cols + ["game_date", "game_pk"]).copy()
    grouped = df.groupby(group_cols, sort=False)
    for col in value_cols:
        shifted = grouped[col].shift(1)
        for window in windows:
            df[f"{prefix}_{col}_avg_last{window}"] = shifted.groupby(
                [df[group_col] for group_col in group_cols], sort=False
            ).transform(lambda series: series.rolling(window, min_periods=1).mean())
            df[f"{prefix}_{col}_sum_last{window}"] = shifted.groupby(
                [df[group_col] for group_col in group_cols], sort=False
            ).transform(lambda series: series.rolling(window, min_periods=1).sum())
    return df


def _add_player_schedule_features(df: pd.DataFrame, player_col: str) -> pd.DataFrame:
    df = df.sort_values([player_col, "game_date", "game_pk"]).copy()
    grouped = df.groupby([player_col, "season"], sort=False)
    df["player_games_played_season"] = grouped.cumcount()
    prior_date = df.groupby(player_col, sort=False)["game_date"].shift(1)
    df["days_since_player_game"] = (df["game_date"] - prior_date).dt.days
    df["is_player_back_to_back"] = (df["days_since_player_game"] == 1).astype(int)
    return df


def _load_weather_features(engine) -> pd.DataFrame:
    return _read_sql(
        """
        with ranked as (
            select
                game_pk,
                temperature_2m_c,
                relative_humidity_2m,
                dew_point_2m_c,
                surface_pressure_hpa,
                pressure_msl_hpa,
                wind_speed_10m_kph,
                wind_direction_10m_deg,
                wind_gusts_10m_kph,
                cloud_cover_percent,
                visibility_m,
                precipitation_mm,
                rain_mm,
                weather_code,
                row_number() over (
                    partition by game_pk
                    order by abs(coalesce(game_time_offset_hours, 9999))
                ) as rn
            from mlb_weather_snapshots
        )
        select *
        from ranked
        where rn = 1
        """,
        engine,
    ).drop(columns=["rn"], errors="ignore")


def _load_park_features(engine) -> pd.DataFrame:
    return _read_sql(
        """
        select
            season,
            venue_id,
            avg(stat_value) as park_factor_avg,
            max(stat_value) filter (where stat_key = 'index_HR') as park_factor_hr,
            avg(environment_factor) as park_environment_factor,
            avg(temperature_factor) as park_temperature_factor,
            avg(elevation_factor) as park_elevation_factor,
            avg(roof_factor) as park_roof_factor
        from mlb_park_factors
        where venue_id is not null
        group by season, venue_id
        """,
        engine,
    )


def _load_batter_context(engine) -> pd.DataFrame:
    return _read_sql(
        """
        select
            s.season,
            s.player_id,
            s.xba as batter_xba,
            s.xslg as batter_xslg,
            s.xwoba as batter_xwoba,
            s.xobp as batter_xobp,
            s.xiso as batter_xiso,
            s.exit_velocity_avg as batter_exit_velocity_avg,
            s.launch_angle_avg as batter_launch_angle_avg,
            s.barrel_batted_rate as batter_barrel_batted_rate,
            s.hard_hit_percent as batter_hard_hit_percent,
            s.sweet_spot_percent as batter_sweet_spot_percent,
            bt.bat_speed as batter_bat_speed,
            bt.fast_swing_rate as batter_fast_swing_rate,
            bt.squared_up_rate as batter_squared_up_rate,
            bt.blast_rate as batter_blast_rate,
            bt.swing_length as batter_swing_length,
            sp.attack_angle as batter_attack_angle,
            sp.attack_direction as batter_attack_direction,
            sp.ideal_attack_angle_rate as batter_ideal_attack_angle_rate,
            sp.swing_path_tilt as batter_swing_path_tilt
        from mlb_statcast_batter_season s
        left join mlb_bat_tracking_batter_season bt
            on bt.season = s.season and bt.player_id = s.player_id
        left join mlb_swing_path_batter_season sp
            on sp.season = s.season and sp.player_id = s.player_id
        """,
        engine,
    )


def _load_pitcher_context(engine) -> pd.DataFrame:
    return _read_sql(
        """
        select
            season,
            player_id,
            batters_faced as pitcher_savant_batters_faced,
            strikeout_percent as pitcher_savant_strikeout_percent,
            walk_percent as pitcher_savant_walk_percent,
            xera as pitcher_xera,
            xba as pitcher_xba,
            xslg as pitcher_xslg,
            xwoba as pitcher_xwoba,
            exit_velocity_avg as pitcher_exit_velocity_avg,
            launch_angle_avg as pitcher_launch_angle_avg,
            barrel_batted_rate as pitcher_barrel_batted_rate,
            hard_hit_percent as pitcher_hard_hit_percent
        from mlb_statcast_pitcher_season
        """,
        engine,
    )


def _load_starting_pitchers(engine) -> pd.DataFrame:
    starters = _read_sql(
        """
        select
            p.game_pk,
            p.team_id,
            p.player_id as starter_pitcher_id,
            p.outs_recorded,
            p.batters_faced,
            p.pitches_thrown,
            p.strikeouts,
            p.walks,
            p.hits_allowed,
            p.home_runs_allowed,
            g.official_date as game_date
        from mlb_player_game_pitching p
        join mlb_games g on g.game_pk = p.game_pk
        where p.is_starter = true
        """,
        engine,
    )
    if starters.empty:
        return starters
    starters["game_date"] = pd.to_datetime(starters["game_date"])
    stat_cols = [
        "outs_recorded",
        "batters_faced",
        "pitches_thrown",
        "strikeouts",
        "walks",
        "hits_allowed",
        "home_runs_allowed",
    ]
    starters[stat_cols] = starters[stat_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    starters = _add_group_rolling(
        starters,
        group_cols="starter_pitcher_id",
        value_cols=stat_cols,
        windows=(5, 10, 20),
        prefix="opp_starter",
    )
    keep_cols = [
        "game_pk",
        "team_id",
        "starter_pitcher_id",
        *[col for col in starters.columns if col.startswith("opp_starter_")],
    ]
    return starters[keep_cols]


def build_batter_training_frame(engine=None, database_url: str | None = None) -> pd.DataFrame:
    engine = engine or get_engine(database_url)
    df = _read_sql(
        """
        select
            b.game_pk,
            b.player_id,
            b.team_id,
            b.batting_order,
            b.plate_appearances,
            b.at_bats,
            b.hits,
            b.doubles,
            b.triples,
            b.home_runs,
            b.total_bases,
            b.walks,
            b.strikeouts,
            b.hit_by_pitch,
            g.official_date as game_date,
            g.season,
            g.home_team_id,
            g.away_team_id,
            g.venue_id,
            g.day_night,
            case when b.team_id = g.home_team_id then 1 else 0 end as is_home,
            case when b.team_id = g.home_team_id then g.away_team_id else g.home_team_id end as opponent_team_id,
            v.elevation,
            v.capacity
        from mlb_player_game_batting b
        join mlb_games g on g.game_pk = b.game_pk
        left join mlb_venues v on v.id = g.venue_id
        where b.plate_appearances is not null
          and b.plate_appearances > 0
          and g.detailed_state in ('Final', 'Game Over', 'Completed Early')
        """,
        engine,
    )
    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"])
    numeric_cols = [
        "batting_order",
        "plate_appearances",
        "at_bats",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "walks",
        "strikeouts",
        "hit_by_pitch",
        "is_home",
        "elevation",
        "capacity",
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df["target_home_run"] = (df["home_runs"].fillna(0) > 0).astype(int)
    df["target_hits"] = df["hits"].fillna(0)
    df["target_total_bases"] = df["total_bases"].fillna(0)

    df = _add_player_schedule_features(df, "player_id")
    df = _add_group_rolling(
        df,
        group_cols="player_id",
        value_cols=[
            "plate_appearances",
            "at_bats",
            "hits",
            "doubles",
            "triples",
            "home_runs",
            "total_bases",
            "walks",
            "strikeouts",
            "hit_by_pitch",
        ],
        windows=(5, 10, 20),
        prefix="batter",
    )
    for window in (5, 10, 20):
        df[f"batter_hit_rate_last{window}"] = _safe_div(
            df[f"batter_hits_sum_last{window}"], df[f"batter_plate_appearances_sum_last{window}"]
        )
        df[f"batter_hr_rate_last{window}"] = _safe_div(
            df[f"batter_home_runs_sum_last{window}"], df[f"batter_plate_appearances_sum_last{window}"]
        )
        df[f"batter_tb_per_pa_last{window}"] = _safe_div(
            df[f"batter_total_bases_sum_last{window}"], df[f"batter_plate_appearances_sum_last{window}"]
        )
        df[f"batter_k_rate_last{window}"] = _safe_div(
            df[f"batter_strikeouts_sum_last{window}"], df[f"batter_plate_appearances_sum_last{window}"]
        )
        df[f"batter_bb_rate_last{window}"] = _safe_div(
            df[f"batter_walks_sum_last{window}"], df[f"batter_plate_appearances_sum_last{window}"]
        )

    team_game = (
        df.groupby(["game_pk", "team_id", "game_date"], as_index=False)
        .agg(
            team_pa=("plate_appearances", "sum"),
            team_hits=("hits", "sum"),
            team_hr=("home_runs", "sum"),
            team_tb=("total_bases", "sum"),
            team_k=("strikeouts", "sum"),
            team_bb=("walks", "sum"),
        )
        .sort_values(["team_id", "game_date", "game_pk"])
    )
    team_game = _add_group_rolling(
        team_game,
        group_cols="team_id",
        value_cols=["team_pa", "team_hits", "team_hr", "team_tb", "team_k", "team_bb"],
        windows=(5, 10, 20),
        prefix="team_batting",
    )
    opponent_team = team_game.rename(columns={"team_id": "opponent_team_id"})
    opponent_cols = [
        "game_pk",
        "opponent_team_id",
        *[col for col in opponent_team.columns if col.startswith("team_batting_")],
    ]
    df = df.merge(opponent_team[opponent_cols], on=["game_pk", "opponent_team_id"], how="left")

    starters = _load_starting_pitchers(engine)
    if not starters.empty:
        starters = starters.rename(columns={"team_id": "opponent_team_id"})
        df = df.merge(starters, on=["game_pk", "opponent_team_id"], how="left")

    for extra in (_load_batter_context(engine), _load_weather_features(engine), _load_park_features(engine)):
        keys = ["season", "player_id"] if "player_id" in extra.columns else ["game_pk"] if "game_pk" in extra.columns else ["season", "venue_id"]
        df = df.merge(extra, on=keys, how="left")
    df["weather_available"] = df["temperature_2m_c"].notna().astype(int)
    df["is_night"] = (df["day_night"].fillna("").str.lower() == "night").astype(int)
    return df


def build_pitcher_training_frame(engine=None, database_url: str | None = None) -> pd.DataFrame:
    engine = engine or get_engine(database_url)
    df = _read_sql(
        """
        select
            p.game_pk,
            p.player_id,
            p.team_id,
            p.outs_recorded,
            p.batters_faced,
            p.pitches_thrown,
            p.strikes,
            p.balls,
            p.hits_allowed,
            p.home_runs_allowed,
            p.earned_runs,
            p.walks,
            p.strikeouts,
            g.official_date as game_date,
            g.season,
            g.home_team_id,
            g.away_team_id,
            g.venue_id,
            g.day_night,
            case when p.team_id = g.home_team_id then 1 else 0 end as is_home,
            case when p.team_id = g.home_team_id then g.away_team_id else g.home_team_id end as opponent_team_id,
            v.elevation,
            v.capacity
        from mlb_player_game_pitching p
        join mlb_games g on g.game_pk = p.game_pk
        left join mlb_venues v on v.id = g.venue_id
        where p.is_starter = true
          and p.strikeouts is not null
          and g.detailed_state in ('Final', 'Game Over', 'Completed Early')
        """,
        engine,
    )
    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"])
    stat_cols = [
        "outs_recorded",
        "batters_faced",
        "pitches_thrown",
        "strikes",
        "balls",
        "hits_allowed",
        "home_runs_allowed",
        "earned_runs",
        "walks",
        "strikeouts",
    ]
    df[stat_cols] = df[stat_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["target_strikeouts"] = df["strikeouts"]
    df = _add_player_schedule_features(df, "player_id")
    df = _add_group_rolling(
        df,
        group_cols="player_id",
        value_cols=stat_cols,
        windows=(5, 10, 20),
        prefix="pitcher",
    )
    for window in (5, 10, 20):
        df[f"pitcher_k_per_bf_last{window}"] = _safe_div(
            df[f"pitcher_strikeouts_sum_last{window}"], df[f"pitcher_batters_faced_sum_last{window}"]
        )
        df[f"pitcher_bb_per_bf_last{window}"] = _safe_div(
            df[f"pitcher_walks_sum_last{window}"], df[f"pitcher_batters_faced_sum_last{window}"]
        )
        df[f"pitcher_outs_avg_last{window}"] = df[f"pitcher_outs_recorded_avg_last{window}"]

    batting = _read_sql(
        """
        select
            b.game_pk,
            b.team_id,
            g.official_date as game_date,
            sum(b.plate_appearances) as opp_pa,
            sum(b.strikeouts) as opp_k,
            sum(b.walks) as opp_bb,
            sum(b.hits) as opp_hits,
            sum(b.home_runs) as opp_hr,
            sum(b.total_bases) as opp_tb
        from mlb_player_game_batting b
        join mlb_games g on g.game_pk = b.game_pk
        where b.plate_appearances is not null
        group by b.game_pk, b.team_id, g.official_date
        """,
        engine,
    )
    if not batting.empty:
        batting["game_date"] = pd.to_datetime(batting["game_date"])
        batting[["opp_pa", "opp_k", "opp_bb", "opp_hits", "opp_hr", "opp_tb"]] = batting[
            ["opp_pa", "opp_k", "opp_bb", "opp_hits", "opp_hr", "opp_tb"]
        ].apply(pd.to_numeric, errors="coerce").fillna(0)
        batting = _add_group_rolling(
            batting,
            group_cols="team_id",
            value_cols=["opp_pa", "opp_k", "opp_bb", "opp_hits", "opp_hr", "opp_tb"],
            windows=(5, 10, 20),
            prefix="opponent_batting",
        )
        batting = batting.rename(columns={"team_id": "opponent_team_id"})
        keep = [
            "game_pk",
            "opponent_team_id",
            *[col for col in batting.columns if col.startswith("opponent_batting_")],
        ]
        df = df.merge(batting[keep], on=["game_pk", "opponent_team_id"], how="left")
        for window in (5, 10, 20):
            df[f"opponent_k_rate_last{window}"] = _safe_div(
                df[f"opponent_batting_opp_k_sum_last{window}"],
                df[f"opponent_batting_opp_pa_sum_last{window}"],
            )

    for extra in (_load_pitcher_context(engine), _load_weather_features(engine), _load_park_features(engine)):
        keys = ["season", "player_id"] if "player_id" in extra.columns else ["game_pk"] if "game_pk" in extra.columns else ["season", "venue_id"]
        df = df.merge(extra, on=keys, how="left")
    df["weather_available"] = df["temperature_2m_c"].notna().astype(int)
    df["is_night"] = (df["day_night"].fillna("").str.lower() == "night").astype(int)
    return df


def model_feature_columns(df: pd.DataFrame, target_col: str) -> list[str]:
    excluded = {
        "game_pk",
        "player_id",
        "team_id",
        "home_team_id",
        "away_team_id",
        "opponent_team_id",
        "venue_id",
        "game_date",
        "day_night",
        "target_home_run",
        "target_hits",
        "target_total_bases",
        "target_strikeouts",
        "plate_appearances",
        "at_bats",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "walks",
        "strikeouts",
        "hit_by_pitch",
        "outs_recorded",
        "batters_faced",
        "pitches_thrown",
        "strikes",
        "balls",
        "hits_allowed",
        "home_runs_allowed",
        "earned_runs",
    }
    numeric_cols = []
    for col in df.columns:
        if col in excluded or col == target_col:
            continue
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
            if df[col].notna().mean() >= 0.05:
                numeric_cols.append(col)
    return sorted(numeric_cols)
