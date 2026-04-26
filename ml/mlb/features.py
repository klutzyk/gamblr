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
    add_sums: bool = True,
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
            if add_sums:
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
            max(stat_value) filter (where stat_key = 'index_HR' and upper(coalesce(bat_side, '')) = 'L') as park_factor_hr_lhb,
            max(stat_value) filter (where stat_key = 'index_HR' and upper(coalesce(bat_side, '')) = 'R') as park_factor_hr_rhb,
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


def _load_batter_batted_ball_history(engine) -> pd.DataFrame:
    history = _read_sql(
        """
        select
            bb.game_pk,
            bb.batter_id as player_id,
            g.official_date as game_date,
            count(*)::float as bbe_count,
            avg(bb.launch_speed) as bbe_avg_launch_speed,
            max(bb.launch_speed) as bbe_max_launch_speed,
            avg(bb.launch_angle) as bbe_avg_launch_angle,
            max(bb.total_distance) as bbe_max_distance,
            avg(bb.estimated_ba_using_speedangle) as bbe_avg_estimated_ba,
            avg(bb.estimated_woba_using_speedangle) as bbe_avg_estimated_woba,
            avg(bb.coord_x) as bbe_avg_coord_x,
            avg(bb.coord_y) as bbe_avg_coord_y,
            avg(case when bb.is_hard_hit then 1.0 else 0.0 end) as bbe_hard_hit_rate,
            avg(case when bb.is_sweet_spot then 1.0 else 0.0 end) as bbe_sweet_spot_rate,
            avg(case when bb.launch_speed_angle = 6 then 1.0 else 0.0 end) as bbe_barrel_rate,
            avg(case when bb.launch_speed >= 95 and bb.launch_angle between 20 and 35 then 1.0 else 0.0 end) as bbe_hr_contact_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%fly%%' then 1.0 else 0.0 end) as bbe_fly_ball_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%line%%' then 1.0 else 0.0 end) as bbe_line_drive_rate
        from mlb_batted_ball_events bb
        join mlb_games g on g.game_pk = bb.game_pk
        where bb.batter_id is not null
        group by bb.game_pk, bb.batter_id, g.official_date
        """,
        engine,
    )
    if history.empty:
        return history

    history["game_date"] = pd.to_datetime(history["game_date"])
    value_cols = [
        "bbe_count",
        "bbe_avg_launch_speed",
        "bbe_max_launch_speed",
        "bbe_avg_launch_angle",
        "bbe_max_distance",
        "bbe_avg_estimated_ba",
        "bbe_avg_estimated_woba",
        "bbe_avg_coord_x",
        "bbe_avg_coord_y",
        "bbe_hard_hit_rate",
        "bbe_sweet_spot_rate",
        "bbe_barrel_rate",
        "bbe_hr_contact_rate",
        "bbe_fly_ball_rate",
        "bbe_line_drive_rate",
    ]
    history[value_cols] = history[value_cols].apply(pd.to_numeric, errors="coerce")
    history = _add_group_rolling(
        history,
        group_cols="player_id",
        value_cols=value_cols,
        windows=(5, 10, 20),
        prefix="batter_bbe",
        add_sums=False,
    )
    keep_cols = [
        "game_pk",
        "player_id",
        *[col for col in history.columns if col.startswith("batter_bbe_")],
    ]
    return history[keep_cols]


def _load_pitcher_pitch_history(engine) -> pd.DataFrame:
    history = _read_sql(
        """
        select
            pe.game_pk,
            pe.pitcher_id as starter_pitcher_id,
            g.official_date as game_date,
            count(*)::float as pitch_count,
            avg(pe.start_speed) as pitch_avg_start_speed,
            max(pe.start_speed) as pitch_max_start_speed,
            avg(pe.spin_rate) as pitch_avg_spin_rate,
            avg(pe.extension) as pitch_avg_extension,
            avg(abs(pe.pfx_x)) as pitch_avg_abs_pfx_x,
            avg(pe.pfx_z) as pitch_avg_pfx_z,
            avg(abs(pe.break_horizontal)) as pitch_avg_abs_break_horizontal,
            avg(pe.break_vertical) as pitch_avg_break_vertical,
            avg(case when pe.is_strike then 1.0 else 0.0 end) as pitch_strike_rate,
            avg(case when pe.is_ball then 1.0 else 0.0 end) as pitch_ball_rate,
            avg(case when pe.is_in_play then 1.0 else 0.0 end) as pitch_in_play_rate,
            avg(case when pe.zone between 1 and 9 then 1.0 else 0.0 end) as pitch_zone_rate,
            avg(case when pe.pitch_type_code in ('FF', 'FA') then 1.0 else 0.0 end) as pitch_four_seam_rate,
            avg(case when pe.pitch_type_code in ('FT', 'SI', 'FC') then 1.0 else 0.0 end) as pitch_sinker_cutter_rate,
            avg(case when pe.pitch_type_code in ('FF', 'FA', 'FT', 'SI', 'FC') then 1.0 else 0.0 end) as pitch_fastball_rate,
            avg(case when pe.pitch_type_code in ('SL', 'ST') then 1.0 else 0.0 end) as pitch_slider_sweeper_rate,
            avg(case when pe.pitch_type_code in ('CU', 'KC', 'SV', 'CS') then 1.0 else 0.0 end) as pitch_curve_rate,
            avg(case when pe.pitch_type_code in ('SL', 'ST', 'CU', 'KC', 'SV', 'CS') then 1.0 else 0.0 end) as pitch_breaking_rate,
            avg(case when pe.pitch_type_code in ('CH', 'FS', 'FO', 'SC') then 1.0 else 0.0 end) as pitch_offspeed_rate,
            avg(case when pe.pitch_type_code = 'CH' then 1.0 else 0.0 end) as pitch_changeup_rate,
            count(distinct pe.pitch_type_code)::float as pitch_type_count
        from mlb_pitch_events pe
        join mlb_games g on g.game_pk = pe.game_pk
        where pe.pitcher_id is not null
        group by pe.game_pk, pe.pitcher_id, g.official_date
        """,
        engine,
    )
    if history.empty:
        return history

    history["game_date"] = pd.to_datetime(history["game_date"])
    value_cols = [
        "pitch_count",
        "pitch_avg_start_speed",
        "pitch_max_start_speed",
        "pitch_avg_spin_rate",
        "pitch_avg_extension",
        "pitch_avg_abs_pfx_x",
        "pitch_avg_pfx_z",
        "pitch_avg_abs_break_horizontal",
        "pitch_avg_break_vertical",
        "pitch_strike_rate",
        "pitch_ball_rate",
        "pitch_in_play_rate",
        "pitch_zone_rate",
        "pitch_four_seam_rate",
        "pitch_sinker_cutter_rate",
        "pitch_fastball_rate",
        "pitch_slider_sweeper_rate",
        "pitch_curve_rate",
        "pitch_breaking_rate",
        "pitch_offspeed_rate",
        "pitch_changeup_rate",
        "pitch_type_count",
    ]
    history[value_cols] = history[value_cols].apply(pd.to_numeric, errors="coerce")
    history = _add_group_rolling(
        history,
        group_cols="starter_pitcher_id",
        value_cols=value_cols,
        windows=(5, 10, 20),
        prefix="opp_starter_pitch",
        add_sums=False,
    )
    keep_cols = [
        "game_pk",
        "starter_pitcher_id",
        *[col for col in history.columns if col.startswith("opp_starter_pitch_")],
    ]
    return history[keep_cols]


def _load_pitcher_batted_ball_allowed_history(engine) -> pd.DataFrame:
    history = _read_sql(
        """
        select
            bb.game_pk,
            bb.pitcher_id as starter_pitcher_id,
            g.official_date as game_date,
            count(*)::float as bbe_allowed_count,
            avg(bb.launch_speed) as bbe_allowed_avg_launch_speed,
            max(bb.launch_speed) as bbe_allowed_max_launch_speed,
            avg(bb.launch_angle) as bbe_allowed_avg_launch_angle,
            max(bb.total_distance) as bbe_allowed_max_distance,
            avg(bb.estimated_ba_using_speedangle) as bbe_allowed_avg_estimated_ba,
            avg(bb.estimated_woba_using_speedangle) as bbe_allowed_avg_estimated_woba,
            avg(bb.coord_x) as bbe_allowed_avg_coord_x,
            avg(bb.coord_y) as bbe_allowed_avg_coord_y,
            avg(case when bb.is_hard_hit then 1.0 else 0.0 end) as bbe_allowed_hard_hit_rate,
            avg(case when bb.is_sweet_spot then 1.0 else 0.0 end) as bbe_allowed_sweet_spot_rate,
            avg(case when bb.launch_speed_angle = 6 then 1.0 else 0.0 end) as bbe_allowed_barrel_rate,
            avg(case when bb.launch_speed >= 95 and bb.launch_angle between 20 and 35 then 1.0 else 0.0 end) as bbe_allowed_hr_contact_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%fly%%' then 1.0 else 0.0 end) as bbe_allowed_fly_ball_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%line%%' then 1.0 else 0.0 end) as bbe_allowed_line_drive_rate
        from mlb_batted_ball_events bb
        join mlb_games g on g.game_pk = bb.game_pk
        where bb.pitcher_id is not null
        group by bb.game_pk, bb.pitcher_id, g.official_date
        """,
        engine,
    )
    if history.empty:
        return history

    history["game_date"] = pd.to_datetime(history["game_date"])
    value_cols = [
        "bbe_allowed_count",
        "bbe_allowed_avg_launch_speed",
        "bbe_allowed_max_launch_speed",
        "bbe_allowed_avg_launch_angle",
        "bbe_allowed_max_distance",
        "bbe_allowed_avg_estimated_ba",
        "bbe_allowed_avg_estimated_woba",
        "bbe_allowed_avg_coord_x",
        "bbe_allowed_avg_coord_y",
        "bbe_allowed_hard_hit_rate",
        "bbe_allowed_sweet_spot_rate",
        "bbe_allowed_barrel_rate",
        "bbe_allowed_hr_contact_rate",
        "bbe_allowed_fly_ball_rate",
        "bbe_allowed_line_drive_rate",
    ]
    history[value_cols] = history[value_cols].apply(pd.to_numeric, errors="coerce")
    history = _add_group_rolling(
        history,
        group_cols="starter_pitcher_id",
        value_cols=value_cols,
        windows=(5, 10, 20),
        prefix="opp_starter_bbe_allowed",
        add_sums=False,
    )
    keep_cols = [
        "game_pk",
        "starter_pitcher_id",
        *[col for col in history.columns if col.startswith("opp_starter_bbe_allowed_")],
    ]
    return history[keep_cols]


def _load_bullpen_history(engine) -> pd.DataFrame:
    relief = _read_sql(
        """
        select
            p.game_pk,
            p.team_id as opponent_team_id,
            g.official_date as game_date,
            count(*)::float as bullpen_pitchers_used,
            sum(coalesce(p.outs_recorded, 0))::float as bullpen_outs_recorded,
            sum(coalesce(p.batters_faced, 0))::float as bullpen_batters_faced,
            sum(coalesce(p.pitches_thrown, 0))::float as bullpen_pitches_thrown,
            sum(coalesce(p.strikeouts, 0))::float as bullpen_strikeouts,
            sum(coalesce(p.walks, 0))::float as bullpen_walks,
            sum(coalesce(p.hits_allowed, 0))::float as bullpen_hits_allowed,
            sum(coalesce(p.home_runs_allowed, 0))::float as bullpen_home_runs_allowed,
            sum(coalesce(p.earned_runs, 0))::float as bullpen_earned_runs
        from mlb_player_game_pitching p
        join mlb_games g on g.game_pk = p.game_pk
        where p.is_starter = false
        group by p.game_pk, p.team_id, g.official_date
        """,
        engine,
    )
    if relief.empty:
        return relief

    relief["game_date"] = pd.to_datetime(relief["game_date"])
    value_cols = [
        "bullpen_pitchers_used",
        "bullpen_outs_recorded",
        "bullpen_batters_faced",
        "bullpen_pitches_thrown",
        "bullpen_strikeouts",
        "bullpen_walks",
        "bullpen_hits_allowed",
        "bullpen_home_runs_allowed",
        "bullpen_earned_runs",
    ]
    relief[value_cols] = relief[value_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    relief = _add_group_rolling(
        relief,
        group_cols="opponent_team_id",
        value_cols=value_cols,
        windows=(3, 5, 10, 20),
        prefix="opponent_bullpen",
    )
    for window in (3, 5, 10, 20):
        relief[f"opponent_bullpen_hr_per_bf_last{window}"] = _safe_div(
            relief[f"opponent_bullpen_bullpen_home_runs_allowed_sum_last{window}"],
            relief[f"opponent_bullpen_bullpen_batters_faced_sum_last{window}"],
        )
        relief[f"opponent_bullpen_k_per_bf_last{window}"] = _safe_div(
            relief[f"opponent_bullpen_bullpen_strikeouts_sum_last{window}"],
            relief[f"opponent_bullpen_bullpen_batters_faced_sum_last{window}"],
        )
        relief[f"opponent_bullpen_bb_per_bf_last{window}"] = _safe_div(
            relief[f"opponent_bullpen_bullpen_walks_sum_last{window}"],
            relief[f"opponent_bullpen_bullpen_batters_faced_sum_last{window}"],
        )
        relief[f"opponent_bullpen_pitches_per_out_last{window}"] = _safe_div(
            relief[f"opponent_bullpen_bullpen_pitches_thrown_sum_last{window}"],
            relief[f"opponent_bullpen_bullpen_outs_recorded_sum_last{window}"],
        )

    relief_bbe = _read_sql(
        """
        select
            bb.game_pk,
            p.team_id as opponent_team_id,
            g.official_date as game_date,
            count(*)::float as bullpen_bbe_allowed_count,
            avg(bb.launch_speed) as bullpen_bbe_allowed_avg_launch_speed,
            max(bb.launch_speed) as bullpen_bbe_allowed_max_launch_speed,
            avg(bb.launch_angle) as bullpen_bbe_allowed_avg_launch_angle,
            max(bb.total_distance) as bullpen_bbe_allowed_max_distance,
            avg(bb.estimated_ba_using_speedangle) as bullpen_bbe_allowed_avg_estimated_ba,
            avg(bb.estimated_woba_using_speedangle) as bullpen_bbe_allowed_avg_estimated_woba,
            avg(case when bb.is_hard_hit then 1.0 else 0.0 end) as bullpen_bbe_allowed_hard_hit_rate,
            avg(case when bb.is_sweet_spot then 1.0 else 0.0 end) as bullpen_bbe_allowed_sweet_spot_rate,
            avg(case when bb.launch_speed_angle = 6 then 1.0 else 0.0 end) as bullpen_bbe_allowed_barrel_rate,
            avg(case when bb.launch_speed >= 95 and bb.launch_angle between 20 and 35 then 1.0 else 0.0 end) as bullpen_bbe_allowed_hr_contact_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%fly%%' then 1.0 else 0.0 end) as bullpen_bbe_allowed_fly_ball_rate,
            avg(case when lower(coalesce(bb.trajectory, '')) like '%%line%%' then 1.0 else 0.0 end) as bullpen_bbe_allowed_line_drive_rate
        from mlb_batted_ball_events bb
        join mlb_player_game_pitching p
            on p.game_pk = bb.game_pk and p.player_id = bb.pitcher_id
        join mlb_games g on g.game_pk = bb.game_pk
        where p.is_starter = false
        group by bb.game_pk, p.team_id, g.official_date
        """,
        engine,
    )
    if not relief_bbe.empty:
        relief_bbe["game_date"] = pd.to_datetime(relief_bbe["game_date"])
        bbe_cols = [
            "bullpen_bbe_allowed_count",
            "bullpen_bbe_allowed_avg_launch_speed",
            "bullpen_bbe_allowed_max_launch_speed",
            "bullpen_bbe_allowed_avg_launch_angle",
            "bullpen_bbe_allowed_max_distance",
            "bullpen_bbe_allowed_avg_estimated_ba",
            "bullpen_bbe_allowed_avg_estimated_woba",
            "bullpen_bbe_allowed_hard_hit_rate",
            "bullpen_bbe_allowed_sweet_spot_rate",
            "bullpen_bbe_allowed_barrel_rate",
            "bullpen_bbe_allowed_hr_contact_rate",
            "bullpen_bbe_allowed_fly_ball_rate",
            "bullpen_bbe_allowed_line_drive_rate",
        ]
        relief_bbe[bbe_cols] = relief_bbe[bbe_cols].apply(pd.to_numeric, errors="coerce")
        relief_bbe = _add_group_rolling(
            relief_bbe,
            group_cols="opponent_team_id",
            value_cols=bbe_cols,
            windows=(3, 5, 10, 20),
            prefix="opponent_bullpen_bbe_allowed",
            add_sums=False,
        )
        keep_bbe = [
            "game_pk",
            "opponent_team_id",
            *[col for col in relief_bbe.columns if col.startswith("opponent_bullpen_bbe_allowed_")],
        ]
        relief = relief.merge(relief_bbe[keep_bbe], on=["game_pk", "opponent_team_id"], how="left")

    keep_cols = [
        "game_pk",
        "opponent_team_id",
        *[col for col in relief.columns if col.startswith("opponent_bullpen_")],
    ]
    return relief[keep_cols]


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


def _add_weather_physics_features(df: pd.DataFrame) -> pd.DataFrame:
    if "temperature_2m_c" not in df.columns:
        return df

    temp_c = pd.to_numeric(df["temperature_2m_c"], errors="coerce")
    temp_k = temp_c + 273.15
    humidity = pd.to_numeric(df.get("relative_humidity_2m"), errors="coerce").clip(0, 100)
    pressure_hpa = pd.to_numeric(df.get("surface_pressure_hpa"), errors="coerce")
    if "pressure_msl_hpa" in df.columns:
        pressure_hpa = pressure_hpa.fillna(pd.to_numeric(df["pressure_msl_hpa"], errors="coerce"))
    wind_kph = pd.to_numeric(df.get("wind_speed_10m_kph"), errors="coerce")
    gust_kph = pd.to_numeric(df.get("wind_gusts_10m_kph"), errors="coerce")
    wind_direction = pd.to_numeric(df.get("wind_direction_10m_deg"), errors="coerce")

    saturation_vapor_pressure_hpa = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    vapor_pressure_pa = (humidity / 100.0) * saturation_vapor_pressure_hpa * 100.0
    pressure_pa = pressure_hpa * 100.0
    dry_air_pressure_pa = pressure_pa - vapor_pressure_pa
    air_density = (dry_air_pressure_pa / (287.05 * temp_k)) + (vapor_pressure_pa / (461.495 * temp_k))
    air_density = air_density.where((temp_k > 0) & (pressure_pa > 0))

    df["temperature_2m_f"] = temp_c * 9.0 / 5.0 + 32.0
    df["wind_speed_10m_mph"] = wind_kph * 0.621371
    df["wind_gusts_10m_mph"] = gust_kph * 0.621371
    df["air_density_kg_m3"] = air_density
    df["air_density_ratio"] = air_density / 1.225
    df["air_density_carry_index"] = 1.225 / air_density.replace(0, np.nan)
    df["temperature_hr_boost_index"] = (df["temperature_2m_f"] - 70.0) * 0.01

    wind_radians = np.deg2rad(wind_direction)
    df["wind_direction_sin"] = np.sin(wind_radians)
    df["wind_direction_cos"] = np.cos(wind_radians)
    df["wind_speed_density_carry"] = df["wind_speed_10m_mph"] * df["air_density_carry_index"]
    return df


def _add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    game_date = pd.to_datetime(df["game_date"])
    df["game_month"] = game_date.dt.month
    df["game_dayofyear"] = game_date.dt.dayofyear
    df["game_weekofyear"] = game_date.dt.isocalendar().week.astype(float)
    return df


def _add_matchup_and_venue_features(df: pd.DataFrame) -> pd.DataFrame:
    batter_side = df.get("batter_bat_side", pd.Series(index=df.index, dtype=object)).fillna("").str.upper()
    pitcher_hand = df.get("starter_pitcher_pitch_hand", pd.Series(index=df.index, dtype=object)).fillna("").str.upper()

    effective_left = (batter_side == "L") | ((batter_side == "S") & (pitcher_hand == "R"))
    effective_right = (batter_side == "R") | ((batter_side == "S") & (pitcher_hand == "L"))
    pitcher_left = pitcher_hand == "L"
    pitcher_right = pitcher_hand == "R"

    df["batter_bats_left"] = effective_left.astype(int)
    df["batter_bats_right"] = effective_right.astype(int)
    df["batter_is_switch"] = (batter_side == "S").astype(int)
    df["starter_pitcher_throws_left"] = pitcher_left.astype(int)
    df["starter_pitcher_throws_right"] = pitcher_right.astype(int)
    df["same_side_matchup"] = ((effective_left & pitcher_left) | (effective_right & pitcher_right)).astype(int)
    df["opposite_side_matchup"] = ((effective_left & pitcher_right) | (effective_right & pitcher_left)).astype(int)

    for col in ["left_line", "left_center", "center", "right_center", "right_line"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if {"left_line", "left_center", "center", "right_center", "right_line"}.issubset(df.columns):
        df["venue_min_corner_distance"] = df[["left_line", "right_line"]].min(axis=1)
        df["venue_avg_corner_distance"] = df[["left_line", "right_line"]].mean(axis=1)
        df["venue_avg_power_alley_distance"] = df[["left_center", "right_center"]].mean(axis=1)
        df["venue_avg_outfield_distance"] = df[["left_line", "left_center", "center", "right_center", "right_line"]].mean(axis=1)

        df["pull_line_distance"] = np.select(
            [effective_left, effective_right],
            [df["right_line"], df["left_line"]],
            default=df["venue_avg_corner_distance"],
        )
        df["pull_gap_distance"] = np.select(
            [effective_left, effective_right],
            [df["right_center"], df["left_center"]],
            default=df["venue_avg_power_alley_distance"],
        )
        df["oppo_line_distance"] = np.select(
            [effective_left, effective_right],
            [df["left_line"], df["right_line"]],
            default=df["venue_avg_corner_distance"],
        )
        df["oppo_gap_distance"] = np.select(
            [effective_left, effective_right],
            [df["left_center"], df["right_center"]],
            default=df["venue_avg_power_alley_distance"],
        )
        df["pull_side_distance_avg"] = pd.DataFrame(
            {"line": df["pull_line_distance"], "gap": df["pull_gap_distance"]}
        ).mean(axis=1)
        df["oppo_side_distance_avg"] = pd.DataFrame(
            {"line": df["oppo_line_distance"], "gap": df["oppo_gap_distance"]}
        ).mean(axis=1)

    if {"park_factor_hr_lhb", "park_factor_hr_rhb"}.issubset(df.columns):
        df["park_factor_hr_batter_side"] = np.select(
            [effective_left, effective_right],
            [df["park_factor_hr_lhb"], df["park_factor_hr_rhb"]],
            default=df.get("park_factor_hr"),
        )
    return df


def _load_starting_pitchers(engine) -> pd.DataFrame:
    starters = _read_sql(
        """
        select
            p.game_pk,
            p.team_id,
            p.player_id as starter_pitcher_id,
            sp.current_age as starter_pitcher_age,
            sp.pitch_hand as starter_pitcher_pitch_hand,
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
        left join mlb_players sp on sp.id = p.player_id
        where p.is_starter = true
        """,
        engine,
    )
    if starters.empty:
        return starters
    starters["game_date"] = pd.to_datetime(starters["game_date"])
    rolling_stat_cols = [
        "outs_recorded",
        "batters_faced",
        "pitches_thrown",
        "strikeouts",
        "walks",
        "hits_allowed",
        "home_runs_allowed",
    ]
    starters[rolling_stat_cols + ["starter_pitcher_age"]] = starters[
        rolling_stat_cols + ["starter_pitcher_age"]
    ].apply(pd.to_numeric, errors="coerce").fillna(0)
    starters = _add_group_rolling(
        starters,
        group_cols="starter_pitcher_id",
        value_cols=rolling_stat_cols,
        windows=(5, 10, 20),
        prefix="opp_starter",
    )
    for extra in (_load_pitcher_pitch_history(engine), _load_pitcher_batted_ball_allowed_history(engine)):
        if not extra.empty:
            starters = starters.merge(extra, on=["game_pk", "starter_pitcher_id"], how="left")
    keep_cols = [
        "game_pk",
        "team_id",
        "starter_pitcher_id",
        "starter_pitcher_age",
        "starter_pitcher_pitch_hand",
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
            bp.current_age as batter_age,
            bp.bat_side as batter_bat_side,
            v.elevation,
            v.capacity,
            v.left_line,
            v.left_center,
            v.center,
            v.right_center,
            v.right_line
        from mlb_player_game_batting b
        join mlb_games g on g.game_pk = b.game_pk
        left join mlb_players bp on bp.id = b.player_id
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
        "batter_age",
        "elevation",
        "capacity",
        "left_line",
        "left_center",
        "center",
        "right_center",
        "right_line",
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

    bullpen = _load_bullpen_history(engine)
    if not bullpen.empty:
        df = df.merge(bullpen, on=["game_pk", "opponent_team_id"], how="left")

    for extra in (_load_batter_context(engine), _load_weather_features(engine), _load_park_features(engine)):
        keys = ["season", "player_id"] if "player_id" in extra.columns else ["game_pk"] if "game_pk" in extra.columns else ["season", "venue_id"]
        df = df.merge(extra, on=keys, how="left")
    batter_bbe = _load_batter_batted_ball_history(engine)
    if not batter_bbe.empty:
        df = df.merge(batter_bbe, on=["game_pk", "player_id"], how="left")
    df["weather_available"] = df["temperature_2m_c"].notna().astype(int)
    df["is_night"] = (df["day_night"].fillna("").str.lower() == "night").astype(int)
    df = _add_weather_physics_features(df)
    df = _add_calendar_features(df)
    df = _add_matchup_and_venue_features(df)
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
