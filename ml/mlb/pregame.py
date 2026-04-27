from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .artifacts import score_frame
from .features import (
    _add_calendar_features,
    _add_matchup_and_venue_features,
    _add_weather_physics_features,
    _load_park_features,
    _load_weather_features,
    _read_sql,
    build_batter_training_frame,
    build_pitcher_training_frame,
    get_engine,
)


IDENTITY_COLUMNS = {
    "game_pk",
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "opponent_team_id",
    "opponent_team_abbreviation",
    "home_team_id",
    "away_team_id",
    "venue_id",
    "game_date",
    "season",
    "day_night",
    "is_home",
    "batting_order",
    "batter_age",
    "batter_bat_side",
    "elevation",
    "capacity",
    "left_line",
    "left_center",
    "center",
    "right_center",
    "right_line",
    "starter_pitcher_id",
    "starter_pitcher_age",
    "starter_pitcher_pitch_hand",
}


@lru_cache(maxsize=4)
def _cached_batter_training_history(database_url: str) -> pd.DataFrame:
    return build_batter_training_frame(database_url=database_url)


@lru_cache(maxsize=4)
def _cached_pitcher_training_history(database_url: str) -> pd.DataFrame:
    return build_pitcher_training_frame(database_url=database_url)


def resolve_prediction_date(day: str = "tomorrow", target_date: str | date | None = None) -> date:
    if target_date:
        return pd.to_datetime(target_date).date()
    today = datetime.now(ZoneInfo("America/New_York")).date()
    if day == "today":
        return today
    if day == "yesterday":
        return today - timedelta(days=1)
    if day in {"tomorrow", "auto"}:
        return today + timedelta(days=1)
    return pd.to_datetime(day).date()


def _load_candidate_batters(engine, target_date: date) -> pd.DataFrame:
    return _read_sql(
        """
        with games as (
            select
                g.game_pk,
                g.official_date as game_date,
                g.start_time_utc,
                g.season,
                g.home_team_id,
                g.away_team_id,
                g.venue_id,
                g.day_night,
                g.probable_home_pitcher_id,
                g.probable_away_pitcher_id,
                g.weather_condition,
                g.temperature_f,
                g.wind_text,
                ht.abbreviation as home_team_abbreviation,
                at.abbreviation as away_team_abbreviation,
                v.name as venue_name,
                v.city as venue_city,
                v.state as venue_state,
                v.roof_type,
                v.turf_type,
                v.elevation,
                v.capacity,
                v.left_line,
                v.left_center,
                v.center,
                v.right_center,
                v.right_line
            from mlb_games g
            left join mlb_teams ht on ht.id = g.home_team_id
            left join mlb_teams at on at.id = g.away_team_id
            left join mlb_venues v on v.id = g.venue_id
            where g.official_date = %(target_date)s
              and coalesce(g.detailed_state, '') not in ('Final', 'Game Over', 'Completed Early')
        ),
        latest_snapshot as (
            select distinct on (snapshot.game_pk)
                snapshot.game_pk,
                snapshot.id as snapshot_id
            from mlb_game_snapshots snapshot
            join games g on g.game_pk = snapshot.game_pk
            order by snapshot.game_pk, snapshot.captured_at desc, snapshot.id desc
        ),
        posted_lineups as (
            select
                l.game_pk,
                l.team_id,
                l.player_id,
                l.batting_order,
                true as has_posted_lineup
            from mlb_lineup_snapshots l
            join latest_snapshot s
                on s.snapshot_id = l.snapshot_id
            where l.is_starter = true
              and l.batting_order is not null
        ),
        teams_needing_roster as (
            select game_pk, home_team_id as team_id from games
            union all
            select game_pk, away_team_id as team_id from games
            except
            select distinct game_pk, team_id from posted_lineups
        ),
        latest_roster as (
            select distinct on (r.team_id, r.player_id)
                t.game_pk,
                r.team_id,
                r.player_id,
                null::integer as batting_order,
                false as has_posted_lineup
            from teams_needing_roster t
            join mlb_roster_snapshots r
                on r.team_id = t.team_id
            where r.roster_type = 'active'
              and r.roster_date <= %(target_date)s
              and r.is_pitcher = false
            order by r.team_id, r.player_id, r.roster_date desc, r.captured_at desc
        ),
        candidates as (
            select * from posted_lineups
            union all
            select * from latest_roster
        ),
        recent_orders as (
            select
                b.player_id,
                percentile_cont(0.5) within group (order by b.batting_order) as recent_batting_order,
                count(*) as recent_starts
            from mlb_player_game_batting b
            join mlb_games g on g.game_pk = b.game_pk
            where g.official_date < %(target_date)s
              and b.batting_order is not null
              and b.plate_appearances > 0
            group by b.player_id
        ),
        recent_batter_games as (
            select
                player_id,
                jsonb_agg(
                    jsonb_build_object(
                        'game_date', game_date,
                        'matchup', matchup,
                        'hits', hits,
                        'home_runs', home_runs,
                        'total_bases', total_bases,
                        'strikeouts', strikeouts,
                        'plate_appearances', plate_appearances
                    )
                    order by game_date desc, game_pk desc
                ) as recent_games
            from (
                select
                    b.player_id,
                    b.game_pk,
                    g.official_date as game_date,
                    at.abbreviation || ' @ ' || ht.abbreviation as matchup,
                    b.hits,
                    b.home_runs,
                    b.total_bases,
                    b.strikeouts,
                    b.plate_appearances,
                    row_number() over (
                        partition by b.player_id
                        order by g.official_date desc, b.game_pk desc
                    ) as rn
                from mlb_player_game_batting b
                join mlb_games g on g.game_pk = b.game_pk
                left join mlb_teams ht on ht.id = g.home_team_id
                left join mlb_teams at on at.id = g.away_team_id
                where g.official_date < %(target_date)s
                  and b.plate_appearances > 0
            ) ranked
            where rn <= 5
            group by player_id
        )
        select
            g.game_pk,
            g.game_date,
            g.start_time_utc,
            g.season,
            c.player_id,
            p.full_name as player_name,
            c.team_id,
            case when c.team_id = g.home_team_id then g.home_team_abbreviation else g.away_team_abbreviation end
                as team_abbreviation,
            case when c.team_id = g.home_team_id then g.away_team_id else g.home_team_id end
                as opponent_team_id,
            case when c.team_id = g.home_team_id then g.away_team_abbreviation else g.home_team_abbreviation end
                as opponent_team_abbreviation,
            g.home_team_id,
            g.away_team_id,
            g.venue_id,
            g.venue_name,
            g.venue_city,
            g.venue_state,
            g.roof_type,
            g.turf_type,
            g.weather_condition,
            g.temperature_f,
            g.wind_text,
            g.day_night,
            case when c.team_id = g.home_team_id then 1 else 0 end as is_home,
            coalesce(c.batting_order, recent_orders.recent_batting_order)::float as batting_order,
            c.has_posted_lineup,
            recent_batter_games.recent_games,
            p.current_age as batter_age,
            p.bat_side as batter_bat_side,
            case
                when c.team_id = g.home_team_id then g.probable_away_pitcher_id
                else g.probable_home_pitcher_id
            end as starter_pitcher_id,
            sp.current_age as starter_pitcher_age,
            sp.pitch_hand as starter_pitcher_pitch_hand,
            g.elevation,
            g.capacity,
            g.left_line,
            g.left_center,
            g.center,
            g.right_center,
            g.right_line
        from candidates c
        join games g on g.game_pk = c.game_pk
        left join mlb_players p on p.id = c.player_id
        left join mlb_players sp on sp.id = case
            when c.team_id = g.home_team_id then g.probable_away_pitcher_id
            else g.probable_home_pitcher_id
        end
        left join recent_orders on recent_orders.player_id = c.player_id
        left join recent_batter_games on recent_batter_games.player_id = c.player_id
        where c.player_id is not null
        """,
        engine,
        params={"target_date": target_date},
    )


def _latest_feature_rows(history: pd.DataFrame, key: str, prefixes: tuple[str, ...]) -> pd.DataFrame:
    cols = [key, *[col for col in history.columns if col.startswith(prefixes)]]
    if key not in history.columns:
        return pd.DataFrame(columns=cols)
    return (
        history.sort_values(["game_date", "game_pk"])
        .drop_duplicates(key, keep="last")[cols]
        .copy()
    )


def build_batter_home_run_pregame_frame(
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
) -> pd.DataFrame:
    provided_engine = engine is not None
    engine = engine or get_engine(database_url)
    resolved_date = resolve_prediction_date(day=day, target_date=target_date)
    candidates = _load_candidate_batters(engine, resolved_date)
    if candidates.empty:
        candidates.attrs["prediction_date"] = resolved_date.isoformat()
        candidates.attrs["missing_model_features"] = []
        return candidates

    candidates["game_date"] = pd.to_datetime(candidates["game_date"])
    history = (
        _cached_batter_training_history(database_url).copy()
        if database_url and not provided_engine
        else build_batter_training_frame(engine=engine)
    )
    if history.empty:
        candidates.attrs["prediction_date"] = resolved_date.isoformat()
        candidates.attrs["missing_model_features"] = []
        return candidates

    history = history[history["game_date"] < pd.Timestamp(resolved_date)].copy()
    player_features = (
        history.sort_values(["player_id", "game_date", "game_pk"])
        .drop_duplicates("player_id", keep="last")
        .drop(columns=[col for col in IDENTITY_COLUMNS if col in history.columns and col != "player_id"], errors="ignore")
    )

    frame = candidates.merge(player_features, on="player_id", how="left")

    starter_features = _latest_feature_rows(
        history,
        "starter_pitcher_id",
        ("opp_starter_",),
    )
    if not starter_features.empty:
        current_starter_cols = [
            col
            for col in frame.columns
            if col.startswith("opp_starter_")
        ]
        frame = frame.drop(columns=current_starter_cols, errors="ignore").merge(
            starter_features,
            on="starter_pitcher_id",
            how="left",
        )

    opponent_features = _latest_feature_rows(
        history,
        "opponent_team_id",
        ("team_batting_", "opp_bullpen_"),
    )
    if not opponent_features.empty:
        current_opponent_cols = [
            col
            for col in frame.columns
            if col.startswith("team_batting_") or col.startswith("opp_bullpen_")
        ]
        frame = frame.drop(columns=current_opponent_cols, errors="ignore").merge(
            opponent_features,
            on="opponent_team_id",
            how="left",
        )

    for extra in (_load_weather_features(engine), _load_park_features(engine)):
        if extra.empty:
            continue
        keys = ["game_pk"] if "game_pk" in extra.columns else ["season", "venue_id"]
        frame = frame.drop(columns=[col for col in extra.columns if col not in keys and col in frame.columns], errors="ignore")
        frame = frame.merge(extra, on=keys, how="left")

    if "temperature_2m_c" in frame.columns:
        frame["weather_available"] = frame["temperature_2m_c"].notna().astype(int)
    else:
        frame["weather_available"] = 0
    frame["is_night"] = (frame["day_night"].fillna("").str.lower() == "night").astype(int)
    frame = _add_weather_physics_features(frame)
    frame = _add_calendar_features(frame)
    frame = _add_matchup_and_venue_features(frame)

    for col in ["target_home_run", "target_hits", "target_total_bases"]:
        frame[col] = np.nan
    return frame


def build_batter_pregame_frame(
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
) -> pd.DataFrame:
    return build_batter_home_run_pregame_frame(
        engine=engine,
        database_url=database_url,
        day=day,
        target_date=target_date,
    )


def _load_candidate_pitchers(engine, target_date: date) -> pd.DataFrame:
    return _read_sql(
        """
        with games as (
            select
                g.game_pk,
                g.official_date as game_date,
                g.start_time_utc,
                g.season,
                g.home_team_id,
                g.away_team_id,
                g.venue_id,
                g.day_night,
                g.probable_home_pitcher_id,
                g.probable_away_pitcher_id,
                g.weather_condition,
                g.temperature_f,
                g.wind_text,
                ht.abbreviation as home_team_abbreviation,
                at.abbreviation as away_team_abbreviation,
                v.name as venue_name,
                v.city as venue_city,
                v.state as venue_state,
                v.roof_type,
                v.turf_type,
                v.elevation,
                v.capacity
            from mlb_games g
            left join mlb_teams ht on ht.id = g.home_team_id
            left join mlb_teams at on at.id = g.away_team_id
            left join mlb_venues v on v.id = g.venue_id
            where g.official_date = %(target_date)s
              and coalesce(g.detailed_state, '') not in ('Final', 'Game Over', 'Completed Early')
        ),
        candidates as (
            select
                game_pk,
                game_date,
                start_time_utc,
                season,
                probable_home_pitcher_id as player_id,
                home_team_id as team_id,
                home_team_abbreviation as team_abbreviation,
                away_team_id as opponent_team_id,
                away_team_abbreviation as opponent_team_abbreviation,
                home_team_id,
                away_team_id,
                venue_id,
                venue_name,
                venue_city,
                venue_state,
                roof_type,
                turf_type,
                weather_condition,
                temperature_f,
                wind_text,
                day_night,
                1 as is_home,
                elevation,
                capacity
            from games
            where probable_home_pitcher_id is not null
            union all
            select
                game_pk,
                game_date,
                start_time_utc,
                season,
                probable_away_pitcher_id as player_id,
                away_team_id as team_id,
                away_team_abbreviation as team_abbreviation,
                home_team_id as opponent_team_id,
                home_team_abbreviation as opponent_team_abbreviation,
                home_team_id,
                away_team_id,
                venue_id,
                venue_name,
                venue_city,
                venue_state,
                roof_type,
                turf_type,
                weather_condition,
                temperature_f,
                wind_text,
                day_night,
                0 as is_home,
                elevation,
                capacity
            from games
            where probable_away_pitcher_id is not null
        ),
        recent_pitcher_games as (
            select
                player_id,
                jsonb_agg(
                    jsonb_build_object(
                        'game_date', game_date,
                        'matchup', matchup,
                        'strikeouts', strikeouts,
                        'innings_pitched', innings_pitched,
                        'pitches_thrown', pitches_thrown,
                        'earned_runs', earned_runs
                    )
                    order by game_date desc, game_pk desc
                ) as recent_games
            from (
                select
                    p.player_id,
                    p.game_pk,
                    g.official_date as game_date,
                    at.abbreviation || ' @ ' || ht.abbreviation as matchup,
                    p.strikeouts,
                    p.innings_pitched,
                    p.pitches_thrown,
                    p.earned_runs,
                    row_number() over (
                        partition by p.player_id
                        order by g.official_date desc, p.game_pk desc
                    ) as rn
                from mlb_player_game_pitching p
                join mlb_games g on g.game_pk = p.game_pk
                left join mlb_teams ht on ht.id = g.home_team_id
                left join mlb_teams at on at.id = g.away_team_id
                where g.official_date < %(target_date)s
                  and p.is_starter = true
            ) ranked
            where rn <= 5
            group by player_id
        )
        select
            c.*,
            recent_pitcher_games.recent_games,
            p.full_name as player_name,
            p.current_age as pitcher_age,
            p.pitch_hand as pitcher_pitch_hand
        from candidates c
        left join mlb_players p on p.id = c.player_id
        left join recent_pitcher_games on recent_pitcher_games.player_id = c.player_id
        """,
        engine,
        params={"target_date": target_date},
    )


def build_pitcher_pregame_frame(
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
) -> pd.DataFrame:
    provided_engine = engine is not None
    engine = engine or get_engine(database_url)
    resolved_date = resolve_prediction_date(day=day, target_date=target_date)
    candidates = _load_candidate_pitchers(engine, resolved_date)
    if candidates.empty:
        candidates.attrs["prediction_date"] = resolved_date.isoformat()
        candidates.attrs["missing_model_features"] = []
        return candidates

    candidates["game_date"] = pd.to_datetime(candidates["game_date"])
    history = (
        _cached_pitcher_training_history(database_url).copy()
        if database_url and not provided_engine
        else build_pitcher_training_frame(engine=engine)
    )
    if history.empty:
        candidates.attrs["prediction_date"] = resolved_date.isoformat()
        candidates.attrs["missing_model_features"] = []
        return candidates

    history = history[history["game_date"] < pd.Timestamp(resolved_date)].copy()
    identity_cols = {
        "game_pk",
        "player_id",
        "team_id",
        "opponent_team_id",
        "home_team_id",
        "away_team_id",
        "venue_id",
        "game_date",
        "season",
        "day_night",
        "is_home",
        "elevation",
        "capacity",
    }
    pitcher_features = (
        history.sort_values(["player_id", "game_date", "game_pk"])
        .drop_duplicates("player_id", keep="last")
        .drop(columns=[col for col in identity_cols if col in history.columns and col != "player_id"], errors="ignore")
    )
    frame = candidates.merge(pitcher_features, on="player_id", how="left")

    opponent_features = _latest_feature_rows(
        history,
        "opponent_team_id",
        ("opponent_batting_", "opponent_k_rate_"),
    )
    if not opponent_features.empty:
        current_opponent_cols = [
            col
            for col in frame.columns
            if col.startswith("opponent_batting_") or col.startswith("opponent_k_rate_")
        ]
        frame = frame.drop(columns=current_opponent_cols, errors="ignore").merge(
            opponent_features,
            on="opponent_team_id",
            how="left",
        )

    for extra in (_load_weather_features(engine), _load_park_features(engine)):
        if extra.empty:
            continue
        keys = ["game_pk"] if "game_pk" in extra.columns else ["season", "venue_id"]
        frame = frame.drop(columns=[col for col in extra.columns if col not in keys and col in frame.columns], errors="ignore")
        frame = frame.merge(extra, on=keys, how="left")

    if "temperature_2m_c" in frame.columns:
        frame["weather_available"] = frame["temperature_2m_c"].notna().astype(int)
    else:
        frame["weather_available"] = 0
    frame["is_night"] = (frame["day_night"].fillna("").str.lower() == "night").astype(int)
    frame = _add_calendar_features(frame)
    frame["target_strikeouts"] = np.nan
    return frame


def score_batter_home_run_pregame(
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    frame = build_batter_home_run_pregame_frame(
        engine=engine,
        database_url=database_url,
        day=day,
        target_date=target_date,
    )
    if frame.empty:
        frame.attrs["prediction_date"] = resolve_prediction_date(day=day, target_date=target_date).isoformat()
        frame.attrs["missing_model_features"] = []
        return frame
    scored = score_frame("batter_home_runs", frame, limit=limit)
    scored.attrs["prediction_date"] = resolve_prediction_date(day=day, target_date=target_date).isoformat()
    scored.attrs["missing_model_features"] = scored.attrs.get("missing_model_features", [])
    return scored


def score_market_pregame(
    market: str,
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    if market in {"batter_home_runs", "batter_hits", "batter_total_bases"}:
        frame = build_batter_pregame_frame(
            engine=engine,
            database_url=database_url,
            day=day,
            target_date=target_date,
        )
    elif market == "pitcher_strikeouts":
        frame = build_pitcher_pregame_frame(
            engine=engine,
            database_url=database_url,
            day=day,
            target_date=target_date,
        )
    else:
        raise ValueError(f"Unsupported pregame MLB market: {market}")

    prediction_date = resolve_prediction_date(day=day, target_date=target_date).isoformat()
    if frame.empty:
        frame.attrs["prediction_date"] = prediction_date
        frame.attrs["missing_model_features"] = []
        return frame

    scored = score_frame(market, frame, limit=limit)
    scored.attrs["prediction_date"] = prediction_date
    scored.attrs["missing_model_features"] = scored.attrs.get("missing_model_features", [])
    return scored


def score_pregame_slate(
    engine=None,
    *,
    database_url: str | None = None,
    day: str = "tomorrow",
    target_date: str | date | None = None,
    limit_per_market: int | None = 60,
) -> dict[str, pd.DataFrame]:
    engine = engine or get_engine(database_url)
    prediction_date = resolve_prediction_date(day=day, target_date=target_date).isoformat()
    results: dict[str, pd.DataFrame] = {}

    batter_frame = build_batter_pregame_frame(
        engine=engine,
        day=day,
        target_date=target_date,
    )
    for market in ("batter_home_runs", "batter_hits", "batter_total_bases"):
        if batter_frame.empty:
            scored = batter_frame.copy()
            scored.attrs["prediction_date"] = prediction_date
            scored.attrs["missing_model_features"] = []
        else:
            scored = score_frame(market, batter_frame, limit=limit_per_market)
            scored.attrs["prediction_date"] = prediction_date
            scored.attrs["missing_model_features"] = scored.attrs.get("missing_model_features", [])
        results[market] = scored

    pitcher_frame = build_pitcher_pregame_frame(
        engine=engine,
        day=day,
        target_date=target_date,
    )
    if pitcher_frame.empty:
        scored_pitchers = pitcher_frame.copy()
        scored_pitchers.attrs["prediction_date"] = prediction_date
        scored_pitchers.attrs["missing_model_features"] = []
    else:
        scored_pitchers = score_frame("pitcher_strikeouts", pitcher_frame, limit=limit_per_market)
        scored_pitchers.attrs["prediction_date"] = prediction_date
        scored_pitchers.attrs["missing_model_features"] = scored_pitchers.attrs.get("missing_model_features", [])
    results["pitcher_strikeouts"] = scored_pitchers

    return results


def scored_rows_for_api(scored: pd.DataFrame, *, limit: int | None = None) -> list[dict[str, Any]]:
    if scored.empty:
        return []
    cols = [
        "game_date",
        "start_time_utc",
        "game_pk",
        "player_id",
        "player_name",
        "team_id",
        "team_abbreviation",
        "opponent_team_id",
        "opponent_team_abbreviation",
        "venue_name",
        "venue_city",
        "venue_state",
        "roof_type",
        "turf_type",
        "weather_condition",
        "temperature_f",
        "wind_text",
        "temperature_2m_c",
        "wind_speed_10m_kph",
        "wind_gusts_10m_kph",
        "park_factor_hr",
        "recent_games",
        "is_home",
        "batting_order",
        "has_posted_lineup",
        "starter_pitcher_id",
        "probability",
        "prediction",
    ]
    rows = scored[[col for col in cols if col in scored.columns]].copy()
    if limit is not None:
        rows = rows.head(limit)
    rows["game_date"] = pd.to_datetime(rows["game_date"]).dt.date.astype(str)
    if "start_time_utc" in rows.columns:
        rows["start_time_utc"] = pd.to_datetime(rows["start_time_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = rows.replace([np.inf, -np.inf], np.nan).astype(object)
    rows = rows.where(pd.notna(rows), None)
    return rows.to_dict("records")
