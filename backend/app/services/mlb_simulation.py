from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


UTC = timezone.utc


LEAGUE_DEFAULTS = {
    "k_rate": 0.225,
    "bb_rate": 0.085,
    "hit_rate": 0.225,
    "hr_rate": 0.031,
    "double_rate": 0.047,
    "triple_rate": 0.004,
    "exit_velocity": 88.4,
    "launch_angle": 12.0,
    "bat_speed": 72.0,
    "barrel_rate": 0.075,
    "hard_hit_rate": 0.39,
    "xba": 0.245,
    "xslg": 0.405,
    "xwoba": 0.315,
}


FALLBACK_PITCH_MIX = [
    ("FF", "4-Seam Fastball", 0.36, 94.0, -4.0, 15.0, 2250.0, 0.34, 0.105, 0.22),
    ("SI", "Sinker", 0.16, 93.0, -12.0, 8.0, 2150.0, 0.35, 0.085, 0.23),
    ("SL", "Slider", 0.21, 85.5, 5.0, 2.0, 2450.0, 0.37, 0.180, 0.18),
    ("CH", "Changeup", 0.13, 86.0, -8.0, 6.0, 1800.0, 0.36, 0.155, 0.19),
    ("CU", "Curveball", 0.10, 79.0, 6.0, -8.0, 2600.0, 0.40, 0.170, 0.17),
    ("FC", "Cutter", 0.04, 90.0, 2.0, 10.0, 2350.0, 0.34, 0.110, 0.21),
]


@dataclass(slots=True)
class PitchTypeProfile:
    code: str
    description: str
    weight: float
    speed_mph: float
    break_horizontal: float
    break_vertical: float
    spin_rate: float
    ball_rate: float
    whiff_rate: float
    in_play_rate: float


@dataclass(slots=True)
class BatterProfile:
    player_id: int
    name: str
    team_id: int
    team_abbreviation: str
    batting_order: int
    bat_side: str | None
    source: str
    pa: float
    k_rate: float
    bb_rate: float
    hit_rate: float
    hr_rate: float
    double_rate: float
    triple_rate: float
    xba: float
    xslg: float
    xwoba: float
    exit_velocity: float
    launch_angle: float
    barrel_rate: float
    hard_hit_rate: float
    bat_speed: float
    squared_up_rate: float
    attack_angle: float


@dataclass(slots=True)
class PitcherProfile:
    player_id: int
    name: str
    team_id: int
    team_abbreviation: str
    throw_side: str | None
    is_bullpen: bool
    batters_faced: float
    k_rate: float
    bb_rate: float
    xba: float
    xslg: float
    xwoba: float
    exit_velocity_allowed: float
    launch_angle_allowed: float
    barrel_rate_allowed: float
    hard_hit_rate_allowed: float
    pitch_mix: list[PitchTypeProfile]


@dataclass(slots=True)
class VenueProfile:
    venue_id: int | None
    name: str | None
    city: str | None
    state: str | None
    roof_type: str | None
    turf_type: str | None
    elevation: float
    left_line: float
    left_center: float
    center: float
    right_center: float
    right_line: float


@dataclass(slots=True)
class WeatherPoint:
    target_time_utc: datetime
    temperature_f: float
    wind_speed_mph: float
    wind_gust_mph: float
    wind_direction_deg: float | None
    pressure_hpa: float | None
    humidity: float | None
    precipitation_probability: float | None
    weather_code: int | None


@dataclass(slots=True)
class GameContext:
    game_pk: int
    official_date: date
    season: int
    start_time_utc: datetime
    home_team_id: int
    away_team_id: int
    home_team: str
    away_team: str
    home_abbreviation: str
    away_abbreviation: str
    home_pitcher_id: int | None
    away_pitcher_id: int | None
    venue: VenueProfile
    weather: list[WeatherPoint]
    home_plate_umpire: dict[str, Any] | None
    game_temperature_f: float | None
    game_wind_text: str | None


@dataclass(slots=True)
class Runner:
    player_id: int
    name: str


@dataclass(slots=True)
class SimulationState:
    inning: int = 1
    half: str = "top"
    outs: int = 0
    bases: list[Runner | None] = field(default_factory=lambda: [None, None, None])
    away_score: int = 0
    home_score: int = 0
    away_index: int = 0
    home_index: int = 0
    home_pitch_count: int = 0
    away_pitch_count: int = 0
    home_bullpen: bool = False
    away_bullpen: bool = False
    pitch_number: int = 0
    clock: datetime | None = None


def _parse_date(value: str | date | None) -> date:
    if isinstance(value, date):
        return value
    if value:
        return date.fromisoformat(str(value)[:10])
    return datetime.now(UTC).date()


def _as_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _rate(value: Any, default: float, *, low: float = 0.0, high: float = 1.0) -> float:
    parsed = _as_float(value, default)
    if parsed > 1.5:
        parsed /= 100.0
    return _clamp(parsed, low, high)


def _safe_div(numerator: Any, denominator: Any, default: float) -> float:
    den = _as_float(denominator, 0.0)
    if den <= 0:
        return default
    return _as_float(numerator, 0.0) / den


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _rows(conn, sql: str, params: dict[str, Any] | None = None, *, expanding: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    stmt = text(sql)
    for key in expanding:
        stmt = stmt.bindparams(bindparam(key, expanding=True))
    return [dict(row) for row in conn.execute(stmt, params or {}).mappings().all()]


def _row(conn, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    result = conn.execute(text(sql), params or {}).mappings().first()
    return dict(result) if result else None


def _iso(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value) if value is not None else None


def _pitch_profiles_from_rows(rows: list[dict[str, Any]]) -> list[PitchTypeProfile]:
    if not rows:
        return [PitchTypeProfile(*item) for item in FALLBACK_PITCH_MIX]

    total = sum(_as_float(row.get("pitch_count"), 0.0) for row in rows) or 1.0
    profiles: list[PitchTypeProfile] = []
    for row in rows:
        code = str(row.get("pitch_type_code") or "FF")
        profiles.append(
            PitchTypeProfile(
                code=code,
                description=str(row.get("pitch_type_description") or code),
                weight=max(0.01, _as_float(row.get("pitch_count"), 1.0) / total),
                speed_mph=_as_float(row.get("speed_mph"), 91.0),
                break_horizontal=_as_float(row.get("break_horizontal"), 0.0),
                break_vertical=_as_float(row.get("break_vertical"), 0.0),
                spin_rate=_as_float(row.get("spin_rate"), 2200.0),
                ball_rate=_rate(row.get("ball_rate"), 0.35, low=0.2, high=0.55),
                whiff_rate=_rate(row.get("whiff_rate"), 0.12, low=0.04, high=0.30),
                in_play_rate=_rate(row.get("in_play_rate"), 0.20, low=0.10, high=0.42),
            )
        )
    return profiles


def _league_pitch_mix(conn, game_date: date) -> list[PitchTypeProfile]:
    lookback = game_date - timedelta(days=540)
    rows = _rows(
        conn,
        """
        select
            coalesce(pitch_type_code, 'FF') as pitch_type_code,
            max(coalesce(pitch_type_description, pitch_type_code, 'Fastball')) as pitch_type_description,
            count(*)::float as pitch_count,
            avg(start_speed) as speed_mph,
            avg(break_horizontal) as break_horizontal,
            avg(break_vertical) as break_vertical,
            avg(spin_rate) as spin_rate,
            avg(case when is_ball then 1.0 else 0.0 end) as ball_rate,
            avg(case when lower(coalesce(call_description, '')) like '%swinging%' then 1.0 else 0.0 end) as whiff_rate,
            avg(case when is_in_play then 1.0 else 0.0 end) as in_play_rate
        from mlb_pitch_events pe
        join mlb_games g on g.game_pk = pe.game_pk
        where g.official_date >= :lookback
          and g.official_date < :game_date
          and pe.pitch_type_code is not null
        group by coalesce(pitch_type_code, 'FF')
        having count(*) >= 100
        order by pitch_count desc
        limit 8
        """,
        {"lookback": lookback, "game_date": game_date},
    )
    return _pitch_profiles_from_rows(rows)


def _load_league_defaults(conn, game_date: date) -> dict[str, Any]:
    defaults = dict(LEAGUE_DEFAULTS)
    defaults["pitch_mix"] = _pitch_profiles_from_rows([])
    return defaults


def list_simulation_games(engine: Engine, *, target_date: str | date) -> dict[str, Any]:
    date_value = _parse_date(target_date)
    with engine.connect() as conn:
        games = _rows(
            conn,
            """
            select
                g.game_pk,
                g.official_date,
                g.start_time_utc,
                g.status_code,
                g.detailed_state,
                ht.id as home_team_id,
                ht.name as home_team,
                ht.abbreviation as home_abbreviation,
                at.id as away_team_id,
                at.name as away_team,
                at.abbreviation as away_abbreviation,
                hp.id as home_pitcher_id,
                hp.full_name as home_pitcher,
                ap.id as away_pitcher_id,
                ap.full_name as away_pitcher,
                v.name as venue_name,
                v.roof_type,
                coalesce(wx.weather_rows, 0) as weather_rows,
                coalesce(lineups.home_lineup_count, 0) as home_lineup_count,
                coalesce(lineups.away_lineup_count, 0) as away_lineup_count
            from mlb_games g
            join mlb_teams ht on ht.id = g.home_team_id
            join mlb_teams at on at.id = g.away_team_id
            left join mlb_players hp on hp.id = g.probable_home_pitcher_id
            left join mlb_players ap on ap.id = g.probable_away_pitcher_id
            left join mlb_venues v on v.id = g.venue_id
            left join lateral (
                select count(*) as weather_rows
                from mlb_weather_snapshots ws
                where ws.game_pk = g.game_pk
            ) wx on true
            left join lateral (
                select
                    count(*) filter (where l.team_id = g.home_team_id and l.is_starter = true) as home_lineup_count,
                    count(*) filter (where l.team_id = g.away_team_id and l.is_starter = true) as away_lineup_count
                from mlb_lineup_snapshots l
                where l.game_pk = g.game_pk
            ) lineups on true
            where g.official_date = :target_date
            order by g.start_time_utc nulls last, g.game_pk
            """,
            {"target_date": date_value},
        )

    return {
        "sport": "mlb",
        "status": "ok",
        "date": date_value.isoformat(),
        "count": len(games),
        "games": [
            {
                **game,
                "official_date": _iso(game.get("official_date")),
                "start_time_utc": _iso(game.get("start_time_utc")),
                "weather_available": _as_int(game.get("weather_rows"), 0) > 0,
                "lineups_available": _as_int(game.get("home_lineup_count"), 0) >= 9
                and _as_int(game.get("away_lineup_count"), 0) >= 9,
            }
            for game in games
        ],
    }


def _load_game_context(conn, game_pk: int) -> GameContext:
    row = _row(
        conn,
        """
        select
            g.game_pk,
            g.official_date,
            g.start_time_utc,
            g.season,
            g.home_team_id,
            g.away_team_id,
            ht.name as home_team,
            at.name as away_team,
            ht.abbreviation as home_abbreviation,
            at.abbreviation as away_abbreviation,
            g.probable_home_pitcher_id as home_pitcher_id,
            g.probable_away_pitcher_id as away_pitcher_id,
            g.temperature_f,
            g.wind_text,
            v.id as venue_id,
            v.name as venue_name,
            v.city as venue_city,
            v.state as venue_state,
            coalesce(v.roof_type, g.roof_type) as roof_type,
            v.turf_type,
            v.elevation,
            v.left_line,
            v.left_center,
            v.center,
            v.right_center,
            v.right_line
        from mlb_games g
        join mlb_teams ht on ht.id = g.home_team_id
        join mlb_teams at on at.id = g.away_team_id
        left join mlb_venues v on v.id = g.venue_id
        where g.game_pk = :game_pk
        """,
        {"game_pk": game_pk},
    )
    if row is None:
        raise ValueError(f"Game {game_pk} was not found.")

    weather = _load_weather(conn, game_pk, row)
    umpire = _row(
        conn,
        """
        select u.id, u.full_name, a.official_type
        from mlb_game_official_assignments a
        join mlb_game_snapshots s on s.id = a.snapshot_id
        join mlb_umpires u on u.id = a.umpire_id
        where a.game_pk = :game_pk
          and a.is_home_plate = true
        order by s.captured_at desc, s.id desc
        limit 1
        """,
        {"game_pk": game_pk},
    )

    start_time = row.get("start_time_utc")
    if isinstance(start_time, datetime) and start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    elif not isinstance(start_time, datetime):
        start_time = datetime.combine(row["official_date"], datetime.min.time(), tzinfo=UTC) + timedelta(hours=23)

    return GameContext(
        game_pk=int(row["game_pk"]),
        official_date=row["official_date"],
        season=int(row["season"]),
        start_time_utc=start_time,
        home_team_id=int(row["home_team_id"]),
        away_team_id=int(row["away_team_id"]),
        home_team=str(row["home_team"]),
        away_team=str(row["away_team"]),
        home_abbreviation=str(row["home_abbreviation"]),
        away_abbreviation=str(row["away_abbreviation"]),
        home_pitcher_id=_as_int(row.get("home_pitcher_id")),
        away_pitcher_id=_as_int(row.get("away_pitcher_id")),
        venue=VenueProfile(
            venue_id=_as_int(row.get("venue_id")),
            name=row.get("venue_name"),
            city=row.get("venue_city"),
            state=row.get("venue_state"),
            roof_type=row.get("roof_type"),
            turf_type=row.get("turf_type"),
            elevation=_as_float(row.get("elevation"), 0.0),
            left_line=_as_float(row.get("left_line"), 330.0),
            left_center=_as_float(row.get("left_center"), 375.0),
            center=_as_float(row.get("center"), 405.0),
            right_center=_as_float(row.get("right_center"), 375.0),
            right_line=_as_float(row.get("right_line"), 330.0),
        ),
        weather=weather,
        home_plate_umpire=umpire,
        game_temperature_f=_as_float(row.get("temperature_f"), math.nan)
        if row.get("temperature_f") is not None
        else None,
        game_wind_text=row.get("wind_text"),
    )


def _load_weather(conn, game_pk: int, game_row: dict[str, Any]) -> list[WeatherPoint]:
    rows = _rows(
        conn,
        """
        select distinct on (target_time_utc)
            target_time_utc,
            temperature_2m_c,
            wind_speed_10m_kph,
            wind_gusts_10m_kph,
            wind_direction_10m_deg,
            surface_pressure_hpa,
            relative_humidity_2m,
            precipitation_probability,
            weather_code
        from mlb_weather_snapshots
        where game_pk = :game_pk
        order by target_time_utc, pulled_at desc
        """,
        {"game_pk": game_pk},
    )
    weather: list[WeatherPoint] = []
    for row in rows:
        target = row.get("target_time_utc")
        if not isinstance(target, datetime):
            continue
        if target.tzinfo is None:
            target = target.replace(tzinfo=UTC)
        temp_f = _as_float(row.get("temperature_2m_c"), 21.1) * 1.8 + 32.0
        wind_mph = _as_float(row.get("wind_speed_10m_kph"), 0.0) * 0.621371
        gust_mph = _as_float(row.get("wind_gusts_10m_kph"), wind_mph / 0.621371) * 0.621371
        weather.append(
            WeatherPoint(
                target_time_utc=target,
                temperature_f=temp_f,
                wind_speed_mph=wind_mph,
                wind_gust_mph=gust_mph,
                wind_direction_deg=_as_float(row.get("wind_direction_10m_deg"), math.nan)
                if row.get("wind_direction_10m_deg") is not None
                else None,
                pressure_hpa=_as_float(row.get("surface_pressure_hpa"), math.nan)
                if row.get("surface_pressure_hpa") is not None
                else None,
                humidity=_as_float(row.get("relative_humidity_2m"), math.nan)
                if row.get("relative_humidity_2m") is not None
                else None,
                precipitation_probability=_as_float(row.get("precipitation_probability"), math.nan)
                if row.get("precipitation_probability") is not None
                else None,
                weather_code=_as_int(row.get("weather_code")),
            )
        )

    if weather:
        return weather

    fallback_time = game_row.get("start_time_utc")
    if not isinstance(fallback_time, datetime):
        fallback_time = datetime.now(UTC)
    if fallback_time.tzinfo is None:
        fallback_time = fallback_time.replace(tzinfo=UTC)
    return [
        WeatherPoint(
            target_time_utc=fallback_time,
            temperature_f=_as_float(game_row.get("temperature_f"), 70.0),
            wind_speed_mph=0.0,
            wind_gust_mph=0.0,
            wind_direction_deg=None,
            pressure_hpa=None,
            humidity=None,
            precipitation_probability=None,
            weather_code=None,
        )
    ]


def _lineup_rows(conn, ctx: GameContext, *, team_id: int, team_abbreviation: str) -> tuple[list[dict[str, Any]], str]:
    posted = _rows(
        conn,
        """
        with latest_snapshot as (
            select id
            from mlb_game_snapshots
            where game_pk = :game_pk
            order by captured_at desc, id desc
            limit 1
        )
        select
            l.player_id,
            p.full_name,
            p.bat_side,
            l.batting_order,
            true as posted
        from latest_snapshot s
        join mlb_lineup_snapshots l on l.snapshot_id = s.id
        join mlb_players p on p.id = l.player_id
        where l.team_id = :team_id
          and l.is_starter = true
          and l.batting_order is not null
        order by l.batting_order
        limit 9
        """,
        {"game_pk": ctx.game_pk, "team_id": team_id},
    )
    if len(posted) >= 9:
        return posted[:9], "posted_lineup"

    predicted = _rows(
        conn,
        """
        select distinct on (pl.player_id)
            pl.player_id,
            coalesce(pl.player_name, p.full_name) as full_name,
            p.bat_side,
            pl.batting_order,
            coalesce(pl.has_posted_lineup, false) as posted,
            coalesce(pl.prediction, pl.probability, 0) as model_value
        from mlb_prediction_logs pl
        join mlb_players p on p.id = pl.player_id
        where pl.game_pk = :game_pk
          and pl.team_id = :team_id
          and pl.market in ('batter_home_runs', 'batter_hits', 'batter_total_bases')
        order by
            pl.player_id,
            coalesce(pl.has_posted_lineup, false) desc,
            pl.batting_order nulls last,
            coalesce(pl.prediction, pl.probability, 0) desc
        """,
        {"game_pk": ctx.game_pk, "team_id": team_id},
    )
    if len(predicted) >= 9:
        predicted.sort(
            key=lambda row: (
                row.get("batting_order") is None,
                _as_float(row.get("batting_order"), 99.0),
                -_as_float(row.get("model_value"), 0.0),
            )
        )
        return predicted[:9], "prediction_log"

    roster = _rows(
        conn,
        """
        with latest_roster as (
            select distinct on (r.player_id)
                r.player_id,
                r.team_id,
                r.roster_date,
                r.captured_at
            from mlb_roster_snapshots r
            where r.team_id = :team_id
              and r.roster_type = 'active'
              and r.roster_date <= :game_date
              and r.is_pitcher = false
            order by r.player_id, r.roster_date desc, r.captured_at desc
        ),
        recent as (
            select
                b.player_id,
                sum(coalesce(b.plate_appearances, 0)) as pa,
                sum(coalesce(b.total_bases, 0)) as total_bases,
                percentile_cont(0.5) within group (order by b.batting_order) as batting_order
            from mlb_player_game_batting b
            join mlb_games g on g.game_pk = b.game_pk
            where b.team_id = :team_id
              and g.official_date < :game_date
              and g.official_date >= :lookback_date
            group by b.player_id
        ),
        statcast as (
            select distinct on (player_id)
                player_id,
                plate_appearances,
                xwoba,
                xslg
            from mlb_statcast_batter_season
            where season <= :season
            order by player_id, season desc
        )
        select
            lr.player_id,
            p.full_name,
            p.bat_side,
            recent.batting_order,
            false as posted,
            coalesce(recent.pa, statcast.plate_appearances, 0) as pa,
            coalesce(recent.total_bases, 0) as total_bases,
            coalesce(statcast.xwoba, 0) as xwoba,
            coalesce(statcast.xslg, 0) as xslg
        from latest_roster lr
        join mlb_players p on p.id = lr.player_id
        left join recent on recent.player_id = lr.player_id
        left join statcast on statcast.player_id = lr.player_id
        order by
            recent.batting_order nulls last,
            coalesce(recent.pa, statcast.plate_appearances, 0) desc,
            coalesce(statcast.xwoba, 0) desc,
            p.full_name
        limit 9
        """,
        {
            "team_id": team_id,
            "game_date": ctx.official_date,
            "lookback_date": ctx.official_date - timedelta(days=540),
            "season": ctx.season,
        },
    )
    if len(roster) < 9:
        raise ValueError(f"Not enough non-pitcher roster rows for {team_abbreviation}; load active rosters first.")
    return roster[:9], "active_roster"


def _load_batter_profiles(
    conn,
    ctx: GameContext,
    lineup_rows: list[dict[str, Any]],
    *,
    team_id: int,
    team_abbreviation: str,
    source: str,
    league: dict[str, Any],
) -> list[BatterProfile]:
    player_ids = [int(row["player_id"]) for row in lineup_rows]
    rows = _rows(
        conn,
        """
        with bs as (
            select distinct on (player_id) *
            from mlb_statcast_batter_season
            where player_id in :player_ids
              and season <= :season
            order by player_id, season desc
        ),
        bt as (
            select distinct on (player_id) *
            from mlb_bat_tracking_batter_season
            where player_id in :player_ids
              and season <= :season
            order by player_id, season desc
        ),
        sp as (
            select distinct on (player_id) *
            from mlb_swing_path_batter_season
            where player_id in :player_ids
              and season <= :season
            order by player_id, season desc
        ),
        recent as (
            select
                b.player_id,
                sum(coalesce(b.plate_appearances, 0))::float as pa,
                sum(coalesce(b.at_bats, 0))::float as ab,
                sum(coalesce(b.hits, 0))::float as hits,
                sum(coalesce(b.doubles, 0))::float as doubles,
                sum(coalesce(b.triples, 0))::float as triples,
                sum(coalesce(b.home_runs, 0))::float as home_runs,
                sum(coalesce(b.walks, 0))::float as walks,
                sum(coalesce(b.strikeouts, 0))::float as strikeouts
            from mlb_player_game_batting b
            join mlb_games g on g.game_pk = b.game_pk
            where b.player_id in :player_ids
              and g.official_date < :game_date
              and g.official_date >= :lookback_date
            group by b.player_id
        )
        select
            p.id as player_id,
            p.full_name,
            p.bat_side,
            bs.plate_appearances as season_pa,
            bs.at_bats as season_ab,
            bs.hits as season_hits,
            bs.home_runs as season_home_runs,
            bs.strikeouts as season_strikeouts,
            bs.walks as season_walks,
            bs.avg,
            bs.obp,
            bs.slg,
            bs.xba,
            bs.xslg,
            bs.xwoba,
            bs.exit_velocity_avg,
            bs.launch_angle_avg,
            bs.barrel_batted_rate,
            bs.hard_hit_percent,
            bs.sweet_spot_percent,
            bt.bat_speed,
            bt.squared_up_rate,
            bt.blast_rate,
            sp.attack_angle,
            sp.attack_direction,
            recent.pa as recent_pa,
            recent.ab as recent_ab,
            recent.hits as recent_hits,
            recent.doubles as recent_doubles,
            recent.triples as recent_triples,
            recent.home_runs as recent_home_runs,
            recent.walks as recent_walks,
            recent.strikeouts as recent_strikeouts
        from mlb_players p
        left join bs on bs.player_id = p.id
        left join bt on bt.player_id = p.id
        left join sp on sp.player_id = p.id
        left join recent on recent.player_id = p.id
        where p.id in :player_ids
        """,
        {
            "player_ids": player_ids,
            "season": ctx.season,
            "game_date": ctx.official_date,
            "lookback_date": ctx.official_date - timedelta(days=540),
        },
        expanding=("player_ids",),
    )
    by_id = {int(row["player_id"]): row for row in rows}
    profiles: list[BatterProfile] = []
    for index, lineup_row in enumerate(lineup_rows):
        player_id = int(lineup_row["player_id"])
        row = by_id.get(player_id, {})
        pa = max(
            _as_float(row.get("season_pa"), 0.0),
            _as_float(row.get("recent_pa"), 0.0),
            60.0,
        )
        ab = max(_as_float(row.get("season_ab"), 0.0), _as_float(row.get("recent_ab"), 0.0), pa * 0.86)
        hits = _as_float(row.get("season_hits"), _as_float(row.get("recent_hits"), pa * league["hit_rate"]))
        home_runs = _as_float(
            row.get("season_home_runs"),
            _as_float(row.get("recent_home_runs"), pa * league["hr_rate"]),
        )
        strikeouts = _as_float(
            row.get("season_strikeouts"),
            _as_float(row.get("recent_strikeouts"), pa * league["k_rate"]),
        )
        walks = _as_float(row.get("season_walks"), _as_float(row.get("recent_walks"), pa * league["bb_rate"]))
        doubles = _as_float(row.get("recent_doubles"), pa * league["double_rate"])
        triples = _as_float(row.get("recent_triples"), pa * league["triple_rate"])
        batting_order = _as_int(lineup_row.get("batting_order"), index + 1) or index + 1
        profiles.append(
            BatterProfile(
                player_id=player_id,
                name=str(lineup_row.get("full_name") or row.get("full_name") or f"Player {player_id}"),
                team_id=team_id,
                team_abbreviation=team_abbreviation,
                batting_order=int(batting_order),
                bat_side=lineup_row.get("bat_side") or row.get("bat_side"),
                source=source,
                pa=pa,
                k_rate=_clamp(strikeouts / pa, 0.08, 0.42),
                bb_rate=_clamp(walks / pa, 0.025, 0.20),
                hit_rate=_clamp(hits / pa, 0.12, 0.36),
                hr_rate=_clamp(home_runs / pa, 0.003, 0.095),
                double_rate=_clamp(doubles / pa, 0.015, 0.10),
                triple_rate=_clamp(triples / pa, 0.0, 0.025),
                xba=_rate(row.get("xba"), hits / ab if ab else league["xba"], low=0.12, high=0.38),
                xslg=_rate(row.get("xslg"), league["xslg"], low=0.22, high=0.75),
                xwoba=_rate(row.get("xwoba"), league["xwoba"], low=0.22, high=0.52),
                exit_velocity=_as_float(row.get("exit_velocity_avg"), league["exit_velocity"]),
                launch_angle=_as_float(row.get("launch_angle_avg"), league["launch_angle"]),
                barrel_rate=_rate(row.get("barrel_batted_rate"), league["barrel_rate"], low=0.005, high=0.25),
                hard_hit_rate=_rate(row.get("hard_hit_percent"), league["hard_hit_rate"], low=0.15, high=0.70),
                bat_speed=_as_float(row.get("bat_speed"), league["bat_speed"]),
                squared_up_rate=_rate(row.get("squared_up_rate"), 0.30, low=0.08, high=0.65),
                attack_angle=_as_float(row.get("attack_angle"), _as_float(row.get("launch_angle_avg"), league["launch_angle"])),
            )
        )
    return profiles


def _pitch_mix_by_pitcher(conn, pitcher_ids: list[int], game_date: date) -> dict[int, list[PitchTypeProfile]]:
    if not pitcher_ids:
        return {}
    rows = _rows(
        conn,
        """
        select
            pe.pitcher_id,
            coalesce(pe.pitch_type_code, 'FF') as pitch_type_code,
            max(coalesce(pe.pitch_type_description, pe.pitch_type_code, 'Pitch')) as pitch_type_description,
            count(*)::float as pitch_count,
            avg(pe.start_speed) as speed_mph,
            avg(pe.break_horizontal) as break_horizontal,
            avg(pe.break_vertical) as break_vertical,
            avg(pe.spin_rate) as spin_rate,
            avg(case when pe.is_ball then 1.0 else 0.0 end) as ball_rate,
            avg(case when lower(coalesce(pe.call_description, '')) like '%swinging%' then 1.0 else 0.0 end) as whiff_rate,
            avg(case when pe.is_in_play then 1.0 else 0.0 end) as in_play_rate
        from mlb_pitch_events pe
        join mlb_games g on g.game_pk = pe.game_pk
        where pe.pitcher_id in :pitcher_ids
          and g.official_date < :game_date
          and g.official_date >= :lookback_date
          and pe.pitch_type_code is not null
        group by pe.pitcher_id, coalesce(pe.pitch_type_code, 'FF')
        having count(*) >= 5
        order by pe.pitcher_id, pitch_count desc
        """,
        {
            "pitcher_ids": pitcher_ids,
            "game_date": game_date,
            "lookback_date": game_date - timedelta(days=720),
        },
        expanding=("pitcher_ids",),
    )
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["pitcher_id"]), []).append(row)
    return {pitcher_id: _pitch_profiles_from_rows(items) for pitcher_id, items in grouped.items()}


def _load_pitcher_profiles(
    conn,
    ctx: GameContext,
    *,
    pitcher_specs: list[tuple[int | None, int, str, bool]],
    league: dict[str, Any],
) -> dict[int, PitcherProfile]:
    pitcher_ids = [int(player_id) for player_id, _, _, is_bullpen in pitcher_specs if player_id and not is_bullpen]
    rows = []
    if pitcher_ids:
        rows = _rows(
            conn,
            """
            with ps as (
                select distinct on (player_id) *
                from mlb_statcast_pitcher_season
                where player_id in :pitcher_ids
                  and season <= :season
                order by player_id, season desc
            ),
            recent as (
                select
                    pg.player_id,
                    sum(coalesce(pg.batters_faced, 0))::float as batters_faced,
                    sum(coalesce(pg.strikeouts, 0))::float as strikeouts,
                    sum(coalesce(pg.walks, 0))::float as walks,
                    sum(coalesce(pg.hits_allowed, 0))::float as hits_allowed,
                    sum(coalesce(pg.home_runs_allowed, 0))::float as home_runs_allowed
                from mlb_player_game_pitching pg
                join mlb_games g on g.game_pk = pg.game_pk
                where pg.player_id in :pitcher_ids
                  and g.official_date < :game_date
                  and g.official_date >= :lookback_date
                group by pg.player_id
            )
            select
                p.id as player_id,
                p.full_name,
                p.pitch_hand,
                ps.batters_faced as season_batters_faced,
                ps.strikeout_percent,
                ps.walk_percent,
                ps.xba,
                ps.xslg,
                ps.xwoba,
                ps.exit_velocity_avg,
                ps.launch_angle_avg,
                ps.barrel_batted_rate,
                ps.hard_hit_percent,
                recent.batters_faced as recent_batters_faced,
                recent.strikeouts as recent_strikeouts,
                recent.walks as recent_walks,
                recent.hits_allowed as recent_hits_allowed
            from mlb_players p
            left join ps on ps.player_id = p.id
            left join recent on recent.player_id = p.id
            where p.id in :pitcher_ids
            """,
            {
                "pitcher_ids": pitcher_ids,
                "season": ctx.season,
                "game_date": ctx.official_date,
                "lookback_date": ctx.official_date - timedelta(days=720),
            },
            expanding=("pitcher_ids",),
        )
    by_id = {int(row["player_id"]): row for row in rows}
    mixes = _pitch_mix_by_pitcher(conn, pitcher_ids, ctx.official_date)
    profiles: dict[int, PitcherProfile] = {}
    for player_id, team_id, team_abbr, is_bullpen in pitcher_specs:
        if is_bullpen:
            profile = _load_bullpen_profile(conn, ctx, team_id=team_id, team_abbreviation=team_abbr, league=league)
            profiles[profile.player_id] = profile
            continue
        if not player_id:
            continue
        row = by_id.get(int(player_id), {})
        bf = max(
            _as_float(row.get("season_batters_faced"), 0.0),
            _as_float(row.get("recent_batters_faced"), 0.0),
            80.0,
        )
        k_rate = _rate(
            row.get("strikeout_percent"),
            _safe_div(row.get("recent_strikeouts"), bf, league["k_rate"]),
            low=0.08,
            high=0.42,
        )
        bb_rate = _rate(
            row.get("walk_percent"),
            _safe_div(row.get("recent_walks"), bf, league["bb_rate"]),
            low=0.025,
            high=0.20,
        )
        profiles[int(player_id)] = PitcherProfile(
            player_id=int(player_id),
            name=str(row.get("full_name") or f"Pitcher {player_id}"),
            team_id=team_id,
            team_abbreviation=team_abbr,
            throw_side=row.get("pitch_hand"),
            is_bullpen=False,
            batters_faced=bf,
            k_rate=k_rate,
            bb_rate=bb_rate,
            xba=_rate(row.get("xba"), league["xba"], low=0.16, high=0.34),
            xslg=_rate(row.get("xslg"), league["xslg"], low=0.25, high=0.65),
            xwoba=_rate(row.get("xwoba"), league["xwoba"], low=0.24, high=0.45),
            exit_velocity_allowed=_as_float(row.get("exit_velocity_avg"), league["exit_velocity"]),
            launch_angle_allowed=_as_float(row.get("launch_angle_avg"), league["launch_angle"]),
            barrel_rate_allowed=_rate(row.get("barrel_batted_rate"), league["barrel_rate"], low=0.02, high=0.18),
            hard_hit_rate_allowed=_rate(row.get("hard_hit_percent"), league["hard_hit_rate"], low=0.22, high=0.60),
            pitch_mix=mixes.get(int(player_id), league["pitch_mix"]),
        )
    return profiles


def _load_bullpen_profile(
    conn,
    ctx: GameContext,
    *,
    team_id: int,
    team_abbreviation: str,
    league: dict[str, Any],
) -> PitcherProfile:
    row = _row(
        conn,
        """
        select
            sum(coalesce(pg.batters_faced, 0))::float as batters_faced,
            sum(coalesce(pg.strikeouts, 0))::float as strikeouts,
            sum(coalesce(pg.walks, 0))::float as walks,
            sum(coalesce(pg.hits_allowed, 0))::float as hits_allowed,
            sum(coalesce(pg.home_runs_allowed, 0))::float as home_runs_allowed
        from mlb_player_game_pitching pg
        join mlb_games g on g.game_pk = pg.game_pk
        where pg.team_id = :team_id
          and coalesce(pg.is_starter, false) = false
          and g.official_date < :game_date
          and g.official_date >= :lookback_date
        """,
        {
            "team_id": team_id,
            "game_date": ctx.official_date,
            "lookback_date": ctx.official_date - timedelta(days=540),
        },
    ) or {}
    bf = max(_as_float(row.get("batters_faced"), 0.0), 100.0)
    bullpen_id = -int(team_id)
    return PitcherProfile(
        player_id=bullpen_id,
        name=f"{team_abbreviation} bullpen",
        team_id=team_id,
        team_abbreviation=team_abbreviation,
        throw_side=None,
        is_bullpen=True,
        batters_faced=bf,
        k_rate=_clamp(_safe_div(row.get("strikeouts"), bf, league["k_rate"] + 0.015), 0.10, 0.36),
        bb_rate=_clamp(_safe_div(row.get("walks"), bf, league["bb_rate"] + 0.005), 0.04, 0.17),
        xba=league["xba"],
        xslg=league["xslg"],
        xwoba=league["xwoba"],
        exit_velocity_allowed=league["exit_velocity"],
        launch_angle_allowed=league["launch_angle"],
        barrel_rate_allowed=league["barrel_rate"],
        hard_hit_rate_allowed=league["hard_hit_rate"],
        pitch_mix=league["pitch_mix"],
    )


def _weather_at(ctx: GameContext, target: datetime | None) -> WeatherPoint:
    if not ctx.weather:
        return WeatherPoint(
            target_time_utc=target or ctx.start_time_utc,
            temperature_f=70.0,
            wind_speed_mph=0.0,
            wind_gust_mph=0.0,
            wind_direction_deg=None,
            pressure_hpa=None,
            humidity=None,
            precipitation_probability=None,
            weather_code=None,
        )
    if target is None:
        return ctx.weather[0]
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    return min(ctx.weather, key=lambda point: abs((point.target_time_utc - target).total_seconds()))


def _weighted_pitch(mix: list[PitchTypeProfile], balls: int, strikes: int, rng: random.Random) -> PitchTypeProfile:
    adjusted: list[tuple[PitchTypeProfile, float]] = []
    for pitch in mix:
        code = pitch.code.upper()
        weight = pitch.weight
        if strikes >= 2 and code in {"SL", "CU", "KC", "SV", "ST", "CH", "FS", "KN"}:
            weight *= 1.3
        if balls >= 3 and code in {"FF", "FA", "SI", "FT", "FC"}:
            weight *= 1.35
        adjusted.append((pitch, weight))
    total = sum(weight for _, weight in adjusted) or 1.0
    draw = rng.random() * total
    running = 0.0
    for pitch, weight in adjusted:
        running += weight
        if draw <= running:
            return pitch
    return adjusted[-1][0]


def _umpire_zone_adjustment(ctx: GameContext) -> float:
    umpire = ctx.home_plate_umpire or {}
    umpire_id = _as_int(umpire.get("id"), 0) or 0
    return ((umpire_id % 17) - 8) * 0.002


def _outfield_wind_mph(weather: WeatherPoint, rng: random.Random) -> float:
    speed = max(weather.wind_speed_mph, weather.wind_gust_mph * 0.55)
    if weather.wind_direction_deg is None:
        return rng.gauss(0.0, speed * 0.15)
    # Without park bearing, this uses a fixed center-field axis as a consistent carry proxy.
    radians = math.radians(weather.wind_direction_deg - 180.0)
    return math.cos(radians) * speed


def _fence_distance(venue: VenueProfile, spray_degrees: float) -> float:
    anchors = [
        (-45.0, venue.left_line),
        (-22.5, venue.left_center),
        (0.0, venue.center),
        (22.5, venue.right_center),
        (45.0, venue.right_line),
    ]
    spray = _clamp(spray_degrees, -45.0, 45.0)
    for index in range(len(anchors) - 1):
        left_angle, left_distance = anchors[index]
        right_angle, right_distance = anchors[index + 1]
        if left_angle <= spray <= right_angle:
            pct = (spray - left_angle) / max(right_angle - left_angle, 1.0)
            return left_distance + (right_distance - left_distance) * pct
    return venue.center


def _field_coordinates(distance: float, spray_degrees: float) -> dict[str, float]:
    radians = math.radians(spray_degrees)
    scaled = _clamp(distance / 450.0, 0.02, 1.08)
    x = 50.0 + math.sin(radians) * scaled * 48.0
    y = 92.0 - math.cos(radians) * scaled * 75.0
    return {"x": round(_clamp(x, 4.0, 96.0), 2), "y": round(_clamp(y, 8.0, 94.0), 2)}


def _simulate_batted_ball(
    *,
    batter: BatterProfile,
    pitcher: PitcherProfile,
    pitch: PitchTypeProfile,
    ctx: GameContext,
    weather: WeatherPoint,
    balls: int,
    strikes: int,
    rng: random.Random,
) -> dict[str, Any]:
    platoon_bonus = 0.9 if batter.bat_side and pitcher.throw_side and batter.bat_side != pitcher.throw_side else -0.4
    quality_delta = (batter.xwoba - pitcher.xwoba) * 18.0
    launch_speed = (
        batter.exit_velocity
        + (batter.bat_speed - LEAGUE_DEFAULTS["bat_speed"]) * 0.38
        + (pitch.speed_mph - 90.0) * 0.06
        + quality_delta
        - (pitcher.exit_velocity_allowed - LEAGUE_DEFAULTS["exit_velocity"]) * 0.35
        + platoon_bonus
        + rng.gauss(0.0, 6.0)
    )
    launch_speed = _clamp(launch_speed, 45.0, 123.0)
    launch_angle = (
        batter.launch_angle * 0.55
        + batter.attack_angle * 0.30
        + pitcher.launch_angle_allowed * 0.15
        + (pitch.break_vertical / 12.0)
        + rng.gauss(0.0, 12.0)
    )
    launch_angle = _clamp(launch_angle, -35.0, 70.0)

    pull_shift = -7.0 if (batter.bat_side or "R").upper().startswith("R") else 7.0
    if pitch.code.upper() in {"SI", "FT", "CH"}:
        pull_shift *= 0.75
    spray = _clamp(rng.gauss(pull_shift, 19.0), -48.0, 48.0)

    theta = math.radians(_clamp(launch_angle, 1.0, 48.0))
    velocity_fps = launch_speed * 1.46667
    vacuum_range = (velocity_fps**2 * math.sin(2 * theta)) / 32.174
    drag_factor = 0.64 + _clamp((launch_speed - 80.0) / 140.0, -0.08, 0.08)
    raw_distance = max(8.0, vacuum_range * drag_factor)
    if launch_angle < 7:
        raw_distance *= _clamp(0.36 + launch_angle * 0.035, 0.08, 0.60)
    if launch_angle > 48:
        raw_distance *= _clamp(1.0 - (launch_angle - 48.0) * 0.025, 0.48, 1.0)

    pressure = weather.pressure_hpa if weather.pressure_hpa and not math.isnan(weather.pressure_hpa) else 1013.0
    weather_factor = 1.0
    weather_factor += (weather.temperature_f - 70.0) * 0.0017
    weather_factor += ctx.venue.elevation * 0.000012
    weather_factor += (1013.0 - pressure) * 0.00045
    wind_out = _outfield_wind_mph(weather, rng)
    distance = raw_distance * weather_factor + wind_out * 2.0 + rng.gauss(0.0, 11.0)
    distance = _clamp(distance, 1.0, 505.0)
    fence = _fence_distance(ctx.venue, spray)
    coords = _field_coordinates(distance, spray)

    if distance >= fence - 2.0 and 18.0 <= launch_angle <= 39.0 and abs(spray) <= 45.0:
        result = "home_run"
    elif launch_angle < 5:
        hit_prob = _clamp(0.25 + (launch_speed - 82.0) * 0.013 + (batter.xba - pitcher.xba) * 0.60, 0.08, 0.62)
        if rng.random() < hit_prob:
            result = "single" if rng.random() > 0.08 else "double"
        else:
            result = "groundout"
    elif launch_angle < 18:
        hit_prob = _clamp(0.50 + (launch_speed - 88.0) * 0.014 + (batter.xba - pitcher.xba) * 0.72, 0.20, 0.82)
        if rng.random() < hit_prob:
            result = "single" if distance < 240 or rng.random() < 0.72 else "double"
        else:
            result = "lineout"
    elif launch_angle <= 42:
        catch_prob = _clamp(0.70 - (launch_speed - 88.0) * 0.013 - (distance - 250.0) * 0.0014, 0.10, 0.82)
        if rng.random() > catch_prob:
            if distance > 345 and rng.random() < 0.18:
                result = "triple"
            elif distance > 245 and rng.random() < 0.46:
                result = "double"
            else:
                result = "single"
        else:
            result = "flyout"
    else:
        hit_prob = _clamp(0.13 + (launch_speed - 92.0) * 0.006, 0.04, 0.30)
        result = "single" if rng.random() < hit_prob else "popup"

    return {
        "result": result,
        "launch_speed": round(launch_speed, 1),
        "launch_angle": round(launch_angle, 1),
        "spray_degrees": round(spray, 1),
        "distance_ft": round(distance, 1),
        "fence_ft": round(fence, 1),
        "field_x": coords["x"],
        "field_y": coords["y"],
        "wind_out_mph": round(wind_out, 1),
    }


def _bases_text(bases: list[Runner | None]) -> str:
    return "".join(str(index + 1) if runner else "-" for index, runner in enumerate(bases))


def _score_runner(state: SimulationState, batting_side: str, runner: Runner | None) -> int:
    if runner is None:
        return 0
    if batting_side == "away":
        state.away_score += 1
    else:
        state.home_score += 1
    return 1


def _advance_walk(state: SimulationState, batting_side: str, batter: BatterProfile) -> int:
    runs = 0
    first, second, third = state.bases
    if first and second and third:
        runs += _score_runner(state, batting_side, third)
        third = second
        second = first
    elif first and second:
        third = second
        second = first
    elif first:
        second = first
    first = Runner(batter.player_id, batter.name)
    state.bases = [first, second, third]
    return runs


def _advance_hit(
    state: SimulationState,
    batting_side: str,
    batter: BatterProfile,
    bases: int,
    batted_ball: dict[str, Any],
    rng: random.Random,
) -> int:
    runs = 0
    first, second, third = state.bases
    ev = _as_float(batted_ball.get("launch_speed"), 88.0)
    if bases == 4:
        for runner in [third, second, first]:
            runs += _score_runner(state, batting_side, runner)
        runs += _score_runner(state, batting_side, Runner(batter.player_id, batter.name))
        state.bases = [None, None, None]
        return runs
    if bases == 3:
        for runner in [third, second, first]:
            runs += _score_runner(state, batting_side, runner)
        state.bases = [None, None, Runner(batter.player_id, batter.name)]
        return runs
    if bases == 2:
        runs += _score_runner(state, batting_side, third)
        runs += _score_runner(state, batting_side, second)
        new_third = None
        if first:
            if rng.random() < _clamp(0.38 + (ev - 88.0) * 0.015, 0.20, 0.70):
                runs += _score_runner(state, batting_side, first)
            else:
                new_third = first
        state.bases = [None, Runner(batter.player_id, batter.name), new_third]
        return runs

    runs += _score_runner(state, batting_side, third)
    new_third = None
    new_second = None
    if second:
        if rng.random() < _clamp(0.58 + (ev - 86.0) * 0.012, 0.36, 0.84):
            runs += _score_runner(state, batting_side, second)
        else:
            new_third = second
    if first:
        if rng.random() < _clamp(0.28 + (ev - 88.0) * 0.012, 0.12, 0.55):
            if new_third:
                new_second = first
            else:
                new_third = first
        else:
            new_second = first
    state.bases = [Runner(batter.player_id, batter.name), new_second, new_third]
    return runs


def _advance_out(
    state: SimulationState,
    batting_side: str,
    result: str,
    batted_ball: dict[str, Any] | None,
    rng: random.Random,
) -> int:
    runs = 0
    if result in {"groundout"} and state.outs < 2 and state.bases[0] and rng.random() < 0.13:
        state.outs += 2
        state.bases[0] = None
        return 0

    state.outs += 1
    if (
        result in {"flyout", "lineout"}
        and state.outs < 3
        and state.bases[2]
        and batted_ball
        and _as_float(batted_ball.get("distance_ft"), 0.0) > 255.0
        and rng.random() < 0.34
    ):
        runs += _score_runner(state, batting_side, state.bases[2])
        state.bases[2] = None
    return runs


def _stat_bucket(stats: dict[int, dict[str, float]], player: BatterProfile | PitcherProfile) -> dict[str, float]:
    bucket = stats.setdefault(
        player.player_id,
        {
            "player_id": player.player_id,
            "name": player.name,
            "team": player.team_abbreviation,
            "pa": 0.0,
            "ab": 0.0,
            "h": 0.0,
            "double": 0.0,
            "triple": 0.0,
            "hr": 0.0,
            "bb": 0.0,
            "k": 0.0,
            "tb": 0.0,
            "rbi": 0.0,
            "pitches": 0.0,
            "bf": 0.0,
            "runs": 0.0,
        },
    )
    return bucket


def _simulate_plate_appearance(
    *,
    ctx: GameContext,
    state: SimulationState,
    batter: BatterProfile,
    pitcher: PitcherProfile,
    batting_side: str,
    rng: random.Random,
    batter_stats: dict[int, dict[str, float]],
    pitcher_stats: dict[int, dict[str, float]],
    pitch_log: list[dict[str, Any]] | None,
    field_events: list[dict[str, Any]] | None,
    pitch_log_limit: int,
) -> dict[str, Any]:
    balls = 0
    strikes = 0
    pitches = 0
    batter_bucket = _stat_bucket(batter_stats, batter)
    pitcher_bucket = _stat_bucket(pitcher_stats, pitcher)
    batter_bucket["pa"] += 1
    pitcher_bucket["bf"] += 1

    while True:
        pitches += 1
        state.pitch_number += 1
        balls_before = balls
        strikes_before = strikes
        outs_before = state.outs
        bases_before = _bases_text(state.bases)
        weather = _weather_at(ctx, state.clock)
        pitch = _weighted_pitch(pitcher.pitch_mix, balls, strikes, rng)
        speed = _clamp(rng.gauss(pitch.speed_mph, 1.8), 66.0, 103.5)
        break_mag = abs(pitch.break_horizontal) + abs(pitch.break_vertical)
        zone_adjustment = _umpire_zone_adjustment(ctx)
        ball_prob = (
            pitch.ball_rate
            + (pitcher.bb_rate - LEAGUE_DEFAULTS["bb_rate"]) * 0.65
            + (batter.bb_rate - LEAGUE_DEFAULTS["bb_rate"]) * 0.25
            - zone_adjustment
        )
        if balls >= 3:
            ball_prob -= 0.08
        if strikes >= 2:
            ball_prob += 0.09
        ball_prob = _clamp(ball_prob, 0.20, 0.56)
        in_zone = rng.random() > ball_prob

        swing_prob = 0.43 + (0.17 if in_zone else -0.10)
        swing_prob += (strikes * 0.035) - (balls * 0.018)
        if strikes >= 2:
            swing_prob += 0.08
        swing_prob += (batter.k_rate - LEAGUE_DEFAULTS["k_rate"]) * 0.12
        swing_prob = _clamp(swing_prob, 0.18, 0.78)
        swings = rng.random() < swing_prob
        call = "ball"
        batted_ball: dict[str, Any] | None = None
        pa_done = False
        pa_result = ""
        runs = 0

        if not swings:
            if in_zone:
                strikes += 1
                call = "called_strike"
            else:
                balls += 1
                call = "ball"
        else:
            whiff_prob = (
                pitch.whiff_rate
                + (pitcher.k_rate - LEAGUE_DEFAULTS["k_rate"]) * 0.45
                + (batter.k_rate - LEAGUE_DEFAULTS["k_rate"]) * 0.32
                + (speed - 92.0) * 0.003
                + break_mag * 0.0014
                - (batter.squared_up_rate - 0.30) * 0.12
            )
            if strikes >= 2:
                whiff_prob *= 0.68
            whiff_prob = _clamp(whiff_prob, 0.035, 0.24)
            if rng.random() < whiff_prob:
                strikes += 1
                call = "swinging_strike"
            else:
                in_play_prob = (
                    pitch.in_play_rate
                    + (batter.hit_rate - LEAGUE_DEFAULTS["hit_rate"]) * 0.32
                    - (pitcher.k_rate - LEAGUE_DEFAULTS["k_rate"]) * 0.10
                    + (batter.hard_hit_rate - pitcher.hard_hit_rate_allowed) * 0.10
                )
                if strikes >= 2:
                    in_play_prob += 0.18
                in_play_prob = _clamp(in_play_prob, 0.24, 0.62)
                if pitches >= 8:
                    in_play_prob += 0.10
                if rng.random() < in_play_prob:
                    call = "in_play"
                    batted_ball = _simulate_batted_ball(
                        batter=batter,
                        pitcher=pitcher,
                        pitch=pitch,
                        ctx=ctx,
                        weather=weather,
                        balls=balls,
                        strikes=strikes,
                        rng=rng,
                    )
                    result = batted_ball["result"]
                    pa_result = result
                    pa_done = True
                    if result in {"single", "double", "triple", "home_run"}:
                        bases = {"single": 1, "double": 2, "triple": 3, "home_run": 4}[result]
                        runs = _advance_hit(state, batting_side, batter, bases, batted_ball, rng)
                        batter_bucket["ab"] += 1
                        batter_bucket["h"] += 1
                        batter_bucket["tb"] += bases
                        batter_bucket["rbi"] += runs
                        if bases == 2:
                            batter_bucket["double"] += 1
                        elif bases == 3:
                            batter_bucket["triple"] += 1
                        elif bases == 4:
                            batter_bucket["hr"] += 1
                        pitcher_bucket["runs"] += runs
                    else:
                        batter_bucket["ab"] += 1
                        runs = _advance_out(state, batting_side, result, batted_ball, rng)
                        batter_bucket["rbi"] += runs
                        pitcher_bucket["runs"] += runs
                else:
                    call = "foul"
                    if strikes < 2:
                        strikes += 1

        pitcher_bucket["pitches"] += 1
        if batting_side == "away":
            state.home_pitch_count += 1
        else:
            state.away_pitch_count += 1

        log_row = {
            "pitch_number": state.pitch_number,
            "inning": state.inning,
            "half": state.half,
            "outs_before": outs_before,
            "balls_before": balls_before,
            "strikes_before": strikes_before,
            "bases_before": bases_before,
            "batter_id": batter.player_id,
            "batter": batter.name,
            "pitcher_id": pitcher.player_id,
            "pitcher": pitcher.name,
            "pitch_type": pitch.code,
            "pitch_description": pitch.description,
            "pitch_mph": round(speed, 1),
            "call": call,
            "weather_time_utc": _iso(weather.target_time_utc),
            "temperature_f": round(weather.temperature_f, 1),
            "wind_speed_mph": round(weather.wind_speed_mph, 1),
            "score": f"{state.away_score}-{state.home_score}",
        }
        if batted_ball:
            log_row.update(
                {
                    "result": batted_ball["result"],
                    "launch_speed": batted_ball["launch_speed"],
                    "launch_angle": batted_ball["launch_angle"],
                    "spray_degrees": batted_ball["spray_degrees"],
                    "distance_ft": batted_ball["distance_ft"],
                    "field_x": batted_ball["field_x"],
                    "field_y": batted_ball["field_y"],
                }
            )
        if pitch_log is not None and len(pitch_log) < pitch_log_limit:
            pitch_log.append(log_row)

        if batted_ball and field_events is not None and len(field_events) < 180:
            field_events.append(
                {
                    "pitch_number": state.pitch_number,
                    "inning": state.inning,
                    "half": state.half,
                    "batter": batter.name,
                    "pitcher": pitcher.name,
                    "result": batted_ball["result"],
                    "launch_speed": batted_ball["launch_speed"],
                    "launch_angle": batted_ball["launch_angle"],
                    "spray_degrees": batted_ball["spray_degrees"],
                    "distance_ft": batted_ball["distance_ft"],
                    "field_x": batted_ball["field_x"],
                    "field_y": batted_ball["field_y"],
                }
            )

        if state.clock:
            state.clock += timedelta(seconds=rng.randint(18, 31))

        if balls >= 4:
            pa_result = "walk"
            batter_bucket["bb"] += 1
            pitcher_bucket["bb"] += 1
            runs = _advance_walk(state, batting_side, batter)
            pitcher_bucket["runs"] += runs
            pa_done = True
        elif strikes >= 3:
            pa_result = "strikeout"
            batter_bucket["ab"] += 1
            batter_bucket["k"] += 1
            pitcher_bucket["k"] += 1
            state.outs += 1
            pa_done = True
        elif pitches >= 13 and not pa_done:
            strikes = 2

        if pa_done:
            if state.clock:
                state.clock += timedelta(seconds=rng.randint(12, 28))
            return {"result": pa_result, "pitches": pitches, "runs": runs}


def _active_pitcher(
    state: SimulationState,
    *,
    pitching_side: str,
    starter: PitcherProfile | None,
    bullpen: PitcherProfile,
    starter_limit: int,
) -> PitcherProfile:
    if starter is None:
        return bullpen
    pitch_count = state.home_pitch_count if pitching_side == "home" else state.away_pitch_count
    bullpen_active = state.home_bullpen if pitching_side == "home" else state.away_bullpen
    if bullpen_active or pitch_count >= starter_limit or (state.inning >= 7 and pitch_count >= 72):
        if pitching_side == "home":
            state.home_bullpen = True
        else:
            state.away_bullpen = True
        return bullpen
    return starter


def _simulate_half_inning(
    *,
    ctx: GameContext,
    state: SimulationState,
    batting_side: str,
    lineup: list[BatterProfile],
    starter: PitcherProfile | None,
    bullpen: PitcherProfile,
    starter_limit: int,
    rng: random.Random,
    batter_stats: dict[int, dict[str, float]],
    pitcher_stats: dict[int, dict[str, float]],
    pitch_log: list[dict[str, Any]] | None,
    field_events: list[dict[str, Any]] | None,
    pitch_log_limit: int,
) -> bool:
    state.outs = 0
    state.bases = [None, None, None]
    state.half = "top" if batting_side == "away" else "bottom"
    pitching_side = "home" if batting_side == "away" else "away"
    while state.outs < 3:
        if state.inning >= 9 and batting_side == "home" and state.home_score > state.away_score:
            return True
        index = state.away_index if batting_side == "away" else state.home_index
        batter = lineup[index % len(lineup)]
        pitcher = _active_pitcher(
            state,
            pitching_side=pitching_side,
            starter=starter,
            bullpen=bullpen,
            starter_limit=starter_limit,
        )
        _simulate_plate_appearance(
            ctx=ctx,
            state=state,
            batter=batter,
            pitcher=pitcher,
            batting_side=batting_side,
            rng=rng,
            batter_stats=batter_stats,
            pitcher_stats=pitcher_stats,
            pitch_log=pitch_log,
            field_events=field_events,
            pitch_log_limit=pitch_log_limit,
        )
        if batting_side == "away":
            state.away_index += 1
        else:
            state.home_index += 1
        if state.inning >= 9 and batting_side == "home" and state.home_score > state.away_score:
            return True
    return False


def _simulate_game_once(
    *,
    ctx: GameContext,
    away_lineup: list[BatterProfile],
    home_lineup: list[BatterProfile],
    away_starter: PitcherProfile | None,
    home_starter: PitcherProfile | None,
    away_bullpen: PitcherProfile,
    home_bullpen: PitcherProfile,
    seed: int,
    capture: bool,
    pitch_log_limit: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    state = SimulationState(clock=ctx.start_time_utc)
    batter_stats: dict[int, dict[str, float]] = {}
    pitcher_stats: dict[int, dict[str, float]] = {}
    pitch_log: list[dict[str, Any]] | None = [] if capture else None
    field_events: list[dict[str, Any]] | None = [] if capture else None
    away_limit = rng.randint(78, 98)
    home_limit = rng.randint(78, 98)

    while state.inning <= 15:
        ended = _simulate_half_inning(
            ctx=ctx,
            state=state,
            batting_side="away",
            lineup=away_lineup,
            starter=home_starter,
            bullpen=home_bullpen,
            starter_limit=home_limit,
            rng=rng,
            batter_stats=batter_stats,
            pitcher_stats=pitcher_stats,
            pitch_log=pitch_log,
            field_events=field_events,
            pitch_log_limit=pitch_log_limit,
        )
        if ended:
            break
        if state.inning >= 9 and state.home_score > state.away_score:
            break
        ended = _simulate_half_inning(
            ctx=ctx,
            state=state,
            batting_side="home",
            lineup=home_lineup,
            starter=away_starter,
            bullpen=away_bullpen,
            starter_limit=away_limit,
            rng=rng,
            batter_stats=batter_stats,
            pitcher_stats=pitcher_stats,
            pitch_log=pitch_log,
            field_events=field_events,
            pitch_log_limit=pitch_log_limit,
        )
        if ended:
            break
        if state.inning >= 9 and state.away_score != state.home_score:
            break
        state.inning += 1

    if state.away_score == state.home_score:
        if rng.random() < 0.5:
            state.away_score += 1
        else:
            state.home_score += 1

    return {
        "away_score": state.away_score,
        "home_score": state.home_score,
        "winner": "home" if state.home_score > state.away_score else "away",
        "innings": state.inning,
        "pitch_count": state.pitch_number,
        "batter_stats": batter_stats,
        "pitcher_stats": pitcher_stats,
        "pitch_log": pitch_log or [],
        "field_events": field_events or [],
    }


def _aggregate_player_rows(
    totals: dict[int, dict[str, float]],
    appearances: dict[int, int],
    iterations: int,
    *,
    kind: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for player_id, stats in totals.items():
        row = {
            "player_id": player_id,
            "name": stats.get("name"),
            "team": stats.get("team"),
        }
        if kind == "batter":
            row.update(
                {
                    "avg_pa": round(stats.get("pa", 0.0) / iterations, 2),
                    "avg_hits": round(stats.get("h", 0.0) / iterations, 3),
                    "avg_total_bases": round(stats.get("tb", 0.0) / iterations, 3),
                    "avg_rbi": round(stats.get("rbi", 0.0) / iterations, 3),
                    "home_run_probability": round(appearances.get(player_id, 0) / iterations, 4),
                    "strikeout_avg": round(stats.get("k", 0.0) / iterations, 3),
                }
            )
        else:
            row.update(
                {
                    "avg_batters_faced": round(stats.get("bf", 0.0) / iterations, 2),
                    "avg_strikeouts": round(stats.get("k", 0.0) / iterations, 3),
                    "avg_walks": round(stats.get("bb", 0.0) / iterations, 3),
                    "avg_pitches": round(stats.get("pitches", 0.0) / iterations, 1),
                    "avg_runs_allowed": round(stats.get("runs", 0.0) / iterations, 3),
                }
            )
        rows.append(row)
    if kind == "batter":
        rows.sort(key=lambda row: (row["home_run_probability"], row["avg_total_bases"], row["avg_hits"]), reverse=True)
    else:
        rows.sort(
            key=lambda row: (
                1 if "bullpen" in str(row.get("name", "")).lower() else 0,
                -float(row.get("avg_strikeouts", 0.0)),
            )
        )
    return rows


def run_game_simulation(
    engine: Engine,
    *,
    game_pk: int,
    iterations: int = 250,
    seed: int | None = None,
    pitch_log_limit: int = 700,
) -> dict[str, Any]:
    iterations = max(1, min(int(iterations), 2000))
    pitch_log_limit = max(100, min(int(pitch_log_limit), 1400))
    base_seed = int(seed if seed is not None else (int(game_pk) * 17 + datetime.now(UTC).toordinal()))

    with engine.connect() as conn:
        ctx = _load_game_context(conn, game_pk)
        league = _load_league_defaults(conn, ctx.official_date)
        away_rows, away_source = _lineup_rows(
            conn,
            ctx,
            team_id=ctx.away_team_id,
            team_abbreviation=ctx.away_abbreviation,
        )
        home_rows, home_source = _lineup_rows(
            conn,
            ctx,
            team_id=ctx.home_team_id,
            team_abbreviation=ctx.home_abbreviation,
        )
        away_lineup = _load_batter_profiles(
            conn,
            ctx,
            away_rows,
            team_id=ctx.away_team_id,
            team_abbreviation=ctx.away_abbreviation,
            source=away_source,
            league=league,
        )
        home_lineup = _load_batter_profiles(
            conn,
            ctx,
            home_rows,
            team_id=ctx.home_team_id,
            team_abbreviation=ctx.home_abbreviation,
            source=home_source,
            league=league,
        )
        pitcher_profiles = _load_pitcher_profiles(
            conn,
            ctx,
            pitcher_specs=[
                (ctx.away_pitcher_id, ctx.away_team_id, ctx.away_abbreviation, False),
                (ctx.home_pitcher_id, ctx.home_team_id, ctx.home_abbreviation, False),
                (-ctx.away_team_id, ctx.away_team_id, ctx.away_abbreviation, True),
                (-ctx.home_team_id, ctx.home_team_id, ctx.home_abbreviation, True),
            ],
            league=league,
        )

    away_starter = pitcher_profiles.get(ctx.away_pitcher_id) if ctx.away_pitcher_id else None
    home_starter = pitcher_profiles.get(ctx.home_pitcher_id) if ctx.home_pitcher_id else None
    away_bullpen = pitcher_profiles[-ctx.away_team_id]
    home_bullpen = pitcher_profiles[-ctx.home_team_id]

    home_wins = 0
    away_scores: list[int] = []
    home_scores: list[int] = []
    innings: list[int] = []
    pitch_counts: list[int] = []
    batter_totals: dict[int, dict[str, float]] = {}
    pitcher_totals: dict[int, dict[str, float]] = {}
    batter_hr_games: dict[int, int] = {}
    captured: dict[str, Any] | None = None

    for index in range(iterations):
        result = _simulate_game_once(
            ctx=ctx,
            away_lineup=away_lineup,
            home_lineup=home_lineup,
            away_starter=away_starter,
            home_starter=home_starter,
            away_bullpen=away_bullpen,
            home_bullpen=home_bullpen,
            seed=base_seed + index * 7919,
            capture=index == 0,
            pitch_log_limit=pitch_log_limit,
        )
        if index == 0:
            captured = result
        home_wins += 1 if result["winner"] == "home" else 0
        away_scores.append(int(result["away_score"]))
        home_scores.append(int(result["home_score"]))
        innings.append(int(result["innings"]))
        pitch_counts.append(int(result["pitch_count"]))
        for player_id, stats in result["batter_stats"].items():
            if player_id not in batter_totals:
                batter_totals[player_id] = dict(stats)
            else:
                total = batter_totals[player_id]
                for key, value in stats.items():
                    if isinstance(value, (int, float)):
                        total[key] = _as_float(total.get(key), 0.0) + float(value)
                    else:
                        total.setdefault(key, value)
            if stats.get("hr", 0.0) > 0:
                batter_hr_games[player_id] = batter_hr_games.get(player_id, 0) + 1
        for player_id, stats in result["pitcher_stats"].items():
            if player_id not in pitcher_totals:
                pitcher_totals[player_id] = dict(stats)
            else:
                total = pitcher_totals[player_id]
                for key, value in stats.items():
                    if isinstance(value, (int, float)):
                        total[key] = _as_float(total.get(key), 0.0) + float(value)
                    else:
                        total.setdefault(key, value)

    captured = captured or {}
    weather_start = _weather_at(ctx, ctx.start_time_utc)
    return {
        "sport": "mlb",
        "status": "simulated",
        "engine_version": "pitch-physics-v1",
        "seed": base_seed,
        "iterations": iterations,
        "game": {
            "game_pk": ctx.game_pk,
            "official_date": ctx.official_date.isoformat(),
            "start_time_utc": ctx.start_time_utc.isoformat(),
            "away_team": ctx.away_team,
            "home_team": ctx.home_team,
            "away_abbreviation": ctx.away_abbreviation,
            "home_abbreviation": ctx.home_abbreviation,
            "venue": {
                "name": ctx.venue.name,
                "city": ctx.venue.city,
                "state": ctx.venue.state,
                "roof_type": ctx.venue.roof_type,
                "turf_type": ctx.venue.turf_type,
                "elevation": ctx.venue.elevation,
                "left_line": ctx.venue.left_line,
                "left_center": ctx.venue.left_center,
                "center": ctx.venue.center,
                "right_center": ctx.venue.right_center,
                "right_line": ctx.venue.right_line,
            },
            "home_plate_umpire": ctx.home_plate_umpire,
            "weather_snapshot_count": len(ctx.weather),
            "weather_at_start": {
                "temperature_f": round(weather_start.temperature_f, 1),
                "wind_speed_mph": round(weather_start.wind_speed_mph, 1),
                "wind_direction_deg": weather_start.wind_direction_deg,
                "precipitation_probability": weather_start.precipitation_probability,
            },
        },
        "inputs": {
            "away_lineup_source": away_lineup[0].source if away_lineup else away_source,
            "home_lineup_source": home_lineup[0].source if home_lineup else home_source,
            "away_starter": away_starter.name if away_starter else away_bullpen.name,
            "home_starter": home_starter.name if home_starter else home_bullpen.name,
            "weather_mode": "snapshots" if len(ctx.weather) > 1 else "game_weather_fallback",
        },
        "summary": {
            "away_win_probability": round((iterations - home_wins) / iterations, 4),
            "home_win_probability": round(home_wins / iterations, 4),
            "away_avg_score": round(sum(away_scores) / iterations, 2),
            "home_avg_score": round(sum(home_scores) / iterations, 2),
            "avg_total_runs": round((sum(away_scores) + sum(home_scores)) / iterations, 2),
            "avg_innings": round(sum(innings) / iterations, 2),
            "avg_pitch_count": round(sum(pitch_counts) / iterations, 1),
            "sample_score": {
                "away": captured.get("away_score"),
                "home": captured.get("home_score"),
            },
        },
        "lineups": {
            "away": [
                {
                    "player_id": player.player_id,
                    "name": player.name,
                    "batting_order": player.batting_order,
                    "bat_side": player.bat_side,
                    "source": player.source,
                }
                for player in away_lineup
            ],
            "home": [
                {
                    "player_id": player.player_id,
                    "name": player.name,
                    "batting_order": player.batting_order,
                    "bat_side": player.bat_side,
                    "source": player.source,
                }
                for player in home_lineup
            ],
        },
        "top_batters": _aggregate_player_rows(batter_totals, batter_hr_games, iterations, kind="batter")[:18],
        "pitchers": _aggregate_player_rows(pitcher_totals, {}, iterations, kind="pitcher")[:8],
        "sample": {
            "pitch_log": captured.get("pitch_log", []),
            "field_events": captured.get("field_events", []),
        },
    }
