from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.mlb.raw_storage import write_json_payload, write_text_payload
from app.models.mlb import (
    MlbBatTrackingBatterSeason,
    MlbBattedBallEvent,
    MlbGame,
    MlbGameSnapshot,
    MlbLineupSnapshot,
    MlbParkFactor,
    MlbPitchEvent,
    MlbPlayer,
    MlbPlayerGameBatting,
    MlbPlayerGamePitching,
    MlbSourcePull,
    MlbStatcastBatterSeason,
    MlbStatcastPitcherSeason,
    MlbSwingPathBatterSeason,
    MlbTeam,
    MlbVenue,
)
from app.services.baseballsavant_client import (
    BATTER_CUSTOM_SELECTIONS,
    PITCHER_CUSTOM_SELECTIONS,
    BaseballSavantClient,
)
from app.services.mlb_statsapi_client import MlbStatsApiClient


UTC = timezone.utc


def _chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "null", "None"):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().replace("%", "")
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _normalize_column(value: str) -> str:
    text = str(value).strip().lower()
    text = text.replace("%", " percent ")
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [_normalize_column(column) for column in normalized.columns]
    return normalized


def _candidate(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            value = record[key]
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            return value
    return None


def _parse_batting_order(value: Any) -> int | None:
    parsed = _safe_int(value)
    if parsed is None:
        return None
    if parsed >= 100:
        return parsed // 100
    return parsed


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, pd.Timestamp):
            cleaned[key] = value.to_pydatetime()
        elif hasattr(value, "item") and not isinstance(value, (str, bytes)):
            try:
                cleaned[key] = value.item()
            except Exception:
                cleaned[key] = value
        elif isinstance(value, float) and pd.isna(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def _game_snapshot_type(status_payload: dict[str, Any] | None) -> str:
    status = status_payload or {}
    abstract_state = _safe_text(status.get("abstractGameState"))
    detailed_state = _safe_text(status.get("detailedState"))
    if abstract_state == "Final" or detailed_state in {"Final", "Game Over", "Completed Early"}:
        return "final"
    if abstract_state == "Live":
        return "live"
    return "pregame"


def _build_team_row(team_payload: dict[str, Any]) -> dict[str, Any] | None:
    team_id = _safe_int(team_payload.get("id"))
    abbreviation = _safe_text(
        _candidate(team_payload, "abbreviation", "fileCode", "teamCode")
    )
    name = _safe_text(_candidate(team_payload, "name", "teamName"))
    if team_id is None or not abbreviation or not name:
        return None

    venue = team_payload.get("venue") or {}
    league = team_payload.get("league") or {}
    division = team_payload.get("division") or {}
    return {
        "id": team_id,
        "abbreviation": abbreviation,
        "name": name,
        "team_name": _safe_text(team_payload.get("teamName")),
        "location_name": _safe_text(team_payload.get("locationName")),
        "franchise_name": _safe_text(team_payload.get("franchiseName")),
        "club_name": _safe_text(team_payload.get("clubName")),
        "league_id": _safe_int(league.get("id")),
        "division_id": _safe_int(division.get("id")),
        "venue_id": _safe_int(venue.get("id")),
        "first_year_of_play": _safe_text(team_payload.get("firstYearOfPlay")),
        "active": bool(team_payload.get("active", True)),
    }


def _build_venue_row(venue_payload: dict[str, Any]) -> dict[str, Any] | None:
    venue_id = _safe_int(venue_payload.get("id"))
    name = _safe_text(venue_payload.get("name"))
    if venue_id is None or not name:
        return None

    location = venue_payload.get("location") or {}
    timezone_payload = venue_payload.get("timeZone") or {}
    field_info = venue_payload.get("fieldInfo") or {}
    return {
        "id": venue_id,
        "name": name,
        "active": bool(venue_payload.get("active", True)),
        "city": _safe_text(location.get("city")),
        "state": _safe_text(_candidate(location, "stateAbbrev", "state")),
        "country": _safe_text(location.get("country")),
        "timezone_id": _safe_text(timezone_payload.get("id")),
        "timezone_offset": _safe_int(timezone_payload.get("offset")),
        "latitude": _safe_float(location.get("defaultCoordinates", {}).get("latitude")),
        "longitude": _safe_float(location.get("defaultCoordinates", {}).get("longitude")),
        "elevation": _safe_int(_candidate(venue_payload, "elevation", "elevationFt")),
        "roof_type": _safe_text(_candidate(field_info, "roofType", "roof_type")),
        "turf_type": _safe_text(_candidate(field_info, "turfType", "turf_type")),
        "left_line": _safe_int(field_info.get("leftLine")),
        "left_center": _safe_int(field_info.get("leftCenter")),
        "center": _safe_int(field_info.get("center")),
        "right_center": _safe_int(field_info.get("rightCenter")),
        "right_line": _safe_int(field_info.get("rightLine")),
        "capacity": _safe_int(venue_payload.get("capacity")),
    }


def _build_player_row(player_payload: dict[str, Any]) -> dict[str, Any] | None:
    player_id = _safe_int(player_payload.get("id"))
    full_name = _safe_text(_candidate(player_payload, "fullName", "full_name", "name"))
    if player_id is None or not full_name:
        return None

    primary_position = player_payload.get("primaryPosition") or {}
    bat_side = player_payload.get("batSide") or {}
    pitch_hand = player_payload.get("pitchHand") or {}
    return {
        "id": player_id,
        "full_name": full_name,
        "first_name": _safe_text(_candidate(player_payload, "firstName", "first_name")),
        "last_name": _safe_text(_candidate(player_payload, "lastName", "last_name")),
        "use_name": _safe_text(_candidate(player_payload, "useName", "use_name")),
        "use_last_name": _safe_text(_candidate(player_payload, "useLastName", "use_last_name")),
        "birth_date": _parse_date(_candidate(player_payload, "birthDate", "birth_date")),
        "current_age": _safe_int(_candidate(player_payload, "currentAge", "age")),
        "bat_side": _safe_text(_candidate(bat_side, "code", "description")),
        "pitch_hand": _safe_text(_candidate(pitch_hand, "code", "description")),
        "primary_position_code": _safe_text(primary_position.get("code")),
        "primary_position_name": _safe_text(primary_position.get("name")),
        "primary_position_abbreviation": _safe_text(primary_position.get("abbreviation")),
        "active": bool(player_payload.get("active", True)),
        "draft_year": _safe_int(_candidate(player_payload, "draftYear", "draft_year")),
        "mlb_debut_date": _parse_date(_candidate(player_payload, "mlbDebutDate", "mlb_debut_date")),
        "last_played_date": _parse_date(_candidate(player_payload, "lastPlayedDate", "last_played_date")),
    }


def _minimal_player_row(player_id: int | None, full_name: str | None) -> dict[str, Any] | None:
    if player_id is None or not full_name:
        return None
    return {
        "id": player_id,
        "full_name": full_name,
        "active": True,
    }


async def _create_source_pull(
    db: AsyncSession,
    *,
    source: str,
    resource_type: str,
    request_url: str,
    request_params: dict[str, Any] | None,
    local_path: str,
    response_format: str,
    season: int | None = None,
    game_pk: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    row_count: int = 0,
    status: str = "completed",
    notes: str | None = None,
    fetched_at: datetime | None = None,
) -> MlbSourcePull:
    pull = MlbSourcePull(
        source=source,
        resource_type=resource_type,
        request_url=request_url,
        request_params=request_params,
        local_path=local_path,
        response_format=response_format,
        season=season,
        game_pk=game_pk,
        start_date=start_date,
        end_date=end_date,
        row_count=row_count,
        status=status,
        notes=notes,
        fetched_at=fetched_at or datetime.now(UTC),
    )
    db.add(pull)
    await db.flush()
    return pull


async def _upsert_rows(
    db: AsyncSession,
    model: type,
    rows: list[dict[str, Any]],
    *,
    conflict_columns: list[str] | None = None,
    constraint: str | None = None,
) -> int:
    if not rows:
        return 0

    cleaned_rows = [_clean_row(row) for row in rows]
    stmt = insert(model).values(cleaned_rows)
    sample = cleaned_rows[0]
    update_columns = [
        key
        for key in sample.keys()
        if key not in set(conflict_columns or []) and key != "id"
    ]

    if update_columns:
        set_map = {column: getattr(stmt.excluded, column) for column in update_columns}
        if constraint:
            stmt = stmt.on_conflict_do_update(constraint=constraint, set_=set_map)
        else:
            stmt = stmt.on_conflict_do_update(index_elements=conflict_columns, set_=set_map)
    else:
        if constraint:
            stmt = stmt.on_conflict_do_nothing(constraint=constraint)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)

    await db.execute(stmt)
    return len(cleaned_rows)


async def _venue_lookup(db: AsyncSession) -> dict[str, int]:
    result = await db.execute(select(MlbVenue.id, MlbVenue.name))
    lookup: dict[str, int] = {}
    for venue_id, venue_name in result.all():
        if venue_id and venue_name:
            lookup[str(venue_name).strip().lower()] = int(venue_id)
    return lookup


def _extract_schedule_game_rows(
    payload: dict[str, Any],
    *,
    season: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[int]]:
    venue_rows: dict[int, dict[str, Any]] = {}
    player_rows: dict[int, dict[str, Any]] = {}
    snapshot_rows: list[dict[str, Any]] = []
    game_rows: list[dict[str, Any]] = []
    game_pks: list[int] = []

    for date_bucket in payload.get("dates") or []:
        for game in date_bucket.get("games") or []:
            game_pk = _safe_int(game.get("gamePk"))
            if game_pk is None:
                continue

            game_pks.append(game_pk)
            venue = game.get("venue") or {}
            venue_row = _build_venue_row(venue)
            if venue_row:
                venue_rows[venue_row["id"]] = venue_row

            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team_id = _safe_int((home.get("team") or {}).get("id"))
            away_team_id = _safe_int((away.get("team") or {}).get("id"))
            if home_team_id is None or away_team_id is None:
                continue

            probable_home = home.get("probablePitcher") or {}
            probable_away = away.get("probablePitcher") or {}
            home_pitcher_row = _minimal_player_row(
                _safe_int(probable_home.get("id")),
                _safe_text(_candidate(probable_home, "fullName", "full_name", "name")),
            )
            away_pitcher_row = _minimal_player_row(
                _safe_int(probable_away.get("id")),
                _safe_text(_candidate(probable_away, "fullName", "full_name", "name")),
            )
            if home_pitcher_row:
                player_rows[home_pitcher_row["id"]] = home_pitcher_row
            if away_pitcher_row:
                player_rows[away_pitcher_row["id"]] = away_pitcher_row

            weather = game.get("weather") or {}
            status = game.get("status") or {}
            game_rows.append(
                {
                    "game_pk": game_pk,
                    "official_date": _parse_date(_candidate(game, "officialDate", "gameDate")),
                    "start_time_utc": _parse_datetime(_candidate(game, "gameDate", "game_datetime")),
                    "season": season,
                    "game_type": _safe_text(game.get("gameType")),
                    "double_header": _safe_text(game.get("doubleHeader")),
                    "game_number": _safe_int(game.get("gameNumber")),
                    "status_code": _safe_text(_candidate(status, "abstractGameCode", "codedGameState")),
                    "detailed_state": _safe_text(status.get("detailedState")),
                    "day_night": _safe_text(game.get("dayNight")),
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "venue_id": _safe_int(venue.get("id")),
                    "home_score": _safe_int(home.get("score")),
                    "away_score": _safe_int(away.get("score")),
                    "probable_home_pitcher_id": _safe_int(probable_home.get("id")),
                    "probable_away_pitcher_id": _safe_int(probable_away.get("id")),
                    "weather_condition": _safe_text(_candidate(weather, "condition", "sky")),
                    "temperature_f": _safe_float(_candidate(weather, "temp", "temperature")),
                    "wind_text": _safe_text(weather.get("wind")),
                    "roof_type": _safe_text(_candidate(venue, "roofType", "roof_type")),
                    "last_ingested_at": datetime.now(UTC),
                }
            )
            snapshot_rows.append(
                {
                    "game_pk": game_pk,
                    "snapshot_type": "schedule",
                    "status_code": _safe_text(_candidate(status, "abstractGameCode", "codedGameState")),
                    "detailed_state": _safe_text(status.get("detailedState")),
                    "weather_condition": _safe_text(_candidate(weather, "condition", "sky")),
                    "temperature_f": _safe_float(_candidate(weather, "temp", "temperature")),
                    "wind_text": _safe_text(weather.get("wind")),
                    "roof_type": _safe_text(_candidate(venue, "roofType", "roof_type")),
                    "probable_home_pitcher_id": _safe_int(probable_home.get("id")),
                    "probable_away_pitcher_id": _safe_int(probable_away.get("id")),
                    "payload": game,
                }
            )

    return list(venue_rows.values()), list(player_rows.values()), game_rows, snapshot_rows, game_pks


def _extract_lineup_rows(
    *,
    snapshot_id: int,
    game_pk: int,
    home_team_id: int,
    away_team_id: int,
    boxscore_teams: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for side, team_id in (("home", home_team_id), ("away", away_team_id)):
        team_box = (boxscore_teams or {}).get(side) or {}
        batting_order_ids = [
            _safe_int(player_id)
            for player_id in (team_box.get("battingOrder") or [])
            if _safe_int(player_id) is not None
        ]
        starter_ids = set(batting_order_ids[:9] or batting_order_ids)
        order_map = {player_id: index + 1 for index, player_id in enumerate(batting_order_ids)}

        for player_entry in (team_box.get("players") or {}).values():
            person = player_entry.get("person") or {}
            player_id = _safe_int(person.get("id"))
            if player_id is None:
                continue

            position = player_entry.get("position") or {}
            status = player_entry.get("status") or {}
            batting_order = _parse_batting_order(player_entry.get("battingOrder")) or order_map.get(player_id)
            is_bench = bool(player_entry.get("bench"))
            is_substitute = bool(player_entry.get("substitute"))
            is_starter = bool(player_id in starter_ids or (batting_order and not is_bench and not is_substitute))
            rows.append(
                {
                    "snapshot_id": snapshot_id,
                    "game_pk": game_pk,
                    "team_id": team_id,
                    "player_id": player_id,
                    "batting_order": batting_order,
                    "position_code": _safe_text(position.get("code")),
                    "position_abbreviation": _safe_text(position.get("abbreviation")),
                    "status_code": _safe_text(status.get("code")),
                    "status_description": _safe_text(status.get("description")),
                    "is_starter": is_starter,
                    "is_bench": is_bench,
                    "is_substitute": is_substitute,
                }
            )
    return rows


def _has_stats(stats_payload: dict[str, Any] | None) -> bool:
    if not stats_payload:
        return False
    return any(_safe_text(value) is not None for value in stats_payload.values())


def _extract_player_game_rows(
    *,
    game_pk: int,
    home_team_id: int,
    away_team_id: int,
    boxscore_teams: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    batting_rows: list[dict[str, Any]] = []
    pitching_rows: list[dict[str, Any]] = []

    for side, team_id in (("home", home_team_id), ("away", away_team_id)):
        team_box = (boxscore_teams or {}).get(side) or {}
        batting_order_ids = [
            _safe_int(player_id)
            for player_id in (team_box.get("battingOrder") or [])
            if _safe_int(player_id) is not None
        ]
        order_map = {player_id: index + 1 for index, player_id in enumerate(batting_order_ids)}
        batter_ids = {
            _safe_int(player_id)
            for player_id in (team_box.get("batters") or [])
            if _safe_int(player_id) is not None
        }
        pitcher_ids = [
            _safe_int(player_id)
            for player_id in (team_box.get("pitchers") or [])
            if _safe_int(player_id) is not None
        ]
        starting_pitcher_id = pitcher_ids[0] if pitcher_ids else None

        for player_entry in (team_box.get("players") or {}).values():
            person = player_entry.get("person") or {}
            player_id = _safe_int(person.get("id"))
            if player_id is None:
                continue

            stats = player_entry.get("stats") or {}
            batting = stats.get("batting") or {}
            pitching = stats.get("pitching") or {}
            batting_order = _parse_batting_order(player_entry.get("battingOrder")) or order_map.get(player_id)

            if player_id in batter_ids or _has_stats(batting):
                batting_rows.append(
                    {
                        "game_pk": game_pk,
                        "player_id": player_id,
                        "team_id": team_id,
                        "batting_order": batting_order,
                        "plate_appearances": _safe_int(_candidate(batting, "plateAppearances", "pa")),
                        "at_bats": _safe_int(_candidate(batting, "atBats", "ab")),
                        "hits": _safe_int(batting.get("hits")),
                        "doubles": _safe_int(_candidate(batting, "doubles", "double")),
                        "triples": _safe_int(_candidate(batting, "triples", "triple")),
                        "home_runs": _safe_int(_candidate(batting, "homeRuns", "hr")),
                        "total_bases": _safe_int(_candidate(batting, "totalBases", "tb")),
                        "runs": _safe_int(batting.get("runs")),
                        "rbi": _safe_int(batting.get("rbi")),
                        "walks": _safe_int(_candidate(batting, "baseOnBalls", "walks", "bb")),
                        "strikeouts": _safe_int(_candidate(batting, "strikeOuts", "so")),
                        "hit_by_pitch": _safe_int(_candidate(batting, "hitByPitch", "hbp")),
                        "stolen_bases": _safe_int(_candidate(batting, "stolenBases", "sb")),
                        "caught_stealing": _safe_int(_candidate(batting, "caughtStealing", "cs")),
                        "left_on_base": _safe_int(_candidate(batting, "leftOnBase", "lob")),
                        "sac_bunts": _safe_int(_candidate(batting, "sacBunts", "sac_bunts")),
                        "sac_flies": _safe_int(_candidate(batting, "sacFlies", "sac_flies")),
                        "summary": _safe_text(player_entry.get("seasonStats", {}).get("batting", {}).get("summary")),
                    }
                )

            if player_id in pitcher_ids or _has_stats(pitching):
                pitching_rows.append(
                    {
                        "game_pk": game_pk,
                        "player_id": player_id,
                        "team_id": team_id,
                        "is_starter": bool(player_id == starting_pitcher_id),
                        "innings_pitched": _safe_text(_candidate(pitching, "inningsPitched", "ip")),
                        "outs_recorded": _safe_int(_candidate(pitching, "outs", "outsRecorded")),
                        "batters_faced": _safe_int(_candidate(pitching, "battersFaced", "bf")),
                        "pitches_thrown": _safe_int(_candidate(pitching, "numberOfPitches", "pitchesThrown")),
                        "strikes": _safe_int(pitching.get("strikes")),
                        "balls": _safe_int(pitching.get("balls")),
                        "hits_allowed": _safe_int(_candidate(pitching, "hits", "hitsAllowed")),
                        "home_runs_allowed": _safe_int(_candidate(pitching, "homeRuns", "hr")),
                        "earned_runs": _safe_int(_candidate(pitching, "earnedRuns", "er")),
                        "walks": _safe_int(_candidate(pitching, "baseOnBalls", "bb")),
                        "strikeouts": _safe_int(_candidate(pitching, "strikeOuts", "so")),
                        "summary": _safe_text(player_entry.get("seasonStats", {}).get("pitching", {}).get("summary")),
                    }
                )

    return batting_rows, pitching_rows


def _coord_value(record: dict[str, Any], *keys: str) -> float | None:
    return _safe_float(_candidate(record, *keys))


def _extract_event_rows(
    *,
    game_pk: int,
    all_plays: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pitch_rows: list[dict[str, Any]] = []
    batted_ball_rows: list[dict[str, Any]] = []

    for play in all_plays or []:
        at_bat_index = _safe_int(play.get("atBatIndex"))
        if at_bat_index is None:
            continue

        about = play.get("about") or {}
        matchup = play.get("matchup") or {}
        result = play.get("result") or {}
        inning = _safe_int(about.get("inning"))
        half_inning = _safe_text(about.get("halfInning"))
        pitcher_id = _safe_int((matchup.get("pitcher") or {}).get("id"))
        batter_id = _safe_int((matchup.get("batter") or {}).get("id"))
        event_type = _safe_text(result.get("eventType"))
        event_description = _safe_text(result.get("description"))

        previous_balls = 0
        previous_strikes = 0
        previous_outs = _safe_int((play.get("count") or {}).get("outs")) or 0

        for event_index, event in enumerate(play.get("playEvents") or []):
            details = event.get("details") or {}
            pitch_data = event.get("pitchData") or {}
            breaks = pitch_data.get("breaks") or {}
            coordinates = pitch_data.get("coordinates") or {}
            count = event.get("count") or {}
            pitch_number = _safe_int(event.get("pitchNumber")) or (event_index + 1)
            play_id = _safe_text(event.get("playId")) or f"{game_pk}:{at_bat_index}:{pitch_number}:{event_index}"
            balls_after = _safe_int(count.get("balls"))
            strikes_after = _safe_int(count.get("strikes"))
            outs_after = _safe_int(count.get("outs"))
            call_code = _safe_text(details.get("code"))
            call_description = _safe_text(details.get("description"))
            pitch_type = details.get("type") or {}
            description = _safe_text(_candidate(details, "description", "event"))

            is_in_play = bool(
                event.get("isInPlay")
                or details.get("isInPlay")
                or call_code == "X"
                or event.get("hitData")
            )
            lowered_description = (call_description or description or "").lower()
            is_ball = bool(call_code == "B" or lowered_description.startswith("ball"))
            is_strike = bool(
                call_code == "S"
                or "strike" in lowered_description
                or call_code in {"C", "F", "M", "Q", "T", "W"}
            )
            is_out = bool("out" in (event_description or "").lower() or "out" in lowered_description)

            if event.get("isPitch") or pitch_data:
                pitch_rows.append(
                    {
                        "game_pk": game_pk,
                        "play_id": play_id,
                        "at_bat_index": at_bat_index,
                        "pitch_number": pitch_number,
                        "inning": inning,
                        "half_inning": half_inning,
                        "pitcher_id": pitcher_id,
                        "batter_id": batter_id,
                        "balls_before": previous_balls,
                        "strikes_before": previous_strikes,
                        "outs_before": previous_outs,
                        "balls_after": balls_after,
                        "strikes_after": strikes_after,
                        "outs_after": outs_after,
                        "pitch_type_code": _safe_text(pitch_type.get("code")),
                        "pitch_type_description": _safe_text(pitch_type.get("description")),
                        "call_code": call_code,
                        "call_description": call_description,
                        "event_type": event_type,
                        "description": description,
                        "is_in_play": is_in_play,
                        "is_strike": is_strike,
                        "is_ball": is_ball,
                        "is_out": is_out,
                        "start_speed": _safe_float(_candidate(pitch_data, "startSpeed", "start_speed")),
                        "end_speed": _safe_float(_candidate(pitch_data, "endSpeed", "end_speed")),
                        "zone": _safe_int(_candidate(pitch_data, "zone", "zoneNumber")),
                        "plate_time": _safe_float(_candidate(pitch_data, "plateTime", "plate_time")),
                        "extension": _safe_float(_candidate(pitch_data, "extension", "releaseExtension")),
                        "spin_rate": _safe_float(_candidate(breaks, "spinRate", "spin_rate")),
                        "spin_direction": _safe_float(_candidate(breaks, "spinDirection", "spin_direction")),
                        "break_angle": _safe_float(_candidate(breaks, "breakAngle", "break_angle")),
                        "break_length": _safe_float(_candidate(breaks, "breakLength", "break_length")),
                        "break_y": _safe_float(_candidate(breaks, "breakY", "break_y")),
                        "break_vertical": _safe_float(_candidate(breaks, "breakVertical", "break_vertical")),
                        "break_vertical_induced": _safe_float(
                            _candidate(breaks, "breakVerticalInduced", "break_vertical_induced")
                        ),
                        "break_horizontal": _safe_float(_candidate(breaks, "breakHorizontal", "break_horizontal")),
                        "pfx_x": _coord_value(coordinates, "pfxX", "pfx_x"),
                        "pfx_z": _coord_value(coordinates, "pfxZ", "pfx_z"),
                        "plate_x": _coord_value(coordinates, "pX", "plateX", "plate_x"),
                        "plate_z": _coord_value(coordinates, "pZ", "plateZ", "plate_z"),
                        "release_pos_x": _coord_value(coordinates, "x0", "releasePosX", "release_pos_x"),
                        "release_pos_y": _coord_value(coordinates, "y0", "releasePosY", "release_pos_y"),
                        "release_pos_z": _coord_value(coordinates, "z0", "releasePosZ", "release_pos_z"),
                        "vx0": _coord_value(coordinates, "vX0", "vx0"),
                        "vy0": _coord_value(coordinates, "vY0", "vy0"),
                        "vz0": _coord_value(coordinates, "vZ0", "vz0"),
                        "ax": _coord_value(coordinates, "aX", "ax"),
                        "ay": _coord_value(coordinates, "aY", "ay"),
                        "az": _coord_value(coordinates, "aZ", "az"),
                    }
                )

            hit_data = event.get("hitData") or {}
            hit_coordinates = hit_data.get("coordinates") or {}
            if hit_data:
                launch_speed = _safe_float(_candidate(hit_data, "launchSpeed", "launch_speed"))
                launch_angle = _safe_float(_candidate(hit_data, "launchAngle", "launch_angle"))
                batted_ball_rows.append(
                    {
                        "game_pk": game_pk,
                        "play_id": play_id,
                        "at_bat_index": at_bat_index,
                        "inning": inning,
                        "half_inning": half_inning,
                        "pitcher_id": pitcher_id,
                        "batter_id": batter_id,
                        "event_type": event_type,
                        "description": event_description,
                        "launch_speed": launch_speed,
                        "launch_angle": launch_angle,
                        "total_distance": _safe_float(_candidate(hit_data, "totalDistance", "total_distance")),
                        "trajectory": _safe_text(_candidate(hit_data, "trajectory", "trajectoryType")),
                        "hardness": _safe_text(hit_data.get("hardness")),
                        "location": _safe_text(hit_data.get("location")),
                        "coord_x": _coord_value(hit_coordinates, "coordX", "x"),
                        "coord_y": _coord_value(hit_coordinates, "coordY", "y"),
                        "is_hard_hit": bool(launch_speed is not None and launch_speed >= 95.0),
                        "is_sweet_spot": bool(
                            launch_angle is not None and 8.0 <= launch_angle <= 32.0
                        ),
                        "estimated_ba_using_speedangle": _safe_float(
                            _candidate(hit_data, "estimatedBAUsingSpeedAngle", "estimated_ba_using_speedangle")
                        ),
                        "estimated_woba_using_speedangle": _safe_float(
                            _candidate(hit_data, "estimatedWobaUsingSpeedAngle", "estimated_woba_using_speedangle")
                        ),
                        "launch_speed_angle": _safe_int(
                            _candidate(hit_data, "launchSpeedAngle", "launch_speed_angle")
                        ),
                    }
                )

            if balls_after is not None:
                previous_balls = balls_after
            if strikes_after is not None:
                previous_strikes = strikes_after
            if outs_after is not None:
                previous_outs = outs_after

    return pitch_rows, batted_ball_rows


async def ingest_teams(
    db: AsyncSession,
    *,
    season: int,
    client: MlbStatsApiClient | None = None,
) -> dict[str, Any]:
    stats_client = client or MlbStatsApiClient()
    teams_payload, teams_url = await stats_client.get_teams(season=season, hydrate="venue")
    raw_teams = write_json_payload("statsapi", "teams", f"season_{season}", teams_payload)

    team_rows = [
        row
        for row in (_build_team_row(team_payload) for team_payload in teams_payload.get("teams") or [])
        if row
    ]
    nested_venue_rows = [
        row
        for row in (
            _build_venue_row((team_payload.get("venue") or {}))
            for team_payload in teams_payload.get("teams") or []
        )
        if row
    ]

    teams_pull = await _create_source_pull(
        db,
        source="statsapi",
        resource_type="teams",
        request_url=teams_url,
        request_params={"season": season},
        local_path=raw_teams.relative_path,
        response_format="json",
        season=season,
        row_count=len(team_rows),
        fetched_at=raw_teams.fetched_at,
    )

    venue_rows_by_id = {row["id"]: row for row in nested_venue_rows}
    venue_ids = sorted(
        {
            _safe_int((team_payload.get("venue") or {}).get("id"))
            for team_payload in teams_payload.get("teams") or []
            if _safe_int((team_payload.get("venue") or {}).get("id")) is not None
        }
    )

    for venue_chunk in _chunked(venue_ids, 25):
        venue_payload, venue_url = await stats_client.get_venues(venue_ids=venue_chunk, season=season)
        raw_venues = write_json_payload(
            "statsapi",
            "venues",
            f"season_{season}_{venue_chunk[0]}_{venue_chunk[-1]}",
            venue_payload,
        )
        venue_rows = [
            row
            for row in (_build_venue_row(venue_payload_row) for venue_payload_row in venue_payload.get("venues") or [])
            if row
        ]
        for row in venue_rows:
            venue_rows_by_id[row["id"]] = row

        await _create_source_pull(
            db,
            source="statsapi",
            resource_type="venues",
            request_url=venue_url,
            request_params={"season": season, "venueIds": venue_chunk},
            local_path=raw_venues.relative_path,
            response_format="json",
            season=season,
            row_count=len(venue_rows),
            fetched_at=raw_venues.fetched_at,
        )

    venue_count = await _upsert_rows(db, MlbVenue, list(venue_rows_by_id.values()), conflict_columns=["id"])
    team_count = await _upsert_rows(db, MlbTeam, team_rows, conflict_columns=["id"])
    await db.commit()

    return {
        "status": "success",
        "season": season,
        "source_pull_id": teams_pull.id,
        "teams_upserted": team_count,
        "venues_upserted": venue_count,
    }


async def ingest_schedule(
    db: AsyncSession,
    *,
    season: int,
    start_date: str | None = None,
    end_date: str | None = None,
    client: MlbStatsApiClient | None = None,
    load_teams_first: bool = True,
) -> dict[str, Any]:
    stats_client = client or MlbStatsApiClient()
    if load_teams_first:
        await ingest_teams(db, season=season, client=stats_client)

    schedule_payload, schedule_url = await stats_client.get_schedule(
        season=season,
        start_date=start_date,
        end_date=end_date,
        game_types="R,F,D,L,W",
    )
    raw_schedule = write_json_payload(
        "statsapi",
        "schedule",
        f"season_{season}_{start_date or 'full'}_{end_date or 'full'}",
        schedule_payload,
    )

    venue_rows, player_rows, game_rows, snapshot_rows, game_pks = _extract_schedule_game_rows(
        schedule_payload,
        season=season,
    )
    start_date_value = _parse_date(start_date)
    end_date_value = _parse_date(end_date)
    source_pull = await _create_source_pull(
        db,
        source="statsapi",
        resource_type="schedule",
        request_url=schedule_url,
        request_params={
            "season": season,
            "startDate": start_date,
            "endDate": end_date,
            "gameTypes": "R,F,D,L,W",
        },
        local_path=raw_schedule.relative_path,
        response_format="json",
        season=season,
        start_date=start_date_value,
        end_date=end_date_value,
        row_count=len(game_rows),
        fetched_at=raw_schedule.fetched_at,
    )

    if venue_rows:
        await _upsert_rows(db, MlbVenue, venue_rows, conflict_columns=["id"])
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])
    await _upsert_rows(db, MlbGame, game_rows, conflict_columns=["game_pk"])

    for snapshot_row in snapshot_rows:
        db.add(
            MlbGameSnapshot(
                game_pk=snapshot_row["game_pk"],
                source_pull_id=source_pull.id,
                snapshot_type=snapshot_row["snapshot_type"],
                captured_at=raw_schedule.fetched_at,
                status_code=snapshot_row["status_code"],
                detailed_state=snapshot_row["detailed_state"],
                weather_condition=snapshot_row["weather_condition"],
                temperature_f=snapshot_row["temperature_f"],
                wind_text=snapshot_row["wind_text"],
                roof_type=snapshot_row["roof_type"],
                probable_home_pitcher_id=snapshot_row["probable_home_pitcher_id"],
                probable_away_pitcher_id=snapshot_row["probable_away_pitcher_id"],
                payload=snapshot_row["payload"],
            )
        )

    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_id": source_pull.id,
        "games_upserted": len(game_rows),
        "snapshots_created": len(snapshot_rows),
        "game_pks": game_pks,
    }


async def ingest_game_feed(
    db: AsyncSession,
    *,
    game_pk: int,
    client: MlbStatsApiClient | None = None,
) -> dict[str, Any]:
    stats_client = client or MlbStatsApiClient()
    payload, request_url = await stats_client.get_game_feed(game_pk=game_pk)
    raw_feed = write_json_payload("statsapi", "feed_live", f"game_{game_pk}", payload)

    game_data = payload.get("gameData") or {}
    live_data = payload.get("liveData") or {}
    boxscore_teams = (live_data.get("boxscore") or {}).get("teams") or {}
    all_plays = (live_data.get("plays") or {}).get("allPlays") or []
    status_payload = game_data.get("status") or {}

    venue_row = _build_venue_row(game_data.get("venue") or {})
    team_rows = [
        row
        for row in (
            _build_team_row((game_data.get("teams") or {}).get("home") or {}),
            _build_team_row((game_data.get("teams") or {}).get("away") or {}),
        )
        if row
    ]
    player_rows = [
        row
        for row in (
            _build_player_row(player_payload)
            for player_payload in (game_data.get("players") or {}).values()
        )
        if row
    ]
    if venue_row:
        await _upsert_rows(db, MlbVenue, [venue_row], conflict_columns=["id"])
    if team_rows:
        await _upsert_rows(db, MlbTeam, team_rows, conflict_columns=["id"])
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])

    home_team = (game_data.get("teams") or {}).get("home") or {}
    away_team = (game_data.get("teams") or {}).get("away") or {}
    home_team_id = _safe_int(home_team.get("id"))
    away_team_id = _safe_int(away_team.get("id"))
    if home_team_id is None or away_team_id is None:
        raise ValueError(f"Game {game_pk} missing home/away team ids in feed payload.")

    probable_home = home_team.get("probablePitcher") or {}
    probable_away = away_team.get("probablePitcher") or {}
    weather = game_data.get("weather") or {}
    game_row = {
        "game_pk": game_pk,
        "official_date": _parse_date(
            _candidate(
                game_data.get("datetime") or {},
                "officialDate",
                "originalDate",
            )
            or game_data.get("officialDate")
        ),
        "start_time_utc": _parse_datetime(_candidate(game_data.get("datetime") or {}, "dateTime", "time")),
        "season": _safe_int((game_data.get("game") or {}).get("season"))
        or _safe_int(game_data.get("season"))
        or datetime.now(UTC).year,
        "game_type": _safe_text(game_data.get("gameType")),
        "double_header": _safe_text(game_data.get("doubleHeader")),
        "game_number": _safe_int(game_data.get("gameNumber")),
        "status_code": _safe_text(_candidate(status_payload, "abstractGameCode", "codedGameState")),
        "detailed_state": _safe_text(status_payload.get("detailedState")),
        "day_night": _safe_text(game_data.get("dayNight")),
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "venue_id": _safe_int((game_data.get("venue") or {}).get("id")),
        "home_score": _safe_int(((live_data.get("boxscore") or {}).get("teams") or {}).get("home", {}).get("teamStats", {}).get("batting", {}).get("runs")),
        "away_score": _safe_int(((live_data.get("boxscore") or {}).get("teams") or {}).get("away", {}).get("teamStats", {}).get("batting", {}).get("runs")),
        "probable_home_pitcher_id": _safe_int(probable_home.get("id")),
        "probable_away_pitcher_id": _safe_int(probable_away.get("id")),
        "weather_condition": _safe_text(_candidate(weather, "condition", "sky")),
        "temperature_f": _safe_float(_candidate(weather, "temp", "temperature")),
        "wind_text": _safe_text(weather.get("wind")),
        "roof_type": _safe_text(_candidate(game_data.get("venue") or {}, "roofType", "roof_type")),
        "last_ingested_at": raw_feed.fetched_at,
    }
    source_pull = await _create_source_pull(
        db,
        source="statsapi",
        resource_type="feed_live",
        request_url=request_url,
        request_params={"gamePk": game_pk},
        local_path=raw_feed.relative_path,
        response_format="json",
        season=_safe_int(game_data.get("season")),
        game_pk=game_pk,
        start_date=game_row["official_date"],
        end_date=game_row["official_date"],
        row_count=len(all_plays),
        fetched_at=raw_feed.fetched_at,
    )
    await _upsert_rows(db, MlbGame, [game_row], conflict_columns=["game_pk"])

    snapshot = MlbGameSnapshot(
        game_pk=game_pk,
        source_pull_id=source_pull.id,
        snapshot_type=_game_snapshot_type(status_payload),
        captured_at=raw_feed.fetched_at,
        status_code=game_row["status_code"],
        detailed_state=game_row["detailed_state"],
        weather_condition=game_row["weather_condition"],
        temperature_f=game_row["temperature_f"],
        wind_text=game_row["wind_text"],
        roof_type=game_row["roof_type"],
        probable_home_pitcher_id=game_row["probable_home_pitcher_id"],
        probable_away_pitcher_id=game_row["probable_away_pitcher_id"],
        payload={"request_url": request_url, "raw_path": raw_feed.relative_path},
    )
    db.add(snapshot)
    await db.flush()

    lineup_rows = _extract_lineup_rows(
        snapshot_id=snapshot.id,
        game_pk=game_pk,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        boxscore_teams=boxscore_teams,
    )
    if lineup_rows:
        await _upsert_rows(
            db,
            MlbLineupSnapshot,
            lineup_rows,
            constraint="uq_mlb_lineup_snapshot_player",
        )

    batting_rows, pitching_rows = _extract_player_game_rows(
        game_pk=game_pk,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        boxscore_teams=boxscore_teams,
    )
    pitch_rows, batted_ball_rows = _extract_event_rows(game_pk=game_pk, all_plays=all_plays)

    await db.execute(delete(MlbPlayerGameBatting).where(MlbPlayerGameBatting.game_pk == game_pk))
    await db.execute(delete(MlbPlayerGamePitching).where(MlbPlayerGamePitching.game_pk == game_pk))
    await db.execute(delete(MlbPitchEvent).where(MlbPitchEvent.game_pk == game_pk))
    await db.execute(delete(MlbBattedBallEvent).where(MlbBattedBallEvent.game_pk == game_pk))

    if batting_rows:
        await _upsert_rows(
            db,
            MlbPlayerGameBatting,
            batting_rows,
            constraint="uq_mlb_player_game_batting",
        )
    if pitching_rows:
        await _upsert_rows(
            db,
            MlbPlayerGamePitching,
            pitching_rows,
            constraint="uq_mlb_player_game_pitching",
        )
    if pitch_rows:
        await _upsert_rows(
            db,
            MlbPitchEvent,
            pitch_rows,
            constraint="uq_mlb_pitch_event_play",
        )
    if batted_ball_rows:
        await _upsert_rows(
            db,
            MlbBattedBallEvent,
            batted_ball_rows,
            constraint="uq_mlb_batted_ball_event_play",
        )

    await db.commit()
    return {
        "status": "success",
        "game_pk": game_pk,
        "source_pull_id": source_pull.id,
        "snapshot_id": snapshot.id,
        "lineup_rows": len(lineup_rows),
        "batting_rows": len(batting_rows),
        "pitching_rows": len(pitching_rows),
        "pitch_events": len(pitch_rows),
        "batted_ball_events": len(batted_ball_rows),
        "snapshot_type": snapshot.snapshot_type,
    }


async def ingest_game_feeds(
    db: AsyncSession,
    *,
    season: int,
    start_date: str,
    end_date: str,
    final_only: bool = False,
    client: MlbStatsApiClient | None = None,
) -> dict[str, Any]:
    stats_client = client or MlbStatsApiClient()
    schedule_result = await ingest_schedule(
        db,
        season=season,
        start_date=start_date,
        end_date=end_date,
        client=stats_client,
        load_teams_first=True,
    )

    start_date_value = _parse_date(start_date)
    end_date_value = _parse_date(end_date)
    target_stmt = select(MlbGame.game_pk).where(MlbGame.season == season)
    if start_date_value is not None:
        target_stmt = target_stmt.where(MlbGame.official_date >= start_date_value)
    if end_date_value is not None:
        target_stmt = target_stmt.where(MlbGame.official_date <= end_date_value)
    if final_only:
        target_stmt = target_stmt.where(
            MlbGame.detailed_state.in_(("Final", "Game Over", "Completed Early"))
        )
    target_stmt = target_stmt.order_by(MlbGame.official_date, MlbGame.game_pk)
    target_result = await db.execute(target_stmt)
    target_game_pks = [int(game_pk) for game_pk in target_result.scalars().all()]

    per_game_results = []
    for game_pk in target_game_pks:
        per_game_results.append(await ingest_game_feed(db, game_pk=game_pk, client=stats_client))

    return {
        "status": "success",
        "season": season,
        "start_date": start_date,
        "end_date": end_date,
        "scheduled_games": len(schedule_result.get("game_pks") or []),
        "target_games": len(target_game_pks),
        "games_ingested": len(per_game_results),
        "schedule": schedule_result,
        "games": per_game_results,
    }


def _savant_player_stub(record: dict[str, Any]) -> dict[str, Any] | None:
    player_id = _safe_int(_candidate(record, "player_id", "playerid", "id", "batter", "pitcher"))
    player_name = _safe_text(_candidate(record, "last_name_first_name", "player_name", "name"))
    return _minimal_player_row(player_id, player_name)


async def ingest_savant_statcast_batters(
    db: AsyncSession,
    *,
    season: int,
    client: BaseballSavantClient | None = None,
    selections: list[str] | None = None,
) -> dict[str, Any]:
    savant_client = client or BaseballSavantClient()
    dataframe, csv_text, request_url = await savant_client.get_custom_leaderboard(
        season=season,
        player_type="batter",
        selections=selections or BATTER_CUSTOM_SELECTIONS,
    )
    normalized = _normalize_dataframe(dataframe)
    raw_csv = write_text_payload("savant", "custom_batter", f"season_{season}", csv_text)
    source_pull = await _create_source_pull(
        db,
        source="savant",
        resource_type="custom_batter",
        request_url=request_url,
        request_params={"season": season, "type": "batter"},
        local_path=raw_csv.relative_path,
        response_format="csv",
        season=season,
        row_count=len(normalized.index),
        fetched_at=raw_csv.fetched_at,
    )

    player_rows = [
        row
        for row in (_savant_player_stub(record) for record in normalized.to_dict(orient="records"))
        if row
    ]
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])

    stat_rows = []
    for record in normalized.to_dict(orient="records"):
        player_id = _safe_int(_candidate(record, "player_id", "playerid", "id"))
        player_name = _safe_text(_candidate(record, "last_name_first_name", "player_name", "name"))
        if player_id is None or not player_name:
            continue
        stat_rows.append(
            {
                "season": season,
                "player_id": player_id,
                "player_name": player_name,
                "team_abbreviation": _safe_text(_candidate(record, "team", "team_abbreviation", "team_name_abbrev")),
                "plate_appearances": _safe_int(record.get("pa")),
                "at_bats": _safe_int(_candidate(record, "ab", "at_bats")),
                "hits": _safe_int(_candidate(record, "hits", "h")),
                "home_runs": _safe_int(_candidate(record, "home_runs", "hr", "hrs")),
                "strikeouts": _safe_int(_candidate(record, "strikeouts", "so")),
                "walks": _safe_int(_candidate(record, "walks", "bb")),
                "avg": _safe_float(_candidate(record, "batting_avg", "avg")),
                "obp": _safe_float(_candidate(record, "on_base_percent", "obp")),
                "slg": _safe_float(_candidate(record, "slg_percent", "slg")),
                "ops": _safe_float(record.get("ops")),
                "iso": _safe_float(record.get("iso")),
                "babip": _safe_float(record.get("babip")),
                "xba": _safe_float(record.get("xba")),
                "xslg": _safe_float(record.get("xslg")),
                "xwoba": _safe_float(record.get("xwoba")),
                "xobp": _safe_float(record.get("xobp")),
                "xiso": _safe_float(record.get("xiso")),
                "exit_velocity_avg": _safe_float(
                    _candidate(record, "exit_velocity_avg", "avg_ev", "avg_hit_speed")
                ),
                "launch_angle_avg": _safe_float(
                    _candidate(record, "launch_angle_avg", "avg_la", "avg_hit_angle")
                ),
                "barrel_batted_rate": _safe_float(record.get("barrel_batted_rate")),
                "hard_hit_percent": _safe_float(record.get("hard_hit_percent")),
                "sweet_spot_percent": _safe_float(record.get("sweet_spot_percent")),
                "source_pull_id": source_pull.id,
            }
        )

    inserted = await _upsert_rows(
        db,
        MlbStatcastBatterSeason,
        stat_rows,
        constraint="uq_mlb_statcast_batter_season",
    )
    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_id": source_pull.id,
        "rows_upserted": inserted,
    }


async def ingest_savant_statcast_pitchers(
    db: AsyncSession,
    *,
    season: int,
    client: BaseballSavantClient | None = None,
    selections: list[str] | None = None,
) -> dict[str, Any]:
    savant_client = client or BaseballSavantClient()
    dataframe, csv_text, request_url = await savant_client.get_custom_leaderboard(
        season=season,
        player_type="pitcher",
        selections=selections or PITCHER_CUSTOM_SELECTIONS,
    )
    normalized = _normalize_dataframe(dataframe)
    raw_csv = write_text_payload("savant", "custom_pitcher", f"season_{season}", csv_text)
    source_pull = await _create_source_pull(
        db,
        source="savant",
        resource_type="custom_pitcher",
        request_url=request_url,
        request_params={"season": season, "type": "pitcher"},
        local_path=raw_csv.relative_path,
        response_format="csv",
        season=season,
        row_count=len(normalized.index),
        fetched_at=raw_csv.fetched_at,
    )

    player_rows = [
        row
        for row in (_savant_player_stub(record) for record in normalized.to_dict(orient="records"))
        if row
    ]
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])

    stat_rows = []
    for record in normalized.to_dict(orient="records"):
        player_id = _safe_int(_candidate(record, "player_id", "playerid", "id"))
        player_name = _safe_text(_candidate(record, "last_name_first_name", "player_name", "name"))
        if player_id is None or not player_name:
            continue
        stat_rows.append(
            {
                "season": season,
                "player_id": player_id,
                "player_name": player_name,
                "team_abbreviation": _safe_text(_candidate(record, "team", "team_abbreviation", "team_name_abbrev")),
                "batters_faced": _safe_int(_candidate(record, "bf", "batters_faced")),
                "strikeout_percent": _safe_float(_candidate(record, "k_percent", "strikeout_percent")),
                "walk_percent": _safe_float(_candidate(record, "bb_percent", "walk_percent")),
                "xera": _safe_float(record.get("xera")),
                "xba": _safe_float(record.get("xba")),
                "xslg": _safe_float(record.get("xslg")),
                "xwoba": _safe_float(record.get("xwoba")),
                "exit_velocity_avg": _safe_float(
                    _candidate(record, "exit_velocity_avg", "avg_ev", "avg_hit_speed")
                ),
                "launch_angle_avg": _safe_float(
                    _candidate(record, "launch_angle_avg", "avg_la", "avg_hit_angle")
                ),
                "barrel_batted_rate": _safe_float(record.get("barrel_batted_rate")),
                "hard_hit_percent": _safe_float(record.get("hard_hit_percent")),
                "source_pull_id": source_pull.id,
            }
        )

    inserted = await _upsert_rows(
        db,
        MlbStatcastPitcherSeason,
        stat_rows,
        constraint="uq_mlb_statcast_pitcher_season",
    )
    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_id": source_pull.id,
        "rows_upserted": inserted,
    }


async def ingest_savant_bat_tracking(
    db: AsyncSession,
    *,
    season: int,
    client: BaseballSavantClient | None = None,
    min_swings: int = 100,
) -> dict[str, Any]:
    savant_client = client or BaseballSavantClient()
    dataframe, csv_text, request_url = await savant_client.get_bat_tracking(
        season=season,
        min_swings=min_swings,
    )
    normalized = _normalize_dataframe(dataframe)
    raw_csv = write_text_payload("savant", "bat_tracking", f"season_{season}", csv_text)
    source_pull = await _create_source_pull(
        db,
        source="savant",
        resource_type="bat_tracking",
        request_url=request_url,
        request_params={"season": season, "minSwings": min_swings},
        local_path=raw_csv.relative_path,
        response_format="csv",
        season=season,
        row_count=len(normalized.index),
        fetched_at=raw_csv.fetched_at,
    )

    player_rows = [
        row
        for row in (_savant_player_stub(record) for record in normalized.to_dict(orient="records"))
        if row
    ]
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])

    stat_rows = []
    for record in normalized.to_dict(orient="records"):
        player_id = _safe_int(_candidate(record, "player_id", "playerid", "id"))
        player_name = _safe_text(_candidate(record, "last_name_first_name", "player_name", "name"))
        if player_id is None or not player_name:
            continue
        blast_per_swing = _safe_float(_candidate(record, "blast_per_swing", "blast_rate"))
        swings = _safe_float(_candidate(record, "swings_competitive", "competitive_swings"))
        stat_rows.append(
            {
                "season": season,
                "player_id": player_id,
                "player_name": player_name,
                "team_abbreviation": _safe_text(_candidate(record, "team", "team_name_abbrev", "team_abbreviation")),
                "bat_speed": _safe_float(
                    _candidate(record, "bat_speed", "avg_sweetspot_speed_mph_qualified", "avg_bat_speed")
                ),
                "fast_swing_rate": _safe_float(
                    _candidate(record, "fast_swing_rate", "fast_swing_percent", "hard_swing_rate")
                ),
                "squared_up_rate": _safe_float(
                    _candidate(
                        record,
                        "squared_up_rate",
                        "squared_up_contact_percent",
                        "squared_up_percent",
                        "squared_up_per_swing",
                        "squared_up_per_bat_contact",
                    )
                ),
                "blast_rate": _safe_float(
                    _candidate(
                        record,
                        "blast_rate",
                        "blasts_contact_percent",
                        "blasts_per_contact",
                        "blast_per_swing",
                        "blast_per_bat_contact",
                    )
                ),
                "blasts": _safe_float(_candidate(record, "blasts", "blast_count"))
                or ((blast_per_swing or 0.0) * (swings or 0.0) if blast_per_swing is not None and swings is not None else None),
                "swing_length": _safe_float(record.get("swing_length")),
                "source_pull_id": source_pull.id,
            }
        )

    inserted = await _upsert_rows(
        db,
        MlbBatTrackingBatterSeason,
        stat_rows,
        constraint="uq_mlb_bat_tracking_batter_season",
    )
    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_id": source_pull.id,
        "rows_upserted": inserted,
    }


async def ingest_savant_swing_path(
    db: AsyncSession,
    *,
    season: int,
    client: BaseballSavantClient | None = None,
    min_swings: int = 100,
) -> dict[str, Any]:
    savant_client = client or BaseballSavantClient()
    dataframe, csv_text, request_url = await savant_client.get_swing_path(
        season=season,
        min_swings=min_swings,
    )
    normalized = _normalize_dataframe(dataframe)
    raw_csv = write_text_payload("savant", "swing_path", f"season_{season}", csv_text)
    source_pull = await _create_source_pull(
        db,
        source="savant",
        resource_type="swing_path",
        request_url=request_url,
        request_params={"season": season, "minSwings": min_swings},
        local_path=raw_csv.relative_path,
        response_format="csv",
        season=season,
        row_count=len(normalized.index),
        fetched_at=raw_csv.fetched_at,
    )

    player_rows = [
        row
        for row in (_savant_player_stub(record) for record in normalized.to_dict(orient="records"))
        if row
    ]
    if player_rows:
        await _upsert_rows(db, MlbPlayer, player_rows, conflict_columns=["id"])

    stat_rows = []
    for record in normalized.to_dict(orient="records"):
        player_id = _safe_int(_candidate(record, "player_id", "playerid", "id"))
        player_name = _safe_text(_candidate(record, "last_name_first_name", "player_name", "name"))
        if player_id is None or not player_name:
            continue
        stat_rows.append(
            {
                "season": season,
                "player_id": player_id,
                "player_name": player_name,
                "team_abbreviation": _safe_text(_candidate(record, "team", "team_name_abbrev", "team_abbreviation")),
                "attack_angle": _safe_float(record.get("attack_angle")),
                "attack_direction": _safe_float(
                    _candidate(record, "attack_direction", "attack_direction_pullopp")
                ),
                "ideal_attack_angle_rate": _safe_float(
                    _candidate(record, "rate_ideal_attack_angle", "ideal_attack_angle_percent")
                ),
                "swing_path_tilt": _safe_float(
                    _candidate(record, "avg_plane_vertical_angle", "swing_path_tilt")
                ),
                "source_pull_id": source_pull.id,
            }
        )

    inserted = await _upsert_rows(
        db,
        MlbSwingPathBatterSeason,
        stat_rows,
        constraint="uq_mlb_swing_path_batter_season",
    )
    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_id": source_pull.id,
        "rows_upserted": inserted,
    }


async def ingest_savant_park_factors(
    db: AsyncSession,
    *,
    season: int,
    client: BaseballSavantClient | None = None,
    combinations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    savant_client = client or BaseballSavantClient()
    venue_name_lookup = await _venue_lookup(db)
    combos = combinations or [
        {"stat": "index_HR", "factor_type": "year", "bat_side": "", "condition": "All", "rolling": ""},
        {"stat": "index_HR", "factor_type": "year", "bat_side": "R", "condition": "All", "rolling": ""},
        {"stat": "index_HR", "factor_type": "year", "bat_side": "L", "condition": "All", "rolling": ""},
        {"stat": "index_HR", "factor_type": "year", "bat_side": "", "condition": "Day", "rolling": ""},
        {"stat": "index_HR", "factor_type": "year", "bat_side": "", "condition": "Night", "rolling": ""},
        {"stat": "index_HR", "factor_type": "year", "bat_side": "", "condition": "All", "rolling": "3"},
    ]

    total_rows = 0
    pull_ids: list[int] = []

    for combo in combos:
        dataframe, csv_text, request_url = await savant_client.get_park_factors(
            season=season,
            stat=combo["stat"],
            factor_type=combo["factor_type"],
            bat_side=combo.get("bat_side", ""),
            condition=combo.get("condition", "All"),
            rolling=combo.get("rolling", ""),
            parks=combo.get("parks", "mlb"),
        )
        normalized = _normalize_dataframe(dataframe)
        combo_slug = "_".join(
            filter(
                None,
                [
                    str(combo["stat"]),
                    str(combo["factor_type"]),
                    str(combo.get("bat_side") or "both"),
                    str(combo.get("condition") or "all"),
                    str(combo.get("rolling") or "1"),
                ],
            )
        )
        raw_csv = write_text_payload("savant", "park_factors", f"season_{season}_{combo_slug}", csv_text)
        source_pull = await _create_source_pull(
            db,
            source="savant",
            resource_type="park_factors",
            request_url=request_url,
            request_params=combo,
            local_path=raw_csv.relative_path,
            response_format="csv",
            season=season,
            row_count=len(normalized.index),
            fetched_at=raw_csv.fetched_at,
        )
        pull_ids.append(source_pull.id)

        normalized_stat_key = _normalize_column(combo["stat"])
        rows = []
        for record in normalized.to_dict(orient="records"):
            venue_name = _safe_text(_candidate(record, "venue_name", "venue", "name", "park_name"))
            stat_value = _safe_float(record.get(normalized_stat_key))
            if not venue_name or stat_value is None:
                continue
            rows.append(
                {
                    "season": season,
                    "venue_id": _safe_int(record.get("venue_id")) or venue_name_lookup.get(venue_name.strip().lower()),
                    "venue_name": venue_name,
                    "factor_type": combo["factor_type"],
                    "stat_key": combo["stat"],
                    "stat_value": stat_value,
                    "bat_side": _safe_text(combo.get("bat_side")) or _safe_text(
                        _candidate(record, "key_bat_side", "bat_side")
                    ),
                    "condition": _safe_text(combo.get("condition")) or _safe_text(
                        _candidate(record, "grouping_venue_conditions", "condition")
                    ),
                    "rolling_value": _safe_text(combo.get("rolling")) or _safe_text(
                        _candidate(record, "key_num_years_rolling", "year_range")
                    ),
                    "tracking": _safe_text(_candidate(record, "tracking", "parks")),
                    "speed_bucket": _safe_text(_candidate(record, "speed", "speed_bucket")),
                    "angle_bucket": _safe_text(_candidate(record, "angle", "angle_bucket")),
                    "temperature_factor": _safe_float(_candidate(record, "temperature", "temperature_factor")),
                    "elevation_factor": _safe_float(_candidate(record, "elevation", "elevation_factor")),
                    "roof_factor": _safe_float(_candidate(record, "roof", "roof_factor")),
                    "environment_factor": _safe_float(_candidate(record, "environment", "environment_factor")),
                    "source_pull_id": source_pull.id,
                }
            )

        total_rows += await _upsert_rows(
            db,
            MlbParkFactor,
            rows,
            constraint="uq_mlb_park_factor_lookup",
        )

    await db.commit()
    return {
        "status": "success",
        "season": season,
        "source_pull_ids": pull_ids,
        "rows_upserted": total_rows,
        "combination_count": len(combos),
    }


async def bootstrap_mlb_ingestion(
    db: AsyncSession,
    *,
    season: int,
    start_date: str,
    end_date: str,
    final_only: bool = False,
    include_savant: bool = True,
) -> dict[str, Any]:
    stats_client = MlbStatsApiClient()
    savant_client = BaseballSavantClient()

    teams_result = await ingest_teams(db, season=season, client=stats_client)
    games_result = await ingest_game_feeds(
        db,
        season=season,
        start_date=start_date,
        end_date=end_date,
        final_only=final_only,
        client=stats_client,
    )

    savant_results = None
    if include_savant:
        savant_results = {
            "statcast_batters": await ingest_savant_statcast_batters(db, season=season, client=savant_client),
            "statcast_pitchers": await ingest_savant_statcast_pitchers(db, season=season, client=savant_client),
            "bat_tracking": await ingest_savant_bat_tracking(db, season=season, client=savant_client),
            "swing_path": await ingest_savant_swing_path(db, season=season, client=savant_client),
            "park_factors": await ingest_savant_park_factors(db, season=season, client=savant_client),
        }

    return {
        "status": "success",
        "season": season,
        "window": {"start_date": start_date, "end_date": end_date},
        "teams": teams_result,
        "schedule": games_result.get("schedule"),
        "games": games_result,
        "savant": savant_results,
    }
