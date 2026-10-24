"""NBA season naming and calendar helpers.

NBA API seasons are identified by the league-year pair (for example
``2026-27``).  The league year turns over on July 1 in US Eastern time, which
also covers preseason, the regular season, and the following playoffs without
depending on the local timezone of the machine running Gamblr.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

NBA_TIMEZONE = ZoneInfo("America/New_York")
_SEASON_RE = re.compile(r"^(?P<start>20\d{2})-(?P<end>\d{2})$")


def season_for_date(value: date) -> str:
    start_year = value.year if value.month >= 7 else value.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def current_nba_season(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now(NBA_TIMEZONE)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=NBA_TIMEZONE)
    else:
        now = now.astimezone(NBA_TIMEZONE)
    return season_for_date(now.date())


def validate_nba_season(season: str) -> str:
    value = str(season or "").strip()
    match = _SEASON_RE.fullmatch(value)
    if not match:
        raise ValueError("season must use NBA format YYYY-YY (for example 2026-27)")
    start_year = int(match.group("start"))
    if int(match.group("end")) != (start_year + 1) % 100:
        raise ValueError(f"invalid consecutive NBA season: {value}")
    return value


def resolve_nba_season(season: str | None = None) -> str:
    return validate_nba_season(season) if season else current_nba_season()


def season_start_year(season: str) -> int:
    return int(validate_nba_season(season).split("-", 1)[0])


def season_date_bounds(season: str) -> tuple[date, date]:
    start_year = season_start_year(season)
    return date(start_year, 7, 1), date(start_year + 1, 6, 30)


def date_is_in_season(value: date, season: str) -> bool:
    start, end = season_date_bounds(season)
    return start <= value <= end


def available_nba_seasons(count: int = 8, current: str | None = None) -> list[str]:
    start_year = season_start_year(current or current_nba_season())
    return [f"{year}-{(year + 1) % 100:02d}" for year in range(start_year, start_year - count, -1)]
