from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from app.core.nba_season import (
    current_nba_season,
    date_is_in_season,
    season_for_date,
    validate_nba_season,
)


def test_season_boundary_uses_nba_league_year():
    assert season_for_date(date(2026, 6, 30)) == "2025-26"
    assert season_for_date(date(2026, 7, 1)) == "2026-27"
    assert current_nba_season(datetime(2026, 10, 24, tzinfo=ZoneInfo("Australia/Sydney"))) == "2026-27"


def test_validates_and_bounds_explicit_seasons():
    assert validate_nba_season("2026-27") == "2026-27"
    assert date_is_in_season(date(2027, 6, 30), "2026-27")
    assert not date_is_in_season(date(2027, 7, 1), "2026-27")
    with pytest.raises(ValueError):
        validate_nba_season("2026-28")
