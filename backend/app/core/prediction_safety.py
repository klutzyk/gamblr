"""Pure prediction eligibility and uncertainty rules."""

from __future__ import annotations

from datetime import datetime, timedelta


def confidence_cap_for_sample(sample_size: int, team_changed: bool = False) -> int:
    games = max(0, int(sample_size or 0))
    if games < 1:
        cap = 42
    elif games < 3:
        cap = 48
    elif games < 8:
        cap = 58
    elif games < 15:
        cap = 68
    else:
        cap = 99
    return min(cap, 55) if team_changed else cap


def betting_input_is_fresh(
    commence_time: datetime,
    odds_updated_at: datetime,
    now: datetime,
    max_odds_age_minutes: int = 30,
) -> bool:
    """Reject started games, future-dated odds, and stale market snapshots."""
    if commence_time <= now or odds_updated_at > now + timedelta(minutes=2):
        return False
    return odds_updated_at >= now - timedelta(minutes=max(1, max_odds_age_minutes))
