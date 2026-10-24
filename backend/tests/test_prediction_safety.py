from datetime import datetime, timedelta, timezone

from app.core.prediction_safety import betting_input_is_fresh, confidence_cap_for_sample


def test_early_season_and_trade_confidence_caps():
    assert confidence_cap_for_sample(0) == 42
    assert confidence_cap_for_sample(2) == 48
    assert confidence_cap_for_sample(7) == 58
    assert confidence_cap_for_sample(20) == 99
    assert confidence_cap_for_sample(20, team_changed=True) == 55


def test_betting_inputs_reject_started_and_stale_markets():
    now = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
    assert betting_input_is_fresh(now + timedelta(hours=1), now - timedelta(minutes=5), now)
    assert not betting_input_is_fresh(now - timedelta(seconds=1), now - timedelta(minutes=5), now)
    assert not betting_input_is_fresh(now + timedelta(hours=1), now - timedelta(minutes=31), now)
