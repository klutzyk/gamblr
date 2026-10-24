from types import SimpleNamespace

import pandas as pd

from app.db.nba.store_schedule import classify_season_type
from app.services.nba_client import NBAClient


def test_schedule_phase_normalization():
    assert classify_season_type({"gameId": "0012600001", "gameLabel": "Preseason"}) == "Preseason"
    assert classify_season_type({"gameId": "0022600001", "gameLabel": ""}) == "Regular Season"
    assert classify_season_type({"gameId": "0022600100", "gameLabel": "Emirates NBA Cup"}) == "Regular Season (NBA Cup)"
    assert classify_season_type({"gameId": "0042600001", "gameLabel": ""}) == "Playoffs"
    assert classify_season_type({"gameId": "0052600001", "gameLabel": ""}) == "Play-In"


def test_v3_boxscore_uses_team_totals_not_starter_bench(monkeypatch):
    players = pd.DataFrame([{"personId": 1, "firstName": "Test", "familyName": "Player", "teamTricode": "BOS", "minutes": "10:00", "points": 3}])
    split = pd.DataFrame([{"teamId": 1, "teamTricode": "BOS", "points": 50}, {"teamId": 1, "teamTricode": "BOS", "points": 60}, {"teamId": 2, "teamTricode": "NYK", "points": 40}, {"teamId": 2, "teamTricode": "NYK", "points": 55}])
    totals = pd.DataFrame([{"teamId": 1, "teamTricode": "BOS", "points": 110}, {"teamId": 2, "teamTricode": "NYK", "points": 95}])

    class FakeBox:
        def __init__(self, **_kwargs):
            pass

        def get_data_frames(self):
            return [players, split, totals]

    import app.services.nba_client as module
    monkeypatch.setattr(module, "boxscoretraditionalv3", SimpleNamespace(BoxScoreTraditionalV3=FakeBox))
    _, teams = NBAClient(max_attempts=1).fetch_game_boxscore("22600001")
    assert len(teams) == 2
    assert sorted(teams["PTS"].tolist()) == [95, 110]
