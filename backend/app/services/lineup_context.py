from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.services.lineup_resolver import LineupResolver
from app.services.rotowire_lineups_client import RotoWireLineupsClient

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


def _target_et_date_for_day(day: str):
    if ZoneInfo:
        base_date = datetime.now(ZoneInfo("America/New_York")).date()
    else:
        base_date = datetime.now().date()
    if day == "today":
        return base_date
    if day == "tomorrow":
        return base_date + timedelta(days=1)
    if day == "yesterday":
        return base_date - timedelta(days=1)
    if day == "two_days_ago":
        return base_date - timedelta(days=2)
    return base_date


def _rotowire_day_for_prediction_day(day: str) -> str | None:
    if ZoneInfo:
        base_date = datetime.now(ZoneInfo("America/New_York")).date()
    else:
        base_date = datetime.now().date()

    if day == "auto":
        if ZoneInfo:
            aus_hour = datetime.now(ZoneInfo("Australia/Sydney")).hour
            target_date = base_date if aus_hour >= 17 else base_date - timedelta(days=1)
        else:
            target_date = base_date
    elif day in {"today", "tomorrow", "yesterday", "two_days_ago"}:
        target_date = _target_et_date_for_day(day)
    else:
        target_date = base_date

    diff_days = (target_date - base_date).days
    if diff_days <= -1:
        return "yesterday"
    if diff_days == 0:
        return "today"
    if diff_days == 1:
        return "tomorrow"
    return None


def attach_schedule_metadata(engine: Engine, lineups_payload: dict[str, Any]):
    games = lineups_payload.get("games", [])
    if not games:
        return lineups_payload

    pair_keys = {
        f"{(g.get('away_team_abbr') or '').upper()}@{(g.get('home_team_abbr') or '').upper()}"
        for g in games
        if g.get("away_team_abbr") and g.get("home_team_abbr")
    }
    if not pair_keys:
        return lineups_payload

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT game_id, game_date, matchup, home_team_id, away_team_id,
                       home_team_abbr, away_team_abbr
                FROM game_schedule
                WHERE game_date >= (CURRENT_DATE - INTERVAL '7 day')
                  AND game_date <= (CURRENT_DATE + INTERVAL '7 day')
                ORDER BY game_date DESC
                """
            ),
            {},
        ).fetchall()

    by_pair = {}
    for r in rows:
        pair = f"{(r[6] or '').upper()}@{(r[5] or '').upper()}"
        if pair in pair_keys and pair not in by_pair:
            by_pair[pair] = r

    for game in games:
        key = f"{(game.get('away_team_abbr') or '').upper()}@{(game.get('home_team_abbr') or '').upper()}"
        row = by_pair.get(key)
        if not row:
            continue
        game["game_id"] = row[0]
        game["game_date"] = row[1]
        if game.get("home_team_id") is None and row[3] is not None:
            game["home_team_id"] = int(row[3])
        if game.get("away_team_id") is None and row[4] is not None:
            game["away_team_id"] = int(row[4])
    return lineups_payload


def _infer_team_starters(engine: Engine, team_abbr: str, target_date, top_n: int = 5):
    df = pd.read_sql(
        text(
            """
            SELECT p.id AS player_id, p.full_name, p.team_abbreviation,
                   pg.minutes, pg.game_date
            FROM players p
            JOIN player_game_stats pg ON pg.player_id = p.id
            WHERE p.team_abbreviation = :team_abbr
              AND pg.game_date < :target_date
              AND pg.game_date >= (:target_date - INTERVAL '40 day')
              AND pg.minutes IS NOT NULL
            ORDER BY pg.game_date DESC
            """
        ),
        engine,
        params={"team_abbr": team_abbr, "target_date": target_date},
    )
    if df.empty:
        return []
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0.0)
    df = df.sort_values(["player_id", "game_date"], ascending=[True, False])
    recent = df.groupby("player_id").head(5)
    rank = (
        recent.groupby(["player_id", "full_name", "team_abbreviation"], as_index=False)[
            "minutes"
        ]
        .mean()
        .sort_values("minutes", ascending=False)
        .head(top_n)
    )
    rows = []
    for _, r in rank.iterrows():
        rows.append(
            {
                "name": r["full_name"],
                "resolved_full_name": r["full_name"],
                "position": None,
                "injury_tag": None,
                "play_pct": 70,
                "resolved_player_id": int(r["player_id"]),
                "team_id": None,
                "name_match_type": "inferred",
                "name_match_score": 0.0,
            }
        )
    return rows


def build_inferred_lineups_from_schedule(engine: Engine, day: str):
    target_date = _target_et_date_for_day(day)
    df_schedule = pd.read_sql(
        text(
            """
            SELECT game_id, game_date, matchup, home_team_id, away_team_id,
                   home_team_abbr, away_team_abbr
            FROM game_schedule
            WHERE game_date = :target_date
            ORDER BY game_id
            """
        ),
        engine,
        params={"target_date": target_date},
    )
    if df_schedule.empty:
        return {
            "source": "schedule_inferred",
            "games_count": 0,
            "games": [],
        }

    games = []
    for _, game in df_schedule.iterrows():
        away_abbr = game["away_team_abbr"]
        home_abbr = game["home_team_abbr"]
        away_starters = _infer_team_starters(engine, away_abbr, target_date)
        home_starters = _infer_team_starters(engine, home_abbr, target_date)
        for s in away_starters:
            s["team_id"] = (
                int(game["away_team_id"]) if pd.notnull(game["away_team_id"]) else None
            )
        for s in home_starters:
            s["team_id"] = (
                int(game["home_team_id"]) if pd.notnull(game["home_team_id"]) else None
            )
        games.append(
            {
                "game_id": game["game_id"],
                "game_date": game["game_date"],
                "matchup": game["matchup"],
                "tipoff_et": None,
                "away_team_abbr": away_abbr,
                "home_team_abbr": home_abbr,
                "away_team_id": int(game["away_team_id"])
                if pd.notnull(game["away_team_id"])
                else None,
                "home_team_id": int(game["home_team_id"])
                if pd.notnull(game["home_team_id"])
                else None,
                "away": {
                    "status": "inferred",
                    "status_text": "Inferred from recent starters/minutes",
                    "team_id": int(game["away_team_id"])
                    if pd.notnull(game["away_team_id"])
                    else None,
                    "starters": away_starters,
                    "all_listed_players": away_starters,
                    "resolved_starters": len(
                        [s for s in away_starters if s.get("resolved_player_id")]
                    ),
                    "all_starters_resolved": len(away_starters) >= 5,
                },
                "home": {
                    "status": "inferred",
                    "status_text": "Inferred from recent starters/minutes",
                    "team_id": int(game["home_team_id"])
                    if pd.notnull(game["home_team_id"])
                    else None,
                    "starters": home_starters,
                    "all_listed_players": home_starters,
                    "resolved_starters": len(
                        [s for s in home_starters if s.get("resolved_player_id")]
                    ),
                    "all_starters_resolved": len(home_starters) >= 5,
                },
                "starter_count_ok": len(away_starters) >= 5 and len(home_starters) >= 5,
            }
        )

    total_starters = sum(
        len(g["away"]["starters"]) + len(g["home"]["starters"]) for g in games
    )
    resolved = sum(
        g["away"]["resolved_starters"] + g["home"]["resolved_starters"] for g in games
    )
    return {
        "source": "schedule_inferred",
        "games_count": len(games),
        "games": games,
        "resolution": {
            "resolved_starters": resolved,
            "total_starters": total_starters,
            "resolution_rate": round((resolved / total_starters), 4)
            if total_starters
            else 0.0,
        },
    }


def fetch_lineups_payload(engine: Engine, day: str) -> dict[str, Any]:
    rotowire_day = _rotowire_day_for_prediction_day(day)
    rotowire_client = RotoWireLineupsClient(timeout=20)
    resolver = LineupResolver(engine)

    payload: dict[str, Any] = {}
    try:
        raw_lineups = rotowire_client.fetch_lineups(day=rotowire_day)
        payload = resolver.enrich_rotowire_payload(raw_lineups)
        payload = attach_schedule_metadata(engine, payload)
    except Exception:
        payload = {}

    if not payload or (payload.get("games_count") or 0) == 0:
        payload = build_inferred_lineups_from_schedule(engine, day)
    return payload


def build_expected_lineup_sets(
    lineups_payload: dict[str, Any],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    expected: dict[str, set[int]] = {}
    excluded: dict[str, set[int]] = {}
    for game in lineups_payload.get("games", []) or []:
        for team_key, abbr_key in [
            ("away", "away_team_abbr"),
            ("home", "home_team_abbr"),
        ]:
            team = game.get(team_key, {}) or {}
            team_abbr = (game.get(abbr_key) or "").upper()
            if not team_abbr:
                continue

            starters = team.get("starters") or []
            may_not = team.get("may_not_play") or []

            def _is_excluded(player: dict[str, Any]) -> bool:
                tag = (player.get("injury_tag") or "").lower()
                play_pct = player.get("play_pct")
                if tag in {"out", "ofs"}:
                    return True
                if play_pct is not None:
                    try:
                        return float(play_pct) <= 0
                    except (TypeError, ValueError):
                        return False
                return False

            for player in starters:
                pid = player.get("resolved_player_id")
                if pid is None:
                    continue
                try:
                    pid = int(pid)
                except (TypeError, ValueError):
                    continue
                if _is_excluded(player):
                    excluded.setdefault(team_abbr, set()).add(pid)
                    continue
                expected.setdefault(team_abbr, set()).add(pid)

            for player in may_not:
                pid = player.get("resolved_player_id")
                if pid is None:
                    continue
                try:
                    pid = int(pid)
                except (TypeError, ValueError):
                    continue
                if _is_excluded(player):
                    excluded.setdefault(team_abbr, set()).add(pid)

    return expected, excluded
