from __future__ import annotations

from datetime import date
from typing import Any
import logging
import time
import re
import requests

import pandas as pd
from nba_api.stats.endpoints import playbyplayv3
from sqlalchemy import text
from requests import exceptions as req_exc

from app.db.store_first_basket import upsert_first_basket_labels
from app.services.nba_headers import CUSTOM_HEADERS

logger = logging.getLogger(__name__)
NBA_CDN_PBP_URL = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"


def _clock_to_elapsed_seconds(clock: str | None) -> float | None:
    if not clock:
        return None
    try:
        # PlayByPlayV3 format: PT11M43.00S
        if clock.startswith("PT"):
            m = re.search(r"PT(\d+)M(\d+(?:\.\d+)?)S", clock)
            if not m:
                return None
            minutes = int(m.group(1))
            seconds = float(m.group(2))
            remaining = minutes * 60 + seconds
            return float(720 - remaining)
        if ":" in clock:
            minutes, seconds = clock.split(":")
            remaining = int(minutes) * 60 + float(seconds)
            return float(720 - remaining)
        return None
    except Exception:
        return None


def _infer_starters_from_pbp(
    pbp_df: pd.DataFrame,
    home_team_id: int | None,
    away_team_id: int | None,
) -> tuple[list[int], list[int]]:
    home: list[int] = []
    away: list[int] = []
    if pbp_df.empty:
        return home, away

    for _, row in pbp_df.iterrows():
        team_id = int(row["teamId"]) if pd.notnull(row.get("teamId")) else None
        pid = int(row["personId"]) if pd.notnull(row.get("personId")) else None
        if not team_id or not pid:
            continue
        if home_team_id and team_id == home_team_id and pid not in home and len(home) < 5:
            home.append(pid)
        elif away_team_id and team_id == away_team_id and pid not in away and len(away) < 5:
            away.append(pid)
        if len(home) >= 5 and len(away) >= 5:
            break
    return home, away


def _extract_game_label(
    game_id: str,
    expected_home_team_id: int | None = None,
    expected_away_team_id: int | None = None,
    expected_home_team_abbr: str | None = None,
    expected_away_team_abbr: str | None = None,
    expected_home_starters: list[int] | None = None,
    expected_away_starters: list[int] | None = None,
    timeout: int = 20,
) -> dict[str, Any] | None:
    def call_with_retry(factory, label: str, retries: int = 3):
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                return factory()
            except (req_exc.ReadTimeout, req_exc.ConnectTimeout, req_exc.ConnectionError) as exc:
                last_err = exc
                if attempt == retries:
                    raise
                wait_s = attempt * 1.5
                logger.info(
                    "[first-basket-labels] retry %s/%s game_id=%s endpoint=%s err=%s wait=%.1fs",
                    attempt,
                    retries,
                    game_id,
                    label,
                    exc.__class__.__name__,
                    wait_s,
                )
                time.sleep(wait_s)
        if last_err:
            raise last_err
        return None

    def fetch_pbp_df_from_cdn() -> pd.DataFrame:
        url = NBA_CDN_PBP_URL.format(game_id=game_id)
        # Try direct first (ignore system proxy), then fallback to env proxy.
        session = requests.Session()
        session.trust_env = False
        try:
            res = session.get(
                url,
                headers=CUSTOM_HEADERS,
                timeout=timeout,
            )
        except Exception:
            res = requests.get(
                url,
                headers=CUSTOM_HEADERS,
                timeout=timeout,
            )
        res.raise_for_status()
        payload = res.json()
        actions = payload.get("game", {}).get("actions", [])
        if not actions:
            return pd.DataFrame()
        df = pd.DataFrame(actions)
        if "period" in df.columns:
            df = df[pd.to_numeric(df["period"], errors="coerce") == 1]
        return df

    def fetch_pbp_df_from_nba_api() -> pd.DataFrame:
        pbp = playbyplayv3.PlayByPlayV3(
            game_id=game_id,
            start_period=1,
            end_period=1,
            timeout=timeout,
            headers=CUSTOM_HEADERS,
        )
        return pbp.get_data_frames()[0]

    pbp_df = call_with_retry(fetch_pbp_df_from_cdn, "cdn_playbyplay_json")
    if pbp_df is None or pbp_df.empty:
        pbp_df = call_with_retry(fetch_pbp_df_from_nba_api, "playbyplayv3")
    if pbp_df.empty:
        return None
    pbp_df = pbp_df.sort_values("actionNumber")

    home_team_id = int(expected_home_team_id) if expected_home_team_id else None
    away_team_id = int(expected_away_team_id) if expected_away_team_id else None
    team_abbr_map = {}
    if home_team_id and expected_home_team_abbr:
        team_abbr_map[home_team_id] = expected_home_team_abbr
    if away_team_id and expected_away_team_abbr:
        team_abbr_map[away_team_id] = expected_away_team_abbr

    home_starters = expected_home_starters or []
    away_starters = expected_away_starters or []
    if len(home_starters) < 5 or len(away_starters) < 5:
        inf_home, inf_away = _infer_starters_from_pbp(pbp_df, home_team_id, away_team_id)
        if len(home_starters) < 5:
            home_starters = inf_home
        if len(away_starters) < 5:
            away_starters = inf_away

    first_score = pbp_df[
        (pd.to_numeric(pbp_df.get("isFieldGoal"), errors="coerce").fillna(0) == 1)
        & (pbp_df.get("shotResult").fillna("").astype(str).str.lower() == "made")
    ].head(1)
    if first_score.empty:
        return None
    first_row = first_score.iloc[0]

    jump_ball = pbp_df[pbp_df.get("actionType").fillna("").astype(str).str.lower() == "jump ball"].head(1)
    jump_row = jump_ball.iloc[0] if not jump_ball.empty else None

    scorer_team_id = int(first_row["teamId"]) if pd.notnull(first_row.get("teamId")) else None
    scorer_id = int(first_row["personId"]) if pd.notnull(first_row.get("personId")) else None
    desc = first_row.get("description")

    result = {
        "game_id": game_id,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_team_abbr": expected_home_team_abbr or team_abbr_map.get(home_team_id),
        "away_team_abbr": expected_away_team_abbr or team_abbr_map.get(away_team_id),
        "first_scoring_team_id": scorer_team_id,
        "first_scoring_team_abbr": team_abbr_map.get(scorer_team_id),
        "first_scorer_player_id": scorer_id,
        "first_scorer_name": first_row.get("playerNameI") or first_row.get("playerName"),
        "first_score_event_num": int(first_row["actionNumber"]) if pd.notnull(first_row.get("actionNumber")) else None,
        "first_score_seconds": _clock_to_elapsed_seconds(first_row.get("clock")),
        "first_score_action_type": first_row.get("actionType") or "Made Shot",
        "first_score_description": desc,
        "winning_jump_ball_team_id": (
            int(jump_row["teamId"])
            if jump_row is not None and pd.notnull(jump_row.get("teamId"))
            else None
        ),
        "winning_jump_ball_team_abbr": (
            team_abbr_map.get(int(jump_row["teamId"]))
            if jump_row is not None and pd.notnull(jump_row.get("teamId"))
            else None
        ),
        "jump_ball_home_player_id": None,
        "jump_ball_away_player_id": None,
        "jump_ball_winner_player_id": (
            int(jump_row["personId"])
            if jump_row is not None and pd.notnull(jump_row.get("personId"))
            else None
        ),
        "home_starter_ids": home_starters,
        "away_starter_ids": away_starters,
        "is_valid_label": True,
        "source": "nba_api",
    }
    return result


def build_first_basket_labels(
    engine,
    season: str | None = None,
    max_games: int | None = None,
    overwrite: bool = False,
    timeout: int = 20,
):
    sql = """
    SELECT game_id, game_date, season, home_team_id, away_team_id, home_team_abbr, away_team_abbr
    FROM game_schedule
    WHERE game_date < :today
    """
    params = {"today": date.today()}
    if season:
        sql += " AND season = :season"
        params["season"] = season
    if not overwrite:
        sql += " AND game_id NOT IN (SELECT game_id FROM first_basket_labels)"
    sql += " ORDER BY game_date DESC"
    if max_games:
        sql += " LIMIT :max_games"
        params["max_games"] = max_games

    df_games = pd.read_sql(text(sql), engine, params=params)
    if df_games.empty:
        logger.info("[first-basket-labels] no games to process")
        return {"inserted": 0, "attempted": 0, "errors": 0}

    logger.info(
        "[first-basket-labels] start season=%s max_games=%s overwrite=%s timeout=%ss rows=%s",
        season,
        max_games,
        overwrite,
        timeout,
        len(df_games),
    )

    rows = []
    errors = 0
    skipped_no_label = 0
    error_kinds: dict[str, int] = {}
    for idx, (_, game) in enumerate(df_games.iterrows(), start=1):
        game_id = str(game["game_id"])
        try:
            row = _extract_game_label(
                game_id,
                expected_home_team_id=(
                    int(game["home_team_id"]) if pd.notnull(game.get("home_team_id")) else None
                ),
                expected_away_team_id=(
                    int(game["away_team_id"]) if pd.notnull(game.get("away_team_id")) else None
                ),
                expected_home_team_abbr=game.get("home_team_abbr"),
                expected_away_team_abbr=game.get("away_team_abbr"),
                timeout=timeout,
            )
            if row is None:
                skipped_no_label += 1
                continue
            row["game_date"] = pd.to_datetime(game["game_date"]).date()
            row["season"] = game.get("season")
            rows.append(row)
            if idx % 10 == 0:
                logger.info(
                    "[first-basket-labels] processed=%s/%s success=%s errors=%s last_game_id=%s",
                    idx,
                    len(df_games),
                    len(rows),
                    errors,
                    game_id,
                )
            time.sleep(0.2)
        except Exception:
            errors += 1
            kind = "unknown"
            try:
                import traceback
                kind = traceback.format_exc().splitlines()[-1][:160]
            except Exception:
                pass
            error_kinds[kind] = error_kinds.get(kind, 0) + 1
            if idx <= 3:
                logger.exception("[first-basket-labels] failed game_id=%s", game_id)
            if idx % 10 == 0:
                logger.info(
                    "[first-basket-labels] processed=%s/%s success=%s errors=%s last_game_id=%s",
                    idx,
                    len(df_games),
                    len(rows),
                    errors,
                    game_id,
                )
            time.sleep(0.2)
            continue

    inserted = upsert_first_basket_labels(engine, rows)
    logger.info(
        "[first-basket-labels] done attempted=%s inserted=%s errors=%s skipped_no_label=%s error_kinds=%s",
        len(df_games),
        inserted,
        errors,
        skipped_no_label,
        error_kinds,
    )
    return {
        "inserted": inserted,
        "attempted": int(len(df_games)),
        "errors": errors,
        "skipped_no_label": skipped_no_label,
        "error_kinds": error_kinds,
    }
