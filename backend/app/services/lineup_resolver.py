import re
from copy import deepcopy
from difflib import SequenceMatcher
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from app.services.cache import cached

NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _normalize_name(name: str | None) -> str:
    if not name:
        return ""
    return NON_ALNUM_RE.sub("", name.lower())


def _alias_key(name: str | None) -> str:
    if not name:
        return ""
    cleaned = re.sub(r"[^A-Za-z\s'\-]", " ", name).strip().lower()
    parts = [p for p in re.split(r"[\s\-]+", cleaned) if p]
    if len(parts) < 2:
        return ""
    first = parts[0][0]
    last = parts[-1].replace("'", "")
    return f"{first}{last}"


class LineupResolver:
    def __init__(self, engine: Engine):
        self.engine = engine

    @cached(ttl_seconds=60 * 5)
    def _load_players_by_team(self) -> dict[str, list[dict[str, Any]]]:
        df = pd.read_sql(
            """
            SELECT id AS player_id, full_name, team_abbreviation
            FROM players
            WHERE full_name IS NOT NULL
              AND team_abbreviation IS NOT NULL
            """,
            self.engine,
        )

        if df.empty:
            return {}

        df["team_abbreviation"] = df["team_abbreviation"].astype(str).str.upper()
        df["norm_name"] = df["full_name"].map(_normalize_name)
        df["alias_key"] = df["full_name"].map(_alias_key)

        grouped: dict[str, list[dict[str, Any]]] = {}
        for team, group in df.groupby("team_abbreviation"):
            grouped[team] = group.to_dict(orient="records")
        return grouped

    @cached(ttl_seconds=60 * 30)
    def _load_team_id_by_abbr(self) -> dict[str, int]:
        df = pd.read_sql(
            """
            SELECT id, abbreviation
            FROM teams
            WHERE abbreviation IS NOT NULL
            """,
            self.engine,
        )
        if df.empty:
            return {}
        return {
            str(r["abbreviation"]).upper(): int(r["id"])
            for _, r in df.iterrows()
            if pd.notnull(r["abbreviation"]) and pd.notnull(r["id"])
        }

    def _resolve_one(self, name: str, team_abbr: str) -> dict[str, Any]:
        teams = self._load_players_by_team()
        candidates = teams.get(team_abbr.upper(), [])
        if not candidates:
            return {
                "resolved_player_id": None,
                "resolved_full_name": None,
                "name_match_type": "unmatched",
                "name_match_score": 0.0,
            }

        norm = _normalize_name(name)
        alias = _alias_key(name)

        exact = [c for c in candidates if c["norm_name"] == norm]
        if len(exact) == 1:
            c = exact[0]
            return {
                "resolved_player_id": int(c["player_id"]),
                "resolved_full_name": c["full_name"],
                "name_match_type": "exact",
                "name_match_score": 1.0,
            }

        alias_hits = [c for c in candidates if c["alias_key"] and c["alias_key"] == alias]
        if len(alias_hits) == 1:
            c = alias_hits[0]
            return {
                "resolved_player_id": int(c["player_id"]),
                "resolved_full_name": c["full_name"],
                "name_match_type": "alias",
                "name_match_score": 0.95,
            }

        scored = []
        for c in candidates:
            score = SequenceMatcher(None, norm, c["norm_name"]).ratio()
            scored.append((score, c))

        if not scored:
            return {
                "resolved_player_id": None,
                "resolved_full_name": None,
                "name_match_type": "unmatched",
                "name_match_score": 0.0,
            }

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_candidate = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0

        if best_score >= 0.88 and (best_score - second_score >= 0.03):
            return {
                "resolved_player_id": int(best_candidate["player_id"]),
                "resolved_full_name": best_candidate["full_name"],
                "name_match_type": "fuzzy",
                "name_match_score": round(float(best_score), 3),
            }

        return {
            "resolved_player_id": None,
            "resolved_full_name": None,
            "name_match_type": "unmatched",
            "name_match_score": round(float(best_score), 3),
        }

    def enrich_rotowire_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = deepcopy(payload)
        total = 0
        resolved = 0
        team_map = self._load_team_id_by_abbr()

        for game in data.get("games", []):
            game["away_team_id"] = team_map.get((game.get("away_team_abbr") or "").upper())
            game["home_team_id"] = team_map.get((game.get("home_team_abbr") or "").upper())
            for side_key, team_key in [("away_team_abbr", "away"), ("home_team_abbr", "home")]:
                team_abbr = game.get(side_key)
                team = game.get(team_key, {})
                starters = team.get("starters", [])
                team_resolved = 0
                team["team_id"] = team_map.get((team_abbr or "").upper())

                for starter in starters:
                    match = self._resolve_one(starter.get("name") or "", team_abbr or "")
                    starter.update(match)
                    starter["team_id"] = team["team_id"]
                    total += 1
                    if match["resolved_player_id"] is not None:
                        resolved += 1
                        team_resolved += 1

                team["resolved_starters"] = team_resolved
                team["all_starters_resolved"] = team_resolved == 5

        data["resolution"] = {
            "resolved_starters": resolved,
            "total_starters": total,
            "resolution_rate": round((resolved / total), 4) if total else 0.0,
        }
        return data
