from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from app.services.cache import cached

JEDIBETS_URL = "https://jedibets.com/nba-first-basket-stats"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _to_int(text: str | None) -> int | None:
    if not text:
        return None
    m = re.search(r"-?\d+", text.replace(",", ""))
    return int(m.group(0)) if m else None


def _to_pct(text: str | None) -> float | None:
    if not text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    return float(m.group(1)) / 100.0 if m else None


class JediBetsFirstBasketClient:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def _parse_player_table(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        tbody = soup.select_one("#player-tbody")
        if not tbody:
            return []

        rows = []
        for tr in tbody.select("tr"):
            name_el = tr.select_one(".player-name")
            team_el = tr.select_one(".team-abbr")
            count_el = tr.select_one(".count")
            if not (name_el and team_el and count_el):
                continue

            shot_breakdown: dict[str, int] = {}
            for badge in tr.select(".shot-type-breakdown .shot-badge"):
                txt = badge.get_text(" ", strip=True)
                if ":" not in txt:
                    continue
                k, v = txt.split(":", 1)
                ival = _to_int(v)
                if ival is not None:
                    shot_breakdown[k.strip().lower()] = ival

            rows.append(
                {
                    "player": name_el.get_text(strip=True),
                    "team": team_el.get_text(strip=True),
                    "first_baskets": _to_int(count_el.get_text(strip=True)),
                    "shot_breakdown": shot_breakdown,
                }
            )
        return rows

    def _parse_team_table(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        tbody = soup.select_one("#team-tbody")
        if not tbody:
            return []

        rows = []
        for tr in tbody.select("tr"):
            tds = tr.select("td")
            if len(tds) < 7:
                continue
            rows.append(
                {
                    "team": tds[0].get_text(strip=True),
                    "games": _to_int(tds[1].get_text(strip=True)),
                    "tip_pct": _to_pct(tds[2].get_text(strip=True)),
                    "first_point_wins": _to_int(tds[3].get_text(strip=True)),
                    "first_point_pct": _to_pct(tds[4].get_text(strip=True)),
                    "first_fg_wins": _to_int(tds[5].get_text(strip=True)),
                    "first_fg_pct": _to_pct(tds[6].get_text(strip=True)),
                }
            )
        return rows

    @cached(ttl_seconds=60 * 10)
    def fetch_stats(self) -> dict[str, Any]:
        res = requests.get(
            JEDIBETS_URL,
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
        )
        res.raise_for_status()

        soup = BeautifulSoup(res.text, "html.parser")
        players = self._parse_player_table(soup)
        teams = self._parse_team_table(soup)

        return {
            "source": "jedibets",
            "url": JEDIBETS_URL,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "players_count": len(players),
            "teams_count": len(teams),
            "players": players,
            "teams": teams,
        }
