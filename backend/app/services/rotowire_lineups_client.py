import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from app.services.cache import cached

ROTOWIRE_LINEUPS_URL = "https://www.rotowire.com/basketball/nba-lineups.php"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
PCT_CLASS_RE = re.compile(r"is-pct-play-(\d+)")


def _extract_play_pct(classes: list[str] | None) -> int | None:
    if not classes:
        return None
    for cls in classes:
        match = PCT_CLASS_RE.match(cls)
        if match:
            return int(match.group(1))
    return None


def _extract_rotowire_player_id(href: str | None) -> int | None:
    if not href:
        return None
    slug = href.rstrip("/").split("-")[-1]
    return int(slug) if slug.isdigit() else None


class RotoWireLineupsClient:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    def _parse_team_list(self, team_list) -> dict[str, Any]:
        status_el = team_list.select_one("li.lineup__status")
        status_text = status_el.get_text(" ", strip=True) if status_el else None
        status = "confirmed" if status_text and "confirmed" in status_text.lower() else "expected"

        starters = []
        may_not_play = []
        all_players = []
        section = "starters"

        for li in team_list.find_all("li", recursive=False):
            classes = li.get("class", [])
            if "lineup__title" in classes and "may not play" in li.get_text(" ", strip=True).lower():
                section = "may_not_play"
                continue

            if "lineup__player" not in classes:
                continue

            link_el = li.select_one("a[href*='/basketball/player/']")
            if link_el is None:
                continue

            name = link_el.get("title") or link_el.get_text(strip=True)
            pos_el = li.select_one(".lineup__pos")
            injury_el = li.select_one(".lineup__inj")
            href = link_el.get("href")
            player = {
                "name": name,
                "position": pos_el.get_text(strip=True) if pos_el else None,
                "injury_tag": injury_el.get_text(strip=True) if injury_el else None,
                "play_pct": _extract_play_pct(li.get("class", [])),
                "rotowire_player_id": _extract_rotowire_player_id(href),
                "rotowire_href": href,
            }

            all_players.append(player)
            if section == "may_not_play":
                may_not_play.append(player)
            elif len(starters) < 5:
                starters.append(player)

        return {
            "status": status,
            "status_text": status_text,
            "starters": starters,
            "may_not_play": may_not_play,
            "all_listed_players": all_players,
        }

    def _parse(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        games = []

        # data-lnum isolates actual game cards and skips promo/tool cards.
        for card in soup.select("div.lineup.is-nba[data-lnum]"):
            tipoff_el = card.select_one(".lineup__time")
            away_abbr_el = card.select_one(".lineup__team.is-visit .lineup__abbr")
            home_abbr_el = card.select_one(".lineup__team.is-home .lineup__abbr")
            away_list = card.select_one("ul.lineup__list.is-visit")
            home_list = card.select_one("ul.lineup__list.is-home")

            if not (away_abbr_el and home_abbr_el and away_list and home_list):
                continue

            away = self._parse_team_list(away_list)
            home = self._parse_team_list(home_list)

            games.append(
                {
                    "tipoff_et": tipoff_el.get_text(strip=True) if tipoff_el else None,
                    "matchup": f"{away_abbr_el.get_text(strip=True)} @ {home_abbr_el.get_text(strip=True)}",
                    "away_team_abbr": away_abbr_el.get_text(strip=True),
                    "home_team_abbr": home_abbr_el.get_text(strip=True),
                    "away": away,
                    "home": home,
                    "starter_count_ok": len(away["starters"]) == 5 and len(home["starters"]) == 5,
                }
            )

        return games

    @cached(ttl_seconds=60 * 2)
    def fetch_lineups(self, day: str | None = None) -> dict[str, Any]:
        url = ROTOWIRE_LINEUPS_URL
        if day in {"today", "tomorrow", "yesterday"}:
            url = f"{ROTOWIRE_LINEUPS_URL}?{urlencode({'date': day})}"

        response = requests.get(
            url,
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
        )
        response.raise_for_status()
        games = self._parse(response.text)
        return {
            "source": "rotowire",
            "url": url,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "games_count": len(games),
            "games": games,
        }
