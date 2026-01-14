import httpx
from app.core.config import settings


class SportsDataClient:
    def __init__(self):
        self.base_url = settings.SPORTSDATA_BASE_URL
        self.api_key = settings.SPORTSDATA_API_KEY

    async def get_player_props_by_game(self, game_id: int):
        url = f"{self.base_url}/odds/json/BettingPlayerPropsByGameID/{game_id}"
        params = {"key": self.api_key}

        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
