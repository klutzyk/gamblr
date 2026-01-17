from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    commonplayerinfo,
    playergamelog,
)
from app.services.cache import cached


class NBAClient:
    def __init__(self, timeout=30):
        self.timeout = timeout

    # cache for 30 mins
    @cached(ttl_seconds=60 * 30)
    def fetch_top_players(self, top_n=30, per_mode="PerGame"):
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed=per_mode,
            timeout=self.timeout,
        )
        return stats.get_data_frames()[0].head(top_n)

    # cache for 1 hr
    @cached(ttl_seconds=60 * 60)
    def fetch_player_info(self, player_id: int):
        player_info = commonplayerinfo.CommonPlayerInfo(
            player_id=player_id,
            timeout=self.timeout,
        )
        return (
            player_info.common_player_info.get_data_frame(),
            player_info.available_seasons.get_data_frame(),
        )

    @cached(ttl_seconds=60 * 15)
    def fetch_player_stats(self, **kwargs):
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            timeout=self.timeout,
            **kwargs,
        )
        return stats.get_data_frames()[0]

    @cached(ttl_seconds=60 * 15)
    def fetch_player_game_log(self, player_id: int, season: str = "2025-26"):
        gamelog = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            timeout=self.timeout,
        )
        return gamelog.get_data_frames()[0]
