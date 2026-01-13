import requests
import pandas as pd
from .nba_params import TRADITIONAL_PARAMS
from nba_api.stats.endpoints import leaguedashplayerstats, commonplayerinfo


class NBAClient:
    """NBA stats client using the nba_api package"""

    # setting default timeout for all requests to 10 seconds
    def __init__(self, timeout=10):
        self.timeout = timeout

    def fetch_top_players(self, top_n=30, per_mode='PerGame'):
        """Fetch top N players from the traditional stats table"""
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed=per_mode,
            timeout=self.timeout
        )
        df = stats.get_data_frames()[0].head(top_n)
        return df

    def fetch_player_info(self, player_id):
        """Fetch basic player info and available seasons"""
        player_info = commonplayerinfo.CommonPlayerInfo(
            player_id=player_id,
            timeout=self.timeout
        )
        # Return available seasons as DataFrame
        df_seasons = player_info.available_seasons.get_data_frame()
        # Return common info as DataFrame
        df_info = player_info.common_player_info.get_data_frame()
        return df_info, df_seasons

    def fetch_player_stats(self, **kwargs):
        """
        Fetch player stats with customizable parameters.

        Example kwargs:
            season="2025-26"
            per_mode_detailed="PerGame"
            measure_type_detailed_defense="Base"
            season_type_all_star="Regular Season"
            month=0
            last_n_games=0
            vs_conference_nullable=""
            player_position_abbreviation_nullable=""
            etc.

        Returns:
            pandas.DataFrame of LeagueDashPlayerStats
        """
        # Call the endpoint with dynamic kwargs
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            timeout=self.timeout,
            **kwargs
        )

        # The main dataset is always the first DataFrame
        df = stats.get_data_frames()[0]
        return df
