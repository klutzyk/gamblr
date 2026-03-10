from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    commonplayerinfo,
    playergamelog,
    teamgamelog,
    leaguedashlineups,
    boxscoretraditionalv2,
)
try:
    from nba_api.stats.endpoints import boxscoretraditionalv3
except ImportError:  # pragma: no cover
    boxscoretraditionalv3 = None
from app.services.cache import cached
import pandas as pd


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

    @cached(ttl_seconds=60 * 15)
    def fetch_team_game_log(self, team_id: int, season: str = "2025-26"):
        gamelog = teamgamelog.TeamGameLog(
            team_id=team_id,
            season=season,
            timeout=self.timeout,
        )
        return gamelog.get_data_frames()[0]

    @cached(ttl_seconds=60 * 60)
    def fetch_team_lineups(
        self,
        team_id: int,
        season: str = "2025-26",
        group_quantity: int = 5,
    ):
        lineups = leaguedashlineups.LeagueDashLineups(
            team_id_nullable=team_id,
            season=season,
            group_quantity=group_quantity,
            measure_type_detailed_defense="Advanced",
            per_mode_detailed="PerGame",
            timeout=self.timeout,
        )
        return lineups.get_data_frames()[0]

    @cached(ttl_seconds=60 * 15)
    def fetch_game_players(self, game_id: str):
        safe_id = str(game_id).zfill(10)
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(
            game_id=safe_id,
            timeout=self.timeout,
        )
        df = box.get_data_frames()[0]
        if df.empty:
            return []
        cols = [c for c in ["PLAYER_ID", "PLAYER_NAME"] if c in df.columns]
        if not cols:
            return []
        return df[cols].dropna().to_dict(orient="records")

    @cached(ttl_seconds=60 * 10)
    def fetch_game_boxscore(self, game_id: str):
        safe_id = str(game_id).zfill(10)

        def _select_frames(frames):
            if not frames:
                return pd.DataFrame(), pd.DataFrame()
            players = pd.DataFrame()
            teams = pd.DataFrame()
            for frame in frames:
                if frame is None or frame.empty:
                    continue
                cols = set(frame.columns)
                has_player_cols = (
                    "personId" in cols
                    or "playerId" in cols
                    or "PLAYER_ID" in cols
                )
                has_team_cols = "teamId" in cols or "TEAM_ID" in cols
                if has_player_cols:
                    if players.empty:
                        players = frame
                # Prefer dedicated team summary frames, not player rows that also include teamId.
                if has_team_cols and not has_player_cols:
                    if teams.empty:
                        teams = frame
            if players.empty:
                for frame in frames:
                    if frame is not None and not frame.empty:
                        players = frame
                        break
            if teams.empty and len(frames) > 1:
                for frame in frames:
                    if frame is not None and not frame.empty and frame is not players:
                        teams = frame
                        break
            return players, teams

        if boxscoretraditionalv3 is None:
            raise RuntimeError("BoxScoreTraditionalV3 is not available in installed nba_api")

        box = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=safe_id,
            start_period=1,
            end_period=10,
            start_range=0,
            end_range=0,
            range_type=0,
            timeout=self.timeout,
        )
        frames = box.get_data_frames()
        players_df, teams_df = _select_frames(frames)

        if players_df is not None and not players_df.empty and "personId" in players_df.columns:
            players_df = players_df.copy()
            name_col = None
            if "firstName" in players_df.columns and "familyName" in players_df.columns:
                players_df["PLAYER_NAME"] = (
                    players_df["firstName"].fillna("").astype(str).str.strip()
                    + " "
                    + players_df["familyName"].fillna("").astype(str).str.strip()
                ).str.strip()
                name_col = "PLAYER_NAME"
            elif "nameI" in players_df.columns:
                players_df["PLAYER_NAME"] = players_df["nameI"]
                name_col = "PLAYER_NAME"
            elif "playerSlug" in players_df.columns:
                players_df["PLAYER_NAME"] = players_df["playerSlug"]
                name_col = "PLAYER_NAME"

            players_df["PLAYER_ID"] = players_df["personId"]
            players_df["TEAM_ABBREVIATION"] = players_df.get("teamTricode")
            players_df["MIN"] = players_df.get("minutes")
            players_df["PTS"] = players_df.get("points")
            players_df["AST"] = players_df.get("assists")
            players_df["REB"] = players_df.get("reboundsTotal")
            players_df["STL"] = players_df.get("steals")
            players_df["BLK"] = players_df.get("blocks")
            players_df["TOV"] = players_df.get("turnovers")
            players_df["FGM"] = players_df.get("fieldGoalsMade")
            players_df["FGA"] = players_df.get("fieldGoalsAttempted")
            players_df["FG3M"] = players_df.get("threePointersMade")
            players_df["FG3A"] = players_df.get("threePointersAttempted")

            keep_cols = [
                "PLAYER_ID",
                "PLAYER_NAME",
                "TEAM_ABBREVIATION",
                "MIN",
                "PTS",
                "AST",
                "REB",
                "STL",
                "BLK",
                "TOV",
                "FGM",
                "FGA",
                "FG3M",
                "FG3A",
            ]
            if name_col is None:
                keep_cols.remove("PLAYER_NAME")
            players_df = players_df[keep_cols]

        if teams_df is not None and not teams_df.empty and "teamId" in teams_df.columns:
            teams_df = teams_df.copy()
            teams_df["TEAM_ID"] = teams_df["teamId"]
            teams_df["TEAM_ABBREVIATION"] = teams_df.get("teamTricode")
            teams_df["PTS"] = teams_df.get("points")
            teams_df["AST"] = teams_df.get("assists")
            teams_df["REB"] = teams_df.get("reboundsTotal")
            teams_df["TOV"] = teams_df.get("turnovers")
            teams_df["FGM"] = teams_df.get("fieldGoalsMade")
            teams_df["FGA"] = teams_df.get("fieldGoalsAttempted")
            teams_df["FG3M"] = teams_df.get("threePointersMade")
            teams_df["FG3A"] = teams_df.get("threePointersAttempted")
            teams_df = teams_df[
                [
                    "TEAM_ID",
                    "TEAM_ABBREVIATION",
                    "PTS",
                    "AST",
                    "REB",
                    "TOV",
                    "FGM",
                    "FGA",
                    "FG3M",
                    "FG3A",
                ]
            ]

        return players_df, teams_df
