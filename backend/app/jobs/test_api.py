from ..services.nba_client import NBAClient
import requests


def main():


def api_tests():
    client = NBAClient()

    # Recent performers: last 5 games, top 10 by fantasy points
    print("Recent performers (last 5 games)")
    df_recent = client.fetch_player_stats(
        season="2025-26",
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
        last_n_games=5
    )
    df_recent_sorted = df_recent.sort_values(
        "NBA_FANTASY_PTS", ascending=False).head(10)
    print(df_recent_sorted[['PLAYER_NAME', 'PTS',
          'AST', 'REB', 'NBA_FANTASY_PTS']])


def direct_tests():
    client = NBAClient()

    # Top 30 players
    print("Top 30 players")
    df_top = client.fetch_top_players()
    print(df_top)

    # Player info (e.g., LeBron James, id=2544)
    print("Player info (e.g., LeBron James, id=2544)")
    df_info, df_seasons = client.fetch_player_info(player_id=2544)
    print(df_info)
    print(df_seasons)

    # First 30 guards in January of 2025-26 season, per game stats
    print("First 30 guards in January of 2025-26 season, per game stats")
    df = client.fetch_player_stats(
        season="2025-26",
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
        month=1,
        player_position_abbreviation_nullable="G",
        last_n_games=0
    )
    print(df[['PLAYER_NAME', 'PTS', 'AST', 'REB']].head(30))

    # Total stats for all players in February, Regular Season
    print("Total stats for all players in February, Regular Season")
    df2 = client.fetch_player_stats(
        season="2025-26",
        per_mode_detailed="Totals",
        measure_type_detailed_defense="Base",
        season_type_all_star="Regular Season",
        month=2
    )
    print(df2.head(10))


if __name__ == "__main__":
    main()
