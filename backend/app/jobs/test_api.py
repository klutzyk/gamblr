from ..services.nba_client import NBAClient


def main():

    client = NBAClient()

    # Top 30 players
    df_top = client.fetch_top_players()
    print(df_top)

    # Player info (e.g., LeBron James, id=2544)
    df_info, df_seasons = client.fetch_player_info(player_id=2544)
    print(df_info)
    print(df_seasons)


if __name__ == "__main__":
    main()
