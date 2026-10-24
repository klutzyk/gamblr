from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import pandas as pd
from app.models.player import Player
from app.models.player_game_stat import PlayerGameStat
from app.core.nba_season import season_for_date


def _clean_value(value):
    return None if value is None or pd.isna(value) else value


# Save a player's provider game log without pruning historical rows.
async def save_last_n_games(
    player_id: int,
    player_name: str,
    team_abbr: str,
    df,
    db: AsyncSession,
):
    # Ensure player exists
    player = await db.get(Player, player_id)
    if not player:
        player = Player(
            id=player_id,
            full_name=player_name,
            team_abbreviation=team_abbr,
        )
        db.add(player)
        await db.flush()
    elif team_abbr and player.team_abbreviation != team_abbr:
        player.team_abbreviation = team_abbr

    # Normalize API dates
    df = df.copy()
    df["GAME_DATE"] = df["GAME_DATE"].apply(
        lambda d: pd.to_datetime(d, errors="raise").date()
    )

    # Load existing games for update checks
    existing_rows_result = await db.execute(
        select(PlayerGameStat).where(PlayerGameStat.player_id == player_id)
    )
    existing_rows = {row[0].game_id: row[0] for row in existing_rows_result.all()}

    if df.empty:
        return 0  # nothing to process

    inserted = 0
    updated = 0
    # Insert new games or reconcile corrections from the provider.
    for _, row in df.iterrows():
        game_id = str(row.get("Game_ID") or row.get("GAME_ID")).zfill(10)
        game_date = row["GAME_DATE"]
        matchup = str(row.get("MATCHUP") or "")
        game_team_abbr = matchup.split(" ", 1)[0].upper() if matchup else team_abbr
        existing = existing_rows.get(game_id)
        if existing:
            has_updates = False
            identity_values = {
                "game_date": game_date,
                "season": season_for_date(game_date),
                "team_abbreviation": game_team_abbr,
                "matchup": matchup,
            }
            for col, value in identity_values.items():
                if value is not None and getattr(existing, col) != value:
                    setattr(existing, col, value)
                    has_updates = True
            for col, key in [
                ("minutes", "MIN"),
                ("points", "PTS"),
                ("assists", "AST"),
                ("rebounds", "REB"),
                ("steals", "STL"),
                ("blocks", "BLK"),
                ("turnovers", "TOV"),
                ("fgm", "FGM"),
                ("fga", "FGA"),
                ("fg3m", "FG3M"),
                ("fg3a", "FG3A"),
            ]:
                value = row.get(key)
                if value is not None and not pd.isna(value) and getattr(existing, col) != value:
                    setattr(existing, col, value)
                    has_updates = True
            if has_updates:
                updated += 1
            continue

        db.add(
            PlayerGameStat(
                player_id=player_id,
                game_id=game_id,
                game_date=game_date,
                season=season_for_date(game_date),
                team_abbreviation=game_team_abbr,
                matchup=matchup,
                minutes=_clean_value(row.get("MIN")),
                points=_clean_value(row.get("PTS")),
                assists=_clean_value(row.get("AST")),
                rebounds=_clean_value(row.get("REB")),
                steals=_clean_value(row.get("STL")),
                blocks=_clean_value(row.get("BLK")),
                turnovers=_clean_value(row.get("TOV")),
                fgm=_clean_value(row.get("FGM")),
                fga=_clean_value(row.get("FGA")),
                fg3m=_clean_value(row.get("FG3M")),
                fg3a=_clean_value(row.get("FG3A")),
            )
        )
        inserted += 1

    await db.commit()

    return inserted + updated
