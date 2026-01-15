from sqlalchemy.ext.asyncio import AsyncSession
from app.models.event import Event
from app.models.bookmaker import Bookmaker
from app.models.market import Market
from app.models.player_prop import PlayerProp
from datetime import datetime


# extract the event odds data from the json returned from the event/odds call
async def save_event_odds(event_data: dict, db: AsyncSession):
    # --- Event ---
    event = Event(
        id=event_data["id"],
        sport_key=event_data["sport_key"],
        sport_title=event_data.get("sport_title"),
        commence_time=datetime.fromisoformat(
            event_data["commence_time"].replace("Z", "+00:00")
        ),
        home_team=event_data["home_team"],
        away_team=event_data["away_team"],
    )
    db.add(event)
    await db.commit()  # save and commit event details before the rest
    await db.refresh(event)

    # --- Bookmakers ---
    for b in event_data.get("bookmakers", []):
        bookmaker = Bookmaker(
            event_id=event.id,
            key=b["key"],
            title=b["title"],
        )
        db.add(bookmaker)
        await db.flush()  # assign an ID without committing

        # --- Markets ---
        for m in b.get("markets", []):
            market = Market(
                bookmaker_id=bookmaker.id,
                key=m["key"],
                last_update=datetime.fromisoformat(
                    m.get("last_update", datetime.utcnow().isoformat()).replace(
                        "Z", "+00:00"
                    )
                ),
            )
            db.add(market)
            await db.flush()  # assign market.id without commiting

            # --- Player Props ---
            for o in m.get("outcomes", []):
                prop = PlayerProp(
                    market_id=market.id,
                    player_name=o["description"],  # players name is descripton
                    side=o["name"],  # "Over" / "Under"
                    price=o["price"],
                    line=o["point"],
                )
                db.add(prop)

    await db.commit()
