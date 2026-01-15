from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.event import Event
from app.models.bookmaker import Bookmaker
from app.models.market import Market
from app.models.player_prop import PlayerProp
from datetime import datetime


# extract the event odds data from the json returned from the event/odds call
async def save_event_odds(event_data: dict, db: AsyncSession):
    # --- Event ---
    # check if event exists
    event = await db.get(Event, event_data["id"])
    if not event:
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
        await db.commit()
        await db.refresh(event)
    else:
        # update existing event
        event.sport_key = event_data["sport_key"]
        event.sport_title = event_data.get("sport_title")
        event.commence_time = datetime.fromisoformat(
            event_data["commence_time"].replace("Z", "+00:00")
        )
        event.home_team = event_data["home_team"]
        event.away_team = event_data["away_team"]
        await db.commit()
        await db.refresh(event)

    # --- Bookmakers ---
    for b in event_data.get("bookmakers", []):
        # check if bookmaker exists
        stmt = select(Bookmaker).where(Bookmaker.key == b["key"])
        res = await db.execute(stmt)
        bookmaker = res.scalar_one_or_none()

        if not bookmaker:
            bookmaker = Bookmaker(
                event_id=event.id,
                key=b["key"],
                title=b["title"],
            )
            db.add(bookmaker)
            await db.flush()  # assign an ID without committing
        else:
            bookmaker.title = b["title"]
            await db.flush()

        # --- Markets ---
        for m in b.get("markets", []):
            stmt = select(Market).where(
                Market.bookmaker_id == bookmaker.id, Market.key == m["key"]
            )
            res = await db.execute(stmt)
            market = res.scalar_one_or_none()

            if not market:
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
                await db.flush()
            else:
                market.last_update = datetime.fromisoformat(
                    m.get("last_update", datetime.utcnow().isoformat()).replace(
                        "Z", "+00:00"
                    )
                )
                await db.flush()

            # --- Player Props ---
            for o in m.get("outcomes", []):
                stmt = select(PlayerProp).where(
                    PlayerProp.market_id == market.id,
                    PlayerProp.player_name == o["description"],
                    PlayerProp.side == o["name"],
                )
                res = await db.execute(stmt)
                prop = res.scalar_one_or_none()

                if not prop:
                    prop = PlayerProp(
                        market_id=market.id,
                        player_name=o["description"],
                        side=o["name"],
                        price=o["price"],
                        line=o["point"],
                    )
                    db.add(prop)
                else:
                    prop.price = o["price"]
                    prop.line = o["point"]

    # final commit after all children added/updated
    await db.commit()
