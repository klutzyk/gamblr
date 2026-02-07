from sqlalchemy import Column, Integer, Text, Date, DateTime
from datetime import datetime
from app.db.base import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True)
    ingest_type = Column(Text, nullable=False)
    since_date = Column(Date, nullable=True)
    season = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default="completed")
    note = Column(Text, nullable=True)
    players_total = Column(Integer, nullable=False, default=0)
    players_saved = Column(Integer, nullable=False, default=0)
    players_skipped = Column(Integer, nullable=False, default=0)
    players_failed = Column(Integer, nullable=False, default=0)
    total_new_games_inserted = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
