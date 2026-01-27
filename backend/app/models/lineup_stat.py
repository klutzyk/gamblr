from sqlalchemy import Column, Integer, Float, Text, ForeignKey, UniqueConstraint
from app.db.base import Base


class LineupStat(Base):
    __tablename__ = "lineup_stats"

    id = Column(Integer, primary_key=True)

    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"))
    season = Column(Text)
    lineup_id = Column(Text)
    lineup = Column(Text)

    minutes = Column(Float)
    off_rating = Column(Float)
    def_rating = Column(Float)
    net_rating = Column(Float)
    pace = Column(Float)
    ast_pct = Column(Float)
    reb_pct = Column(Float)

    __table_args__ = (
        UniqueConstraint("team_id", "season", "lineup_id", name="uq_lineup"),
    )
