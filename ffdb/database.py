"""
SQLAlchemy ORM models and database initialization for the FF college football database.
"""

import json
from contextlib import contextmanager
from datetime import date
from typing import Generator

from sqlalchemy import (
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


class Player(Base):
    """
    Canonical player identity record. One row per unique player.
    name_variants stores a JSON array of alternate spellings to aid fuzzy matching.
    """

    __tablename__ = "players"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cfbd_id = Column(Integer, unique=True, nullable=True)  # CFBD API player ID
    full_name = Column(String, nullable=False)
    position = Column(String)  # WR, RB, TE, QB, OL, etc.
    height_inches = Column(Float)
    weight_lbs = Column(Float)
    date_of_birth = Column(Date)
    hometown = Column(String)
    home_state = Column(String)
    name_variants = Column(Text, default="[]")  # JSON array of alternate spellings

    seasons = relationship("CFBPlayerSeason", back_populates="player", cascade="all, delete-orphan")
    recruiting = relationship("Recruiting", back_populates="player", cascade="all, delete-orphan")

    def get_name_variants(self) -> list[str]:
        return json.loads(self.name_variants or "[]")

    def add_name_variant(self, variant: str) -> None:
        variants = self.get_name_variants()
        if variant not in variants:
            variants.append(variant)
            self.name_variants = json.dumps(variants)

    def __repr__(self) -> str:
        return f"<Player id={self.id} name={self.full_name!r} pos={self.position}>"


class CFBPlayerSeason(Base):
    """
    Per-season college football statistics for a single player.
    Derived metrics (reception share, dominator rating, etc.) are stored
    after team-season data is available.
    """

    __tablename__ = "cfb_player_seasons"
    __table_args__ = (
        UniqueConstraint("player_id", "season_year", "team", name="uq_player_season_team"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    season_year = Column(Integer, nullable=False)
    team = Column(String)
    conference = Column(String)
    games_played = Column(Integer)

    # --- Passing ---
    pass_completions = Column(Integer)
    pass_attempts = Column(Integer)
    pass_yards = Column(Integer)
    pass_tds = Column(Integer)
    interceptions = Column(Integer)

    # --- Rushing ---
    rush_attempts = Column(Integer)
    rush_yards = Column(Integer)
    rush_tds = Column(Integer)

    # --- Receiving ---
    targets = Column(Integer)
    receptions = Column(Integer)
    rec_yards = Column(Integer)
    rec_tds = Column(Integer)

    # --- Usage (from CFBD PlayersApi.get_player_usage) ---
    usage_overall = Column(Float)
    usage_pass = Column(Float)
    usage_rush = Column(Float)
    usage_1st_down = Column(Float)
    usage_2nd_down = Column(Float)
    usage_3rd_down = Column(Float)
    usage_standard_downs = Column(Float)
    usage_passing_downs = Column(Float)

    # --- PPA (from CFBD MetricsApi.get_player_season_ppa) ---
    ppa_avg_overall = Column(Float)
    ppa_avg_pass = Column(Float)
    ppa_avg_rush = Column(Float)

    # --- Derived metrics (computed during populate, stored for query speed) ---
    rec_yards_per_team_pass_att = Column(Float)  # player rec_yards / team pass_attempts
    dominator_rating = Column(Float)             # player rec_yards / team total rec_yards
    reception_share = Column(Float)              # player receptions / team total receptions
    age_at_season_start = Column(Float)          # fractional years (e.g., 20.5)

    player = relationship("Player", back_populates="seasons")

    def __repr__(self) -> str:
        return f"<CFBPlayerSeason player_id={self.player_id} year={self.season_year} team={self.team}>"


class CFBTeamSeason(Base):
    """
    Team-level season statistics used as denominators for derived player metrics.
    """

    __tablename__ = "cfb_team_seasons"
    __table_args__ = (
        UniqueConstraint("team", "season_year", name="uq_team_season"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    team = Column(String, nullable=False)
    season_year = Column(Integer, nullable=False)
    conference = Column(String)
    games = Column(Integer)

    # Denominators for derived metrics
    pass_attempts = Column(Integer)   # team pass attempts (for rec yards per team pass att)
    total_receptions = Column(Integer)
    total_rec_yards = Column(Integer)
    total_rush_yards = Column(Integer)

    # Strength metrics (from CFBD RatingsApi)
    sp_plus_rating = Column(Float)
    sos_rating = Column(Float)       # Strength of schedule

    def __repr__(self) -> str:
        return f"<CFBTeamSeason team={self.team!r} year={self.season_year}>"


class Recruiting(Base):
    """
    Recruiting profile for a player (from CFBD RecruitingApi).
    """

    __tablename__ = "recruiting"
    __table_args__ = (
        UniqueConstraint("player_id", "recruit_year", name="uq_recruiting_player_year"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    recruit_year = Column(Integer)
    stars = Column(Integer)          # 1-5
    rating = Column(Float)           # 247Sports composite (0.0–1.0 scale)
    ranking_national = Column(Integer)
    ranking_position = Column(Integer)
    state = Column(String)
    school = Column(String)          # High school name
    classification = Column(String)  # HighSchool, JUCO, PrepSchool

    player = relationship("Player", back_populates="recruiting")

    def __repr__(self) -> str:
        return f"<Recruiting player_id={self.player_id} year={self.recruit_year} stars={self.stars}>"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> None:
    """Create all tables if they don't exist."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    engine.dispose()


def _make_engine(db_path: str):
    return create_engine(
        f"sqlite:///{db_path}",
        echo=False,
        connect_args={"check_same_thread": False},
    )


@contextmanager
def get_session(db_path: str) -> Generator[Session, None, None]:
    """Context manager that yields a SQLAlchemy Session and commits/rolls back."""
    engine = _make_engine(db_path)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()
