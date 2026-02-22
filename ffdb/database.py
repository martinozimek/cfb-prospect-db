"""
SQLAlchemy ORM models and database initialization for the FF college football database.
"""

import json
from contextlib import contextmanager
from datetime import date
from typing import Generator

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

__all__ = [
    "Base",
    "Player",
    "CFBPlayerSeason",
    "CFBTeamSeason",
    "Recruiting",
    "NFLDraftPick",
    "NFLCombineResult",
    "DataIngestionLog",
    "init_db",
    "get_session",
]


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
    draft_pick = relationship("NFLDraftPick", back_populates="player", uselist=False, cascade="all, delete-orphan")
    combine = relationship("NFLCombineResult", back_populates="player", uselist=False, cascade="all, delete-orphan")

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
    sp_plus_rating = Column(Float)    # SP+ overall rating (Bill Connelly)
    sos_rating = Column(Float)        # SP+ SOS — null from API, reserved for future use
    srs_rating = Column(Float)        # Simple Rating System (margin + schedule)
    fpi_sos_rank = Column(Integer)    # ESPN FPI strength-of-schedule rank (lower = harder)

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


class NFLDraftPick(Base):
    """
    NFL Draft pick record for a player.
    One row per player (players are only drafted once).
    """

    __tablename__ = "nfl_draft_picks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, unique=True)
    draft_year = Column(Integer)
    draft_round = Column(Integer)
    overall_pick = Column(Integer)       # Overall pick number (1 = first overall)
    nfl_team = Column(String)
    position_drafted = Column(String)

    # Draft capital score: a normalized 0–100 value derived from overall_pick.
    # Higher = better draft capital. Computed externally and stored here.
    draft_capital_score = Column(Float)

    player = relationship("Player", back_populates="draft_pick")

    def __repr__(self) -> str:
        return f"<NFLDraftPick player_id={self.player_id} year={self.draft_year} pick={self.overall_pick}>"


class NFLCombineResult(Base):
    """
    NFL Combine measurables for a player.
    Source: Pro Football Reference combine data.
    """

    __tablename__ = "nfl_combine_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, unique=True)
    combine_year = Column(Integer)
    college = Column(String)
    position = Column(String)

    # Measurables
    height_inches = Column(Float)    # Height at combine (inches)
    weight_lbs = Column(Float)       # Weight at combine (lbs)
    forty_time = Column(Float)       # 40-yard dash (seconds)
    vertical_jump = Column(Float)    # Vertical jump (inches)
    broad_jump = Column(Integer)     # Broad jump (inches)
    three_cone = Column(Float)       # 3-cone drill (seconds)
    shuttle = Column(Float)          # 20-yard shuttle (seconds)
    bench_press = Column(Integer)    # Bench press reps (225 lbs)

    # Derived
    speed_score = Column(Float)      # Bill Barnwell Speed Score: (weight * 200) / (40_time^4)

    player = relationship("Player", back_populates="combine")

    def __repr__(self) -> str:
        return f"<NFLCombineResult player_id={self.player_id} year={self.combine_year} forty={self.forty_time}>"


class DataIngestionLog(Base):
    """
    Tracks when each data source was last ingested.
    Used by scripts/refresh.py to detect new data and avoid redundant fetches.
    """

    __tablename__ = "data_ingestion_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)   # e.g. "cfbd_seasons", "nflverse_combine"
    scope = Column(String)                    # e.g. "year=2025", "years=2021-2025"
    last_run_utc = Column(DateTime, nullable=True)  # set explicitly at ingest time
    rows_affected = Column(Integer)
    status = Column(String)                   # "ok", "error", "skipped"
    notes = Column(Text)

    def __repr__(self) -> str:
        return f"<DataIngestionLog source={self.source!r} scope={self.scope!r} status={self.status}>"


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
