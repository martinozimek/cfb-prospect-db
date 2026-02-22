"""
ffdb — Fantasy Football College Football Database
==================================================

The primary entry point is FFDatabase:

    from ffdb import FFDatabase

    db = FFDatabase()                         # auto-discovers ff.db via .env
    db = FFDatabase("path/to/ff.db")         # explicit path

Quick-start examples
--------------------
    # Full player profile (identity + college seasons + combine + draft)
    profile = db.get_profile("Carnell Tate")

    # Fuzzy name search
    player = db.find_player("Emeka Egbuka")
    players = db.find_players("Smith")      # returns ranked (Player, score) list

    # College seasons for a player (returns list of CFBPlayerSeason ORM objects)
    seasons = db.get_cfb_seasons(player.id)

    # Career aggregate stats
    career = db.get_cfb_career(player.id)

    # Derived metrics by season
    metrics = db.get_player_metrics(player.id)

    # Filter players by position / team / year
    wrs = db.search_players(position="WR", min_year=2023, min_games=6)

    # NFL draft class lookup
    picks = db.search_draft_class(year=2024, position="WR")

    # Check data freshness / ingestion log
    status = db.get_ingestion_status()

ORM models are also importable for direct SQLAlchemy queries:

    from ffdb import Player, CFBPlayerSeason, CFBTeamSeason
    from ffdb import NFLDraftPick, NFLCombineResult, Recruiting
    from ffdb.database import get_session
"""

from ffdb.queries import FFDatabase
from ffdb.database import (
    Player,
    CFBPlayerSeason,
    CFBTeamSeason,
    NFLDraftPick,
    NFLCombineResult,
    Recruiting,
    DataIngestionLog,
)

__all__ = [
    # Primary interface
    "FFDatabase",
    # ORM models (for direct queries)
    "Player",
    "CFBPlayerSeason",
    "CFBTeamSeason",
    "NFLDraftPick",
    "NFLCombineResult",
    "Recruiting",
    "DataIngestionLog",
]
