#!/usr/bin/env python3
"""
Backfill CFBTeamSeason.games and recompute rec_yards_per_team_pass_att
for all years 2007-2025.

Fetches team season stats from the CFBD API for each year, updates the
CFBTeamSeason.games field, and re-runs _compute_derived() on all player
seasons for that year to produce prorated rec_yards_per_team_pass_att.

Usage:
    cd cfb-prospect-db
    python scripts/backfill_team_games.py

Takes ~5-10 minutes (one API call per year, ~19 years).
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dotenv
dotenv.load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

from ffdb.database import get_session, CFBTeamSeason, CFBPlayerSeason
from ffdb.collectors.cfbd_collector import CFBDCollector

# Import helpers from populate_db (they are module-level functions)
from scripts.populate_db import (
    _parse_team_stats,
    _extract_team_denominators,
    _upsert_team_season,
    _compute_derived,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = os.environ["FF_DB_PATH"]
API_KEY = os.environ["CFBD_API_KEY"]
YEARS = list(range(2007, 2026))


def main() -> None:
    collector = CFBDCollector(API_KEY)

    with get_session(DB_PATH) as session:
        for year in YEARS:
            log.info("Year %d — fetching team season stats …", year)
            raw = collector.fetch_team_season_stats(year)
            team_stats_map = _parse_team_stats(raw)

            updated_teams = 0
            for team_name, stats in team_stats_map.items():
                denoms = _extract_team_denominators(stats)
                _upsert_team_season(session, team_name, year, denoms)
                updated_teams += 1

            session.flush()

            # Re-index team rows for fast lookup
            team_rows: dict[tuple, CFBTeamSeason] = {
                (ts.team, ts.season_year): ts
                for ts in session.query(CFBTeamSeason)
                                 .filter(CFBTeamSeason.season_year == year).all()
            }

            # Recompute rec_yards_per_team_pass_att for every player season this year
            player_seasons = (
                session.query(CFBPlayerSeason)
                       .filter(CFBPlayerSeason.season_year == year).all()
            )
            updated_players = 0
            for ps in player_seasons:
                team_row = team_rows.get((ps.team, year))
                if team_row:
                    _compute_derived(ps, team_row)
                    updated_players += 1

            session.commit()
            log.info(
                "  Year %d: %d teams updated (games populated), %d player seasons recomputed",
                year,
                updated_teams,
                updated_players,
            )

    log.info("Backfill complete.")


if __name__ == "__main__":
    main()
