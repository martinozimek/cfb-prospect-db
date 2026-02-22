"""
Bulk data loading script for the FF college football database.

Usage:
    python scripts/populate_db.py --start-year 2020 --end-year 2024
    python scripts/populate_db.py --start-year 2011 --end-year 2024 --db ff.db

This script is IDEMPOTENT: re-running it updates existing records rather
than creating duplicates.

API call budget per year (free tier = 1,000/month):
    1. get_player_season_stats
    2. get_team_season_stats
    3. get_player_usage
    4. get_player_season_ppa
    5. get_sp_plus_ratings
    6. get_recruiting_players
    ─────────────────────────
    6 calls × N years  (14 years = 84 calls; well within the free limit)
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any, Optional

# Allow running as `python scripts/populate_db.py` from the FF root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_api_key, get_db_path
from ffdb.collectors.cfbd_collector import CFBDCollector
from ffdb.database import (
    CFBPlayerSeason,
    CFBTeamSeason,
    Player,
    Recruiting,
    get_session,
    init_db,
)
from ffdb.utils.player_index import PlayerIndex

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _age_at_date(dob: Optional[date], ref_date: date) -> Optional[float]:
    """Fractional years between dob and ref_date."""
    if dob is None:
        return None
    delta = ref_date - dob
    return round(delta.days / 365.25, 2)


# ---------------------------------------------------------------------------
# Team stats parsing
# ---------------------------------------------------------------------------

def _parse_team_stats(raw_rows: list[Any]) -> dict[str, dict[str, Any]]:
    """
    Convert a flat list of TeamSeasonStat rows into a dict keyed by team name.
    Each value is a dict of {stat_type: value}.

    CFBD returns team stats as: team, statName, statValue  (one row per stat).
    """
    teams: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        team = getattr(row, "team", None)
        stat_name = getattr(row, "stat_name", None) or getattr(row, "type", None)
        stat_val_obj = getattr(row, "stat_value", None)
        stat_val = getattr(stat_val_obj, "actual_instance", None) if stat_val_obj is not None else getattr(row, "stat", None)
        if team and stat_name:
            teams.setdefault(team, {})[stat_name] = stat_val
    return teams


def _extract_team_denominators(team_stats: dict[str, Any]) -> dict[str, Optional[int]]:
    """
    Pull the specific denominators we need from a team's stat dict.
    Handles the CFBD stat naming conventions.
    """
    # Try common CFBD stat names; the API uses camelCase keys like passAttempts
    def get(*keys) -> Optional[int]:
        for k in keys:
            v = team_stats.get(k)
            if v is not None:
                return _safe_int(v)
        return None

    return {
        "pass_attempts": get("passAttempts", "pass_attempts", "passingAttempts"),
        "total_receptions": get("passCompletions", "receptions", "passReceptions"),
        "total_rec_yards": get("netPassingYards", "passingYards", "receivingYards"),
        "total_rush_yards": get("rushingYards", "netRushingYards"),
    }


# ---------------------------------------------------------------------------
# Player stat parsing
# ---------------------------------------------------------------------------

def _parse_player_stats(raw_rows: list[Any]) -> dict[int, dict[str, Any]]:
    """
    CFBD player season stats come back as one row per (player, stat_category, stat_type).
    Pivot them into a dict keyed by player_id with all stats as sub-keys.

    Returns: {player_id: {field: value, ...}}
    """
    players: dict[int, dict[str, Any]] = {}

    for row in raw_rows:
        pid = getattr(row, "player_id", None) or getattr(row, "id", None)
        if pid is None:
            continue
        pid = int(pid)

        if pid not in players:
            players[pid] = {
                "player_id": pid,
                "player_name": getattr(row, "player", None) or getattr(row, "name", None),
                "team": getattr(row, "team", None),
                "conference": getattr(row, "conference", None),
                "position": getattr(row, "position", None),
            }

        category = (getattr(row, "category", None) or "").lower()
        stat_type = (getattr(row, "stat_type", None) or getattr(row, "type", None) or "").lower()
        stat_val = getattr(row, "stat", None)

        # Map CFBD category/type combos to our schema fields
        key = f"{category}_{stat_type}"
        FIELD_MAP = {
            "passing_completions": "pass_completions",
            "passing_att": "pass_attempts",
            "passing_yds": "pass_yards",
            "passing_td": "pass_tds",
            "passing_int": "interceptions",
            "rushing_car": "rush_attempts",
            "rushing_yds": "rush_yards",
            "rushing_td": "rush_tds",
            "receiving_rec": "receptions",
            "receiving_yds": "rec_yards",
            "receiving_td": "rec_tds",
            "receiving_long": None,    # not stored
        }
        db_field = FIELD_MAP.get(key)
        if db_field:
            players[pid][db_field] = _safe_int(stat_val)

    return players


# ---------------------------------------------------------------------------
# Usage parsing
# ---------------------------------------------------------------------------

def _parse_usage(raw_rows: list[Any]) -> dict[int, dict[str, Any]]:
    """Index usage rows by player_id."""
    result: dict[int, dict[str, Any]] = {}
    for row in raw_rows:
        pid = _safe_int(getattr(row, "id", None))
        if pid is None:
            continue
        usage = getattr(row, "usage", None)
        result[pid] = {
            "usage_overall": _safe_float(getattr(usage, "overall", None)) if usage else None,
            "usage_pass": _safe_float(getattr(usage, "var_pass", None)) if usage else None,
            "usage_rush": _safe_float(getattr(usage, "rush", None)) if usage else None,
            "usage_1st_down": _safe_float(getattr(usage, "first_down", None)) if usage else None,
            "usage_2nd_down": _safe_float(getattr(usage, "second_down", None)) if usage else None,
            "usage_3rd_down": _safe_float(getattr(usage, "third_down", None)) if usage else None,
            "usage_standard_downs": _safe_float(getattr(usage, "standard_downs", None)) if usage else None,
            "usage_passing_downs": _safe_float(getattr(usage, "passing_downs", None)) if usage else None,
        }
    return result


# ---------------------------------------------------------------------------
# PPA parsing
# ---------------------------------------------------------------------------

def _parse_ppa(raw_rows: list[Any]) -> dict[int, dict[str, Any]]:
    """Index PPA rows by player_id."""
    result: dict[int, dict[str, Any]] = {}
    for row in raw_rows:
        pid = _safe_int(getattr(row, "id", None))
        if pid is None:
            continue
        avg = getattr(row, "average_ppa", None)
        result[pid] = {
            "ppa_avg_overall": _safe_float(getattr(avg, "all", None)) if avg else None,
            "ppa_avg_pass": _safe_float(getattr(avg, "var_pass", None)) if avg else None,
            "ppa_avg_rush": _safe_float(getattr(avg, "rush", None)) if avg else None,
        }
    return result


# ---------------------------------------------------------------------------
# SP+ ratings parsing
# ---------------------------------------------------------------------------

def _parse_sp_ratings(raw_rows: list[Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        team = getattr(row, "team", None)
        if team:
            sos = getattr(row, "sos", None)
            result[team] = {
                "sp_plus_rating": _safe_float(getattr(row, "rating", None)),
                "sos_rating": _safe_float(sos) if isinstance(sos, (int, float)) else None,
            }
    return result


# ---------------------------------------------------------------------------
# DB upsert helpers
# ---------------------------------------------------------------------------

def _upsert_player(session, pid: int, data: dict) -> Player:
    """Get-or-create a Player record by CFBD player_id."""
    player = session.query(Player).filter(Player.cfbd_id == pid).first()
    if player is None:
        player = Player(
            cfbd_id=pid,
            full_name=data.get("player_name") or f"Unknown_{pid}",
            position=data.get("position"),
        )
        session.add(player)
        session.flush()
    else:
        # Update position if we have it and it's missing
        if not player.position and data.get("position"):
            player.position = data["position"]
        if not player.full_name or player.full_name.startswith("Unknown_"):
            if data.get("player_name"):
                player.full_name = data["player_name"]
    return player


def _upsert_player_season(session, player: Player, year: int, data: dict) -> CFBPlayerSeason:
    """Get-or-create a CFBPlayerSeason record."""
    team = data.get("team") or ""
    season = (
        session.query(CFBPlayerSeason)
        .filter(
            CFBPlayerSeason.player_id == player.id,
            CFBPlayerSeason.season_year == year,
            CFBPlayerSeason.team == team,
        )
        .first()
    )
    if season is None:
        season = CFBPlayerSeason(
            player_id=player.id,
            season_year=year,
            team=team,
        )
        session.add(season)

    # Update all fields from data
    season.conference = data.get("conference")
    season.games_played = data.get("games_played")
    season.pass_completions = data.get("pass_completions")
    season.pass_attempts = data.get("pass_attempts")
    season.pass_yards = data.get("pass_yards")
    season.pass_tds = data.get("pass_tds")
    season.interceptions = data.get("interceptions")
    season.rush_attempts = data.get("rush_attempts")
    season.rush_yards = data.get("rush_yards")
    season.rush_tds = data.get("rush_tds")
    season.targets = data.get("targets")
    season.receptions = data.get("receptions")
    season.rec_yards = data.get("rec_yards")
    season.rec_tds = data.get("rec_tds")
    return season


def _upsert_team_season(session, team: str, year: int, data: dict) -> CFBTeamSeason:
    row = (
        session.query(CFBTeamSeason)
        .filter(CFBTeamSeason.team == team, CFBTeamSeason.season_year == year)
        .first()
    )
    if row is None:
        row = CFBTeamSeason(team=team, season_year=year)
        session.add(row)

    row.conference = data.get("conference")
    row.pass_attempts = data.get("pass_attempts")
    row.total_receptions = data.get("total_receptions")
    row.total_rec_yards = data.get("total_rec_yards")
    row.total_rush_yards = data.get("total_rush_yards")
    row.sp_plus_rating = data.get("sp_plus_rating")
    row.sos_rating = data.get("sos_rating")
    return row


# ---------------------------------------------------------------------------
# Derived metric calculation
# ---------------------------------------------------------------------------

def _compute_derived(season: CFBPlayerSeason, team_row: Optional[CFBTeamSeason]) -> None:
    """Populate derived fields on a CFBPlayerSeason from team denominators."""
    if team_row is None:
        return

    rec_yards = season.rec_yards or 0
    receptions = season.receptions or 0

    if team_row.pass_attempts and team_row.pass_attempts > 0:
        season.rec_yards_per_team_pass_att = round(rec_yards / team_row.pass_attempts, 4)

    if team_row.total_rec_yards and team_row.total_rec_yards > 0:
        season.dominator_rating = round(rec_yards / team_row.total_rec_yards, 4)

    if team_row.total_receptions and team_row.total_receptions > 0:
        season.reception_share = round(receptions / team_row.total_receptions, 4)


# ---------------------------------------------------------------------------
# Main per-year ingestion
# ---------------------------------------------------------------------------

def ingest_year(collector: CFBDCollector, db_path: str, year: int) -> None:
    logger.info("=" * 60)
    logger.info("Ingesting year %d", year)
    logger.info("=" * 60)

    # 1. Fetch all data from CFBD
    raw_player_stats = collector.fetch_player_season_stats(year)
    raw_team_stats = collector.fetch_team_season_stats(year)
    raw_usage = collector.fetch_player_usage(year)
    raw_ppa = collector.fetch_player_season_ppa(year)
    raw_sp = collector.fetch_sp_plus_ratings(year)
    game_counts = collector.fetch_player_game_counts(year)  # CFBD player_id → games_played

    # 2. Parse into lookup dicts
    player_stats = _parse_player_stats(raw_player_stats)

    # Merge games_played from game counts (keyed by CFBD player_id)
    for pid, count in game_counts.items():
        if pid in player_stats:
            player_stats[pid]["games_played"] = count
    team_stats_by_name = _parse_team_stats(raw_team_stats)
    usage_by_pid = _parse_usage(raw_usage)
    ppa_by_pid = _parse_ppa(raw_ppa)
    sp_by_team = _parse_sp_ratings(raw_sp)

    with get_session(db_path) as session:
        # 3. Upsert team seasons first (we need them for derived metrics)
        team_rows: dict[str, CFBTeamSeason] = {}
        for team_name, stats in team_stats_by_name.items():
            denom = _extract_team_denominators(stats)
            sp_data = sp_by_team.get(team_name, {})
            team_data = {**denom, **sp_data}
            team_row = _upsert_team_season(session, team_name, year, team_data)
            team_rows[team_name] = team_row
        session.flush()

        # 4. Upsert player seasons
        season_count = 0
        ref_date = date(year, 9, 1)  # ~start of college football season

        for pid, data in player_stats.items():
            player = _upsert_player(session, pid, data)

            # Merge usage and PPA data
            usage_data = usage_by_pid.get(pid, {})
            ppa_data = ppa_by_pid.get(pid, {})
            merged = {**data, **usage_data, **ppa_data}

            season = _upsert_player_season(session, player, year, merged)

            # Apply usage
            for field, val in usage_data.items():
                setattr(season, field, val)

            # Apply PPA
            for field, val in ppa_data.items():
                setattr(season, field, val)

            # Age
            if player.date_of_birth:
                season.age_at_season_start = _age_at_date(player.date_of_birth, ref_date)

            # Derived metrics
            team_row = team_rows.get(data.get("team") or "")
            _compute_derived(season, team_row)

            season_count += 1

        session.flush()
        logger.info("  Upserted %d player-seasons for %d", season_count, year)


# ---------------------------------------------------------------------------
# Recruiting ingestion (separate pass, keyed by name not ID)
# ---------------------------------------------------------------------------

def ingest_recruiting(
    collector: CFBDCollector,
    db_path: str,
    year: int,
    index: PlayerIndex,
) -> None:
    """
    Ingest recruiting class data for a single year.

    Only links recruits to players already in the DB (matched by name).
    Does NOT create placeholder Player rows for unmatched recruits — those
    recruits are simply skipped (they won't have college season data anyway).
    Uses PlayerIndex for fast in-memory matching instead of per-row DB scans.
    """
    raw = collector.fetch_recruiting(year)
    if not raw:
        return

    with get_session(db_path) as session:
        matched = skipped = 0
        seen_pids: set[int] = set()

        for recruit in raw:
            name = getattr(recruit, "name", None)
            if not name:
                skipped += 1
                continue

            pid = index.find(name, threshold=88)
            if pid is None or pid in seen_pids:
                skipped += 1
                continue
            seen_pids.add(pid)

            existing = (
                session.query(Recruiting)
                .filter(
                    Recruiting.player_id == pid,
                    Recruiting.recruit_year == year,
                )
                .first()
            )
            if existing is None:
                existing = Recruiting(player_id=pid, recruit_year=year)
                session.add(existing)

            existing.stars = _safe_int(getattr(recruit, "stars", None))
            existing.rating = _safe_float(getattr(recruit, "rating", None))
            existing.ranking_national = _safe_int(getattr(recruit, "ranking", None))
            existing.ranking_position = _safe_int(getattr(recruit, "position_ranking", None))
            existing.state = getattr(recruit, "state_province", None)
            existing.school = getattr(recruit, "school", None)
            existing.classification = getattr(recruit, "recruit_type", None)
            matched += 1

        logger.info(
            "  Recruiting class %d: matched %d / %d (skipped %d unrecognized)",
            year, matched, len(raw), skipped,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate the FF college football database from the CFBD API."
    )
    parser.add_argument(
        "--start-year", type=int, default=2021,
        help="First season year to fetch (default: 2021)"
    )
    parser.add_argument(
        "--end-year", type=int, default=2025,
        help="Last season year to fetch inclusive (default: 2025)"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to SQLite database file (default: from .env FF_DB_PATH)"
    )
    parser.add_argument(
        "--skip-recruiting", action="store_true",
        help="Skip fetching recruiting data (saves ~1 API call per year)"
    )
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    api_key = get_api_key()

    logger.info("Database: %s", db_path)
    logger.info("Year range: %d – %d", args.start_year, args.end_year)

    # Ensure tables exist
    init_db(db_path)

    collector = CFBDCollector(api_key)

    for year in range(args.start_year, args.end_year + 1):
        ingest_year(collector, db_path, year)

    if not args.skip_recruiting:
        # Build index AFTER all ingest_year calls so newly-added players are included
        logger.info("Building player name index for recruiting matching...")
        index = PlayerIndex(db_path)
        for year in range(args.start_year, args.end_year + 1):
            # Recruiting class year = year the player enrolled in college
            # A player in the 2021 season was likely recruited in 2019-2021
            # We ingest the class matching the season year as a reasonable default
            ingest_recruiting(collector, db_path, year, index)

    logger.info("Done. Database saved to %s", db_path)


if __name__ == "__main__":
    main()
