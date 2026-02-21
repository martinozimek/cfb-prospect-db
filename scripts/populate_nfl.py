"""
Populate the database with NFL-side data:
  1. NFL Combine results (measurables, speed score) — from nflverse
  2. NFL Draft picks (round, overall pick, draft capital) — from nflverse
  3. Player height/weight from combine (authoritative for drafted players)
  4. CFBD roster height/weight (for undrafted / non-combine players)
  5. FPI SOS rank + SRS rating per team-season (supplemental strength metrics)

Usage:
    python scripts/populate_nfl.py                          # sane defaults
    python scripts/populate_nfl.py --combine-years 2025     # single year
    python scripts/populate_nfl.py --skip-rosters           # skip slow roster fetch

All operations are idempotent (safe to re-run).
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from rapidfuzz import fuzz, process

from config import get_api_key, get_db_path
from ffdb.collectors.cfbd_collector import CFBDCollector
from ffdb.collectors.pfr_collector import NFLVerseCollector
from ffdb.database import (
    CFBTeamSeason,
    NFLCombineResult,
    NFLDraftPick,
    Player,
    get_session,
    init_db,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Fast in-memory player index (built once, reused for all matching)
# ---------------------------------------------------------------------------

class PlayerIndex:
    """
    Loads all players from the DB into memory once and provides fast
    exact + fuzzy name matching without repeated DB queries.
    """

    def __init__(self, db_path: str):
        self._players: dict[int, Player] = {}          # id → Player (detached)
        self._exact: dict[str, int] = {}               # lowercase name → player_id
        self._candidates: list[tuple[str, int]] = []   # (name, player_id) for fuzzy

        with get_session(db_path) as session:
            for p in session.query(Player).all():
                self._players[p.id] = Player(
                    id=p.id,
                    cfbd_id=p.cfbd_id,
                    full_name=p.full_name,
                    position=p.position,
                    height_inches=p.height_inches,
                    weight_lbs=p.weight_lbs,
                )
                lower = p.full_name.lower()
                self._exact[lower] = p.id
                self._candidates.append((p.full_name, p.id))
                for variant in json.loads(p.name_variants or "[]"):
                    self._exact[variant.lower()] = p.id
                    self._candidates.append((variant, p.id))

        logger.info("PlayerIndex built: %d players, %d name candidates.",
                    len(self._players), len(self._candidates))

    def find(self, name: str, threshold: int = 85) -> Optional[int]:
        """Return player_id for best match, or None if below threshold."""
        if not name:
            return None
        # Exact match first (fast path)
        pid = self._exact.get(name.lower())
        if pid is not None:
            return pid
        # Fuzzy match
        if not self._candidates:
            return None
        names = [c[0] for c in self._candidates]
        pids  = [c[1] for c in self._candidates]
        result = process.extractOne(name, names, scorer=fuzz.WRatio)
        if result and result[1] >= threshold:
            idx = names.index(result[0])
            return pids[idx]
        return None


# ---------------------------------------------------------------------------
# Combine ingestion
# ---------------------------------------------------------------------------

def ingest_combine(
    nflverse: NFLVerseCollector,
    db_path: str,
    year: int,
    index: PlayerIndex,
) -> None:
    rows = nflverse.combine_rows(year, positions=None)
    if not rows:
        return

    with get_session(db_path) as session:
        matched = skipped = 0
        seen_pids: set[int] = set()
        for row in rows:
            pid = index.find(row["name"])
            if pid is None or pid in seen_pids:
                skipped += 1
                continue

            player = session.query(Player).filter(Player.id == pid).first()
            if player is None:
                skipped += 1
                continue

            seen_pids.add(pid)

            # Update Player physical attributes (combine is authoritative)
            if row["height_inches"] and not player.height_inches:
                player.height_inches = row["height_inches"]
            if row["weight_lbs"] and not player.weight_lbs:
                player.weight_lbs = row["weight_lbs"]

            combine = session.query(NFLCombineResult).filter(
                NFLCombineResult.player_id == pid
            ).first()
            if combine is None:
                combine = NFLCombineResult(player_id=pid)
                session.add(combine)

            combine.combine_year = year
            combine.college     = row.get("college")
            combine.position    = row.get("position")
            combine.height_inches = row.get("height_inches")
            combine.weight_lbs  = row.get("weight_lbs")
            combine.forty_time  = row.get("forty_time")
            combine.vertical_jump = row.get("vertical_jump")
            combine.broad_jump  = row.get("broad_jump")
            combine.three_cone  = row.get("three_cone")
            combine.shuttle     = row.get("shuttle")
            combine.bench_press = row.get("bench_press")
            combine.speed_score = row.get("speed_score")
            matched += 1

    logger.info("  Combine %d: matched %d / %d (skipped %d unrecognized)",
                year, matched, len(rows), skipped)


# ---------------------------------------------------------------------------
# Draft ingestion
# ---------------------------------------------------------------------------

def ingest_draft(
    nflverse: NFLVerseCollector,
    db_path: str,
    year: int,
    index: PlayerIndex,
) -> None:
    rows = nflverse.draft_rows(year, positions=None)
    if not rows:
        return

    with get_session(db_path) as session:
        matched = skipped = 0
        seen_pids: set[int] = set()
        for row in rows:
            pid = index.find(row["name"])
            if pid is None or pid in seen_pids:
                skipped += 1
                continue

            player = session.query(Player).filter(Player.id == pid).first()
            if player is None:
                skipped += 1
                continue

            seen_pids.add(pid)

            if not player.position and row.get("position"):
                player.position = row["position"]

            pick = session.query(NFLDraftPick).filter(
                NFLDraftPick.player_id == pid
            ).first()
            if pick is None:
                pick = NFLDraftPick(player_id=pid)
                session.add(pick)

            pick.draft_year        = year
            pick.draft_round       = row.get("draft_round")
            pick.overall_pick      = row.get("overall_pick")
            pick.nfl_team          = row.get("nfl_team")
            pick.position_drafted  = row.get("position")
            pick.draft_capital_score = row.get("draft_capital_score")
            matched += 1

    logger.info("  Draft %d: matched %d / %d (skipped %d unrecognized)",
                year, matched, len(rows), skipped)


# ---------------------------------------------------------------------------
# Roster height/weight ingestion
# ---------------------------------------------------------------------------

def ingest_rosters(cfbd_col: CFBDCollector, db_path: str, year: int) -> None:
    logger.info("Ingesting rosters for %d (height/weight by CFBD ID)...", year)

    teams = cfbd_col.fetch_all_teams(year=year)
    team_names = [t.school for t in teams if t.school]
    logger.info("  Found %d FBS teams.", len(team_names))

    # Build CFBD-ID → player.id map for O(1) lookup
    with get_session(db_path) as session:
        cfbd_id_map: dict[int, int] = {
            p.cfbd_id: p.id
            for p in session.query(Player).filter(Player.cfbd_id.isnot(None)).all()
        }

    total_updated = 0
    for team_name in team_names:
        try:
            roster = cfbd_col.fetch_roster(team=team_name, year=year)
        except Exception as exc:
            logger.warning("  Roster fetch failed for %s: %s", team_name, exc)
            continue

        updates: list[dict] = []
        for p in roster:
            cfbd_id = _safe_int(getattr(p, "id", None))
            if cfbd_id not in cfbd_id_map:
                continue
            ht = _safe_float(getattr(p, "height", None))
            wt = _safe_float(getattr(p, "weight", None))
            city = getattr(p, "home_city", None)
            state = getattr(p, "home_state", None)
            if ht or wt or city or state:
                updates.append({
                    "player_db_id": cfbd_id_map[cfbd_id],
                    "height_inches": ht,
                    "weight_lbs": wt,
                    "hometown": city,
                    "home_state": state,
                })

        if not updates:
            continue

        with get_session(db_path) as session:
            for u in updates:
                player = session.query(Player).filter(Player.id == u["player_db_id"]).first()
                if player is None:
                    continue
                if not player.height_inches and u["height_inches"]:
                    player.height_inches = u["height_inches"]
                if not player.weight_lbs and u["weight_lbs"]:
                    player.weight_lbs = u["weight_lbs"]
                if not player.hometown and u["hometown"]:
                    player.hometown = u["hometown"]
                if not player.home_state and u["home_state"]:
                    player.home_state = u["home_state"]
                total_updated += 1

    logger.info("  Roster %d: updated %d player records.", year, total_updated)


# ---------------------------------------------------------------------------
# Supplemental strength metrics (FPI SOS rank + SRS rating)
# ---------------------------------------------------------------------------

def ingest_strength_metrics(api_key: str, db_path: str, year: int) -> None:
    import cfbd
    cfg = cfbd.Configuration(access_token=api_key)

    logger.info("Ingesting FPI SOS + SRS for %d...", year)
    fpi_by_team: dict[str, int] = {}
    srs_by_team: dict[str, float] = {}

    with cfbd.ApiClient(cfg) as client:
        api = cfbd.RatingsApi(client)
        try:
            for row in (api.get_fpi(year=year) or []):
                resume = getattr(row, "resume_ranks", None)
                sos_rank = getattr(resume, "strength_of_schedule", None) if resume else None
                fpi_by_team[row.team] = _safe_int(sos_rank)
        except Exception as exc:
            logger.warning("  FPI fetch failed: %s", exc)

        try:
            for row in (api.get_srs(year=year) or []):
                srs_by_team[row.team] = _safe_float(getattr(row, "rating", None))
        except Exception as exc:
            logger.warning("  SRS fetch failed: %s", exc)

    with get_session(db_path) as session:
        updated = 0
        for team, sos_rank in fpi_by_team.items():
            ts = session.query(CFBTeamSeason).filter(
                CFBTeamSeason.team == team,
                CFBTeamSeason.season_year == year,
            ).first()
            if ts is None:
                continue
            ts.fpi_sos_rank = sos_rank
            ts.srs_rating   = srs_by_team.get(team)
            updated += 1
        logger.info("  Updated %d team-season strength records for %d.", updated, year)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate DB with NFL combine, draft, roster, and strength data."
    )
    parser.add_argument("--db", type=str, default=None)
    parser.add_argument(
        "--combine-years", type=int, nargs="+",
        default=list(range(2021, 2026)),
        help="Combine years to ingest (default: 2021-2025, matching our college data window)",
    )
    parser.add_argument(
        "--draft-years", type=int, nargs="+",
        default=list(range(2021, 2026)),
        help="Draft years to ingest (default: 2021-2025)",
    )
    parser.add_argument(
        "--roster-years", type=int, nargs="+",
        default=[2024],
        help="Roster years to fetch for height/weight (default: 2024 only)",
    )
    parser.add_argument(
        "--strength-years", type=int, nargs="+",
        default=list(range(2021, 2025)),
        help="Years to fetch FPI SOS + SRS for (default: 2021-2024)",
    )
    parser.add_argument("--skip-combine",  action="store_true")
    parser.add_argument("--skip-draft",    action="store_true")
    parser.add_argument("--skip-rosters",  action="store_true")
    parser.add_argument("--skip-strength", action="store_true")
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    api_key = get_api_key()
    init_db(db_path)

    # Build in-memory player index once
    logger.info("Building player name index...")
    index = PlayerIndex(db_path)

    nflverse = NFLVerseCollector()
    cfbd_col = CFBDCollector(api_key)

    if not args.skip_combine:
        logger.info("=== Combine ingestion ===")
        for year in args.combine_years:
            ingest_combine(nflverse, db_path, year, index)

    if not args.skip_draft:
        logger.info("=== Draft pick ingestion ===")
        for year in args.draft_years:
            ingest_draft(nflverse, db_path, year, index)

    if not args.skip_rosters:
        logger.info("=== Roster ingestion ===")
        for year in args.roster_years:
            ingest_rosters(cfbd_col, db_path, year)

    if not args.skip_strength:
        logger.info("=== Strength metrics ingestion ===")
        for year in args.strength_years:
            ingest_strength_metrics(api_key, db_path, year)

    logger.info("Done.")


if __name__ == "__main__":
    main()
