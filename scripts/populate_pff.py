"""
Ingest PFF (Pro Football Focus) college player season data into the ff.db database.

This script is a STUB — it documents the expected CSV format and implements the full
ingestion pipeline so that when a PFF+ subscription is purchased, data can be loaded
with a single command.

SUBSCRIPTION REQUIRED
---------------------
PFF+ costs ~$120/yr and provides downloadable CSV exports for college players:
  https://www.pff.com/college-football/stats/receiving

Expected export workflow:
  1. Navigate to PFF → College → Stats → Receiving
  2. Set filters: position=WR (repeat for TE, RB), seasons=2014-present, min_snaps=50
  3. Export CSV for each position × season
  4. Concatenate exports, then run:

     python scripts/populate_pff.py --csv pff_export.csv

All operations are idempotent (safe to re-run; rows are upserted by player_id + season_year).

EXPECTED CSV COLUMNS (PFF export format, ~2024)
-----------------------------------------------
Required:
  player          — full name (used for fuzzy matching to players table)
  position        — "WR", "TE", or "RB"
  team_name       — college team (used with player + year to disambiguate)
  season          — season year (integer)

Volume (all raw counts):
  routes          — routes_run
  targets         — targets (actual charted targets)
  receptions      — receptions
  yards           — rec_yards
  tds             — rec_tds
  games           — games_played

Efficiency:
  yards_per_route_run     — yprr
  yards_per_target        — yards_per_target (if exported; else computed)
  yards_per_reception     — yards_per_reception (if exported; else computed)
  catch_pct               — catch_rate (receptions / targets)

Grades:
  grades_recv_route       — receiving_grade (PFF 0-100)
  grades_route            — route_grade (PFF 0-100) [optional]

Separation & Depth:
  avg_depth_of_target     — avg_depth_of_target (ADOT)
  avg_yards_after_catch   — yac_per_rec (per reception)
  yards_after_catch       — yards_after_catch (total)

Discipline:
  drops                   — drops
  drop_pct                — drop_rate (drops / targets)

Contested:
  contested_targets       — contested_targets
  contested_catches       — contested_catches
  contested_catch_pct     — contested_catch_rate

Note: PFF column names vary slightly across exports and seasons. The mapper below
handles the most common variants. Add aliases to _COL_MAP as needed.

Usage:
    python scripts/populate_pff.py --csv pff_export.csv
    python scripts/populate_pff.py --csv pff_export.csv --dry-run
    python scripts/populate_pff.py --csv pff_export.csv --min-routes 50
    python scripts/populate_pff.py --csv pff_export.csv --wipe
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_db_path
from ffdb.database import (
    PFFPlayerSeason,
    Player,
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
# Column name map: PFF export header → our canonical field name
# Add variants here when PFF changes export format between seasons.
# ---------------------------------------------------------------------------
_COL_MAP = {
    # Identity
    "player":               "player",
    "player_name":          "player",
    "position":             "position",
    "team_name":            "team",
    "team":                 "team",
    "season":               "season_year",
    "year":                 "season_year",

    # Volume
    "routes":               "routes_run",
    "routes_run":           "routes_run",
    "targets":              "targets",
    "receptions":           "receptions",
    "yards":                "rec_yards",
    "receiving_yards":      "rec_yards",
    "tds":                  "rec_tds",
    "receiving_tds":        "rec_tds",
    "games":                "games_played",
    "games_played":         "games_played",

    # Efficiency
    "yards_per_route_run":  "yprr",
    "yprr":                 "yprr",
    "yards_per_target":     "yards_per_target",
    "yards_per_reception":  "yards_per_reception",
    "catch_pct":            "catch_rate",
    "catch_rate":           "catch_rate",

    # Grades
    "grades_recv_route":    "receiving_grade",
    "recv_grade":           "receiving_grade",
    "receiving_grade":      "receiving_grade",
    "grades_route":         "route_grade",
    "route_grade":          "route_grade",

    # Separation / depth
    "avg_depth_of_target":  "avg_depth_of_target",
    "adot":                 "avg_depth_of_target",
    "avg_yards_after_catch":"yac_per_rec",
    "yac_per_rec":          "yac_per_rec",
    "yards_after_catch":    "yards_after_catch",
    "total_yac":            "yards_after_catch",

    # Discipline
    "drops":                "drops",
    "drop_pct":             "drop_rate",
    "drop_rate":            "drop_rate",

    # Contested
    "contested_targets":    "contested_targets",
    "contested_catches":    "contested_catches",
    "contested_catch_pct":  "contested_catch_rate",
    "contested_catch_rate": "contested_catch_rate",
}


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val not in (None, "", "nan", "N/A") else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(float(val)) if val not in (None, "", "nan", "N/A") else None
    except (TypeError, ValueError):
        return None


def _normalize_row(raw: dict) -> dict:
    """Map PFF export columns to our canonical field names."""
    out = {}
    for k, v in raw.items():
        canonical = _COL_MAP.get(k.lower().strip())
        if canonical:
            out[canonical] = v
    return out


def _compute_derived(row: dict) -> dict:
    """Fill in derivable fields if not present in the export."""
    routes = _safe_int(row.get("routes_run"))
    games = _safe_int(row.get("games_played"))
    targets = _safe_int(row.get("targets"))
    receptions = _safe_int(row.get("receptions"))
    rec_yards_raw = _safe_int(row.get("rec_yards"))
    rec_tds = _safe_int(row.get("rec_tds"))
    drops = _safe_int(row.get("drops"))
    cont_targets = _safe_int(row.get("contested_targets"))
    cont_catches = _safe_int(row.get("contested_catches"))

    if row.get("yprr") is None and rec_yards_raw and routes:
        row["yprr"] = round(rec_yards_raw / routes, 4)

    if row.get("yards_per_target") is None and rec_yards_raw and targets:
        row["yards_per_target"] = round(rec_yards_raw / targets, 4)

    if row.get("yards_per_reception") is None and rec_yards_raw and receptions:
        row["yards_per_reception"] = round(rec_yards_raw / receptions, 4)

    if row.get("catch_rate") is None and receptions is not None and targets:
        row["catch_rate"] = round(receptions / targets, 4)

    if row.get("td_rate") is None and rec_tds is not None and targets:
        row["td_rate"] = round(rec_tds / targets, 4)

    if row.get("drop_rate") is None and drops is not None and targets:
        row["drop_rate"] = round(drops / targets, 4)

    if row.get("contested_catch_rate") is None and cont_catches is not None and cont_targets:
        row["contested_catch_rate"] = round(cont_catches / cont_targets, 4)

    if row.get("routes_per_game") is None and routes and games:
        row["routes_per_game"] = round(routes / games, 2)

    return row


def ingest_csv(csv_path: str, db_path: str, min_routes: int = 30,
               dry_run: bool = False, wipe: bool = False) -> None:
    import csv

    logger.info("Reading %s", csv_path)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    logger.info("Read %d rows from CSV", len(raw_rows))

    init_db(db_path)

    with get_session(db_path) as session:
        if wipe:
            deleted = session.query(PFFPlayerSeason).delete()
            logger.warning("Wiped %d existing PFF rows", deleted)

        # Build player index for fuzzy matching
        index = PlayerIndex(session)

        inserted = updated = skipped = unmatched = 0

        for raw in raw_rows:
            row = _normalize_row(raw)

            player_name = row.get("player", "").strip()
            season_year = _safe_int(row.get("season_year"))
            position = row.get("position", "").strip().upper()
            team = row.get("team", "").strip()

            if not player_name or not season_year:
                skipped += 1
                continue

            routes = _safe_int(row.get("routes_run"))
            if routes is not None and routes < min_routes:
                skipped += 1
                continue

            # Fuzzy match to players table
            matches = index.find(player_name, threshold=80)
            player_id = None
            for candidate, score in matches:
                # Prefer position match
                if candidate.position and position and candidate.position != position:
                    continue
                player_id = candidate.id
                break
            if player_id is None and matches:
                player_id = matches[0][0].id  # fall back to best name match

            if player_id is None:
                logger.debug("No match for %r (%s %s)", player_name, position, season_year)
                unmatched += 1
                continue

            row = _compute_derived(row)

            # Upsert
            existing = (
                session.query(PFFPlayerSeason)
                .filter_by(player_id=player_id, season_year=season_year)
                .first()
            )
            if existing is None:
                obj = PFFPlayerSeason(
                    player_id=player_id,
                    season_year=season_year,
                    team=team or None,
                    position=position or None,
                )
                session.add(obj)
                inserted += 1
            else:
                obj = existing
                updated += 1

            # Write all numeric fields
            for field in [
                "routes_run", "targets", "receptions", "rec_yards", "rec_tds",
                "games_played", "yprr", "yards_per_target", "yards_per_reception",
                "catch_rate", "td_rate", "receiving_grade", "route_grade",
                "target_separation", "avg_depth_of_target", "yards_after_catch",
                "yac_per_rec", "drops", "drop_rate", "contested_targets",
                "contested_catches", "contested_catch_rate", "routes_per_game",
            ]:
                raw_val = row.get(field)
                if raw_val not in (None, "", "nan"):
                    int_fields = {"routes_run", "targets", "receptions", "rec_yards",
                                  "rec_tds", "games_played", "drops", "contested_targets",
                                  "contested_catches"}
                    setattr(obj, field, _safe_int(raw_val) if field in int_fields
                            else _safe_float(raw_val))

        if dry_run:
            session.rollback()
            logger.info("DRY RUN — no changes written")
        else:
            logger.info(
                "PFF ingest complete: %d inserted, %d updated, %d skipped (low routes), "
                "%d unmatched", inserted, updated, skipped, unmatched
            )

    logger.info("Done. DB: %s", db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest PFF college season CSV into ff.db")
    parser.add_argument("--csv", required=True, help="Path to PFF export CSV file")
    parser.add_argument(
        "--min-routes", type=int, default=30,
        help="Minimum routes run to include a player-season (default: 30)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and match without writing to the database"
    )
    parser.add_argument(
        "--wipe", action="store_true",
        help="Delete all existing PFF rows before ingesting (fresh load)"
    )
    args = parser.parse_args()

    db_path = get_db_path()
    ingest_csv(
        csv_path=args.csv,
        db_path=db_path,
        min_routes=args.min_routes,
        dry_run=args.dry_run,
        wipe=args.wipe,
    )


if __name__ == "__main__":
    main()
