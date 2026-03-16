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
    PFFQBSeason,
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
    "routes":                           "routes_run",
    "routes_run":                       "routes_run",
    "targets":                          "targets",
    "receptions":                       "receptions",
    "yards":                            "rec_yards",
    "receiving_yards":                  "rec_yards",
    "tds":                              "rec_tds",
    "touchdowns":                       "rec_tds",           # PFF export uses 'touchdowns'
    "receiving_tds":                    "rec_tds",
    "games":                            "games_played",
    "games_played":                     "games_played",
    "player_game_count":                "games_played",      # PFF export uses 'player_game_count'

    # Efficiency
    "yards_per_route_run":              "yprr",
    "yprr":                             "yprr",
    "yards_per_target":                 "yards_per_target",
    "yards_per_reception":              "yards_per_reception",
    "catch_pct":                        "catch_rate",
    "catch_rate":                       "catch_rate",
    "caught_percent":                   "catch_rate",        # PFF export uses 'caught_percent'

    # Grades — PFF export uses 'grades_pass_route' for route-running/receiving grade
    "grades_pass_route":                "receiving_grade",   # PFF actual column name
    "grades_recv_route":                "receiving_grade",   # legacy alias
    "recv_grade":                       "receiving_grade",
    "receiving_grade":                  "receiving_grade",
    "grades_route":                     "route_grade",
    "route_grade":                      "route_grade",
    "grades_offense":                   "route_grade",       # fallback: overall offense grade

    # Separation / depth
    "avg_depth_of_target":              "avg_depth_of_target",
    "adot":                             "avg_depth_of_target",
    "avg_yards_after_catch":            "yac_per_rec",
    "yards_after_catch_per_reception":  "yac_per_rec",       # PFF export variant
    "yac_per_rec":                      "yac_per_rec",
    "yards_after_catch":                "yards_after_catch",
    "total_yac":                        "yards_after_catch",

    # Discipline
    "drops":                            "drops",
    "drop_pct":                         "drop_rate",
    "drop_rate":                        "drop_rate",

    # Contested
    "contested_targets":                "contested_targets",
    "contested_catches":                "contested_catches",
    "contested_receptions":             "contested_catches", # PFF export variant
    "contested_catch_pct":              "contested_catch_rate",
    "contested_catch_rate":             "contested_catch_rate",
}

# Column map for rushing exports
_RUSH_COL_MAP = {
    "player":           "player",
    "player_name":      "player",
    "position":         "position",
    "team_name":        "team",
    "team":             "team",
    "season":           "season_year",
    "year":             "season_year",
    "grades_run":       "rush_grade",
    "attempts":         "rush_attempts",
    "yards":            "rush_yards",
    "touchdowns":       "rush_tds",
    "ypa":              "rush_ypa",
    "yards_after_contact": "yards_after_contact",
    "yco_attempt":      "yco_attempt",
    "avoided_tackles":  "avoided_tackles_rush",
    "elusive_rating":   "elusive_rating",
    "breakaway_percent":"breakaway_percent",
    "run_plays":        "run_plays",
    "grades_offense":   "offense_grade",
    # Also capture receiving metrics that appear in rushing export
    "routes":           "routes_run",
    "routes_run":       "routes_run",
    "receptions":       "receptions",
    "rec_yards":        "rec_yards",
    "yprr":             "yprr",
    "targets":          "targets",
    "player_game_count":"games_played",
}

# Column map for QB passing exports
_PASS_COL_MAP = {
    "player":               "player",
    "player_name":          "player",
    "position":             "position",
    "team_name":            "team",
    "team":                 "team",
    "season":               "season_year",
    "year":                 "season_year",
    "grades_pass":          "passing_grade",
    "completion_percent":   "completion_percent",
    "qb_rating":            "qb_rating",
    "btt_rate":             "btt_rate",
    "twp_rate":             "twp_rate",
    "avg_depth_of_target":  "avg_depth_of_target",
    "avg_time_to_throw":    "avg_time_to_throw",
    "ypa":                  "ypa",
    "dropbacks":            "dropbacks",
    "attempts":             "attempts",
    "yards":                "pass_yards",
    "touchdowns":           "pass_tds",
    "interceptions":        "interceptions",
    "player_game_count":    "games_played",
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


def _normalize_row(raw: dict, col_map: dict = None) -> dict:
    """Map PFF export columns to our canonical field names."""
    if col_map is None:
        col_map = _COL_MAP
    out = {}
    for k, v in raw.items():
        canonical = col_map.get(k.lower().strip())
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


def _ingest_qb_csv(raw_rows: list, db_path: str, fname_year: Optional[int],
                   dry_run: bool = False) -> None:
    """Ingest QB passing CSV rows into pff_qb_seasons table."""
    init_db(db_path)
    inserted = updated = skipped = 0
    with get_session(db_path) as session:
        _seen: set[tuple] = set()
        for raw in raw_rows:
            row = _normalize_row(raw, _PASS_COL_MAP)
            player_name = row.get("player", "").strip()
            season_year = _safe_int(row.get("season_year")) or fname_year
            if not player_name or not season_year:
                skipped += 1
                continue
            key = (player_name, season_year)
            if key in _seen:
                skipped += 1
                continue
            _seen.add(key)

            attempts = _safe_int(row.get("attempts"))
            if attempts is not None and attempts < 100:
                skipped += 1
                continue

            if not dry_run:
                existing = (
                    session.query(PFFQBSeason)
                    .filter_by(player_name=player_name, season_year=season_year)
                    .first()
                )
                if existing is None:
                    obj = PFFQBSeason(player_name=player_name, season_year=season_year,
                                      team=row.get("team") or None)
                    session.add(obj)
                    inserted += 1
                else:
                    obj = existing
                    updated += 1
                for field in ["passing_grade", "completion_percent", "qb_rating",
                               "btt_rate", "twp_rate", "avg_depth_of_target",
                               "avg_time_to_throw", "ypa"]:
                    val = row.get(field)
                    if val not in (None, "", "nan"):
                        setattr(obj, field, float(val))
                for field in ["dropbacks", "attempts", "pass_yards", "pass_tds", "interceptions"]:
                    val = row.get(field)
                    if val not in (None, "", "nan"):
                        setattr(obj, field, int(float(val)))
    logger.info("QB ingest complete: %d inserted, %d updated, %d skipped", inserted, updated, skipped)


def _ingest_variant_csv(raw_rows: list, db_path: str, fname_year: Optional[int],
                        variant: str, dry_run: bool = False) -> None:
    """
    Ingest a PFF split CSV (receiving-depth/concept/scheme) into the
    depth_data / concept_data / scheme_data JSON columns on PFFPlayerSeason.

    Matches rows by PFF player_id + season_year to existing PFFPlayerSeason
    rows. Only writes if the target column is currently NULL (use --force
    to overwrite; not yet implemented — re-run with --wipe to reset).

    Each row is serialized as a sub-dict keyed by the split label derived
    from the filename or a 'split_type' column if present.  Because split
    CSVs contain one row per player per season (all splits concatenated as
    wide columns), we store the full column dict minus identity fields.
    """
    import json
    IDENTITY_COLS = {"player_id", "player", "player_name", "position",
                     "team_name", "team", "season", "year", "season_year",
                     "franchise_id"}
    column_map = {"depth": "depth_data", "concept": "concept_data",
                  "scheme": "scheme_data"}
    db_column = column_map[variant]

    init_db(db_path)
    from ffdb.utils.player_index import PlayerIndex
    index = PlayerIndex(db_path)
    updated = skipped = unmatched = 0

    with get_session(db_path) as session:
        for raw in raw_rows:
            player_name = (raw.get("player") or raw.get("player_name") or "").strip()
            season_year = _safe_int(raw.get("season") or raw.get("year")) or fname_year
            if not player_name or not season_year:
                skipped += 1
                continue

            # Build split payload — all non-identity columns
            payload = {k: v for k, v in raw.items()
                       if k.lower() not in IDENTITY_COLS and v not in ("", None)}
            if not payload:
                skipped += 1
                continue

            player_id = index.find(player_name, threshold=80)
            if player_id is None:
                unmatched += 1
                continue

            obj = (
                session.query(PFFPlayerSeason)
                .filter_by(player_id=player_id, season_year=season_year)
                .first()
            )
            if obj is None:
                unmatched += 1
                continue

            current = getattr(obj, db_column)
            if current is not None:
                skipped += 1  # already populated — skip
                continue

            if not dry_run:
                setattr(obj, db_column, json.dumps(payload))
            updated += 1

    logger.info(
        "Variant %s ingest complete: %d updated, %d skipped, %d unmatched",
        variant, updated, skipped, unmatched
    )


def _ingest_qb_variant_csv(raw_rows: list, db_path: str, fname_year: Optional[int],
                            variant: str, dry_run: bool = False) -> None:
    """
    Ingest PFF QB split CSVs (passing-depth/pressure/concept) into the
    pass_depth_data / pass_pressure_data / pass_concept_data JSON columns
    on PFFQBSeason.
    """
    import json
    IDENTITY_COLS = {"player_id", "player", "player_name", "position",
                     "team_name", "team", "season", "year", "season_year",
                     "franchise_id"}
    column_map = {"depth": "pass_depth_data", "pressure": "pass_pressure_data",
                  "concept": "pass_concept_data"}
    db_column = column_map[variant]

    init_db(db_path)
    updated = skipped = unmatched = 0

    with get_session(db_path) as session:
        for raw in raw_rows:
            player_name = (raw.get("player") or raw.get("player_name") or "").strip()
            season_year = _safe_int(raw.get("season") or raw.get("year")) or fname_year
            if not player_name or not season_year:
                skipped += 1
                continue

            payload = {k: v for k, v in raw.items()
                       if k.lower() not in IDENTITY_COLS and v not in ("", None)}
            if not payload:
                skipped += 1
                continue

            obj = (
                session.query(PFFQBSeason)
                .filter_by(player_name=player_name, season_year=season_year)
                .first()
            )
            if obj is None:
                unmatched += 1
                continue

            current = getattr(obj, db_column)
            if current is not None:
                skipped += 1
                continue

            if not dry_run:
                setattr(obj, db_column, json.dumps(payload))
            updated += 1

    logger.info(
        "QB variant %s ingest complete: %d updated, %d skipped, %d unmatched",
        variant, updated, skipped, unmatched
    )


def ingest_variant_batch(csv_paths: list, db_path: str, dry_run: bool = False) -> None:
    """
    Batch ingest multiple PFF split/variant CSVs (receiving-depth/concept/scheme
    and passing-depth/pressure/concept) using a single shared PlayerIndex.

    Much faster than calling ingest_csv() per file because:
    - PlayerIndex is built once (~1s)
    - name→player_id lookups are cached across files (same players appear in
      many year-files, so we pay the fuzzy-match cost only once per unique name)

    Each CSV path must follow the naming convention:
        pff_{base_type}-{variant}_{POS}_{YEAR}.csv
    """
    import csv as csv_mod
    import json
    import re

    IDENTITY_COLS = frozenset({"player_id", "player", "player_name", "position",
                                "team_name", "team", "season", "year",
                                "season_year", "franchise_id"})
    PLAYER_COL_MAP = {"depth": "depth_data", "concept": "concept_data",
                      "scheme": "scheme_data"}
    QB_COL_MAP = {"depth": "pass_depth_data", "pressure": "pass_pressure_data",
                  "concept": "pass_concept_data"}

    init_db(db_path)
    from ffdb.utils.player_index import PlayerIndex
    index = PlayerIndex(db_path)
    name_cache: dict[str, Optional[int]] = {}  # player_name → internal player_id

    total_updated = total_skipped = total_unmatched = 0

    for csv_path in csv_paths:
        m = re.search(
            r"pff_(receiving|rushing|passing)-(depth|concept|scheme|pressure)_\w+_(\d{4})\.csv$",
            str(csv_path), re.IGNORECASE
        )
        if not m:
            logger.warning("Skipping unrecognised variant file: %s", csv_path)
            continue
        base_type = m.group(1).lower()
        variant   = m.group(2).lower()
        fname_year = int(m.group(3))

        is_qb = (base_type == "passing")
        db_column = QB_COL_MAP.get(variant) if is_qb else PLAYER_COL_MAP.get(variant)
        if db_column is None:
            logger.warning("Unknown variant %r in %s — skipping", variant, csv_path)
            continue

        logger.info("[BATCH] %s  →  %s", Path(csv_path).name, db_column)

        with open(csv_path, newline="", encoding="utf-8") as f:
            raw_rows = list(csv_mod.DictReader(f))

        updated = skipped = unmatched = 0
        with get_session(db_path) as session:
            for raw in raw_rows:
                player_name = (raw.get("player") or raw.get("player_name") or "").strip()
                season_year = _safe_int(raw.get("season") or raw.get("year")) or fname_year
                if not player_name or not season_year:
                    skipped += 1
                    continue

                payload = {k: v for k, v in raw.items()
                           if k.lower() not in IDENTITY_COLS and v not in ("", None)}
                if not payload:
                    skipped += 1
                    continue

                # Use cached lookup for repeated player names across year-files
                if player_name not in name_cache:
                    name_cache[player_name] = index.find(player_name, threshold=80)
                player_id = name_cache[player_name]

                if player_id is None:
                    unmatched += 1
                    continue

                if is_qb:
                    obj = (
                        session.query(PFFQBSeason)
                        .filter_by(player_name=player_name, season_year=season_year)
                        .first()
                    )
                else:
                    obj = (
                        session.query(PFFPlayerSeason)
                        .filter_by(player_id=player_id, season_year=season_year)
                        .first()
                    )
                if obj is None:
                    unmatched += 1
                    continue

                current = getattr(obj, db_column)
                if current is not None:
                    skipped += 1
                    continue

                if not dry_run:
                    setattr(obj, db_column, json.dumps(payload))
                updated += 1

        logger.info(
            "  %s: %d updated, %d skipped, %d unmatched",
            Path(csv_path).name, updated, skipped, unmatched
        )
        total_updated += updated
        total_skipped += skipped
        total_unmatched += unmatched

    logger.info(
        "Batch complete: %d updated, %d skipped, %d unmatched across %d files",
        total_updated, total_skipped, total_unmatched, len(csv_paths)
    )


def ingest_csv(csv_path: str, db_path: str, min_routes: int = 30,
               dry_run: bool = False, wipe: bool = False) -> None:
    import csv, re

    logger.info("Reading %s", csv_path)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    logger.info("Read %d rows from CSV", len(raw_rows))

    # Detect stat_type and optional variant from filename:
    #   pff_{base_type}[-{variant}]_{POS}_{YEAR}.csv
    # e.g. pff_receiving_WR_2025.csv, pff_receiving-depth_WR_2025.csv
    _stat_type = "receiving"
    _variant: Optional[str] = None
    _fname_year: Optional[int] = None
    m = re.search(
        r"pff_(receiving|rushing|passing)(?:-(depth|concept|scheme|pressure))?_\w+_(\d{4})\.csv$",
        str(csv_path), re.IGNORECASE
    )
    if m:
        _stat_type = m.group(1).lower()
        _variant   = m.group(2).lower() if m.group(2) else None
        _fname_year = int(m.group(3))
    else:
        # Legacy filename: pff_{POS}_{YEAR}.csv
        m2 = re.search(r"_(\d{4})\.csv$", str(csv_path), re.IGNORECASE)
        if m2:
            _fname_year = int(m2.group(1))
    logger.info("Stat type: %s | Variant: %s | Year from filename: %s",
                _stat_type, _variant, _fname_year)

    # Dispatch to variant ingest for split CSVs
    if _variant in ("depth", "concept", "scheme"):
        _ingest_variant_csv(raw_rows, db_path, _fname_year, _variant, dry_run=dry_run)
        return

    # Dispatch to QB variant ingest for QB split CSVs
    if _stat_type == "passing" and _variant in ("depth", "pressure", "concept"):
        _ingest_qb_variant_csv(raw_rows, db_path, _fname_year, _variant, dry_run=dry_run)
        return

    # Dispatch to QB ingest for passing CSVs
    if _stat_type == "passing":
        _ingest_qb_csv(raw_rows, db_path, _fname_year, dry_run=dry_run)
        return

    init_db(db_path)

    with get_session(db_path) as session:
        if wipe:
            deleted = session.query(PFFPlayerSeason).delete()
            logger.warning("Wiped %d existing PFF rows", deleted)

        # Build player index for fuzzy matching
        index = PlayerIndex(db_path)

        inserted = updated = skipped = unmatched = 0
        _seen: set[tuple] = set()  # (player_id, season_year) dedup within this CSV

        col_map = _RUSH_COL_MAP if _stat_type == "rushing" else _COL_MAP
        for raw in raw_rows:
            row = _normalize_row(raw, col_map)

            player_name = row.get("player", "").strip()
            season_year = _safe_int(row.get("season_year")) or _fname_year
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
            player_id = index.find(player_name, threshold=80)

            if player_id is None:
                logger.debug("No match for %r (%s %s)", player_name, position, season_year)
                unmatched += 1
                continue

            # Skip duplicate (player_id, season_year) within this CSV
            key = (player_id, season_year)
            if key in _seen:
                skipped += 1
                continue
            _seen.add(key)

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

            # Write all numeric fields — receiving + extended + rushing (skips None/empty)
            _RECEIVING_FIELDS = [
                "routes_run", "targets", "receptions", "rec_yards", "rec_tds",
                "games_played", "yprr", "yards_per_target", "yards_per_reception",
                "catch_rate", "td_rate", "receiving_grade", "route_grade",
                "target_separation", "avg_depth_of_target", "yards_after_catch",
                "yac_per_rec", "drops", "drop_rate", "contested_targets",
                "contested_catches", "contested_catch_rate", "routes_per_game",
                "offense_grade", "pass_block_grade", "slot_rate", "wide_rate",
                "inline_rate", "targeted_qb_rating",
            ]
            _RUSHING_FIELDS = [
                "rush_grade", "rush_attempts", "rush_yards", "rush_tds", "rush_ypa",
                "yards_after_contact", "yco_attempt", "avoided_tackles_rush",
                "elusive_rating", "breakaway_percent", "run_plays",
                # Also update receiving fields that appear in rushing export
                "routes_run", "receptions", "rec_yards", "yprr", "targets", "games_played",
            ]
            all_fields = _RUSHING_FIELDS if _stat_type == "rushing" else _RECEIVING_FIELDS
            int_fields = {"routes_run", "targets", "receptions", "rec_yards", "rec_tds",
                          "games_played", "drops", "contested_targets", "contested_catches",
                          "rush_attempts", "rush_yards", "rush_tds",
                          "yards_after_contact", "avoided_tackles_rush", "run_plays"}
            for field in all_fields:
                raw_val = row.get(field)
                if raw_val not in (None, "", "nan"):
                    if field in int_fields:
                        setattr(obj, field, _safe_int(raw_val))
                    else:
                        setattr(obj, field, _safe_float(raw_val))

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
    parser.add_argument("--csv", help="Path to PFF export CSV file")
    parser.add_argument(
        "--batch-variants", nargs="+", metavar="CSV",
        help="Batch-ingest multiple variant CSVs (depth/concept/scheme/pressure) "
             "using a shared PlayerIndex (faster than --csv per file)"
    )
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

    if args.batch_variants:
        ingest_variant_batch(
            csv_paths=args.batch_variants,
            db_path=db_path,
            dry_run=args.dry_run,
        )
        return

    if not args.csv:
        parser.error("Either --csv or --batch-variants is required")

    ingest_csv(
        csv_path=args.csv,
        db_path=db_path,
        min_routes=args.min_routes,
        dry_run=args.dry_run,
        wipe=args.wipe,
    )


if __name__ == "__main__":
    main()
