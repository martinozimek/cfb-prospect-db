"""
Master refresh orchestrator for cfb-prospect-db.

Checks all data sources for new data and re-ingests as needed.
All underlying populate_* scripts are idempotent — safe to re-run.

Usage:
    python scripts/refresh.py                    # smart refresh (new data only)
    python scripts/refresh.py --full             # wipe log and re-ingest everything
    python scripts/refresh.py --check            # check for updates, no writes (exit 1 if updates available)
    python scripts/refresh.py --source=combine   # refresh one source only
    python scripts/refresh.py --source=seasons --years 2024 2025

Sources: seasons, combine, draft, rosters, strength, recruiting
"""

import argparse
import logging
import sys
import urllib.request
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_api_key, get_db_path
from ffdb.database import DataIngestionLog, get_session, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------

ALL_SOURCES = ["seasons", "recruiting", "combine", "draft", "rosters", "strength"]

# nflverse GitHub release tags to check for freshness
_NFLVERSE_RELEASE_TAGS = {
    "combine": "combine",
    "draft":   "draft_picks",
}
_NFLVERSE_API = "https://api.github.com/repos/nflverse/nflverse-data/releases/tags/{tag}"


# ---------------------------------------------------------------------------
# Log helpers
# ---------------------------------------------------------------------------

def _get_last_run(db_path: str, source: str, scope: str) -> Optional[datetime]:
    """Return last successful run time (UTC) for a source+scope, or None."""
    with get_session(db_path) as session:
        row = (
            session.query(DataIngestionLog)
            .filter(
                DataIngestionLog.source == source,
                DataIngestionLog.scope == scope,
                DataIngestionLog.status == "ok",
            )
            .order_by(DataIngestionLog.last_run_utc.desc())
            .first()
        )
        if row and row.last_run_utc:
            dt = row.last_run_utc
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    return None


def _log_run(
    db_path: str,
    source: str,
    scope: str,
    status: str = "ok",
    rows_affected: Optional[int] = None,
    notes: Optional[str] = None,
) -> None:
    with get_session(db_path) as session:
        entry = DataIngestionLog(
            source=source,
            scope=scope,
            last_run_utc=datetime.now(timezone.utc),
            rows_affected=rows_affected,
            status=status,
            notes=notes,
        )
        session.add(entry)


# ---------------------------------------------------------------------------
# nflverse freshness check
# ---------------------------------------------------------------------------

def _nflverse_updated_at(tag: str) -> Optional[datetime]:
    """Fetch the `updated_at` timestamp for a nflverse GitHub release."""
    url = _NFLVERSE_API.format(tag=tag)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "cfb-prospect-db/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        # The release itself has an `published_at` field
        ts = data.get("published_at") or data.get("created_at")
        if ts:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as exc:
        logger.warning("  Could not check nflverse release for '%s': %s", tag, exc)
    return None


def _cfbd_has_new_season(current_max_year: int) -> bool:
    """Return True if CFBD likely has data for a year we haven't ingested."""
    current_year = datetime.now().year
    # College football season data for year Y is available from ~Aug Y onward
    if current_year > current_max_year:
        return True
    # If it's past August of the current max year, data may have been updated
    if datetime.now().month >= 8:
        return True
    return False


# ---------------------------------------------------------------------------
# Check mode — print status table, no writes
# ---------------------------------------------------------------------------

def check_updates(db_path: str, years: list[int]) -> bool:
    """
    Print a status table of each source vs its last ingestion.
    Returns True if any updates are available.
    """
    updates_available = False
    print("\n{:<18} {:<22} {:<26} {}".format("Source", "Scope", "Last Run (UTC)", "Status"))
    print("-" * 80)

    # College seasons
    for year in years:
        scope = f"year={year}"
        last = _get_last_run(db_path, "cfbd_seasons", scope)
        last_str = last.strftime("%Y-%m-%d %H:%M") if last else "never"
        flag = "up to date" if last else "NEEDS INGEST"
        if not last:
            updates_available = True
        print(f"  {'seasons':<16} {scope:<22} {last_str:<26} {flag}")

    # nflverse combine
    for tag_key, tag_val in _NFLVERSE_RELEASE_TAGS.items():
        scope = "all"
        last = _get_last_run(db_path, f"nflverse_{tag_key}", scope)
        last_str = last.strftime("%Y-%m-%d %H:%M") if last else "never"
        remote_ts = _nflverse_updated_at(tag_val)
        if remote_ts and last and remote_ts > last:
            flag = f"UPDATED {remote_ts.strftime('%Y-%m-%d')}"
            updates_available = True
        elif not last:
            flag = "NEEDS INGEST"
            updates_available = True
        else:
            flag = "up to date"
        print(f"  {tag_key:<16} {scope:<22} {last_str:<26} {flag}")

    print()
    return updates_available


# ---------------------------------------------------------------------------
# Per-source refresh functions
# ---------------------------------------------------------------------------

def refresh_seasons(db_path: str, api_key: str, years: list[int], force: bool) -> None:
    from scripts.populate_db import ingest_year, ingest_recruiting
    from ffdb.collectors.cfbd_collector import CFBDCollector
    from ffdb.utils.player_index import PlayerIndex

    collector = CFBDCollector(api_key)
    for year in years:
        scope = f"year={year}"
        if not force and _get_last_run(db_path, "cfbd_seasons", scope):
            logger.info("  seasons %d: already ingested, skipping (use --full to force)", year)
            continue
        logger.info("  Ingesting college seasons for %d...", year)
        try:
            ingest_year(collector, db_path, year)
            _log_run(db_path, "cfbd_seasons", scope, status="ok")
        except Exception as exc:
            logger.error("  seasons %d failed: %s", year, exc)
            _log_run(db_path, "cfbd_seasons", scope, status="error", notes=str(exc))

    # Recruiting pass (after all season data is loaded)
    logger.info("  Building player index for recruiting...")
    index = PlayerIndex(db_path)
    for year in years:
        scope = f"year={year}"
        if not force and _get_last_run(db_path, "cfbd_recruiting", scope):
            continue
        try:
            ingest_recruiting(collector, db_path, year, index)
            _log_run(db_path, "cfbd_recruiting", scope, status="ok")
        except Exception as exc:
            logger.error("  recruiting %d failed: %s", year, exc)
            _log_run(db_path, "cfbd_recruiting", scope, status="error", notes=str(exc))


def refresh_combine(db_path: str, years: list[int], force: bool) -> None:
    from scripts.populate_nfl import ingest_combine
    from ffdb.collectors.pfr_collector import NFLVerseCollector
    from ffdb.utils.player_index import PlayerIndex

    nflverse = NFLVerseCollector()
    index = PlayerIndex(db_path)
    for year in years:
        scope = f"year={year}"
        if not force:
            last = _get_last_run(db_path, "nflverse_combine", scope)
            remote = _nflverse_updated_at("combine")
            if last and (remote is None or remote <= last):
                logger.info("  combine %d: up to date, skipping", year)
                continue
        try:
            ingest_combine(nflverse, db_path, year, index)
            _log_run(db_path, "nflverse_combine", scope, status="ok")
        except Exception as exc:
            logger.error("  combine %d failed: %s", year, exc)
            _log_run(db_path, "nflverse_combine", scope, status="error", notes=str(exc))


def refresh_draft(db_path: str, years: list[int], force: bool) -> None:
    from scripts.populate_nfl import ingest_draft
    from ffdb.collectors.pfr_collector import NFLVerseCollector
    from ffdb.utils.player_index import PlayerIndex

    nflverse = NFLVerseCollector()
    index = PlayerIndex(db_path)
    for year in years:
        scope = f"year={year}"
        if not force:
            last = _get_last_run(db_path, "nflverse_draft", scope)
            remote = _nflverse_updated_at("draft_picks")
            if last and (remote is None or remote <= last):
                logger.info("  draft %d: up to date, skipping", year)
                continue
        try:
            ingest_draft(nflverse, db_path, year, index)
            _log_run(db_path, "nflverse_draft", scope, status="ok")
        except Exception as exc:
            logger.error("  draft %d failed: %s", year, exc)
            _log_run(db_path, "nflverse_draft", scope, status="error", notes=str(exc))


def refresh_rosters(db_path: str, api_key: str, years: list[int], force: bool) -> None:
    from scripts.populate_nfl import ingest_rosters
    from ffdb.collectors.cfbd_collector import CFBDCollector

    cfbd_col = CFBDCollector(api_key)
    for year in years:
        scope = f"year={year}"
        if not force and _get_last_run(db_path, "cfbd_rosters", scope):
            logger.info("  rosters %d: already ingested, skipping", year)
            continue
        try:
            ingest_rosters(cfbd_col, db_path, year)
            _log_run(db_path, "cfbd_rosters", scope, status="ok")
        except Exception as exc:
            logger.error("  rosters %d failed: %s", year, exc)
            _log_run(db_path, "cfbd_rosters", scope, status="error", notes=str(exc))


def refresh_strength(db_path: str, api_key: str, years: list[int], force: bool) -> None:
    from scripts.populate_nfl import ingest_strength_metrics

    for year in years:
        scope = f"year={year}"
        if not force and _get_last_run(db_path, "cfbd_strength", scope):
            logger.info("  strength %d: already ingested, skipping", year)
            continue
        try:
            ingest_strength_metrics(api_key, db_path, year)
            _log_run(db_path, "cfbd_strength", scope, status="ok")
        except Exception as exc:
            logger.error("  strength %d failed: %s", year, exc)
            _log_run(db_path, "cfbd_strength", scope, status="error", notes=str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh cfb-prospect-db data sources. Smart (incremental) by default."
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Ignore ingestion log and re-ingest everything from scratch.",
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Check for new data without writing anything. Exits 1 if updates available.",
    )
    parser.add_argument(
        "--source", type=str, choices=ALL_SOURCES + ["all"], default="all",
        help="Refresh a single source only (default: all).",
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=list(range(2021, 2027)),
        help="Season years to consider (default: 2021-2026).",
    )
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    api_key = get_api_key()
    init_db(db_path)

    if args.check:
        has_updates = check_updates(db_path, args.years)
        sys.exit(1 if has_updates else 0)

    force = args.full
    sources = ALL_SOURCES if args.source == "all" else [args.source]

    logger.info("Refresh mode: %s | Sources: %s | Years: %s",
                "FULL" if force else "incremental",
                ", ".join(sources),
                args.years)

    if "seasons" in sources or "recruiting" in sources:
        logger.info("=== College seasons + recruiting ===")
        refresh_seasons(db_path, api_key, args.years, force)

    if "combine" in sources:
        logger.info("=== NFL Combine ===")
        refresh_combine(db_path, args.years, force)

    if "draft" in sources:
        logger.info("=== NFL Draft ===")
        refresh_draft(db_path, args.years, force)

    if "rosters" in sources:
        logger.info("=== Rosters (height/weight) ===")
        refresh_rosters(db_path, api_key, [args.years[-1]], force)  # roster: latest year only

    if "strength" in sources:
        logger.info("=== Strength metrics (FPI SOS + SRS) ===")
        refresh_strength(db_path, api_key, args.years, force)

    logger.info("Done.")


if __name__ == "__main__":
    main()
