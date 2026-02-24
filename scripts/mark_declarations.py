"""
Mark players in cfb-prospect-db as declared for a given NFL Draft year.

Two sources are combined:
  1. nflmockdraftdatabase.com consensus big board (all positions).
     The board may lag late-declaring players, so source 2 fills the gap.
  2. SUPPLEMENT_2026 — hard-coded list of known-declared WR/RB/TE who are
     absent from the big board (typically late/high-profile declarers added
     after the board was last indexed).

For each declared name the script:
  - Fuzzy-matches against cfb Player records that had a 2025 season.
  - Auto-marks matches with score >= MIN_AUTO_SCORE.
  - Flags candidates in 70–85 range for manual review.
  - Reports names with no cfb match (player may be missing from cfb DB).

Usage:
    python scripts/mark_declarations.py                   # default: 2026 class
    python scripts/mark_declarations.py --draft-year 2026
    python scripts/mark_declarations.py --dry-run         # preview only, no DB writes
    python scripts/mark_declarations.py --report report.csv
    python scripts/mark_declarations.py --wipe            # clear all 2026 marks first
"""

import argparse
import csv
import html as html_module
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_db_path
from ffdb.database import CFBPlayerSeason, Player, get_session, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Matching thresholds ────────────────────────────────────────────────────────

MIN_AUTO_SCORE = 85   # >= this → auto-mark
REVIEW_SCORE   = 70   # >= this but < MIN_AUTO_SCORE → flag for review
# Below REVIEW_SCORE → no match

# ─── Supplement list ────────────────────────────────────────────────────────────
#
# Players who declared for the 2026 NFL Draft but are absent from the
# nflmockdraftdatabase consensus board (typically late or high-profile
# declarers added after the board was last indexed).
#
# Format: (player_name, position, last_college_team)
# - player_name: common name spelling (fuzzy matching handles minor variants)
# - position: WR / RB / TE (used as secondary match filter; None = any)
# - last_college_team: optional hint for disambiguation (None = skip)
#
SUPPLEMENT_2026: list[tuple[str, Optional[str], Optional[str]]] = [
    # NOTE: All entries here played their LAST college season in 2025 (CFBD season_year=2025).
    # Players who declared after the 2024 college season (e.g., Cam Ward, Shedeur Sanders,
    # Jeanty, Warren, Fannin) are 2025 NFL Draft picks and NOT included here.
    #
    # This supplement adds known 2026 prospects who are likely absent from the
    # nflmockdraftdatabase board due to late indexing or board methodology.

    # ── Running Backs (2025 college season confirmed in cfb DB) ──────────────
    ("Ahmad Hardy",     "RB", "Missouri"),         # 1649 rush yds, top non-board RB
    ("Kewan Lacy",      "RB", "Ole Miss"),          # 1567 rush yds
    ("Bo Jackson",      "RB", "Ohio State"),        # 1090 rush yds, OSU
    ("Mark Fletcher",   "RB", "Miami"),             # 1192 rush yds
    ("Kaytron Allen",   "RB", "Penn State"),        # 1303 rush yds (on board #119, safety net)
    ("Nate Frazier",    "RB", "Georgia"),           # 947 rush yds
    ("Jordan Marshall", "RB", "Michigan"),          # 932 rush yds

    # ── Wide Receivers (2025 college season confirmed) ───────────────────────
    ("Jeremiah Smith",  "WR", "Ohio State"),        # 1243 rec yds, clear #1 WR prospect
    ("Malachi Toney",   "WR", "Miami"),             # 1211 rec yds
    ("Eric McAlister",  "WR", "TCU"),               # 1190 rec yds
    ("Duce Robinson",   "WR", "Florida State"),     # 1074 rec yds
    ("Chris Brazzell",  "WR", "Tennessee"),         # 1017 rec yds (on board, safety net)
    ("Cam Coleman",     "WR", "Auburn"),            # 725 rec yds

    # ── Tight Ends (2025 college season confirmed) ───────────────────────────
    ("Kenyon Sadiq",    "TE", "Oregon"),            # 560 rec yds (on board #19, safety net)
    ("Eli Stowers",     "TE", "Vanderbilt"),        # 765 rec yds (on board #64, safety net)
    ("Michael Trigg",   "TE", "Baylor"),            # 694 rec yds (on board #87, safety net)
    ("Tanner Koziol",   "TE", "Houston"),           # 727 rec yds, not on board
    ("Carsen Ryan",     "TE", "BYU"),               # 620 rec yds
]

# ─── Big board scraping (same logic as populate_bigboard.py) ───────────────────

_BOARD_URL = (
    "https://www.nflmockdraftdatabase.com/big-boards/{year}/"
    "consensus-big-board-{year}-nfl-draft"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _fetch_board_all_positions(year: int) -> list[dict]:
    """
    Fetch ALL players from the nflmockdraftdatabase big board for *year*
    (not filtered to WR/RB/TE). Returns list of dicts:
      {player_name, position, consensus_rank, draft_year}
    """
    url = _BOARD_URL.format(year=year)
    logger.info("Fetching %s ...", url)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
    except requests.RequestException as exc:
        logger.warning("Network error fetching board: %s", exc)
        return []

    if resp.status_code != 200:
        logger.warning("Board returned status %d.", resp.status_code)
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    react_div = soup.find("div", attrs={"data-react-class": True})
    if not react_div:
        logger.warning("No React props div found — page structure may have changed.")
        return []

    raw_props = react_div.get("data-react-props", "")
    try:
        data = json.loads(html_module.unescape(raw_props))
    except json.JSONDecodeError as exc:
        logger.warning("JSON parse error: %s", exc)
        return []

    # Locate selections list
    selections = None
    for top_key in ("mock", "big_board", "board"):
        section = data.get(top_key, {})
        if isinstance(section, dict):
            selections = section.get("selections") or section.get("players")
        elif isinstance(section, list):
            selections = section
        if selections:
            break
    if not selections:
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and "player" in v[0]:
                selections = v
                break

    if not selections:
        logger.warning("Could not locate selections list. Top-level keys: %s", list(data.keys()))
        return []

    rows = []
    for entry in selections:
        player = entry.get("player") or entry
        name = (
            player.get("name") or player.get("full_name") or entry.get("player_name") or ""
        ).strip()
        pos = (
            player.get("position") or entry.get("position") or ""
        ).upper().strip()
        consensus_sub = entry.get("consensus") or {}
        rank = (
            consensus_sub.get("pick")
            or entry.get("pick")
            or entry.get("rank")
            or entry.get("consensus_pick")
        )
        if not name or rank is None:
            continue
        try:
            rank = int(rank)
        except (ValueError, TypeError):
            continue
        rows.append({"player_name": name, "position": pos, "draft_year": year, "consensus_rank": rank})

    logger.info("  Board: %d total prospects found.", len(rows))
    return rows


# ─── CFB player loading ────────────────────────────────────────────────────────

def _load_cfb_candidates(db_path: str, last_season: int) -> list[dict]:
    """
    Load all cfb Player records that had a `last_season` season entry.
    Returns list of dicts: {id, full_name, position, last_team, last_season_year}
    """
    from sqlalchemy import func

    with get_session(db_path) as session:
        # last season year per player
        last_year_by_player: dict[int, int] = dict(
            session.query(CFBPlayerSeason.player_id, func.max(CFBPlayerSeason.season_year))
            .group_by(CFBPlayerSeason.player_id)
            .all()
        )
        last_team_by_player: dict[int, str] = {}
        for row in (
            session.query(CFBPlayerSeason.player_id, CFBPlayerSeason.team, CFBPlayerSeason.season_year)
            .filter(CFBPlayerSeason.season_year == last_season)
            .all()
        ):
            last_team_by_player[row[0]] = row[1]

        players_with_last_season = {
            pid for pid, yr in last_year_by_player.items() if yr == last_season
        }

        candidates = []
        for player in (
            session.query(Player)
            .filter(Player.id.in_(list(players_with_last_season)))
            .all()
        ):
            candidates.append({
                "id": player.id,
                "full_name": player.full_name,
                "position": player.position,
                "last_team": last_team_by_player.get(player.id),
                "last_season_year": last_year_by_player.get(player.id),
                "name_variants": player.get_name_variants(),
            })

    logger.info("Loaded %d cfb players with a %d season.", len(candidates), last_season)
    return candidates


# ─── Fuzzy matching ────────────────────────────────────────────────────────────

def _best_match(
    declared_name: str,
    declared_pos: Optional[str],
    candidates: list[dict],
) -> Optional[tuple[dict, float]]:
    """
    Find the best cfb candidate for a declared player name.
    Returns (candidate_dict, score) or None if nothing clears REVIEW_SCORE.
    """
    best_cand = None
    best_score = 0.0

    for c in candidates:
        # Position filter (skip only when both sides have a position and they differ)
        if declared_pos and c["position"] and declared_pos != c["position"]:
            continue

        # Match against canonical name and all variants
        names_to_try = [c["full_name"]] + c["name_variants"]
        score = max(fuzz.WRatio(declared_name, n) for n in names_to_try)
        if score > best_score:
            best_score = score
            best_cand = c

    if best_score >= REVIEW_SCORE:
        return best_cand, best_score
    return None


# ─── Main logic ───────────────────────────────────────────────────────────────

def build_declared_list(draft_year: int) -> list[dict]:
    """
    Combine nflmockdraftdatabase board + supplement into a deduplicated
    list of declared prospects.
    Returns list of dicts: {player_name, position, source, consensus_rank}.
    """
    board_rows = _fetch_board_all_positions(draft_year)
    time.sleep(1)  # polite delay

    # Build dedup set (normalized lower name)
    seen_names: set[str] = set()
    declared: list[dict] = []

    for row in board_rows:
        key = row["player_name"].lower().strip()
        if key not in seen_names:
            seen_names.add(key)
            declared.append({
                "player_name": row["player_name"],
                "position": row["position"] or None,
                "source": "bigboard",
                "consensus_rank": row["consensus_rank"],
            })

    # Add supplement entries not already on board
    supp_added = 0
    for name, pos, _team in (SUPPLEMENT_2026 if draft_year == 2026 else []):
        key = name.lower().strip()
        if key not in seen_names:
            seen_names.add(key)
            declared.append({
                "player_name": name,
                "position": pos,
                "source": "supplement",
                "consensus_rank": None,
            })
            supp_added += 1

    logger.info(
        "Declared list: %d from board + %d from supplement = %d total.",
        len(declared) - supp_added, supp_added, len(declared),
    )
    return declared


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mark cfb players as declared for a given NFL Draft year."
    )
    parser.add_argument("--db", type=str, default=None)
    parser.add_argument("--draft-year", type=int, default=2026)
    parser.add_argument("--last-cfb-season", type=int, default=None,
                        help="Last college season for this draft class (default: draft_year - 1).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview matches without writing to DB.")
    parser.add_argument("--report", type=str, default=None,
                        help="Write match report CSV to this path.")
    parser.add_argument("--wipe", action="store_true",
                        help="Clear all declared_draft_year == draft_year marks before running.")
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    draft_year = args.draft_year
    last_cfb_season = args.last_cfb_season or (draft_year - 1)

    if not args.dry_run:
        init_db(db_path)

    # Optional wipe
    if args.wipe and not args.dry_run:
        with get_session(db_path) as session:
            wiped = (
                session.query(Player)
                .filter(Player.declared_draft_year == draft_year)
                .update({"declared_draft_year": None}, synchronize_session=False)
            )
        logger.info("Wiped declared_draft_year=%d from %d players.", draft_year, wiped)

    declared_list = build_declared_list(draft_year)
    candidates = _load_cfb_candidates(db_path, last_cfb_season)

    report_rows = []
    auto_matched = 0
    review_needed = 0
    no_match = 0

    for entry in declared_list:
        name = entry["player_name"]
        pos  = entry["position"]

        result = _best_match(name, pos, candidates)
        if result is None:
            status = "NO_MATCH"
            no_match += 1
            cfb_id, cfb_name, score = None, None, None
        else:
            cand, score = result
            cfb_id   = cand["id"]
            cfb_name = cand["full_name"]
            if score >= MIN_AUTO_SCORE:
                status = "MATCHED"
                auto_matched += 1
                if not args.dry_run:
                    with get_session(db_path) as session:
                        player = session.query(Player).filter(Player.id == cfb_id).first()
                        if player:
                            player.declared_draft_year = draft_year
            else:
                status = "REVIEW"
                review_needed += 1

        report_rows.append({
            "declared_name":   name,
            "declared_pos":    pos,
            "source":          entry["source"],
            "consensus_rank":  entry["consensus_rank"],
            "status":          status,
            "cfb_id":          cfb_id,
            "cfb_name":        cfb_name,
            "match_score":     round(score, 1) if score else None,
        })

        if status == "REVIEW":
            logger.warning(
                "  [REVIEW] %r → %r (score=%.0f) — needs manual check.",
                name, cfb_name, score,
            )
        elif status == "NO_MATCH":
            logger.warning("  [NO_MATCH] %r — not found in cfb DB.", name)

    # Summary
    logger.info(
        "Results: %d MATCHED | %d REVIEW | %d NO_MATCH (of %d declared).",
        auto_matched, review_needed, no_match, len(declared_list),
    )

    # Write report CSV
    if args.report:
        report_path = Path(args.report)
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(report_rows[0].keys()))
            writer.writeheader()
            writer.writerows(report_rows)
        logger.info("Report written to %s", report_path)

    if args.dry_run:
        logger.info("Dry run — no DB changes made.")

    # Print summary table for REVIEW and NO_MATCH
    review_rows = [r for r in report_rows if r["status"] == "REVIEW"]
    no_match_rows = [r for r in report_rows if r["status"] == "NO_MATCH"]

    if review_rows:
        print("\nREVIEW — check these matches manually:")
        print(f"  {'Declared Name':<30} {'Pos':<4} {'CFB Match':<30} Score")
        print("  " + "-" * 75)
        for r in review_rows:
            print(
                f"  {r['declared_name']:<30} {(r['declared_pos'] or ''):<4} "
                f"{(r['cfb_name'] or ''):<30} {r['match_score']}"
            )

    if no_match_rows:
        print(f"\nNO_MATCH — {len(no_match_rows)} declared players not found in cfb DB:")
        skill_pos = {"WR", "RB", "TE"}
        skill_missing = [r for r in no_match_rows if (r["declared_pos"] or "") in skill_pos]
        other_missing = [r for r in no_match_rows if (r["declared_pos"] or "") not in skill_pos]
        if skill_missing:
            print("  Skill positions (WR/RB/TE) — may need manual cfb linking:")
            for r in skill_missing:
                print(f"    {r['declared_name']:<30} {(r['declared_pos'] or ''):<4} rank={r['consensus_rank']}")
        if other_missing:
            print(f"  Other positions ({len(other_missing)} total — normal, model only covers WR/RB/TE)")


if __name__ == "__main__":
    main()
