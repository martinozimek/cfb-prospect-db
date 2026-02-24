"""
ZAP-inspired component sheet for the 2026 NFL Draft class.

Computes the measurable inputs to the ZAP Model for every WR/RB/TE
who had a 2025 CFB season in our database.

Published ZAP Model inputs (2025 guide):
  WR:  Breakout Score (adj rec yds/team pass att, SOS-adj, age-weighted career),
       career rush yards, early-declare flag, Teammate Score, weight
       [draft capital layered on top — TBD post-draft]
  RB:  Breakout Score (same methodology), best-season reception share,
       Teammate Score, age, weight, Speed Score
       [draft capital layered on top — TBD post-draft]
  TE:  Draft capital (TBD), Speed Score, career yards/route run (not in DB),
       age-at-draft

What this script produces
--------------------------
  - Breakout Score proxy:
      For each qualifying season (≥6 games), compute prorated rec_yds / team_pass_att,
      then apply SOS multiplier from FPI SOS rank.
      Report the career-best SOS-adjusted season and the age at that season.
  - Teammate Score:
      Sum of draft_capital_score for all WR/RB/TE players who played at the
      same college (any season in our DB) and were drafted 2021-2025.
  - Early declare flag:
      True if the player has ≤3 years of CFB data in our window, meaning
      they could be a sophomore or junior declare.
  - Speed Score, Weight, Age at draft: from combine / player tables.

Usage:
    python scripts/zap_components.py                      # 2025 season, WR/RB/TE
    python scripts/zap_components.py --top 50             # top-50 per position
    python scripts/zap_components.py --output zap_2026.csv
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_db_path
from ffdb.database import (
    CFBPlayerSeason,
    CFBTeamSeason,
    NFLCombineResult,
    NFLDraftPick,
    Player,
    Recruiting,
    get_session,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

MIN_GAMES = 6              # ZAP rule: season must have ≥6 games to count
PRORATE_TARGET = 12        # Prorate player stats to this many games
SOS_NEUTRAL_RANK = 65      # Approximate median FPI SOS rank (FBS ~130 teams)
SOS_MAX_ADJUST   = 0.15    # ±15% max SOS multiplier adjustment
MIN_TEAM_PASS_ATT = 200    # Exclude option-offense seasons (Navy ~182, Army ~126)
                           # to prevent artificial inflation of rec_yds/team_pass_att
POSITIONS = {"WR", "RB", "TE"}
TEAMMATE_DRAFT_YEARS = list(range(2021, 2026))  # Years to scan for Teammate Score

APPROX_DRAFT_DATE_YEAR = 2026  # For age-at-draft calculation


# ─── Helpers ──────────────────────────────────────────────────────────────────

def sos_multiplier(fpi_rank: Optional[int]) -> float:
    """
    Translate FPI SOS rank → multiplier for rec_yds / team_pass_att.

    FPI SOS rank 1 = hardest schedule → multiply > 1.0 (boost adjusted stats).
    FPI SOS rank 130 = easiest schedule → multiply < 1.0 (penalize stats).

    Calibration (approximate, based on ZAP guide description of SOS adjustment):
      rank  1 →  1.15
      rank 65 →  1.00   (neutral)
      rank 130 → 0.85
    """
    if fpi_rank is None:
        return 1.0
    r = max(1, min(int(fpi_rank), 130))
    return round(1.0 + (SOS_NEUTRAL_RANK - r) / SOS_NEUTRAL_RANK * SOS_MAX_ADJUST, 4)


def prorated_rate(
    rec_yards_per_team_pass_att: Optional[float],
    rec_yards: Optional[int],
    games_played: Optional[int],
    team_pass_att: Optional[int],
) -> Optional[float]:
    """
    Return the raw rec_yds / team_pass_att rate, prorated when possible.

    Priority:
      1. If games_played is known and the stored rate is available, prorate
         by scaling rec_yards to PRORATE_TARGET-game equivalent.
      2. If games_played is unknown but the stored rate is available, use it
         as-is (no proration possible).
      3. If none of the above, return None.

    Minimum games threshold: if games_played is known and < MIN_GAMES,
    the season is excluded (ZAP rule).
    """
    # Explicit games_played below threshold → exclude season
    if games_played is not None and games_played < MIN_GAMES:
        return None

    # Prefer live computation with proration when we have all pieces
    if (rec_yards is not None and games_played is not None
            and team_pass_att is not None and team_pass_att > 0):
        scale = min(PRORATE_TARGET / games_played, 2.0)   # cap at 2× for injury safety
        return (rec_yards * scale) / team_pass_att

    # Fall back to the pre-computed stored rate (no proration)
    if rec_yards_per_team_pass_att is not None:
        return rec_yards_per_team_pass_att

    return None


# ─── Teammate Score ────────────────────────────────────────────────────────────

def build_teammate_score_map(db_path: str) -> dict[str, float]:
    """
    Pre-compute Teammate Score for every school in our DB.

    Teammate Score for school T = sum of draft_capital_score for all
    WR/RB/TE players who:
      - played at least one season at school T in our DB window (2021-2025)
      - were drafted in any of TEAMMATE_DRAFT_YEARS
    Each player is counted once (deduplicated by player_id).
    """
    logger.info("Building Teammate Score map...")
    with get_session(db_path) as session:
        # One row per (player_id, team) pair — distinct drafted skill players
        drafted_rows = (
            session.query(
                CFBPlayerSeason.team,
                CFBPlayerSeason.player_id,
                NFLDraftPick.draft_capital_score,
            )
            .join(NFLDraftPick, NFLDraftPick.player_id == CFBPlayerSeason.player_id)
            .join(Player, Player.id == CFBPlayerSeason.player_id)
            .filter(
                Player.position.in_(["WR", "RB", "TE"]),
                NFLDraftPick.draft_year.in_(TEAMMATE_DRAFT_YEARS),
                NFLDraftPick.draft_capital_score.isnot(None),
            )
            .all()
        )

    # Deduplicate: each player_id counted once per team
    seen: set[tuple[str, int]] = set()
    team_scores: dict[str, float] = {}
    for team, pid, cap in drafted_rows:
        if not team or not cap:
            continue
        key = (team, pid)
        if key in seen:
            continue
        seen.add(key)
        team_scores[team] = team_scores.get(team, 0.0) + cap

    logger.info("  Teammate Score populated for %d schools.", len(team_scores))
    return team_scores


# ─── Core computation ─────────────────────────────────────────────────────────

def compute_components(db_path: str, last_season: int, declared_only: bool = False) -> pd.DataFrame:
    """
    Return a DataFrame of ZAP component inputs for all WR/RB/TE players
    who had a `last_season` season entry in the DB.
    """
    teammate_scores = build_teammate_score_map(db_path)
    rows = []

    with get_session(db_path) as session:

        # Players with a qualifying season in `last_season`
        q = (
            session.query(Player)
            .join(CFBPlayerSeason, CFBPlayerSeason.player_id == Player.id)
            .filter(
                CFBPlayerSeason.season_year == last_season,
                Player.position.in_(list(POSITIONS)),
            )
        )
        if declared_only:
            q = q.filter(Player.declared_draft_year == last_season + 1)
        prospects = q.distinct().all()

        logger.info(
            "Processing %d WR/RB/TE prospects with a %d season...",
            len(prospects), last_season,
        )

        for player in prospects:
            # ── All college seasons in DB ──────────────────────────────────
            all_seasons = (
                session.query(CFBPlayerSeason)
                .filter(CFBPlayerSeason.player_id == player.id)
                .order_by(CFBPlayerSeason.season_year)
                .all()
            )

            season_stats = []
            career_rush_yds = 0
            best_rec_share = None

            for s in all_seasons:
                # Team season data (denominators + SOS)
                ts = (
                    session.query(CFBTeamSeason)
                    .filter(
                        CFBTeamSeason.team == s.team,
                        CFBTeamSeason.season_year == s.season_year,
                    )
                    .first()
                )

                team_pass_att = ts.pass_attempts if ts else None
                fpi_rank      = ts.fpi_sos_rank  if ts else None

                # Skip option-offense seasons — inflated ratio is noise, not signal
                if team_pass_att is not None and team_pass_att < MIN_TEAM_PASS_ATT:
                    continue

                raw = prorated_rate(
                    s.rec_yards_per_team_pass_att,
                    s.rec_yards,
                    s.games_played,
                    team_pass_att,
                )
                mult = sos_multiplier(fpi_rank)
                adj = round(raw * mult, 4) if raw is not None else None

                season_stats.append({
                    "year":      s.season_year,
                    "team":      s.team,
                    "games":     s.games_played,
                    "age":       s.age_at_season_start,
                    "rec_yds":   s.rec_yards,
                    "raw_rate":  round(raw, 4) if raw is not None else None,
                    "fpi_rank":  fpi_rank,
                    "team_pass_att": team_pass_att,
                    "sos_mult":  mult,
                    "adj_rate":  adj,
                    "rec_share": s.reception_share,
                })

                if s.rush_yards:
                    career_rush_yds += s.rush_yards
                if s.reception_share is not None:
                    if best_rec_share is None or s.reception_share > best_rec_share:
                        best_rec_share = s.reception_share

            if not season_stats:
                continue

            # ── Breakout Score proxy ───────────────────────────────────────
            valid = [ss for ss in season_stats if ss["adj_rate"] is not None]
            if valid:
                best_ss = max(valid, key=lambda x: x["adj_rate"])
            else:
                best_ss = {}

            # Formatted season-by-season detail for CSV
            detail_parts = []
            for ss in season_stats:
                age_str = f"{ss['age']:.1f}" if ss["age"] else "?"
                rate_str = f"{ss['adj_rate']:.3f}" if ss["adj_rate"] else "N/A"
                detail_parts.append(f"{ss['year']}({age_str})={rate_str}")
            seasons_detail = " | ".join(detail_parts)

            # ── Career span & early-declare heuristic ─────────────────────
            season_years = [s.season_year for s in all_seasons]
            first_year   = min(season_years)
            last_year    = max(season_years)
            years_played = last_year - first_year + 1
            # Heuristic: ≤3 seasons in our window → plausible early declare
            # (could be soph/junior year; 4 = typical full career)
            early_declare = len(all_seasons) <= 3

            last_team = next(
                (s.team for s in reversed(all_seasons) if s.team), None
            )

            # ── Combine data ───────────────────────────────────────────────
            combine = (
                session.query(NFLCombineResult)
                .filter(NFLCombineResult.player_id == player.id)
                .first()
            )
            speed_score     = combine.speed_score if combine else None
            combine_weight  = combine.weight_lbs  if combine else None
            forty_time      = combine.forty_time  if combine else None

            weight = combine_weight or player.weight_lbs

            # ── Estimate season ages from recruit year (when DOB unknown) ──
            recruit_row = (
                session.query(Recruiting)
                .filter(Recruiting.player_id == player.id)
                .order_by(Recruiting.recruit_year)
                .first()
            )
            recruit_year = recruit_row.recruit_year if recruit_row else None
            if recruit_year:
                for ss in season_stats:
                    if ss["age"] is None:
                        # Freshman year = recruit_year; typical age 18.5 at Sept 1
                        ss["age"] = round(18.5 + (ss["year"] - recruit_year), 1)

            # ── Age at draft ───────────────────────────────────────────────
            age_at_draft = None
            if player.date_of_birth:
                from datetime import date as _date
                draft_ref = _date(APPROX_DRAFT_DATE_YEAR, 4, 24)
                age_at_draft = round(
                    (draft_ref - player.date_of_birth).days / 365.25, 1
                )
            elif recruit_year:
                # Approximate: recruited at ~17.5, draft ~4 years later
                age_at_draft = round(17.5 + (APPROX_DRAFT_DATE_YEAR - recruit_year), 1)

            # ── Teammate Score ─────────────────────────────────────────────
            t_score = teammate_scores.get(last_team) if last_team else None

            rows.append({
                "player_id":        player.id,
                "name":             player.full_name,
                "position":         player.position,
                "last_team":        last_team,
                # Breakout Score proxy
                "best_adj_rate":    best_ss.get("adj_rate"),
                "best_season_year": best_ss.get("year"),
                "best_season_age":  round(best_ss["age"], 1) if best_ss.get("age") else None,
                "best_fpi_rank":    best_ss.get("fpi_rank"),
                "best_sos_mult":    best_ss.get("sos_mult"),
                # WR-specific
                "career_rush_yds":  career_rush_yds or None,
                # RB-specific
                "best_rec_share":   round(best_rec_share, 3) if best_rec_share else None,
                # Size / athleticism
                "weight_lbs":       weight,
                "speed_score":      speed_score,
                "forty_time":       forty_time,
                # Teammate Score
                "teammate_score":   round(t_score, 1) if t_score else None,
                # Context
                "early_declare":    early_declare,
                "years_in_db":      len(all_seasons),
                "age_at_draft":     age_at_draft,
                "draft_capital":    None,   # TBD post-draft
                # Transparency
                "seasons_detail":   seasons_detail,
            })

    df = pd.DataFrame(rows)
    return df


# ─── Display ──────────────────────────────────────────────────────────────────

_WR_COLS = [
    ("name",            "Name",          "<20"),
    ("last_team",       "School",        "<16"),
    ("best_adj_rate",   "BestAdjRate",   ">11"),
    ("best_season_age", "AgeAtBest",     ">9"),
    ("best_fpi_rank",   "SOSRank",       ">7"),
    ("career_rush_yds", "RushYds",       ">7"),
    ("weight_lbs",      "Wt",            ">6"),
    ("teammate_score",  "TmScore",       ">7"),
    ("early_declare",   "EarlyDec",      ">8"),
    ("age_at_draft",    "AgeDraft",      ">8"),
]

_RB_COLS = [
    ("name",            "Name",          "<20"),
    ("last_team",       "School",        "<16"),
    ("best_adj_rate",   "BestAdjRate",   ">11"),
    ("best_season_age", "AgeAtBest",     ">9"),
    ("best_fpi_rank",   "SOSRank",       ">7"),
    ("best_rec_share",  "RecShare",      ">8"),
    ("weight_lbs",      "Wt",            ">6"),
    ("speed_score",     "SpeedScore",    ">10"),
    ("teammate_score",  "TmScore",       ">7"),
    ("age_at_draft",    "AgeDraft",      ">8"),
]

_TE_COLS = [
    ("name",            "Name",          "<20"),
    ("last_team",       "School",        "<16"),
    ("best_adj_rate",   "BestAdjRate",   ">11"),
    ("best_season_age", "AgeAtBest",     ">9"),
    ("best_fpi_rank",   "SOSRank",       ">7"),
    ("speed_score",     "SpeedScore",    ">10"),
    ("forty_time",      "Forty",         ">6"),
    ("weight_lbs",      "Wt",            ">6"),
    ("teammate_score",  "TmScore",       ">7"),
    ("age_at_draft",    "AgeDraft",      ">8"),
]

_POS_COLS = {"WR": _WR_COLS, "RB": _RB_COLS, "TE": _TE_COLS}


def fmt(val, spec: str) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return format("-", spec)
    if isinstance(val, bool):
        return format("Y" if val else "N", spec)
    if isinstance(val, float):
        return format(f"{val:.2f}", spec)
    return format(str(val), spec)


def print_position_table(df: pd.DataFrame, position: str, top_n: int) -> None:
    pos_df = df[df["position"] == position].copy()
    if pos_df.empty:
        print(f"\nNo {position} data found.")
        return

    pos_df = pos_df.sort_values("best_adj_rate", ascending=False, na_position="last")
    pos_df = pos_df.head(top_n)

    col_specs = _POS_COLS[position]
    header_parts = [format(hdr, spec) for _, hdr, spec in col_specs]
    sep = "-" * (sum(len(h) for h in header_parts) + len(header_parts) - 1)

    print(f"\n{'='*len(sep)}")
    print(f"  {position} — ZAP Component Inputs (2026 Draft Class | last CFB season: 2025)")
    print(f"  NOTE: 'BestAdjRate' = prorated rec yds / team pass att × SOS multiplier")
    print(f"        Higher = better. Age-at-best: younger production is more predictive.")
    print(f"        Draft capital column will populate post-April-2026 draft.")
    print(f"{'='*len(sep)}")
    print("  " + " ".join(header_parts))
    print("  " + sep)

    for _, row in pos_df.iterrows():
        parts = [fmt(row.get(col), spec) for col, _, spec in col_specs]
        print("  " + " ".join(parts))

    print()


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute ZAP component inputs for a draft class."
    )
    parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB.")
    parser.add_argument(
        "--season", type=int, default=2025,
        help="Last CFB season year for the draft class (default: 2025).",
    )
    parser.add_argument(
        "--positions", nargs="+", default=["WR", "RB", "TE"],
        choices=["WR", "RB", "TE"],
    )
    parser.add_argument(
        "--top", type=int, default=40,
        help="Number of players to display per position (default: 40).",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Optional CSV output path (e.g. zap_2026.csv).",
    )
    parser.add_argument(
        "--declared-only", action="store_true",
        help="Only include players with declared_draft_year == season+1 in the DB.",
    )
    args = parser.parse_args()

    db_path = args.db or get_db_path()

    logger.info(
        "Computing ZAP component sheet for %d draft class%s...",
        args.season + 1,
        " (declared only)" if args.declared_only else "",
    )
    df = compute_components(db_path, last_season=args.season, declared_only=args.declared_only)

    if df.empty:
        logger.error("No data returned. Check DB path and season year.")
        sys.exit(1)

    logger.info("Total prospects found: %d", len(df))

    for pos in args.positions:
        print_position_table(df, pos, args.top)

    if args.output:
        # Full CSV with season detail
        df.to_csv(args.output, index=False)
        logger.info("Full component table written to %s", args.output)


if __name__ == "__main__":
    main()
