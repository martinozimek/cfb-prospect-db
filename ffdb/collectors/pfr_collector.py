"""
nflverse data collector.

Pulls NFL Combine and Draft pick data from the nflverse GitHub data releases.
These are maintained CSV files that are freely accessible without scraping.

Sources:
  Combine:     https://github.com/nflverse/nflverse-data/releases/download/combine/combine.csv
  Draft picks: https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv

Usage:
    collector = NFLVerseCollector()
    combine_df = collector.fetch_combine()          # returns pandas DataFrame
    draft_df   = collector.fetch_draft()            # returns pandas DataFrame
    combine_rows = collector.combine_rows(2025)     # returns list[dict] for a single year
    draft_rows   = collector.draft_rows(2025)       # returns list[dict] for a single year
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_COMBINE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/combine/combine.csv"
)
_DRAFT_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv"
)

_POSITIONS_OF_INTEREST = {"WR", "RB", "TE"}


def _height_to_inches(ht: str) -> Optional[float]:
    """Convert '6-1' or '6-01' format to total inches."""
    if not ht or not isinstance(ht, str):
        return None
    parts = ht.strip().split("-")
    if len(parts) == 2:
        try:
            return float(parts[0]) * 12 + float(parts[1])
        except ValueError:
            pass
    return None


def _speed_score(weight_lbs: Optional[float], forty_time: Optional[float]) -> Optional[float]:
    """
    Bill Barnwell's Speed Score: (weight * 200) / (forty_time ^ 4)
    Baseline ~100. Only meaningful for RBs; stored for WR/TE as well.
    """
    if weight_lbs and forty_time and forty_time > 0:
        return round((weight_lbs * 200) / (forty_time ** 4), 1)
    return None


def _pick_to_draft_capital(overall_pick) -> Optional[float]:
    """
    Convert overall NFL Draft pick number to a 0–100 draft capital score.

    Uses exponential decay calibrated to approximate the Johnson Trade Value Chart:
      Pick 1   → 100.0
      Pick 19  →  65.6
      Pick 32  →  49.0
      Pick 64  →  23.4
      Pick 100 →  10.3
      Pick 256 →   0.3
    """
    import math
    try:
        pick = int(overall_pick)
    except (TypeError, ValueError):
        return None
    if pick <= 0:
        return None
    return round(100.0 * math.exp(-0.023 * (pick - 1)), 1)


class NFLVerseCollector:
    """
    Fetches NFL Combine and Draft data from nflverse GitHub data releases.
    Data is cached in-memory after the first fetch per session.
    """

    def __init__(self):
        self._combine_df: Optional[pd.DataFrame] = None
        self._draft_df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Raw DataFrame accessors (cached)
    # ------------------------------------------------------------------

    def fetch_combine(self) -> pd.DataFrame:
        """Return the full nflverse combine DataFrame (all years, all positions)."""
        if self._combine_df is None:
            logger.info("Fetching nflverse combine data from GitHub...")
            self._combine_df = pd.read_csv(_COMBINE_URL, low_memory=False)
            logger.info("  Loaded %d combine rows.", len(self._combine_df))
        return self._combine_df

    def fetch_draft(self) -> pd.DataFrame:
        """Return the full nflverse draft picks DataFrame (all years, all positions)."""
        if self._draft_df is None:
            logger.info("Fetching nflverse draft picks data from GitHub...")
            self._draft_df = pd.read_csv(_DRAFT_URL, low_memory=False)
            logger.info("  Loaded %d draft rows.", len(self._draft_df))
        return self._draft_df

    # ------------------------------------------------------------------
    # Structured row accessors (list[dict] for DB ingestion)
    # ------------------------------------------------------------------

    def combine_rows(
        self,
        year: int,
        positions: Optional[set] = None,
    ) -> list[dict]:
        """
        Return combine rows for a specific year as a list of dicts.

        Each dict has:
            name, college, position, nflverse_cfb_id,
            height_inches, weight_lbs, forty_time, vertical_jump,
            broad_jump, three_cone, shuttle, bench_press,
            speed_score, combine_year
        """
        df = self.fetch_combine()
        pos_filter = positions or _POSITIONS_OF_INTEREST
        mask = (df["season"] == year) & (df["pos"].isin(pos_filter))
        subset = df[mask].copy()

        rows = []
        for _, row in subset.iterrows():
            wt = row.get("wt")
            forty = row.get("forty")
            wt = float(wt) if pd.notna(wt) else None
            forty = float(forty) if pd.notna(forty) else None

            rows.append({
                "name": row.get("player_name") or "",
                "college": row.get("school") or "",
                "position": row.get("pos") or "",
                "nflverse_cfb_id": row.get("cfb_id") or "",   # slug e.g. "emeka-egbuka-1"
                "height_inches": _height_to_inches(row.get("ht")),
                "weight_lbs": wt,
                "forty_time": forty,
                "vertical_jump": float(row["vertical"]) if pd.notna(row.get("vertical")) else None,
                "broad_jump": int(row["broad_jump"]) if pd.notna(row.get("broad_jump")) else None,
                "three_cone": float(row["cone"]) if pd.notna(row.get("cone")) else None,
                "shuttle": float(row["shuttle"]) if pd.notna(row.get("shuttle")) else None,
                "bench_press": int(row["bench"]) if pd.notna(row.get("bench")) else None,
                "speed_score": _speed_score(wt, forty),
                "combine_year": year,
            })

        logger.info("  %d %s combine rows for %d", len(rows), pos_filter, year)
        return rows

    def draft_rows(
        self,
        year: int,
        positions: Optional[set] = None,
    ) -> list[dict]:
        """
        Return draft pick rows for a specific year as a list of dicts.

        Each dict has:
            name, college, position, nfl_team, draft_year,
            draft_round, overall_pick, draft_capital_score,
            nflverse_cfb_id
        """
        df = self.fetch_draft()
        pos_filter = positions or _POSITIONS_OF_INTEREST
        mask = (df["season"] == year) & (df["position"].isin(pos_filter))
        subset = df[mask].copy()

        rows = []
        for _, row in subset.iterrows():
            pick = row.get("pick")
            rows.append({
                "name": row.get("pfr_player_name") or "",
                "college": row.get("college") or "",
                "position": row.get("position") or "",
                "nfl_team": row.get("team") or "",
                "draft_year": year,
                "draft_round": int(row["round"]) if pd.notna(row.get("round")) else None,
                "overall_pick": int(pick) if pd.notna(pick) else None,
                "draft_capital_score": _pick_to_draft_capital(pick),
                "nflverse_cfb_id": row.get("cfb_player_id") or "",
            })

        logger.info("  %d %s draft rows for %d", len(rows), pos_filter, year)
        return rows
