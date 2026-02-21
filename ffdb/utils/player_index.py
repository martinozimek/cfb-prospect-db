"""
In-memory player index for fast name matching across ingestion scripts.

Loads all players from the DB once and provides O(1) exact lookup
plus rapidfuzz fuzzy matching — avoiding repeated full-table scans.
"""

import json
import logging
from typing import Optional

from rapidfuzz import fuzz, process

from ffdb.database import Player, get_session

logger = logging.getLogger(__name__)


class PlayerIndex:
    """
    Loads all players from the DB into memory once and provides fast
    exact + fuzzy name matching without repeated DB queries.

    Usage
    -----
    index = PlayerIndex(db_path)
    player_id = index.find("Emeka Egbuka")   # exact or fuzzy
    """

    def __init__(self, db_path: str):
        self._exact: dict[str, int] = {}               # lowercase name → player_id
        self._candidates: list[tuple[str, int]] = []   # (name, player_id) for fuzzy
        self._count = 0

        with get_session(db_path) as session:
            for p in session.query(Player).all():
                self._count += 1
                lower = p.full_name.lower()
                self._exact[lower] = p.id
                self._candidates.append((p.full_name, p.id))
                for variant in json.loads(p.name_variants or "[]"):
                    self._exact[variant.lower()] = p.id
                    self._candidates.append((variant, p.id))

        logger.info(
            "PlayerIndex built: %d players, %d name candidates.",
            self._count, len(self._candidates),
        )

    def find(self, name: str, threshold: int = 85) -> Optional[int]:
        """
        Return player_id for the best name match, or None if below threshold.

        Tries exact (case-insensitive) match first; falls back to rapidfuzz
        WRatio fuzzy matching.
        """
        if not name:
            return None
        # Fast path: exact match
        pid = self._exact.get(name.lower())
        if pid is not None:
            return pid
        # Fuzzy fallback
        if not self._candidates:
            return None
        names = [c[0] for c in self._candidates]
        pids  = [c[1] for c in self._candidates]
        result = process.extractOne(name, names, scorer=fuzz.WRatio)
        if result and result[1] >= threshold:
            idx = names.index(result[0])
            return pids[idx]
        return None

    def add(self, player_id: int, name: str) -> None:
        """Register a newly-inserted player so subsequent finds work."""
        lower = name.lower()
        self._exact[lower] = player_id
        self._candidates.append((name, player_id))
        self._count += 1
