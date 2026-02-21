"""
Fuzzy player name resolution using rapidfuzz.

Searches both the canonical full_name and any entries stored in the
name_variants JSON array on each Player record.
"""

import json
from typing import Optional

from rapidfuzz import fuzz, process
from sqlalchemy.orm import Session

from ffdb.database import Player


def _all_name_candidates(session: Session) -> list[tuple[str, int]]:
    """
    Return a flat list of (name_string, player_id) pairs covering
    both full_name and every stored name variant.
    """
    candidates: list[tuple[str, int]] = []
    for player in session.query(Player).all():
        candidates.append((player.full_name, player.id))
        for variant in json.loads(player.name_variants or "[]"):
            candidates.append((variant, player.id))
    return candidates


def find_player(
    session: Session,
    name_query: str,
    threshold: int = 80,
    limit: int = 5,
) -> list[tuple[Player, float]]:
    """
    Fuzzy-search for players by name.

    Returns a list of (Player, score) tuples, sorted descending by score,
    filtered to matches >= threshold. Returns an empty list if nothing matches.

    Parameters
    ----------
    session:    Active SQLAlchemy session.
    name_query: The name string to search for (handles typos, abbreviations, etc.).
    threshold:  Minimum fuzzy score (0–100) to include in results.
    limit:      Maximum number of results to return.
    """
    candidates = _all_name_candidates(session)
    if not candidates:
        return []

    names = [c[0] for c in candidates]
    player_ids = [c[1] for c in candidates]

    results = process.extract(
        name_query,
        names,
        scorer=fuzz.WRatio,
        limit=limit * 3,  # over-fetch, then deduplicate by player_id
    )

    # Deduplicate: keep highest score per player_id
    seen_ids: dict[int, float] = {}
    for match_name, score, idx in results:
        pid = player_ids[idx]
        if score >= threshold and (pid not in seen_ids or score > seen_ids[pid]):
            seen_ids[pid] = score

    if not seen_ids:
        return []

    # Fetch Player objects and sort by score descending
    players_by_id = {
        p.id: p
        for p in session.query(Player).filter(Player.id.in_(seen_ids.keys())).all()
    }

    ranked = sorted(seen_ids.items(), key=lambda x: x[1], reverse=True)
    return [(players_by_id[pid], score) for pid, score in ranked[:limit] if pid in players_by_id]


def find_player_one(
    session: Session,
    name_query: str,
    threshold: int = 80,
) -> Optional[Player]:
    """
    Return the single best-matching Player, or None if no match clears the threshold.
    Raises ValueError if the result is ambiguous (top two results within 5 points of each other).
    """
    results = find_player(session, name_query, threshold=threshold, limit=2)
    if not results:
        return None
    if len(results) == 2 and abs(results[0][1] - results[1][1]) < 5:
        names = [r[0].full_name for r in results]
        raise ValueError(
            f"Ambiguous name {name_query!r}. Did you mean one of: {names}? "
            "Use find_player() for ranked results."
        )
    return results[0][0]


def add_name_variant(session: Session, player_id: int, variant: str) -> None:
    """Register an alternate spelling for a player so future lookups find them."""
    player = session.get(Player, player_id)
    if player is None:
        raise ValueError(f"No player with id={player_id}")
    player.add_name_variant(variant)
