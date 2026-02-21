"""
High-level query interface for the FF college football database.

Usage:
    from ffdb import FFDatabase

    db = FFDatabase("ff.db")

    player = db.find_player("Luther Burden")
    print(db.get_cfb_career(player.id))
    print(db.get_player_metrics(player.id, year=2022))
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional

from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session, sessionmaker

from ffdb.database import CFBPlayerSeason, CFBTeamSeason, Player, Recruiting, init_db
from ffdb.utils.name_matching import find_player, find_player_one


class FFDatabase:
    """
    Main entry point for querying the FF college football database.

    Manages its own connection pool; all public methods open and close
    sessions internally so the caller never has to manage transactions.
    """

    def __init__(self, db_path: str, create_tables: bool = True):
        self._db_path = db_path
        if create_tables:
            init_db(db_path)
        self._engine = create_engine(
            f"sqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False},
        )
        self._Session = sessionmaker(bind=self._engine, autoflush=False, autocommit=False)

    def _session(self) -> Session:
        return self._Session()

    # ------------------------------------------------------------------
    # Player lookup
    # ------------------------------------------------------------------

    def find_player(
        self,
        name: str,
        threshold: int = 80,
    ) -> Optional[Player]:
        """
        Return the best-matching Player by fuzzy name search, or None.

        Raises ValueError if the result is ambiguous (two candidates within
        5 fuzzy-score points). In that case use find_players() instead.
        """
        with self._Session() as session:
            return find_player_one(session, name, threshold=threshold)

    def find_players(
        self,
        name: str,
        threshold: int = 70,
        limit: int = 5,
    ) -> list[tuple[Player, float]]:
        """
        Return ranked (Player, score) matches for a name string.
        Useful when the name is ambiguous or partially known.
        """
        with self._Session() as session:
            return find_player(session, name, threshold=threshold, limit=limit)

    def get_player(self, player_id: int) -> Optional[Player]:
        """Fetch a Player by internal database ID."""
        with self._Session() as session:
            return session.get(Player, player_id)

    def get_player_by_cfbd_id(self, cfbd_id: int) -> Optional[Player]:
        """Fetch a Player by their CFBD API ID."""
        with self._Session() as session:
            return session.query(Player).filter(Player.cfbd_id == cfbd_id).first()

    # ------------------------------------------------------------------
    # College season stats
    # ------------------------------------------------------------------

    def get_cfb_seasons(self, player_id: int) -> list[CFBPlayerSeason]:
        """Return all college seasons for a player, sorted by year ascending."""
        with self._Session() as session:
            return (
                session.query(CFBPlayerSeason)
                .filter(CFBPlayerSeason.player_id == player_id)
                .order_by(CFBPlayerSeason.season_year)
                .all()
            )

    def get_cfb_season(self, player_id: int, year: int) -> Optional[CFBPlayerSeason]:
        """Return a single season record for a player and year (None if not found)."""
        with self._Session() as session:
            return (
                session.query(CFBPlayerSeason)
                .filter(
                    CFBPlayerSeason.player_id == player_id,
                    CFBPlayerSeason.season_year == year,
                )
                .first()
            )

    # ------------------------------------------------------------------
    # Career aggregates
    # ------------------------------------------------------------------

    def get_cfb_career(self, player_id: int) -> dict[str, Any]:
        """
        Return cumulative and per-game career college stats for a player.

        Returns a dict with:
            seasons_played, total_games, cumulative stats,
            per_game stats, best_season_rec_yards_per_team_pass_att,
            peak_dominator_rating, peak_reception_share.
        """
        seasons = self.get_cfb_seasons(player_id)
        if not seasons:
            return {"player_id": player_id, "seasons_played": 0}

        def safe_sum(attr: str) -> Optional[float]:
            vals = [getattr(s, attr) for s in seasons if getattr(s, attr) is not None]
            return sum(vals) if vals else None

        def safe_max(attr: str) -> Optional[float]:
            vals = [getattr(s, attr) for s in seasons if getattr(s, attr) is not None]
            return max(vals) if vals else None

        total_games = safe_sum("games_played") or 0

        cumulative = {
            "pass_completions": safe_sum("pass_completions"),
            "pass_attempts": safe_sum("pass_attempts"),
            "pass_yards": safe_sum("pass_yards"),
            "pass_tds": safe_sum("pass_tds"),
            "interceptions": safe_sum("interceptions"),
            "rush_attempts": safe_sum("rush_attempts"),
            "rush_yards": safe_sum("rush_yards"),
            "rush_tds": safe_sum("rush_tds"),
            "targets": safe_sum("targets"),
            "receptions": safe_sum("receptions"),
            "rec_yards": safe_sum("rec_yards"),
            "rec_tds": safe_sum("rec_tds"),
        }

        def per_game(key: str) -> Optional[float]:
            val = cumulative.get(key)
            if val is not None and total_games > 0:
                return round(val / total_games, 3)
            return None

        return {
            "player_id": player_id,
            "seasons_played": len(seasons),
            "total_games": total_games,
            "cumulative": cumulative,
            "per_game": {k: per_game(k) for k in cumulative},
            "peak_rec_yards_per_team_pass_att": safe_max("rec_yards_per_team_pass_att"),
            "peak_dominator_rating": safe_max("dominator_rating"),
            "peak_reception_share": safe_max("reception_share"),
            "peak_ppa_overall": safe_max("ppa_avg_overall"),
        }

    # ------------------------------------------------------------------
    # Derived metrics view
    # ------------------------------------------------------------------

    def get_player_metrics(
        self,
        player_id: int,
        year: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Return derived metrics for a player across all seasons, or for a specific year.

        Each row in the returned list is a dict with:
            season_year, team, games_played, age_at_season_start,
            rec_yards_per_team_pass_att, dominator_rating, reception_share,
            usage_overall, usage_pass, ppa_avg_overall, ppa_avg_pass.
        """
        with self._Session() as session:
            q = session.query(CFBPlayerSeason).filter(
                CFBPlayerSeason.player_id == player_id
            )
            if year is not None:
                q = q.filter(CFBPlayerSeason.season_year == year)
            rows = q.order_by(CFBPlayerSeason.season_year).all()

        return [
            {
                "season_year": r.season_year,
                "team": r.team,
                "games_played": r.games_played,
                "age_at_season_start": r.age_at_season_start,
                "rec_yards_per_team_pass_att": r.rec_yards_per_team_pass_att,
                "dominator_rating": r.dominator_rating,
                "reception_share": r.reception_share,
                "usage_overall": r.usage_overall,
                "usage_pass": r.usage_pass,
                "usage_rush": r.usage_rush,
                "usage_standard_downs": r.usage_standard_downs,
                "usage_passing_downs": r.usage_passing_downs,
                "ppa_avg_overall": r.ppa_avg_overall,
                "ppa_avg_pass": r.ppa_avg_pass,
                "ppa_avg_rush": r.ppa_avg_rush,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Player search / filtering
    # ------------------------------------------------------------------

    def search_players(
        self,
        position: Optional[str] = None,
        team: Optional[str] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
        min_games: int = 1,
    ) -> list[dict[str, Any]]:
        """
        Find players matching optional filters. Returns a list of dicts with
        player identity info and the years they appear in the DB.

        Parameters
        ----------
        position:  Filter to a position string (e.g. "WR", "RB", "TE").
        team:      Filter to a team name (case-insensitive substring match).
        min_year:  Only include players with at least one season >= min_year.
        max_year:  Only include players with at least one season <= max_year.
        min_games: Minimum games played in any season to be included.
        """
        with self._Session() as session:
            q = (
                session.query(Player, CFBPlayerSeason)
                .join(CFBPlayerSeason, CFBPlayerSeason.player_id == Player.id)
            )
            if position:
                q = q.filter(Player.position == position.upper())
            if team:
                q = q.filter(CFBPlayerSeason.team.ilike(f"%{team}%"))
            if min_year:
                q = q.filter(CFBPlayerSeason.season_year >= min_year)
            if max_year:
                q = q.filter(CFBPlayerSeason.season_year <= max_year)
            if min_games:
                q = q.filter(CFBPlayerSeason.games_played >= min_games)

            rows = q.order_by(Player.full_name, CFBPlayerSeason.season_year).all()

        # Deduplicate: group by player
        seen: dict[int, dict] = {}
        for player, season in rows:
            if player.id not in seen:
                seen[player.id] = {
                    "player_id": player.id,
                    "full_name": player.full_name,
                    "position": player.position,
                    "height_inches": player.height_inches,
                    "weight_lbs": player.weight_lbs,
                    "seasons": [],
                }
            seen[player.id]["seasons"].append(season.season_year)

        return list(seen.values())

    # ------------------------------------------------------------------
    # Team season stats
    # ------------------------------------------------------------------

    def get_team_season(self, team: str, year: int) -> Optional[CFBTeamSeason]:
        """Return team-level season stats for a specific team and year."""
        with self._Session() as session:
            return (
                session.query(CFBTeamSeason)
                .filter(
                    CFBTeamSeason.team == team,
                    CFBTeamSeason.season_year == year,
                )
                .first()
            )

    # ------------------------------------------------------------------
    # Recruiting
    # ------------------------------------------------------------------

    def get_recruiting(self, player_id: int) -> Optional[Recruiting]:
        """Return recruiting profile for a player (if available)."""
        with self._Session() as session:
            return (
                session.query(Recruiting)
                .filter(Recruiting.player_id == player_id)
                .first()
            )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def add_name_variant(self, player_id: int, variant: str) -> None:
        """Register an alternate name spelling for a player."""
        from ffdb.utils.name_matching import add_name_variant as _add
        with self._Session() as session:
            _add(session, player_id, variant)
            session.commit()

    def close(self) -> None:
        self._engine.dispose()
