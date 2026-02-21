"""
High-level query interface for the FF college football database.

Usage:
    from ffdb import FFDatabase

    db = FFDatabase()                        # auto-discovers ff.db via .env
    db = FFDatabase("path/to/ff.db")        # explicit path

    profile = db.get_profile("Luther Burden")   # full player profile in one call
    player  = db.find_player("Emeka Egbuka")    # returns Player ORM object
    print(db.get_cfb_career(player.id))
    print(db.get_player_metrics(player.id, year=2022))
"""

from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ffdb.database import (
    CFBPlayerSeason,
    CFBTeamSeason,
    NFLCombineResult,
    NFLDraftPick,
    Player,
    Recruiting,
    init_db,
)
from ffdb.utils.name_matching import find_player, find_player_one


class FFDatabase:
    """
    Main entry point for querying the FF college football database.

    Manages its own connection pool; all public methods open and close
    sessions internally so the caller never has to manage transactions.
    """

    def __init__(self, db_path: Optional[str] = None, create_tables: bool = True):
        if db_path is None:
            import sys
            from pathlib import Path
            _root = Path(__file__).parent.parent
            if str(_root) not in sys.path:
                sys.path.insert(0, str(_root))
            from config import get_db_path
            db_path = get_db_path()
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

    # ------------------------------------------------------------------
    # NFL Combine
    # ------------------------------------------------------------------

    def get_combine(self, player_id: int) -> Optional[dict[str, Any]]:
        """
        Return NFL Combine measurables for a player, or None if not in the DB.

        Returned dict keys:
            combine_year, position, college,
            height_inches, weight_lbs, forty_time, vertical_jump,
            broad_jump, three_cone, shuttle, bench_press, speed_score
        """
        with self._Session() as session:
            row = (
                session.query(NFLCombineResult)
                .filter(NFLCombineResult.player_id == player_id)
                .first()
            )
            if row is None:
                return None
            return {
                "combine_year": row.combine_year,
                "position": row.position,
                "college": row.college,
                "height_inches": row.height_inches,
                "weight_lbs": row.weight_lbs,
                "forty_time": row.forty_time,
                "vertical_jump": row.vertical_jump,
                "broad_jump": row.broad_jump,
                "three_cone": row.three_cone,
                "shuttle": row.shuttle,
                "bench_press": row.bench_press,
                "speed_score": row.speed_score,
            }

    # ------------------------------------------------------------------
    # NFL Draft
    # ------------------------------------------------------------------

    def get_draft_pick(self, player_id: int) -> Optional[dict[str, Any]]:
        """
        Return NFL Draft pick info for a player, or None if undrafted.

        Returned dict keys:
            draft_year, draft_round, overall_pick, nfl_team,
            position_drafted, draft_capital_score
        """
        with self._Session() as session:
            row = (
                session.query(NFLDraftPick)
                .filter(NFLDraftPick.player_id == player_id)
                .first()
            )
            if row is None:
                return None
            return {
                "draft_year": row.draft_year,
                "draft_round": row.draft_round,
                "overall_pick": row.overall_pick,
                "nfl_team": row.nfl_team,
                "position_drafted": row.position_drafted,
                "draft_capital_score": row.draft_capital_score,
            }

    def search_draft_class(
        self,
        year: int,
        position: Optional[str] = None,
        max_round: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Return all drafted WR/RB/TE players from a given draft year.

        Parameters
        ----------
        year:      Draft year (e.g. 2024).
        position:  Filter to 'WR', 'RB', or 'TE' (optional).
        max_round: Only return picks up to and including this round (optional).

        Each dict includes player identity + draft info, sorted by overall_pick.
        """
        with self._Session() as session:
            q = (
                session.query(Player, NFLDraftPick)
                .join(NFLDraftPick, NFLDraftPick.player_id == Player.id)
                .filter(NFLDraftPick.draft_year == year)
            )
            if position:
                q = q.filter(NFLDraftPick.position_drafted == position.upper())
            if max_round:
                q = q.filter(NFLDraftPick.draft_round <= max_round)
            rows = q.order_by(NFLDraftPick.overall_pick).all()

        return [
            {
                "player_id": p.id,
                "full_name": p.full_name,
                "position": pick.position_drafted,
                "draft_year": pick.draft_year,
                "draft_round": pick.draft_round,
                "overall_pick": pick.overall_pick,
                "nfl_team": pick.nfl_team,
                "draft_capital_score": pick.draft_capital_score,
                "height_inches": p.height_inches,
                "weight_lbs": p.weight_lbs,
            }
            for p, pick in rows
        ]

    # ------------------------------------------------------------------
    # Full player profile (all data in one call)
    # ------------------------------------------------------------------

    def get_profile(
        self,
        name: str,
        threshold: int = 80,
    ) -> Optional[dict[str, Any]]:
        """
        Return a comprehensive player profile by name, or None if not found.

        Bundles identity, all college seasons, career aggregates, derived
        metrics, recruiting, NFL combine, and NFL draft data into one dict.

        Example
        -------
        >>> db = FFDatabase()
        >>> p = db.get_profile("Emeka Egbuka")
        >>> p["draft"]["draft_round"]
        1
        >>> p["combine"]["forty_time"]
        4.34
        """
        with self._Session() as session:
            player = find_player_one(session, name, threshold=threshold)
            if player is None:
                return None
            pid = player.id

            # --- identity ---
            identity = {
                "player_id": pid,
                "cfbd_id": player.cfbd_id,
                "full_name": player.full_name,
                "position": player.position,
                "height_inches": player.height_inches,
                "weight_lbs": player.weight_lbs,
                "hometown": player.hometown,
                "home_state": player.home_state,
            }

            # --- recruiting ---
            rec_row = (
                session.query(Recruiting)
                .filter(Recruiting.player_id == pid)
                .order_by(Recruiting.recruit_year.desc())
                .first()
            )
            recruiting = None
            if rec_row:
                recruiting = {
                    "recruit_year": rec_row.recruit_year,
                    "stars": rec_row.stars,
                    "rating": rec_row.rating,
                    "ranking_national": rec_row.ranking_national,
                    "ranking_position": rec_row.ranking_position,
                    "classification": rec_row.classification,
                }

            # --- college seasons ---
            season_rows = (
                session.query(CFBPlayerSeason)
                .filter(CFBPlayerSeason.player_id == pid)
                .order_by(CFBPlayerSeason.season_year)
                .all()
            )
            seasons = [
                {
                    "season_year": s.season_year,
                    "team": s.team,
                    "conference": s.conference,
                    "games_played": s.games_played,
                    "targets": s.targets,
                    "receptions": s.receptions,
                    "rec_yards": s.rec_yards,
                    "rec_tds": s.rec_tds,
                    "rush_attempts": s.rush_attempts,
                    "rush_yards": s.rush_yards,
                    "rush_tds": s.rush_tds,
                    "age_at_season_start": s.age_at_season_start,
                    "rec_yards_per_team_pass_att": s.rec_yards_per_team_pass_att,
                    "dominator_rating": s.dominator_rating,
                    "reception_share": s.reception_share,
                    "usage_overall": s.usage_overall,
                    "usage_pass": s.usage_pass,
                    "usage_rush": s.usage_rush,
                    "usage_passing_downs": s.usage_passing_downs,
                    "ppa_avg_overall": s.ppa_avg_overall,
                    "ppa_avg_pass": s.ppa_avg_pass,
                }
                for s in season_rows
            ]

            # --- combine ---
            comb_row = (
                session.query(NFLCombineResult)
                .filter(NFLCombineResult.player_id == pid)
                .first()
            )
            combine = None
            if comb_row:
                combine = {
                    "combine_year": comb_row.combine_year,
                    "height_inches": comb_row.height_inches,
                    "weight_lbs": comb_row.weight_lbs,
                    "forty_time": comb_row.forty_time,
                    "vertical_jump": comb_row.vertical_jump,
                    "broad_jump": comb_row.broad_jump,
                    "three_cone": comb_row.three_cone,
                    "shuttle": comb_row.shuttle,
                    "bench_press": comb_row.bench_press,
                    "speed_score": comb_row.speed_score,
                }

            # --- draft ---
            pick_row = (
                session.query(NFLDraftPick)
                .filter(NFLDraftPick.player_id == pid)
                .first()
            )
            draft = None
            if pick_row:
                draft = {
                    "draft_year": pick_row.draft_year,
                    "draft_round": pick_row.draft_round,
                    "overall_pick": pick_row.overall_pick,
                    "nfl_team": pick_row.nfl_team,
                    "position_drafted": pick_row.position_drafted,
                    "draft_capital_score": pick_row.draft_capital_score,
                }

        return {
            **identity,
            "recruiting": recruiting,
            "seasons": seasons,
            "combine": combine,
            "draft": draft,
            "career": self.get_cfb_career(pid),
        }

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
