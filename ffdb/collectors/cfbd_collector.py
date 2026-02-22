"""
CFBD API data collector.

Wraps the official cfbd Python client to fetch player and team data
for bulk ingestion into the local SQLite database.

API documentation: https://apinext.collegefootballdata.com
Python client:     pip install cfbd
"""

import logging
import time
from typing import Any, Optional

import cfbd
from cfbd.rest import ApiException

logger = logging.getLogger(__name__)


class CFBDCollector:
    """
    Fetches college football data from the CFBD API.

    All methods return raw cfbd model objects (or dicts). Parsing and
    DB insertion are handled by the populate script to keep concerns
    separated.
    """

    def __init__(self, api_key: str, request_delay: float = 0.25):
        """
        Parameters
        ----------
        api_key:       Bearer token from collegefootballdata.com.
        request_delay: Seconds to wait between API calls (courtesy throttle).
        """
        self._config = cfbd.Configuration(access_token=api_key)
        self._delay = request_delay

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _players_api(self, client: cfbd.ApiClient) -> cfbd.PlayersApi:
        return cfbd.PlayersApi(client)

    def _stats_api(self, client: cfbd.ApiClient) -> cfbd.StatsApi:
        return cfbd.StatsApi(client)

    def _metrics_api(self, client: cfbd.ApiClient) -> cfbd.MetricsApi:
        return cfbd.MetricsApi(client)

    def _ratings_api(self, client: cfbd.ApiClient) -> cfbd.RatingsApi:
        return cfbd.RatingsApi(client)

    def _recruiting_api(self, client: cfbd.ApiClient) -> cfbd.RecruitingApi:
        return cfbd.RecruitingApi(client)

    def _games_api(self, client: cfbd.ApiClient) -> cfbd.GamesApi:
        return cfbd.GamesApi(client)

    def _sleep(self) -> None:
        time.sleep(self._delay)

    def _call(self, fn, *args, **kwargs) -> Any:
        """Call an API method with basic retry on transient errors."""
        for attempt in range(3):
            try:
                result = fn(*args, **kwargs)
                self._sleep()
                return result
            except ApiException as exc:
                if exc.status in (429, 503) and attempt < 2:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited (status %s). Waiting %ds...", exc.status, wait)
                    time.sleep(wait)
                else:
                    raise
        return None  # unreachable

    # ------------------------------------------------------------------
    # Player season statistics
    # ------------------------------------------------------------------

    def fetch_player_season_stats(self, year: int) -> list[Any]:
        """
        Fetch all individual player season stats for a given year.
        Returns a list of PlayerSeasonStat objects from the CFBD API.

        Each object has attributes: season, player_id, player, team, conference,
        category, stat_type, stat.
        """
        logger.info("Fetching player season stats for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._stats_api(client)
            # Fetch multiple stat categories in one call (API returns all if no category filter)
            result = self._call(api.get_player_season_stats, year=year)
        logger.info("  Retrieved %d player-stat rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Team season statistics (denominators)
    # ------------------------------------------------------------------

    def fetch_team_season_stats(self, year: int) -> list[Any]:
        """
        Fetch team-level season statistics for a given year.
        Used to compute denominators (team pass attempts, team receptions, etc.).
        Returns a list of TeamSeasonStat objects.
        """
        logger.info("Fetching team season stats for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._stats_api(client)
            result = self._call(api.get_team_stats, year=year)
        logger.info("  Retrieved %d team-stat rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Player usage
    # ------------------------------------------------------------------

    def fetch_player_usage(self, year: int, exclude_garbage_time: bool = True) -> list[Any]:
        """
        Fetch player usage metrics (snap-count share by down/situation) for a year.
        Returns a list of PlayerUsage objects.
        """
        logger.info("Fetching player usage for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._players_api(client)
            result = self._call(
                api.get_player_usage,
                year=year,
                exclude_garbage_time=exclude_garbage_time,
            )
        logger.info("  Retrieved %d usage rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # PPA metrics
    # ------------------------------------------------------------------

    def fetch_player_season_ppa(
        self,
        year: int,
        exclude_garbage_time: bool = True,
    ) -> list[Any]:
        """
        Fetch per-player season PPA (Predicted Points Added) for a year.
        Returns a list of PlayerSeasonPpa objects.
        """
        logger.info("Fetching player season PPA for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._metrics_api(client)
            result = self._call(
                api.get_predicted_points_added_by_player_season,
                year=year,
                exclude_garbage_time=exclude_garbage_time,
            )
        logger.info("  Retrieved %d PPA rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # SP+ / Strength of Schedule ratings
    # ------------------------------------------------------------------

    def fetch_sp_plus_ratings(self, year: int) -> list[Any]:
        """
        Fetch SP+ ratings by team for a year (used for strength-of-schedule context).
        Returns a list of TeamSPRating objects.
        """
        logger.info("Fetching SP+ ratings for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._ratings_api(client)
            result = self._call(api.get_sp, year=year)
        logger.info("  Retrieved %d SP+ rating rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Recruiting
    # ------------------------------------------------------------------

    def fetch_recruiting(self, year: int) -> list[Any]:
        """
        Fetch individual player recruiting data for a class year.
        Returns a list of Recruit objects.
        """
        logger.info("Fetching recruiting data for class of %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._recruiting_api(client)
            result = self._call(api.get_recruits, year=year)
        logger.info("  Retrieved %d recruits for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Player search
    # ------------------------------------------------------------------

    def search_player(self, name: str, position: Optional[str] = None) -> list[Any]:
        """
        Search for a player by name via the CFBD API.
        Useful for resolving a player's CFBD ID and basic identity info.
        Returns a list of PlayerSearchResult objects.
        """
        with cfbd.ApiClient(self._config) as client:
            api = self._players_api(client)
            kwargs: dict[str, Any] = {"search_term": name}
            if position:
                kwargs["position"] = position
            result = self._call(api.search_players, **kwargs)
        return result or []

    # ------------------------------------------------------------------
    # Rosters (for player identity / DOB when available)
    # ------------------------------------------------------------------

    def fetch_roster(self, team: str, year: int) -> list[Any]:
        """
        Fetch the roster for a specific team and year.
        Roster entries include height, weight, hometown, position.
        """
        with cfbd.ApiClient(self._config) as client:
            from cfbd import TeamsApi
            api = TeamsApi(client)
            result = self._call(api.get_roster, team=team, year=year)
        return result or []

    def fetch_player_game_counts(self, year: int) -> dict[int, int]:
        """
        Return a dict mapping CFBD player_id (int) → games_played for a given year.

        Strategy: loop regular-season weeks 1-16 (covers regular season + conference
        championships). One additional postseason call picks up bowl games.
        Typically 17-18 API calls per year — well within free tier limits.
        """
        player_games: dict[int, set] = {}

        def _absorb(rows: list) -> None:
            for game in (rows or []):
                gid = getattr(game, "id", None)
                if gid is None:
                    return
                for team in (getattr(game, "teams", None) or []):
                    for cat in (getattr(team, "categories", None) or []):
                        for stat_type in (getattr(cat, "types", None) or []):
                            for ath in (getattr(stat_type, "athletes", None) or []):
                                raw_pid = getattr(ath, "id", None)
                                try:
                                    pid = int(raw_pid)
                                except (TypeError, ValueError):
                                    continue
                                player_games.setdefault(pid, set()).add(gid)

        logger.info("  Fetching per-game player counts for %d...", year)
        with cfbd.ApiClient(self._config) as client:
            api = self._games_api(client)

            # Regular season: weeks 1-16 (week 0 is not a valid CFBD week)
            for week in range(1, 17):
                try:
                    rows = self._call(api.get_game_player_stats, year=year, week=week)
                    _absorb(rows)
                except Exception as exc:
                    logger.debug("  game counts week %d: %s", week, exc)

            # Postseason bowl games — loop by FBS team to capture all bowl participants
            # This adds ~130 calls but ensures no bowl games are missed; only run for
            # teams that appear in our DB to keep within rate limits
            try:
                teams = self.fetch_all_teams(year=year)
                # Sample: only teams with bowl appearances (heuristic: top ~60 programs)
                # In practice we fetch all FBS independents since week-loop misses them
                independents = [
                    getattr(t, "school", None)
                    for t in teams
                    if getattr(t, "conference", None) in ("FBS Independents",)
                    and getattr(t, "school", None)
                ]
                for team_name in independents:
                    try:
                        rows = self._call(api.get_game_player_stats, year=year, team=team_name)
                        _absorb(rows)
                    except Exception as exc:
                        logger.debug("  game counts independent %r: %s", team_name, exc)
            except Exception as exc:
                logger.warning("  game counts postseason/independent pass failed: %s", exc)

        result = {pid: len(gids) for pid, gids in player_games.items()}
        logger.info("  Built game counts for %d players in %d", len(result), year)
        return result

    def fetch_all_teams(self, year: Optional[int] = None) -> list[Any]:
        """Return a list of all FBS teams (optionally filtered to an active year)."""
        with cfbd.ApiClient(self._config) as client:
            from cfbd import TeamsApi
            api = TeamsApi(client)
            kwargs: dict[str, Any] = {}
            if year:
                kwargs["year"] = year
            result = self._call(api.get_fbs_teams, **kwargs)
        return result or []
