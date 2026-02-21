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
            result = self._call(api.get_recruiting_players, year=year)
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
