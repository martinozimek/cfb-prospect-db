"""
CFBD API data collector using direct HTTP requests.

Replaces the official cfbd Python package which has pydantic v2 incompatibilities.
Returns SimpleNamespace objects that mirror the cfbd package's attribute names so
that all downstream parsing code (populate_db.py, etc.) works without changes.

API documentation: https://apinext.collegefootballdata.com
"""

import logging
import re
import time
import types
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://apinext.collegefootballdata.com"


# ---------------------------------------------------------------------------
# camelCase → snake_case converter
# ---------------------------------------------------------------------------

def _snake(name: str) -> str:
    """Convert camelCase / PascalCase to snake_case, handling acronyms."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


# Rename overrides applied *after* snake_case conversion.
# "stat_value" → "stat":  team stats statValue is a raw number from the API;
#                          populate_db._parse_team_stats falls back to row.stat.
# "pass" → "var_pass":    `pass` is a Python keyword; cfbd package uses var_pass.
_FIELD_OVERRIDES: dict[str, str] = {
    "stat_value": "stat",
    "pass": "var_pass",
}


def _to_ns(obj: Any) -> Any:
    """Recursively convert dict/list to SimpleNamespace with snake_case keys."""
    if isinstance(obj, dict):
        d: dict[str, Any] = {}
        for k, v in obj.items():
            key = _snake(k)
            key = _FIELD_OVERRIDES.get(key, key)
            d[key] = _to_ns(v)
        return types.SimpleNamespace(**d)
    elif isinstance(obj, list):
        return [_to_ns(item) for item in obj]
    else:
        return obj


# ---------------------------------------------------------------------------
# Thin ApiException to match the cfbd package's interface
# ---------------------------------------------------------------------------

class ApiException(Exception):
    def __init__(self, status: int, message: str = ""):
        self.status = status
        self.message = message
        super().__init__(f"API error {status}: {message}")


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class CFBDCollector:
    """
    Fetches college football data from the CFBD API via direct HTTP requests.

    Returns SimpleNamespace objects (attribute-access compatible with the old
    cfbd Python package) so downstream parsing code needs no changes.
    """

    def __init__(self, api_key: str, request_delay: float = 0.25):
        self._delay = request_delay
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    def _sleep(self) -> None:
        time.sleep(self._delay)

    def _get(self, path: str, params: dict | None = None) -> list[Any]:
        """GET request with retry on 429/503. Returns list of SimpleNamespace."""
        url = f"{_BASE_URL}{path}"
        for attempt in range(3):
            try:
                resp = self._session.get(url, params=params or {}, timeout=30)
                if resp.status_code in (429, 503) and attempt < 2:
                    wait = 10 * (attempt + 1)
                    logger.warning(
                        "Rate limited (status %s). Waiting %ds...", resp.status_code, wait
                    )
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    raise ApiException(resp.status_code, resp.text[:300])
                content_type = resp.headers.get("content-type", "")
                if "json" not in content_type:
                    raise ApiException(0, f"Non-JSON response ({content_type}): {resp.text[:100]}")
                data = resp.json()
                self._sleep()
                return _to_ns(data)
            except requests.RequestException as exc:
                if attempt < 2:
                    time.sleep(5)
                else:
                    raise ApiException(0, str(exc))
        return []

    # ------------------------------------------------------------------
    # Player season statistics
    # ------------------------------------------------------------------

    def fetch_player_season_stats(self, year: int) -> list[Any]:
        """
        Fetch all individual player season stats for a given year.
        Returns SimpleNamespace objects with attributes:
          player_id, player, team, conference, category, stat_type, stat.
        """
        logger.info("Fetching player season stats for %d...", year)
        result = self._get("/stats/player/season", {"year": year})
        logger.info("  Retrieved %d player-stat rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Team season statistics (denominators)
    # ------------------------------------------------------------------

    def fetch_team_season_stats(self, year: int) -> list[Any]:
        """
        Fetch team-level season statistics for a given year.
        Returns SimpleNamespace objects with attributes: team, stat_name, stat.
        Note: statValue from the API is mapped to `stat` (not stat_value) so that
        populate_db._parse_team_stats picks it up via its fallback path.
        """
        logger.info("Fetching team season stats for %d...", year)
        result = self._get("/stats/season", {"year": year})
        logger.info("  Retrieved %d team-stat rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Player usage
    # ------------------------------------------------------------------

    def fetch_player_usage(self, year: int, exclude_garbage_time: bool = True) -> list[Any]:
        """
        Fetch player usage metrics for a year.
        Returns SimpleNamespace objects with attributes:
          id, usage.overall, usage.var_pass, usage.rush, usage.first_down, etc.
        """
        logger.info("Fetching player usage for %d...", year)
        result = self._get("/player/usage", {
            "year": year,
            "excludeGarbageTime": "true" if exclude_garbage_time else "false",
        })
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
        Fetch per-player season PPA for a year.
        Returns SimpleNamespace objects with:
          id, average_ppa.all, average_ppa.var_pass, average_ppa.rush.
        """
        logger.info("Fetching player season PPA for %d...", year)
        result = self._get("/ppa/players/season", {
            "year": year,
            "excludeGarbageTime": "true" if exclude_garbage_time else "false",
        })
        logger.info("  Retrieved %d PPA rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # SP+ / Strength of Schedule ratings
    # ------------------------------------------------------------------

    def fetch_sp_plus_ratings(self, year: int) -> list[Any]:
        """
        Fetch SP+ ratings by team for a year.
        Returns SimpleNamespace objects with: team, rating, sos.
        """
        logger.info("Fetching SP+ ratings for %d...", year)
        result = self._get("/ratings/sp", {"year": year})
        logger.info("  Retrieved %d SP+ rating rows for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Recruiting
    # ------------------------------------------------------------------

    def fetch_recruiting(self, year: int) -> list[Any]:
        """
        Fetch individual player recruiting data for a class year.
        Returns SimpleNamespace objects with:
          name, stars, rating, ranking, position_ranking, state_province,
          school, recruit_type.
        """
        logger.info("Fetching recruiting data for class of %d...", year)
        result = self._get("/recruiting/players", {"year": year})
        logger.info("  Retrieved %d recruits for %d", len(result) if result else 0, year)
        return result or []

    # ------------------------------------------------------------------
    # Player search
    # ------------------------------------------------------------------

    def search_player(self, name: str, position: Optional[str] = None) -> list[Any]:
        """Search for a player by name."""
        params: dict[str, Any] = {"search": name}
        if position:
            params["position"] = position
        return self._get("/players", params) or []

    # ------------------------------------------------------------------
    # Rosters
    # ------------------------------------------------------------------

    def fetch_roster(self, team: str, year: int) -> list[Any]:
        """Fetch the roster for a specific team and year."""
        return self._get("/teams/roster", {"team": team, "year": year}) or []

    # ------------------------------------------------------------------
    # Game-level player stats (for games_played counts)
    # ------------------------------------------------------------------

    def fetch_player_game_counts(self, year: int) -> dict[int, int]:
        """
        Return a dict mapping CFBD player_id (int) → games_played for a given year.

        Strategy: loop regular-season weeks 1-16, then postseason for independents.
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

        for week in range(1, 17):
            try:
                rows = self._get("/games/players", {"year": year, "week": week})
                _absorb(rows)
            except Exception as exc:
                logger.debug("  game counts week %d: %s", week, exc)

        try:
            teams = self.fetch_all_teams(year=year)
            independents = [
                getattr(t, "school", None)
                for t in teams
                if getattr(t, "conference", None) in ("FBS Independents",)
                and getattr(t, "school", None)
            ]
            for team_name in independents:
                try:
                    rows = self._get("/games/players", {"year": year, "team": team_name})
                    _absorb(rows)
                except Exception as exc:
                    logger.debug("  game counts independent %r: %s", team_name, exc)
        except Exception as exc:
            logger.warning("  game counts postseason/independent pass failed: %s", exc)

        result = {pid: len(gids) for pid, gids in player_games.items()}
        logger.info("  Built game counts for %d players in %d", len(result), year)
        return result

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def fetch_all_teams(self, year: Optional[int] = None) -> list[Any]:
        """Return a list of all FBS teams."""
        params: dict[str, Any] = {}
        if year:
            params["year"] = year
        return self._get("/teams/fbs", params) or []
