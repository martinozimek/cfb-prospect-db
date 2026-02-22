# cfb-prospect-db

A personal SQLite database and Python library for college football prospect analysis. Aggregates CFBD stats, recruiting data, NFL combine results, and draft capital into a single queryable store — with a pre-built ZAP component sheet for the current draft class.

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env          # fill in FF_DB_PATH and CFBD_API_KEY
```

Get a free CFBD API key at [collegefootballdata.com](https://collegefootballdata.com).

---

## Populate the database

```bash
# College stats (2021–2025)
python scripts/populate_db.py

# NFL combine, draft capital, rosters, strength metrics
python scripts/populate_nfl.py
```

Both scripts are **idempotent** — safe to re-run.

### Refresh / incremental update

```bash
python scripts/refresh.py              # smart refresh (checks log, skips unchanged)
python scripts/refresh.py --check      # see what's stale without writing anything
python scripts/refresh.py --full       # force full re-ingest of everything
python scripts/refresh.py --source=combine --years 2026   # single source
```

---

## Python library usage

```python
from ffdb import FFDatabase

db = FFDatabase()   # reads FF_DB_PATH from .env automatically
```

### Full player profile (recommended starting point)

```python
profile = db.get_profile("Carnell Tate")

# Returns a dict with every data point we have:
profile["full_name"]            # "Carnell Tate"
profile["position"]             # "WR"
profile["weight_lbs"]           # 191.0
profile["seasons"]              # list of per-season dicts
profile["combine"]              # combine measurables (or None)
profile["draft"]                # draft pick info (or None)
profile["recruiting"]           # recruiting stars/rating (or None)
profile["career"]               # cumulative + per-game career totals
```

### Player lookup

```python
# Fuzzy name match — raises ValueError if ambiguous
player = db.find_player("Emeka Egbuka")

# Ranked candidates when name is ambiguous
results = db.find_players("Smith", threshold=70)
# → [(Player, 94.2), (Player, 88.1), ...]

# By database ID or CFBD API ID
player = db.get_player(player_id=1234)
player = db.get_player_by_cfbd_id(cfbd_id=4429084)
```

### College season stats

```python
# All seasons sorted by year
seasons = db.get_cfb_seasons(player.id)
for s in seasons:
    print(s.season_year, s.team, s.rec_yards, s.games_played)

# Single season
s = db.get_cfb_season(player.id, year=2024)

# Career aggregates (cumulative + per-game + peak metrics)
career = db.get_cfb_career(player.id)
career["total_games"]                     # int
career["cumulative"]["rec_yards"]         # total receiving yards
career["per_game"]["rec_yards"]           # yards per game
career["peak_dominator_rating"]           # best single-season dominator rating
career["peak_rec_yards_per_team_pass_att"] # best SOS-denominator rate
```

### Derived metrics

```python
# Per-season derived metrics (SOS-adjusted rates, dominator, PPA, usage)
metrics = db.get_player_metrics(player.id)
# → list of dicts, one per season:
# [{"season_year": 2023, "rec_yards_per_team_pass_att": 0.247,
#   "dominator_rating": 0.38, "reception_share": 0.22,
#   "usage_overall": 0.19, "ppa_avg_overall": 0.41, ...}, ...]
```

### Search and filter

```python
# All WRs with 6+ games in 2024–2025
wrs = db.search_players(position="WR", min_year=2024, min_games=6)
# → [{"player_id": ..., "full_name": ..., "position": "WR", "seasons": [2024, 2025]}, ...]

# 2024 draft class, WR only, rounds 1-3
picks = db.search_draft_class(year=2024, position="WR", max_round=3)
```

### NFL data

```python
# Combine measurables
combine = db.get_combine(player.id)
combine["forty_time"]    # 4.34
combine["speed_score"]   # 107.3

# Draft pick
pick = db.get_draft_pick(player.id)
pick["overall_pick"]         # 12
pick["draft_capital_score"]  # 77.6  (0–100, exponential decay)
```

### Recruiting

```python
rec = db.get_recruiting(player.id)
rec.stars            # 4
rec.rating           # 0.9132
rec.ranking_national # 47
```

### Data freshness

```python
status = db.get_ingestion_status()
for row in status:
    print(row["source"], row["scope"], row["last_run_utc"], row["status"])
```

---

## ZAP component sheet (2026 draft class)

```bash
python scripts/zap_components.py
```

Outputs a ranked table of WR/RB/TE prospects with:
- **BestAdjRate** — SOS-adjusted prorated rec_yds/team_pass_att (Breakout Score proxy)
- **AgeAtBest** — age at best season (estimated from recruit year when DOB unavailable)
- **EarlyDeclare** — True if ≤3 college seasons in DB
- **TeammateSc** — sum of draft_capital_score for same-school skill players drafted 2021–2025
- **Weight**, **SpeedScore** — from NFL combine
- **DraftCapital** — post-draft (shown as 0 for current prospects pre-draft)

```bash
# Save to CSV
python scripts/zap_components.py --output zap_2026.csv

# Show top 20 per position for 2024 season
python scripts/zap_components.py --season 2024 --top 20
```

---

## Database schema

| Table | Description |
|---|---|
| `players` | Canonical player identity (name, position, height/weight, DOB) |
| `cfb_player_seasons` | Per-season college stats + derived metrics (dominator, SOS rate, usage, PPA) |
| `cfb_team_seasons` | Team denominators + strength metrics (SP+, FPI SOS rank, SRS) |
| `recruiting` | 247Sports composite ratings, stars, national ranking |
| `nfl_draft_picks` | Round, pick, team, draft capital score (0–100) |
| `nfl_combine_results` | Measurables + Speed Score |
| `data_ingestion_log` | Refresh timestamps per source/scope |

---

## Direct ORM access

For advanced queries, import models directly:

```python
from ffdb import Player, CFBPlayerSeason, CFBTeamSeason, NFLDraftPick
from ffdb.database import get_session
from config import get_db_path

with get_session(get_db_path()) as session:
    top_wrs = (
        session.query(Player, CFBPlayerSeason)
        .join(CFBPlayerSeason, CFBPlayerSeason.player_id == Player.id)
        .filter(Player.position == "WR", CFBPlayerSeason.season_year == 2025)
        .order_by(CFBPlayerSeason.rec_yards.desc())
        .limit(10)
        .all()
    )
```

---

## Project structure

```
cfb-prospect-db/
├── ffdb/
│   ├── __init__.py          ← import FFDatabase and ORM models from here
│   ├── database.py          ← SQLAlchemy ORM models
│   ├── queries.py           ← FFDatabase high-level API
│   ├── collectors/
│   │   ├── cfbd_collector.py    ← CFBD API wrapper
│   │   └── pfr_collector.py     ← nflverse data (combine, draft)
│   └── utils/
│       ├── player_index.py      ← fast in-memory fuzzy matching for ingestion
│       └── name_matching.py     ← session-based fuzzy player search
├── scripts/
│   ├── populate_db.py       ← ingest CFBD college stats
│   ├── populate_nfl.py      ← ingest combine, draft, rosters, strength
│   ├── refresh.py           ← master refresh orchestrator
│   └── zap_components.py    ← ZAP component sheet for current draft class
├── config.py                ← reads .env
├── requirements.txt
└── .env.example
```
