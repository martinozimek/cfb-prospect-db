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

## Quick Start — 5-minute tour

```python
from ffdb import FFDatabase
db = FFDatabase()   # reads FF_DB_PATH from .env automatically

# 1. Look up any player by name (fuzzy — typos are fine)
profile = db.get_profile("Emeka Egbuka")
print(profile["position"])          # WR
print(profile["career"]["peak_dominator_rating"])   # 0.42
print(profile["combine"]["speed_score"])            # 103.7
print(profile["draft"]["draft_capital_score"])      # 81.2

# 2. Find all WRs drafted 2023 in the first 2 rounds
picks = db.search_draft_class(year=2023, position="WR", max_round=2)
for p in picks:
    print(p["full_name"], p["overall_pick"])

# 3. Get season-by-season stats for a player
player = db.find_player("CJ Stroud")  # QB fine too
seasons = db.get_cfb_seasons(player.id)
for s in seasons:
    print(s.season_year, s.team, f"{s.rec_yards or s.pass_yards} yds")

# 4. Compare dominator ratings across the 2026 WR class
wrs = db.search_players(position="WR", min_year=2025, min_games=6)
for w in wrs:
    m = db.get_player_metrics(w["player_id"])
    best = max((x["dominator_rating"] or 0 for x in m), default=0)
    print(f"{w['full_name']:25s}  dominator={best:.3f}")
```

---

## Quick Plots

```python
import matplotlib.pyplot as plt
import pandas as pd
from ffdb import FFDatabase
db = FFDatabase()
```

### Plot 1 — Dominator rating vs. draft capital (2018–2022 WRs)

```python
picks = db.search_draft_class(year=2020, position="WR")
rows = []
for p in picks:
    m  = db.get_player_metrics(p["player_id"])
    dp = db.get_draft_pick(p["player_id"])
    if not m or not dp:
        continue
    best_dom = max((x["dominator_rating"] or 0 for x in m), default=0)
    rows.append({"name": p["full_name"],
                 "dominator": best_dom,
                 "draft_capital": dp["draft_capital_score"]})

df = pd.DataFrame(rows).dropna()
fig, ax = plt.subplots(figsize=(8, 5))
ax.scatter(df["dominator"], df["draft_capital"], s=60, alpha=0.7)
for _, r in df.iterrows():
    ax.annotate(r["name"].split()[-1], (r["dominator"], r["draft_capital"]),
                fontsize=7, xytext=(3, 2), textcoords="offset points")
ax.set_xlabel("Best College Dominator Rating")
ax.set_ylabel("Draft Capital Score (0–100)")
ax.set_title("WR Dominator vs. Draft Capital — 2020 Class")
plt.tight_layout()
plt.savefig("dominator_vs_capital.png", dpi=150)
plt.show()
```

### Plot 2 — Age progression chart for a single prospect

```python
player = db.find_player("Tetairoa McMillan")
metrics = db.get_player_metrics(player.id)

years  = [m["season_year"] for m in metrics]
dom    = [m["dominator_rating"] or 0 for m in metrics]
ages   = [m["age_at_season_start"] or 0 for m in metrics]
rec_rate = [m["rec_yards_per_team_pass_att"] or 0 for m in metrics]

fig, ax1 = plt.subplots(figsize=(7, 4))
ax2 = ax1.twinx()
ax1.bar(years, dom, alpha=0.5, color="steelblue", label="Dominator Rating")
ax2.plot(years, rec_rate, "o-", color="tomato", label="Rec Rate (YPTA)")
ax1.set_xlabel("Season")
ax1.set_ylabel("Dominator Rating", color="steelblue")
ax2.set_ylabel("Rec Yds / Team Pass Att", color="tomato")
ax1.set_title(f"{player.full_name} — Season-by-Season Profile  (age at start shown)")
for y, yr, ag in zip(dom, years, ages):
    ax1.text(yr, y + 0.005, f"age {ag:.1f}", ha="center", fontsize=7)
fig.legend(loc="upper left", bbox_to_anchor=(0.12, 0.88))
plt.tight_layout()
plt.savefig("player_profile.png", dpi=150)
plt.show()
```

### Plot 3 — Speed score distribution by position (2024 combine class)

```python
from ffdb import NFLCombineResult
from ffdb.database import get_session
from config import get_db_path

with get_session(get_db_path()) as s:
    rows = s.query(NFLCombineResult).filter(
        NFLCombineResult.combine_year == 2024,
        NFLCombineResult.position.in_(["WR", "RB", "TE"]),
        NFLCombineResult.speed_score.isnot(None),
    ).all()

df = pd.DataFrame([{
    "position": r.position,
    "speed_score": r.speed_score,
    "forty_time": r.forty_time,
} for r in rows])

fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=False)
for ax, pos in zip(axes, ["WR", "RB", "TE"]):
    sub = df[df["position"] == pos]["speed_score"].dropna()
    ax.hist(sub, bins=15, color={"WR": "steelblue", "RB": "tomato", "TE": "seagreen"}[pos],
            alpha=0.8, edgecolor="white")
    ax.axvline(sub.mean(), color="black", linestyle="--", linewidth=1)
    ax.set_title(f"{pos}  (n={len(sub)}, mean={sub.mean():.1f})")
    ax.set_xlabel("Speed Score")
    ax.set_ylabel("Count")
fig.suptitle("Speed Score Distribution by Position — 2024 Combine", fontweight="bold")
plt.tight_layout()
plt.savefig("speed_score_dist.png", dpi=150)
plt.show()
```

### Plot 4 — Export any cohort to pandas for custom analysis

```python
# All skill-position seasons from 2020–2025 with 6+ games → DataFrame
wrs = db.search_players(position="WR", min_year=2020, min_games=6)

rows = []
for w in wrs:
    for m in db.get_player_metrics(w["player_id"]):
        if m["season_year"] < 2020:
            continue
        rows.append({"name": w["full_name"], **m})

df = pd.DataFrame(rows)
df.to_csv("wr_seasons_2020_2025.csv", index=False)
print(df[["name", "season_year", "dominator_rating",
          "rec_yards_per_team_pass_att", "ppa_avg_pass"]].head(20))
```

---

## Data coverage (current DB)

| Source | Coverage | Notes |
|---|---|---|
| College seasons | 2007–2025 (~150k rows) | 100% games_played |
| Draft picks (WR/RB/TE) | 2011–2025 (1,071 rows) | 100% |
| NFL combine | 2011–2025 (1,503 rows) | ~76–79% of drafted players |
| `dominator_rating`, `rec_yards_per_team_pass_att` | 100% for 2014+ | Sparse pre-2014 (CFBD API gap) |
| `ppa_avg_pass`, `usage_overall` | ~73% for WR (2015+) | CFBD gap 2011-2014 |
| `three_cone`, `shuttle` | ~50% of combine attendees | Players who ran the drill |
| Recruiting (247Sports) | 2018–2025 | 2011–2017 blocked until March 2026 CFBD quota reset |
| `consensus_rank` (big board) | 2016–2026 | No free source pre-2016 |
| College `targets` | Not reliably tracked | CFBD does not consistently report targets; `reception_share` is the proxy |

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
