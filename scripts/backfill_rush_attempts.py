"""
One-time migration: backfill rush_attempts in cfb_team_seasons table.
Uses CFBD /stats/season endpoint (same one populate_db.py uses for team stats).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_db_path
from ffdb.database import CFBTeamSeason, Base
from ffdb.collectors.cfbd_collector import CFBDCollector
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import text, inspect as sa_inspect

db_path = get_db_path()
engine = create_engine(f"sqlite:///{db_path}", echo=False)
# SQLite requires explicit ALTER TABLE for new columns on existing tables
with engine.connect() as conn:
    existing_cols = [c["name"] for c in sa_inspect(engine).get_columns("cfb_team_seasons")]
    if "rush_attempts" not in existing_cols:
        conn.execute(text("ALTER TABLE cfb_team_seasons ADD COLUMN rush_attempts INTEGER"))
        conn.commit()
        print("Added rush_attempts column to cfb_team_seasons")
    else:
        print("rush_attempts column already exists")
SessionLocal = sessionmaker(bind=engine)

from config import get_api_key
col = CFBDCollector(api_key=get_api_key())

with SessionLocal() as session:
    # Determine which years are in the DB
    years = sorted(set(
        r[0] for r in session.query(CFBTeamSeason.season_year).distinct()
    ))
    print(f"Years in DB: {years[0]}–{years[-1]} ({len(years)} total)")

    total_updated = 0
    for year in years:
        raw = col.fetch_team_season_stats(year)
        if not raw:
            print(f"  {year}: no data from API")
            continue

        # raw is long-format: one row per (team, stat_name, stat_value)
        # First pivot to {team: {stat_name: value}}
        team_stats: dict[str, dict] = {}
        for row in raw:
            team = getattr(row, "team", None)
            stat_name = getattr(row, "stat_name", None) or getattr(row, "type", None)
            stat_val_obj = getattr(row, "stat_value", None)
            stat_val = (
                getattr(stat_val_obj, "actual_instance", None)
                if stat_val_obj is not None
                else getattr(row, "stat", None)
            )
            if team and stat_name:
                team_stats.setdefault(team, {})[stat_name] = stat_val

        # Extract rush_attempts using same key variants as _extract_team_denominators
        def _get(stats, *keys):
            for k in keys:
                v = stats.get(k)
                if v is not None:
                    try:
                        return int(v)
                    except (ValueError, TypeError):
                        pass
            return None

        team_rush = {}
        for team, stats in team_stats.items():
            ra = _get(stats, "rushingAttempts", "rush_attempts", "rushingCarries")
            if ra is not None:
                team_rush[team] = ra

        if not team_rush:
            print(f"  {year}: parsed 0 rush_attempts")
            if raw:
                sample = raw[0]
                print(f"    sample attrs: {[a for a in dir(sample) if not a.startswith('_')][:20]}")
                # Print all stat_names for first team to see what's available
                first_team = getattr(sample, "team", None)
                names = [getattr(r, "stat_name", None) for r in raw if getattr(r, "team", None) == first_team]
                print(f"    stat_names for {first_team}: {names[:30]}")
            continue

        # Update DB rows
        rows = session.query(CFBTeamSeason).filter(
            CFBTeamSeason.season_year == year
        ).all()
        updated = 0
        for db_row in rows:
            ra = team_rush.get(db_row.team)
            if ra is not None:
                db_row.rush_attempts = ra
                updated += 1
        session.commit()
        total_updated += updated
        print(f"  {year}: updated {updated}/{len(rows)} rows  (API returned {len(team_rush)} teams)")

    print(f"\nDone. Total rows updated: {total_updated}")
