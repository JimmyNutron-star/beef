#!/usr/bin/env python3
"""
db.py — build odibets.db from odds.csv and results.csv

Self-contained:
  * writes schema.sql next to this script if it doesn't exist
  * drops & rebuilds odibets.db on each run
  * loads both CSVs into staging tables and normalized tables

Usage:
    uv run db.py
    uv run db.py --odds my_odds.csv --results my_results.csv --db my.db
    uv run db.py --keep          # do NOT delete an existing db first
    uv run db.py --schema-only   # write schema.sql and exit
"""

import argparse
import csv
import sqlite3
from pathlib import Path

# ------------------------------------------------------------------ paths
HERE = Path(__file__).resolve().parent
SCHEMA_PATH = HERE / "schema.sql"

# ------------------------------------------------------------------ schema
SCHEMA_SQL = """\
-- ============================================================ dimensions
CREATE TABLE IF NOT EXISTS competition (
    competition_id   INTEGER PRIMARY KEY,
    name             TEXT
);

CREATE TABLE IF NOT EXISTS season (
    season_id        INTEGER PRIMARY KEY,
    competition_id   INTEGER NOT NULL REFERENCES competition(competition_id)
);

CREATE TABLE IF NOT EXISTS round (
    round_id         INTEGER PRIMARY KEY,
    round_start      TEXT NOT NULL,
    round_end        TEXT,
    competition_id   INTEGER REFERENCES competition(competition_id),
    UNIQUE(round_start)
);

CREATE TABLE IF NOT EXISTS team (
    team_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    image_url        TEXT
);

-- ============================================================ facts
CREATE TABLE IF NOT EXISTS match (
    match_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_match_id  TEXT,
    round_id         INTEGER NOT NULL REFERENCES round(round_id),
    season_id        INTEGER REFERENCES season(season_id),
    home_team_id     INTEGER NOT NULL REFERENCES team(team_id),
    away_team_id     INTEGER NOT NULL REFERENCES team(team_id),
    start_time       TEXT,
    end_time         TEXT,
    status           TEXT,
    rid              TEXT,
    UNIQUE(round_id, home_team_id, away_team_id)
);

CREATE TABLE IF NOT EXISTS match_result (
    match_id         INTEGER PRIMARY KEY REFERENCES match(match_id),
    result           TEXT,
    ht_score         TEXT,
    scoring_time     TEXT,
    previous_score   TEXT
);

CREATE TABLE IF NOT EXISTS market (
    market_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS odd_snapshot (
    odd_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id         INTEGER NOT NULL REFERENCES match(match_id),
    market_id        INTEGER NOT NULL REFERENCES market(market_id),
    selection        TEXT NOT NULL,
    odd              REAL NOT NULL,
    scraped_at       TEXT NOT NULL,
    UNIQUE(match_id, market_id, selection, scraped_at)
);

-- ============================================================ staging
CREATE TABLE IF NOT EXISTS staging_odds_raw (
    scraped_at       TEXT,
    round_start      TEXT,
    round_id         INTEGER,
    home             TEXT,
    away             TEXT,
    market           TEXT,
    selection        TEXT,
    odd              REAL
);

CREATE TABLE IF NOT EXISTS staging_results_raw (
    parent_match_id  TEXT,
    round_id         INTEGER,
    season_id        INTEGER,
    start_time       TEXT,
    end_time         TEXT,
    home_team        TEXT,
    away_team        TEXT,
    home_id          TEXT,
    away_id          TEXT,
    h_img            TEXT,
    a_img            TEXT,
    result           TEXT,
    ht_score         TEXT,
    scoring_time     TEXT,
    previous_score   TEXT,
    status           TEXT,
    rid              TEXT,
    competition_id   INTEGER
);

-- ============================================================ indexes
CREATE INDEX IF NOT EXISTS idx_round_start      ON round(round_start);
CREATE INDEX IF NOT EXISTS idx_match_round      ON match(round_id);
CREATE INDEX IF NOT EXISTS idx_match_teams      ON match(home_team_id, away_team_id);
CREATE INDEX IF NOT EXISTS idx_odd_match        ON odd_snapshot(match_id);
CREATE INDEX IF NOT EXISTS idx_odd_market       ON odd_snapshot(market_id);
CREATE INDEX IF NOT EXISTS idx_odd_scraped      ON odd_snapshot(scraped_at);
CREATE INDEX IF NOT EXISTS idx_staging_odds_rnd ON staging_odds_raw(round_id);
CREATE INDEX IF NOT EXISTS idx_staging_res_rnd  ON staging_results_raw(round_id);
"""


def write_schema_if_missing(force=False):
    """Write schema.sql next to this script unless it already exists."""
    if SCHEMA_PATH.exists() and not force:
        print(f"  schema : {SCHEMA_PATH} (exists, keeping)")
        return
    SCHEMA_PATH.write_text(SCHEMA_SQL, encoding="utf-8")
    print(f"  schema : {SCHEMA_PATH} (written)")


# ------------------------------------------------------------------ connection
def get_conn(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def initialize_live_database(db_path: Path):
    """Create missing tables without deleting or rebuilding an existing DB."""
    conn = get_conn(db_path)
    try:
        with conn:
            conn.executescript(SCHEMA_SQL)
    finally:
        conn.close()


# ------------------------------------------------------------------ helpers
def ensure_competition(conn, cid, name="Virtuals"):
    conn.execute(
        "INSERT OR IGNORE INTO competition(competition_id, name) VALUES (?, ?)",
        (cid, name),
    )


def ensure_season(conn, sid, cid):
    if sid is None:
        return
    conn.execute(
        "INSERT OR IGNORE INTO season(season_id, competition_id) VALUES (?, ?)",
        (sid, cid),
    )


def ensure_round(conn, round_id, round_start, cid):
    conn.execute(
        "INSERT OR IGNORE INTO round(round_id, round_start, competition_id) "
        "VALUES (?, ?, ?)",
        (round_id, round_start, cid),
    )
    conn.execute(
        "UPDATE round SET round_start = COALESCE(round_start, ?) WHERE round_id = ?",
        (round_start, round_id),
    )


def ensure_team(conn, name, img=None):
    if not name:
        return None
    conn.execute(
        "INSERT OR IGNORE INTO team(name, image_url) VALUES (?, ?)", (name, img)
    )
    row = conn.execute("SELECT team_id FROM team WHERE name = ?", (name,)).fetchone()
    if img and row:
        conn.execute(
            "UPDATE team SET image_url = COALESCE(image_url, ?) WHERE team_id = ?",
            (img, row[0]),
        )
    return row[0]


def ensure_match(conn, round_id, home_id, away_id, **kw):
    conn.execute(
        "INSERT OR IGNORE INTO match"
        "(parent_match_id, round_id, season_id, home_team_id, away_team_id,"
        " start_time, end_time, status, rid) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            kw.get("parent_match_id"), round_id, kw.get("season_id"),
            home_id, away_id,
            kw.get("start_time"), kw.get("end_time"),
            kw.get("status"), kw.get("rid"),
        ),
    )
    row = conn.execute(
        "SELECT match_id FROM match "
        "WHERE round_id = ? AND home_team_id = ? AND away_team_id = ?",
        (round_id, home_id, away_id),
    ).fetchone()
    conn.execute(
        "UPDATE match SET "
        "parent_match_id = COALESCE(parent_match_id, ?), "
        "season_id = COALESCE(season_id, ?), "
        "start_time = COALESCE(start_time, ?), "
        "end_time = COALESCE(end_time, ?), "
        "status = COALESCE(status, ?), "
        "rid = COALESCE(rid, ?) "
        "WHERE match_id = ?",
        (
            kw.get("parent_match_id"), kw.get("season_id"),
            kw.get("start_time"), kw.get("end_time"),
            kw.get("status"), kw.get("rid"), row[0],
        ),
    )
    return row[0]


def ensure_market(conn, name):
    conn.execute("INSERT OR IGNORE INTO market(name) VALUES (?)", (name,))
    return conn.execute(
        "SELECT market_id FROM market WHERE name = ?", (name,)
    ).fetchone()[0]


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _save_result_rows(conn, rows):
    saved = 0
    for r in rows:
        rid = _to_int(r.get("round_id"))
        start_time = r.get("start_time")
        if rid is None or not start_time:
            continue

        cid = _to_int(r.get("competition_id")) or 1
        season_id = _to_int(r.get("season_id"))
        ensure_competition(conn, cid)
        ensure_season(conn, season_id, cid)
        ensure_round(conn, rid, start_time, cid)

        home_id = ensure_team(conn, r.get("home_team"), r.get("h_img"))
        away_id = ensure_team(conn, r.get("away_team"), r.get("a_img"))
        if home_id is None or away_id is None:
            continue

        match_id = ensure_match(
            conn, rid, home_id, away_id,
            parent_match_id=r.get("parent_match_id"),
            season_id=season_id,
            start_time=start_time,
            end_time=r.get("end_time"),
            status=r.get("status"),
            rid=r.get("rid"),
        )
        end_time = r.get("end_time")
        if end_time:
            conn.execute(
                "UPDATE round SET round_end = CASE "
                "WHEN round_end IS NULL OR round_end < ? THEN ? "
                "ELSE round_end END WHERE round_id = ?",
                (end_time, end_time, rid),
            )
        conn.execute(
            "INSERT OR REPLACE INTO match_result"
            "(match_id, result, ht_score, scoring_time, previous_score) "
            "VALUES (?, ?, ?, ?, ?)",
            (match_id, r.get("result"), r.get("ht_score"),
             r.get("scoring_time"), r.get("previous_score")),
        )
        saved += 1
    return saved


def save_result_rows(conn, rows):
    """Idempotently upsert result CSV rows into normalized tables."""
    with conn:
        return _save_result_rows(conn, rows)


def _save_odds_rows(conn, rows):
    saved = 0
    for r in rows:
        rid = _to_int(r.get("round_id"))
        round_start = r.get("round_start")
        scraped_at = r.get("scraped_at")
        if rid is None or not round_start or not scraped_at:
            continue
        if not r.get("home") or not r.get("away") or not r.get("market"):
            continue
        if not r.get("selection"):
            continue
        try:
            odd_value = float(r.get("odd"))
        except (TypeError, ValueError):
            continue

        ensure_competition(conn, 1)
        ensure_round(conn, rid, round_start, 1)
        home_id = ensure_team(conn, r.get("home"))
        away_id = ensure_team(conn, r.get("away"))
        if home_id is None or away_id is None:
            continue
        match_id = ensure_match(conn, rid, home_id, away_id)
        market_id = ensure_market(conn, r.get("market"))
        cursor = conn.execute(
            "INSERT OR IGNORE INTO odd_snapshot"
            "(match_id, market_id, selection, odd, scraped_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (match_id, market_id, r.get("selection"), odd_value, scraped_at),
        )
        saved += cursor.rowcount
    return saved


def save_odds_rows(conn, rows):
    """Insert new odds snapshots, ignoring already-stored snapshots."""
    with conn:
        return _save_odds_rows(conn, rows)


# ------------------------------------------------------------------ importers
def import_results(conn, path: Path):
    """Load results.csv → staging + normalize into match/match_result."""
    if not path.exists():
        print(f"  {path.name}: missing, skipping")
        return 0
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  {path.name}: {len(rows)} rows")

    inserted = 0
    with conn:
        for r in rows:
            conn.execute(
                "INSERT INTO staging_results_raw VALUES "
                "(:parent_match_id,:round_id,:season_id,:start_time,:end_time,"
                " :home_team,:away_team,:home_id,:away_id,:h_img,:a_img,"
                " :result,:ht_score,:scoring_time,:previous_score,:status,"
                " :rid,:competition_id)",
                r,
            )
        inserted = _save_result_rows(conn, rows)
    return inserted


def import_odds(conn, path: Path):
    """Load odds.csv → staging + normalize into match/market/odd_snapshot."""
    if not path.exists():
        print(f"  {path.name}: missing, skipping")
        return 0
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  {path.name}: {len(rows)} rows")

    inserted = 0
    with conn:
        for r in rows:
            conn.execute(
                "INSERT INTO staging_odds_raw VALUES "
                "(:scraped_at,:round_start,:round_id,:home,:away,"
                " :market,:selection,:odd)",
                r,
            )
        inserted = _save_odds_rows(conn, rows)
    return inserted


# ------------------------------------------------------------------ build
def build(db_path: Path, odds_path: Path, results_path: Path, keep: bool):
    write_schema_if_missing()

    if db_path.exists():
        if keep:
            print(f"  db     : {db_path} (keeping existing — appending)")
        else:
            print(f"  db     : {db_path} (deleting for fresh build)")
            db_path.unlink()
    else:
        print(f"  db     : {db_path} (new)")

    conn = get_conn(db_path)
    with conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    print("\n→ importing results.csv")
    n_res = import_results(conn, results_path)
    print("→ importing odds.csv")
    n_odd = import_odds(conn, odds_path)

    # fill round_end from match data if missing
    with conn:
        conn.execute("""
            UPDATE round
            SET round_end = (
                SELECT MAX(m.end_time) FROM match m
                WHERE m.round_id = round.round_id
            )
            WHERE round_end IS NULL
        """)

    # quick summary
    counts = {}
    for t in ("competition", "season", "round", "team", "match",
              "match_result", "market", "odd_snapshot",
              "staging_odds_raw", "staging_results_raw"):
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    conn.close()

    print(f"\n✅ built {db_path}")
    print(f"   results rows : {n_res}")
    print(f"   odds rows    : {n_odd}")
    print("   table counts :")
    for t, n in counts.items():
        print(f"     {t:<22} {n:>7}")


# ------------------------------------------------------------------ cli
def main():
    ap = argparse.ArgumentParser(description="Build odibets.db from CSV files")
    ap.add_argument("--db",      type=Path, default=HERE / "odibets.db")
    ap.add_argument("--odds",    type=Path, default=HERE / "odds.csv")
    ap.add_argument("--results", type=Path, default=HERE / "results.csv")
    ap.add_argument("--keep",    action="store_true",
                    help="append to existing db instead of dropping it")
    ap.add_argument("--schema-only", action="store_true",
                    help="write schema.sql and exit without building the db")
    ap.add_argument("--force-schema", action="store_true",
                    help="overwrite schema.sql even if it exists")
    args = ap.parse_args()

    print("odibets db builder")
    print(f"  workdir: {HERE}\n")

    if args.force_schema:
        write_schema_if_missing(force=True)
    else:
        write_schema_if_missing()

    if args.schema_only:
        print(f"\n✅ schema written to {SCHEMA_PATH}")
        return

    build(args.db, args.odds, args.results, args.keep)


if __name__ == "__main__":
    main()