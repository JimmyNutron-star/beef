#!/usr/bin/env python3
"""
db.py — build odibets.db from all odds_*.csv and results_*.csv files.

Discovers:
    odds.csv,    odds_1.csv .. odds_N.csv
    results.csv, results_1.csv .. results_N.csv

Writes schema.sql next to this file on every run (rewrites if outdated).

Usage:
    uv run db.py                 # fresh rebuild
    uv run db.py --keep          # append to existing db
    uv run db.py --force-schema  # force schema.sql rewrite
    uv run db.py --schema-only   # write schema.sql and exit
"""

import argparse
import csv
import io
import sqlite3
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCHEMA_PATH = HERE / "schema.sql"

COMPETITIONS = {1: "English", 2: "Spanish", 3: "Kenyan", 4: "Italian"}
ODDS_FIELDS = (
    "scraped_at", "round_start", "round_id", "competition_id",
    "home", "away", "market", "selection", "odd",
)
RESULT_FIELDS = (
    "parent_match_id", "round_id", "season_id", "start_time", "end_time",
    "home_team", "away_team", "home_id", "away_id", "h_img", "a_img",
    "result", "ht_score", "scoring_time", "previous_score", "status", "rid",
    "competition_id",
)

# ------------------------------------------------------------------ schema
SCHEMA_SQL = """\
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS competition (
    competition_id   INTEGER PRIMARY KEY,
    name             TEXT
);

CREATE TABLE IF NOT EXISTS season (
    season_id        INTEGER PRIMARY KEY,
    competition_id   INTEGER NOT NULL REFERENCES competition(competition_id)
);

-- Rounds are keyed by (competition_id, round_start) because the odds feed
-- and results feed use DIFFERENT round_id spaces for the same round.
CREATE TABLE IF NOT EXISTS round (
    round_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    competition_id   INTEGER NOT NULL REFERENCES competition(competition_id),
    round_start      TEXT NOT NULL,
    round_end        TEXT,
    UNIQUE(competition_id, round_start)
);

-- Teams are scoped per competition so "Liverpool" in English != "Liverpool"
-- in another league (if such a name ever collides).
CREATE TABLE IF NOT EXISTS team (
    team_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    competition_id   INTEGER NOT NULL REFERENCES competition(competition_id),
    name             TEXT NOT NULL,
    image_url        TEXT,
    UNIQUE(competition_id, name)
);

CREATE TABLE IF NOT EXISTS market (
    market_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS match (
    match_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    round_key        INTEGER NOT NULL REFERENCES round(round_key),
    season_id        INTEGER REFERENCES season(season_id),
    parent_match_id  TEXT,
    home_team_id     INTEGER NOT NULL REFERENCES team(team_id),
    away_team_id     INTEGER NOT NULL REFERENCES team(team_id),
    start_time       TEXT,
    end_time         TEXT,
    status           TEXT,
    rid              TEXT,
    UNIQUE(round_key, home_team_id, away_team_id)
);

CREATE TABLE IF NOT EXISTS match_result (
    match_id         INTEGER PRIMARY KEY REFERENCES match(match_id),
    result           TEXT,
    ht_score         TEXT,
    scoring_time     TEXT,
    previous_score   TEXT
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

CREATE TABLE IF NOT EXISTS import_state (
    path TEXT PRIMARY KEY,
    byte_offset INTEGER NOT NULL,
    inode INTEGER,
    mtime_ns INTEGER
);

-- ------------------------------------------------------------------ staging
CREATE TABLE IF NOT EXISTS staging_odds_raw (
    scraped_at       TEXT,
    round_start      TEXT,
    round_id         INTEGER,
    competition_id   INTEGER,
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

-- ------------------------------------------------------------------ indexes
CREATE INDEX IF NOT EXISTS idx_round_comp_start ON round(competition_id, round_start);
CREATE INDEX IF NOT EXISTS idx_team_comp_name   ON team(competition_id, name);
CREATE INDEX IF NOT EXISTS idx_match_round      ON match(round_key);
CREATE INDEX IF NOT EXISTS idx_match_teams      ON match(home_team_id, away_team_id);
CREATE INDEX IF NOT EXISTS idx_odd_match        ON odd_snapshot(match_id);
CREATE INDEX IF NOT EXISTS idx_odd_market       ON odd_snapshot(market_id);
CREATE INDEX IF NOT EXISTS idx_odd_scraped      ON odd_snapshot(scraped_at);
CREATE INDEX IF NOT EXISTS idx_staging_odds_rnd ON staging_odds_raw(round_id);
CREATE INDEX IF NOT EXISTS idx_staging_res_rnd  ON staging_results_raw(round_id);
"""


# ------------------------------------------------------------------ schema
def write_schema(force=False):
    """Write schema.sql; rewrite automatically if content differs."""
    if SCHEMA_PATH.exists() and not force:
        existing = SCHEMA_PATH.read_text(encoding="utf-8")
        if existing.strip() == SCHEMA_SQL.strip():
            print(f"  schema : {SCHEMA_PATH} (up to date)")
            return
        print(f"  schema : {SCHEMA_PATH} (outdated, rewriting)")
    SCHEMA_PATH.write_text(SCHEMA_SQL, encoding="utf-8")
    print(f"  schema : {SCHEMA_PATH} (written)")


# ------------------------------------------------------------------ helpers
def get_conn(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -16000")
    conn.execute("PRAGMA mmap_size = 33554432")
    return conn


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# In-memory caches — reset at start of each build() to avoid cross-run pollution
_round_cache = {}
_team_cache  = {}
_match_cache = {}


def ensure_competition(conn, cid):
    conn.execute(
        "INSERT OR IGNORE INTO competition(competition_id, name) VALUES (?, ?)",
        (cid, COMPETITIONS.get(cid, f"Competition {cid}")),
    )


def ensure_season(conn, sid, cid):
    if sid is None:
        return
    conn.execute(
        "INSERT OR IGNORE INTO season(season_id, competition_id) VALUES (?, ?)",
        (sid, cid),
    )


def ensure_round(conn, cid, start, end=None):
    """Upsert round on (competition_id, round_start); return round_key."""
    key = (cid, start)
    if key in _round_cache:
        rk = _round_cache[key]
        if end:
            conn.execute(
                "UPDATE round SET round_end = COALESCE(round_end, ?) "
                "WHERE round_key = ?",
                (end, rk),
            )
        return rk

    conn.execute(
        "INSERT OR IGNORE INTO round (competition_id, round_start, round_end) "
        "VALUES (?, ?, ?)",
        (cid, start, end),
    )
    if end:
        conn.execute(
            "UPDATE round SET round_end = COALESCE(round_end, ?) "
            "WHERE competition_id = ? AND round_start = ?",
            (end, cid, start),
        )
    rk = conn.execute(
        "SELECT round_key FROM round WHERE competition_id = ? AND round_start = ?",
        (cid, start),
    ).fetchone()[0]
    _round_cache[key] = rk
    return rk


def ensure_team(conn, cid, name, img=None):
    """Upsert team on (competition_id, name); return team_id."""
    if not name:
        return None
    key = (cid, name)
    if key in _team_cache:
        tid = _team_cache[key]
        if img:
            conn.execute(
                "UPDATE team SET image_url = COALESCE(image_url, ?) "
                "WHERE team_id = ?",
                (img, tid),
            )
        return tid

    conn.execute(
        "INSERT OR IGNORE INTO team (competition_id, name, image_url) "
        "VALUES (?, ?, ?)",
        (cid, name, img),
    )
    tid = conn.execute(
        "SELECT team_id FROM team WHERE competition_id = ? AND name = ?",
        (cid, name),
    ).fetchone()[0]
    _team_cache[key] = tid
    return tid


def ensure_match(conn, round_key, hid, aid, **kw):
    """Upsert match on (round_key, home, away); fill in NULLs from later feeds."""
    key = (round_key, hid, aid)
    if key not in _match_cache:
        conn.execute(
            "INSERT OR IGNORE INTO match "
            "(round_key, season_id, parent_match_id, home_team_id, away_team_id, "
            " start_time, end_time, status, rid) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                round_key, kw.get("season_id"), kw.get("parent_match_id"),
                hid, aid,
                kw.get("start_time"), kw.get("end_time"),
                kw.get("status"), kw.get("rid"),
            ),
        )
        _match_cache[key] = conn.execute(
            "SELECT match_id FROM match "
            "WHERE round_key = ? AND home_team_id = ? AND away_team_id = ?",
            (round_key, hid, aid),
        ).fetchone()[0]

    mid = _match_cache[key]

    # fill in any fields we didn't have before
    updates = {k: v for k, v in kw.items() if v is not None}
    if updates:
        set_clause = ", ".join(f"{k}=COALESCE({k}, ?)" for k in updates)
        conn.execute(
            f"UPDATE match SET {set_clause} WHERE match_id = ?",
            (*updates.values(), mid),
        )
    return mid


def ensure_market(conn, name):
    conn.execute("INSERT OR IGNORE INTO market(name) VALUES (?)", (name,))
    return conn.execute(
        "SELECT market_id FROM market WHERE name = ?", (name,)
    ).fetchone()[0]


# ------------------------------------------------------------------ discover
def _get_import_state(conn, path: Path):
    key = str(path.resolve())
    row = conn.execute(
        "SELECT byte_offset, inode, mtime_ns "
        "FROM import_state WHERE path = ?",
        (key,),
    ).fetchone()
    return row


def _set_import_state(conn, path: Path, byte_offset, stat):
    conn.execute(
        "INSERT INTO import_state(path, byte_offset, inode, mtime_ns) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(path) DO UPDATE SET "
        "byte_offset=excluded.byte_offset, inode=excluded.inode, "
        "mtime_ns=excluded.mtime_ns",
        (
            str(path.resolve()),
            byte_offset,
            getattr(stat, "st_ino", None),
            getattr(stat, "st_mtime_ns", None),
        ),
    )


def _read_csv_delta(conn, path: Path, fields):
    if not path.exists():
        return [], 0, False

    stat = path.stat()
    state = _get_import_state(conn, path)
    inode = getattr(stat, "st_ino", None)
    full = (
        state is None
        or state[1] is not None and inode is not None and state[1] != inode
        or stat.st_size < state[0]
    )

    if full:
        with open(path, "rb") as f:
            data = f.read()
        newline = data.rfind(b"\n")
        complete_size = newline + 1 if newline >= 0 else 0
        if complete_size == 0:
            return [], 0, True
        text = data[:complete_size].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        return list(reader), complete_size, True

    offset = state[0]
    with open(path, "rb") as f:
        f.seek(offset)
        data = f.read()

    newline = data.rfind(b"\n")
    if newline < 0:
        return [], offset, False

    complete = data[:newline + 1]
    text = complete.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text, newline=""), fieldnames=fields)
    rows = list(reader)
    return rows, offset + len(complete), False


def discover_files(prefix):
    """Return legacy <prefix>.csv plus <prefix>_<n>.csv."""
    files = []
    single = HERE / f"{prefix}.csv"
    if single.exists():
        files.append(single)
    files.extend(sorted(HERE.glob(f"{prefix}_[0-9]*.csv")))
    return files


# ------------------------------------------------------------------ importers
def import_results(conn, path: Path):
    """results.csv → staging + round/team/match/match_result."""
    if not path.exists():
        print(f"  {path.name}: missing, skipping")
        return 0
    rows, byte_offset, full = _read_csv_delta(conn, path, RESULT_FIELDS)
    print(f"  {path.name}: {len(rows)} rows")

    stage_tuples = [
        (
            r.get("parent_match_id"), r.get("round_id"), r.get("season_id"),
            r.get("start_time"), r.get("end_time"),
            r.get("home_team"), r.get("away_team"),
            r.get("home_id"), r.get("away_id"),
            r.get("h_img"), r.get("a_img"),
            r.get("result"), r.get("ht_score"), r.get("scoring_time"),
            r.get("previous_score"), r.get("status"), r.get("rid"),
            _to_int(r.get("competition_id")) or 1,
        )
        for r in rows
    ]

    inserted = 0
    try:
        with conn:
            if full and not getattr(import_results, "_staging_cleared", False):
                conn.execute("DELETE FROM staging_results_raw")
                import_results._staging_cleared = True
            if full:
                conn.executemany(
                    "INSERT INTO staging_results_raw "
                    "(parent_match_id, round_id, season_id, start_time, end_time, "
                    " home_team, away_team, home_id, away_id, h_img, a_img, "
                    " result, ht_score, scoring_time, previous_score, status, rid, "
                    " competition_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    stage_tuples,
                )
            else:
                conn.executemany(
                    "INSERT INTO staging_results_raw "
                    "(parent_match_id, round_id, season_id, start_time, end_time, "
                    " home_team, away_team, home_id, away_id, h_img, a_img, "
                    " result, ht_score, scoring_time, previous_score, status, rid, "
                    " competition_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    stage_tuples,
                )
            for r in rows:
                cid = _to_int(r.get("competition_id")) or 1
                start = r.get("start_time")
                if not start:
                    continue
                ensure_competition(conn, cid)
                ensure_season(conn, _to_int(r.get("season_id")), cid)
                round_key = ensure_round(conn, cid, start, r.get("end_time"))
                hid = ensure_team(conn, cid, r.get("home_team"), r.get("h_img"))
                aid = ensure_team(conn, cid, r.get("away_team"), r.get("a_img"))
                if hid is None or aid is None:
                    continue
                mid = ensure_match(
                    conn, round_key, hid, aid,
                    season_id=_to_int(r.get("season_id")),
                    parent_match_id=r.get("parent_match_id"),
                    start_time=start,
                    end_time=r.get("end_time"),
                    status=r.get("status"),
                    rid=r.get("rid"),
                )
                conn.execute(
                    "INSERT OR REPLACE INTO match_result "
                    "(match_id, result, ht_score, scoring_time, previous_score) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (mid, r.get("result"), r.get("ht_score"),
                     r.get("scoring_time"), r.get("previous_score")),
                )
                inserted += 1
            stat = path.stat()
            _set_import_state(conn, path, byte_offset, stat)
    except (OSError, sqlite3.Error, csv.Error, UnicodeError) as e:
        print(f"  {path.name}: import failed: {e}")
        return 0
    return inserted


def import_odds(conn, path: Path):
    """odds.csv → staging + round/team/match/market/odd_snapshot."""
    if not path.exists():
        print(f"  {path.name}: missing, skipping")
        return 0
    rows, byte_offset, full = _read_csv_delta(conn, path, ODDS_FIELDS)
    print(f"  {path.name}: {len(rows)} rows")

    stage_tuples = [
        (
            r.get("scraped_at"), r.get("round_start"),
            r.get("round_id"), _to_int(r.get("competition_id")) or 1,
            r.get("home"), r.get("away"),
            r.get("market"), r.get("selection"), r.get("odd"),
        )
        for r in rows
    ]

    inserted = 0
    try:
        with conn:
            if full and not getattr(import_odds, "_staging_cleared", False):
                conn.execute("DELETE FROM staging_odds_raw")
                import_odds._staging_cleared = True
            conn.executemany(
                "INSERT INTO staging_odds_raw "
                "(scraped_at, round_start, round_id, competition_id, "
                " home, away, market, selection, odd) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                stage_tuples,
            )
            for r in rows:
                cid = _to_int(r.get("competition_id")) or 1
                start = r.get("round_start")
                if not start:
                    continue
                ensure_competition(conn, cid)
                round_key = ensure_round(conn, cid, start, None)
                hid = ensure_team(conn, cid, r.get("home"))
                aid = ensure_team(conn, cid, r.get("away"))
                if hid is None or aid is None:
                    continue
                mid = ensure_match(conn, round_key, hid, aid, start_time=start)
                market_name = r.get("market")
                if not market_name:
                    continue
                mkid = ensure_market(conn, market_name)
                try:
                    odd_val = float(r.get("odd"))
                except (TypeError, ValueError):
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO odd_snapshot "
                    "(match_id, market_id, selection, odd, scraped_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (mid, mkid, r.get("selection"), odd_val, r.get("scraped_at")),
                )
                inserted += 1
            stat = path.stat()
            _set_import_state(conn, path, byte_offset, stat)
    except (OSError, sqlite3.Error, csv.Error, UnicodeError) as e:
        print(f"  {path.name}: import failed: {e}")
        return 0
    return inserted


# ------------------------------------------------------------------ build
def _detect_old_schema(db_path: Path):
    """Return True if db exists but has the old (non-round_key) schema."""
    if not db_path.exists():
        return False
    try:
        conn = sqlite3.connect(db_path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(round)")}
        conn.close()
    except Exception:
        return False
    return "round_key" not in cols


def build(db_path: Path, keep: bool):
    write_schema()

    if db_path.exists():
        if _detect_old_schema(db_path):
            print(f"  db     : {db_path} (old schema detected — rebuilding)")
            db_path.unlink()
            keep = False
        elif keep:
            print(f"  db     : {db_path} (keeping — appending)")
        else:
            print(f"  db     : {db_path} (deleting for fresh build)")
            db_path.unlink()
    else:
        print(f"  db     : {db_path} (new)")

    # reset caches
    _round_cache.clear()
    _team_cache.clear()
    _match_cache.clear()
    import_odds._staging_cleared = False
    import_results._staging_cleared = False

    conn = get_conn(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    res_files  = discover_files("results")
    odds_files = discover_files("odds")

    print(f"\n→ importing {len(res_files)} results file(s)")
    n_res = sum(import_results(conn, p) for p in res_files)

    print(f"→ importing {len(odds_files)} odds file(s)")
    n_odd = sum(import_odds(conn, p) for p in odds_files)

    # Sanity counts
    counts = {}
    for t in ("competition", "season", "round", "team", "match",
              "match_result", "market", "odd_snapshot",
              "staging_odds_raw", "staging_results_raw"):
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    # Cross-feed join check
    with_odds = conn.execute("""
        SELECT COUNT(DISTINCT m.match_id) FROM match m
        WHERE EXISTS (SELECT 1 FROM odd_snapshot o WHERE o.match_id = m.match_id)
    """).fetchone()[0]
    with_results = conn.execute("""
        SELECT COUNT(DISTINCT m.match_id) FROM match m
        WHERE EXISTS (SELECT 1 FROM match_result r WHERE r.match_id = m.match_id)
    """).fetchone()[0]
    joined = conn.execute("""
        SELECT COUNT(*) FROM match m
        JOIN match_result r ON r.match_id = m.match_id
        WHERE EXISTS (SELECT 1 FROM odd_snapshot o WHERE o.match_id = m.match_id)
    """).fetchone()[0]

    conn.close()

    print(f"\nbuilt {db_path}")
    print(f"   results rows  : {n_res}")
    print(f"   odds rows     : {n_odd}")
    print(f"   matches w/odds     : {with_odds}")
    print(f"   matches w/results  : {with_results}")
    print(f"   matches w/BOTH     : {joined}")
    print("   table counts :")
    for t, n in counts.items():
        print(f"     {t:<22} {n:>8}")
    if n_res and n_odd and joined == 0:
        print("\nWARNING: results and odds imported but 0 matches joined!")
        print("   Check that results.start_time == odds.round_start exactly.")
        print("   Run: SELECT DISTINCT round_start FROM round LIMIT 5;")
        print("        SELECT DISTINCT start_time FROM match LIMIT 5;")


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description="Build odibets.db from CSV files")
    ap.add_argument("--db", type=Path, default=HERE / "odibets.db")
    ap.add_argument("--keep", action="store_true",
                    help="append to existing db instead of dropping it")
    ap.add_argument("--schema-only", action="store_true")
    ap.add_argument("--force-schema", action="store_true")
    args = ap.parse_args()

    print("odibets db builder")
    print(f"  workdir: {HERE}\n")

    if args.schema_only:
        write_schema(force=args.force_schema)
        print(f"\nschema written to {SCHEMA_PATH}")
        return

    build(args.db, args.keep)


if __name__ == "__main__":
    main()