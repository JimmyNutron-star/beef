#!/usr/bin/env python3
"""
odibets_enricher.py — poll driven by real API round timestamps.

Fix:
  * data.periods is empty in every sample response, so probing future
    periods (level=3) is the only way to learn the next round's actual
    start_time / end_time.  The probe uses those returned values directly
    (no prediction).
  * Display never shows a past round — always the next future round
    closest to now.  If nothing is found yet, shows "searching…" and
    re-probes every 10 s.
"""

import csv
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ------------------------------------------------------------------ config
BASE_URL  = "https://odibets.com/pxy2/virtuals"
UA_STRING = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/156.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA_STRING,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://odibets.com/virtuals",
    "Origin": "https://odibets.com",
    "X-Requested-With": "XMLHttpRequest",
}

COMPETITION_ID     = 1
MAX_WORKERS        = 8
TIMEOUT            = 20
RETRIES            = 2
RETRY_BACKOFF      = 1.0
CYCLE_INTERVAL     = 30
MAX_ROUND_ATTEMPTS = 3

ROUND_DURATION_SECONDS   = 35
ROUND_END_BUFFER_SECONDS = 3
PROBE_REFRESH_INTERVAL   = 10   # re-probe every 10s in fallback mode

# Probe at these offsets from now; first hit wins (closest future round).
PROBE_OFFSETS_SEC = [0, 30, 60, 90, 120, 180, 300]

OUT_CSV = Path("results.csv")

FIELDS = [
    "parent_match_id", "round_id", "season_id",
    "start_time", "end_time",
    "home_team", "away_team",
    "home_id", "away_id",
    "h_img", "a_img",
    "result", "ht_score", "scoring_time",
    "previous_score", "status",
    "rid", "competition_id",
]

# ------------------------------------------------------------------ utils
def fmt_now():
    return datetime.now().strftime("%H:%M:%S")

def sort_key(row):
    st = row.get("start_time") or ""
    rid = row.get("round_id") or 0
    try:
        rid = int(rid)
    except (TypeError, ValueError):
        rid = 0
    return (st, rid)

_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return _thread_local.session

# ------------------------------------------------------------------ http
def build_params(period, level, tab):
    return {
        "competition_id": COMPETITION_ID,
        "tab": tab,
        "period": period,
        "level": level,
        "platform": "desktop",
        "view": "odileague",
        "ua": UA_STRING,
        "resource": "virtuals",
    }

def fetch_json(session, period, level, tab, label=""):
    params = build_params(period, level, tab)
    for attempt in range(1, RETRIES + 1):
        t0 = time.perf_counter()
        try:
            r = session.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            dt = time.perf_counter() - t0
            if attempt == RETRIES:
                if label:
                    print(f"    {label} FAILED after {dt:.2f}s: {e}", flush=True)
                return None
            if label:
                print(f"    {label} attempt {attempt} failed ({dt:.2f}s): {e} → retry",
                      flush=True)
            time.sleep(RETRY_BACKOFF * attempt)
    return None

# ------------------------------------------------------------------ parsing
def _parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None

def parse_matches(payload):
    if not payload:
        return []
    data = payload.get("data") or {}
    ht = {
        str(h.get("ParentMatchID")): h
        for h in data.get("half_time") or []
        if h.get("ParentMatchID")
    }
    out = []
    for m in data.get("matches") or []:
        pm = m.get("parent_match_id")
        h = ht.get(str(pm), {})
        out.append({
            "parent_match_id": pm,
            "round_id":        m.get("round_id"),
            "season_id":       m.get("season_id"),
            "start_time":      m.get("start_time"),
            "end_time":        m.get("end_time"),
            "home_team":       m.get("home_team"),
            "away_team":       m.get("away_team"),
            "home_id":         m.get("home_id"),
            "away_id":         m.get("away_id"),
            "h_img":           m.get("h_img"),
            "a_img":           m.get("a_img"),
            "result":          m.get("result"),
            "ht_score":        h.get("HtScore", ""),
            "scoring_time":    h.get("ScoringTime", ""),
            "previous_score":  m.get("previous_score"),
            "status":          m.get("status"),
            "rid":             m.get("rid"),
            "competition_id":  m.get("competition_id"),
        })
    return out

# ------------------------------------------------------------------ CSV I/O
def load_csv():
    matches = {}
    done_rounds = set()
    if not OUT_CSV.exists():
        return matches, done_rounds
    try:
        with open(OUT_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get("parent_match_id")
                if key:
                    matches[key] = row
                st = row.get("start_time")
                if st:
                    done_rounds.add(st)
    except Exception as e:
        print(f"⚠️  results.csv unreadable ({e}); starting fresh")
        return {}, set()
    return matches, done_rounds

def write_csv(matches):
    rows = sorted(matches.values(), key=sort_key)
    tmp = OUT_CSV.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(OUT_CSV)

# ------------------------------------------------------------------ worker
def worker(rnd):
    session = get_session()
    t0 = time.perf_counter()
    payload = fetch_json(session, rnd["start_time"], level=3, tab="",
                         label=f"[round {rnd['round_id']}]")
    dt = time.perf_counter() - t0
    if payload is None:
        return rnd, [], dt, False
    return rnd, parse_matches(payload), dt, True

# ------------------------------------------------------------------ timer state
_round_timer = {
    "next_start": None,
    "next_end":   None,
}

def _promote_expired_next(now):
    """If the watched round has passed its end, clear it so we re-probe."""
    ne = _round_timer["next_end"]
    if ne is not None and now >= ne:
        _round_timer["next_start"] = None
        _round_timer["next_end"]   = None

def find_next_round(session, now):
    """
    Probe level=3 at increasing future offsets, take the FIRST round
    whose end_time is still in the future.  Returns (start_dt, end_dt).
    Uses only real timestamps returned by the API.
    """
    for offset in PROBE_OFFSETS_SEC:
        probe_str = (now + timedelta(seconds=offset)).strftime("%Y-%m-%d %H:%M:%S")
        payload = fetch_json(session, probe_str, level=3, tab="")
        if not payload:
            continue
        data = payload.get("data") or {}

        best = None

        # Periods list (usually empty, but check anyway)
        for p in data.get("periods") or []:
            st, et = p.get("start_time"), p.get("end_time")
            if not st or not et:
                continue
            dt_s, dt_e = _parse_dt(st), _parse_dt(et)
            if dt_s and dt_e and dt_e > now:
                if best is None or dt_s < best[0]:
                    best = (dt_s, dt_e)

        # Matches (the reliable path)
        for m in data.get("matches") or []:
            st, et = m.get("start_time"), m.get("end_time")
            if not st or not et:
                continue
            dt_s = _parse_dt(st)
            dt_e = _parse_dt(et)
            if dt_s and dt_e and dt_e > now:
                if best is None or dt_s < best[0]:
                    best = (dt_s, dt_e)

        if best is not None:
            return best

    return None, None

def refresh_next_round(session, now=None, force=False):
    now = now or datetime.now()
    _promote_expired_next(now)
    if _round_timer["next_end"] is not None and not force:
        return
    ns, ne = find_next_round(session, now)
    if ns is not None:
        _round_timer["next_start"] = ns
        _round_timer["next_end"]   = ne

def _fmt_secs(seconds):
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s:02d}s" if m else f"{s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"

# ------------------------------------------------------------------ waiting
def _wait_until_end(session):
    """Sleep until the watched round's end_time + buffer. Returns trigger."""
    width = 0
    try:
        while True:
            now = datetime.now()
            ns = _round_timer["next_start"]
            ne = _round_timer["next_end"]

            if ns is None or ne is None:
                return "lost"

            target = ne + timedelta(seconds=ROUND_END_BUFFER_SECONDS)
            delta  = (target - now).total_seconds()
            if delta <= 0:
                return "ended"

            if now >= ne:
                line = (f"  ⏳ round {ns.strftime('%H:%M:%S')}–{ne.strftime('%H:%M:%S')} "
                        f"finished, polling in {_fmt_secs(delta)}")
            elif now >= ns:
                line = (f"  ⏳ 🔴 LIVE {ns.strftime('%H:%M:%S')}–{ne.strftime('%H:%M:%S')} "
                        f"({_fmt_secs((ne - now).total_seconds())} left)")
            else:
                line = (f"  ⏳ next round {ns.strftime('%H:%M:%S')}–{ne.strftime('%H:%M:%S')} "
                        f"in {_fmt_secs((ns - now).total_seconds())}  "
                        f"(wake in {_fmt_secs(delta)})")

            pad = max(0, width - len(line))
            print(f"\r{line}{' ' * pad}", end="", flush=True)
            width = len(line)
            time.sleep(1)
    finally:
        print(f"\r{' ' * (width + 5)}\r", end="", flush=True)

def _wait_probing(session, fallback_secs):
    """No round known yet. Keep probing every PROBE_REFRESH_INTERVAL s."""
    hard_end   = time.monotonic() + fallback_secs
    last_probe = 0.0
    width      = 0
    try:
        while time.monotonic() < hard_end:
            now = datetime.now()

            if time.monotonic() - last_probe >= PROBE_REFRESH_INTERVAL:
                last_probe = time.monotonic()
                refresh_next_round(session, now, force=True)
                if _round_timer["next_end"] is not None:
                    return _wait_until_end(session)

            remaining = hard_end - time.monotonic()
            line = (f"  🔍 searching for next round…  "
                    f"(re-probe in {_fmt_secs(min(remaining, PROBE_REFRESH_INTERVAL))})")
            pad = max(0, width - len(line))
            print(f"\r{line}{' ' * pad}", end="", flush=True)
            width = len(line)
            time.sleep(1)
    finally:
        print(f"\r{' ' * (width + 5)}\r", end="", flush=True)
    return "fallback"

def wait_for_round_end(fallback_secs=CYCLE_INTERVAL):
    session = get_session()
    now = datetime.now()
    _promote_expired_next(now)

    if _round_timer["next_end"] is None:
        refresh_next_round(session, now, force=True)

    if _round_timer["next_end"] is None:
        return _wait_probing(session, fallback_secs)

    return _wait_until_end(session)

# ------------------------------------------------------------------ cycle
def run_cycle(matches, done_rounds, attempts, skipped):
    session = get_session()
    period = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    payload = fetch_json(session, period, level=2, tab="results", label="[P1]")
    p1 = time.perf_counter() - t0

    if payload is None:
        print(f"  ⚠️  phase 1 failed ({p1:.1f}s)")
        return 0

    data = payload.get("data") or {}
    seen   = set()
    rounds = []

    def _collect(items):
        for it in items or []:
            st = it.get("start_time")
            if st and st not in seen:
                seen.add(st)
                rounds.append({
                    "round_id":   it.get("round_id"),
                    "start_time": st,
                    "end_time":   it.get("end_time"),
                })

    _collect(data.get("results"))

    if not rounds:
        print(f"  no rounds in data.results ({p1:.1f}s)")
        _collect(data.get("matches"))
        if rounds:
            print(f"  [debug] recovered {len(rounds)} rounds from data.matches")

    if not rounds:
        return 0

    new_rounds = [
        r for r in rounds
        if r["start_time"] not in done_rounds and r["start_time"] not in skipped
    ]

    if not new_rounds:
        print(f"  {len(rounds)} rounds seen, 0 new  (phase 1: {p1:.1f}s)")
        return 0

    print(f"  {len(rounds)} rounds seen, {len(new_rounds)} to fetch  "
          f"(phase 1: {p1:.1f}s)")

    t0 = time.perf_counter()
    added = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(worker, rnd) for rnd in new_rounds]
        for fut in as_completed(futures):
            rnd, ms, dt, ok = fut.result()
            st = rnd["start_time"]
            if ok and ms:
                new_here = 0
                for m in ms:
                    key = m["parent_match_id"]
                    if key and key not in matches:
                        matches[key] = m
                        new_here += 1
                done_rounds.add(st)
                attempts.pop(st, None)
                added += new_here
                print(f"    round {rnd['round_id']:<5} {st}  "
                      f"{dt*1000:>5.0f}ms  +{new_here:>3} new")
            else:
                n = attempts.get(st, 0) + 1
                attempts[st] = n
                tag = "FAIL" if not ok else "EMPTY"
                if n >= MAX_ROUND_ATTEMPTS:
                    skipped.add(st)
                    print(f"    round {rnd['round_id']:<5} {st}  {tag}  "
                          f"→ skipped after {n} attempts")
                else:
                    print(f"    round {rnd['round_id']:<5} {st}  {tag}  "
                          f"(attempt {n}/{MAX_ROUND_ATTEMPTS})")
    p2 = time.perf_counter() - t0

    if added == 0:
        print(f"  phase 2: no new matches  ({p2:.1f}s)")
    return added

# ------------------------------------------------------------------ main
def do_cycle(matches, done_rounds, attempts, skipped):
    t0 = time.perf_counter()
    try:
        added = run_cycle(matches, done_rounds, attempts, skipped)
    except Exception as e:
        print(f"  ❌ cycle error: {e}")
        added = 0
    elapsed = time.perf_counter() - t0
    if added > 0:
        try:
            write_csv(matches)
            print(f"  ✅ +{added} new (total {len(matches)})  "
                  f"cycle {elapsed:.1f}s  → {OUT_CSV}")
        except Exception as e:
            print(f"  ⚠️  write error: {e}")
    else:
        print(f"  💤 no new data  (cycle {elapsed:.1f}s)")
    return added

def main():
    matches, done_rounds = load_csv()

    print(f"📊 Loaded {OUT_CSV}")
    print(f"   matches           : {len(matches)}")
    print(f"   completed rounds  : {len(done_rounds)}")
    print(f"   fallback interval : {CYCLE_INTERVAL}s")
    print(f"   round buffer      : {ROUND_END_BUFFER_SECONDS}s after end_time")
    print(f"   timer mode        : probe future periods, show next future round")
    print(f"   Ctrl+C to stop\n")

    attempts = {}
    skipped  = set()
    cycle = 0

    try:
        cycle += 1
        print(f"── cycle {cycle} @ {fmt_now()}  [startup] ──")
        do_cycle(matches, done_rounds, attempts, skipped)

        while True:
            reason = wait_for_round_end(fallback_secs=CYCLE_INTERVAL)
            cycle += 1
            print(f"── cycle {cycle} @ {fmt_now()}  [trigger: {reason}] ──")
            do_cycle(matches, done_rounds, attempts, skipped)

    except KeyboardInterrupt:
        print(f"\n⏹  Ctrl+C — saving final snapshot...")
        try:
            write_csv(matches)
        except Exception as e:
            print(f"  ⚠️  save error: {e}")
        print(f"   Matches : {len(matches)}")
        print(f"   Rounds  : {len(done_rounds)} completed, {len(skipped)} skipped")
        print(f"   File    : {OUT_CSV}")
        print(f"   Bye 👋")

if __name__ == "__main__":
    main()
