#!/usr/bin/env python3
"""
odibets_enricher.py — match metadata scraper (multi-league).

Usage:
    uv run odibets_enricher.py --competition 1
    uv run odibets_enricher.py --competition all

Writes  results_<cid>.csv  by default.
"""

import argparse
import csv
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

# ------------------------------------------------------------------ config
BASE_URL = "https://odibets.com/pxy2/virtuals"
UA_STRING = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
             "AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/158.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA_STRING,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://odibets.com/virtuals",
    "Origin": "https://odibets.com",
    "X-Requested-With": "XMLHttpRequest",
}

COMPETITIONS = {1: "English", 2: "Spanish", 3: "Kenyan", 4: "Italian"}

MAX_WORKERS        = 8
TIMEOUT            = 20
RETRIES            = 2
RETRY_BACKOFF      = 1.0
CYCLE_INTERVAL     = 30
MAX_ROUND_ATTEMPTS = 3
ROUND_END_BUFFER_SECONDS = 3
PROBE_REFRESH_INTERVAL   = 10
PROBE_OFFSETS_SEC        = [0, 30, 60, 90, 120, 180, 300]
TZ_NAME = "Africa/Nairobi"

_tls = threading.local()
_stop_event = threading.Event()

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

# ------------------------------------------------------------------ time
_FIXED_OFFSET_HOURS = {"Africa/Nairobi": 3, "UTC": 0}


def _resolve_tz(tz):
    if not tz:
        return None
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz)
        except (ZoneInfoNotFoundError, KeyError, OSError):
            pass
    hours = _FIXED_OFFSET_HOURS.get(tz)
    return timezone(timedelta(hours=hours)) if hours is not None else None


_TZ = _resolve_tz(TZ_NAME)


def _now():
    return datetime.now(_TZ) if _TZ else datetime.now()


def _parse_dt(s):
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=_TZ) if _TZ else dt
    except (TypeError, ValueError):
        return None

# ------------------------------------------------------------------ utils
def fmt_now():
    return _now().strftime("%H:%M:%S")


def sort_key(row):
    st = row.get("start_time") or ""
    rid = row.get("round_id") or 0
    try:
        rid = int(rid)
    except (TypeError, ValueError):
        rid = 0
    return (st, rid)


def _fmt_secs(seconds):
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s:02d}s" if m else f"{s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"

# ------------------------------------------------------------------ session
_thread_local = threading.local()


def _set_league(cid, out_csv, quiet=False):
    _tls.cid = cid
    _tls.out_csv = Path(out_csv)
    _tls.quiet = quiet
    _tls.tag = f"[L{cid}]" if quiet else ""
    _tls.round_timer = {"next_start": None, "next_end": None}


def _cid():
    return getattr(_tls, "cid", 1)


def _out_csv():
    return getattr(_tls, "out_csv", Path("results.csv"))


def _quiet():
    return getattr(_tls, "quiet", False)


def _tag():
    return getattr(_tls, "tag", "")


def _rtimer():
    if not hasattr(_tls, "round_timer"):
        _tls.round_timer = {"next_start": None, "next_end": None}
    return _tls.round_timer


def _log(*args, **kwargs):
    prefix = _tag()
    if not prefix:
        print(*args, **kwargs)
        return
    allowed = {k: v for k, v in kwargs.items() if k in ("file", "flush", "end")}
    msg = " ".join(str(a) for a in args)
    print(f"{prefix} {msg}", **allowed)


def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        retry = requests.adapters.Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS,
            max_retries=retry,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return _thread_local.session

# ------------------------------------------------------------------ http
def build_params(period, level, tab):
    return {
        "competition_id": str(_cid()),
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
            r = session.get(BASE_URL, params=params, headers=HEADERS,
                            timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            dt = time.perf_counter() - t0
            if attempt == RETRIES:
                if label:
                    _log(f"    {label} FAILED after {dt:.2f}s: {e}",
                         flush=True)
                return None
            if label:
                _log(f"    {label} attempt {attempt} failed "
                     f"({dt:.2f}s): {e} → retry", flush=True)
            time.sleep(RETRY_BACKOFF * attempt)
    return None

# ------------------------------------------------------------------ parsing
def parse_matches(payload):
    if not payload:
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
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
            "competition_id":  m.get("competition_id") or _cid(),
        })
    return out

# ------------------------------------------------------------------ csv
def load_csv():
    matches = {}
    done_rounds = set()
    csv_path = _out_csv()
    if not csv_path.exists():
        return matches, done_rounds
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get("parent_match_id")
                if key:
                    matches[key] = row
                st = row.get("start_time")
                status = str(row.get("status") or "").strip().lower()
                if st and status in {"ft", "finished", "final", "ended",
                                     "complete", "completed"}:
                    done_rounds.add(st)
    except Exception as e:
        _log(f"⚠️  {csv_path} unreadable ({e}); starting fresh")
        return {}, set()
    return matches, done_rounds


def write_csv_compact(matches):
    csv_path = _out_csv()
    rows = sorted(matches.values(), key=sort_key)
    tmp = csv_path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(csv_path)


def append_csv_rows(new_rows):
    if not new_rows:
        return
    csv_path = _out_csv()
    is_new = not csv_path.exists() or csv_path.stat().st_size == 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        if is_new:
            w.writeheader()
        w.writerows(sorted(new_rows, key=sort_key))

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

def _promote_expired_next(now):
    timer = _rtimer()
    ne = timer["next_end"]
    if ne is not None and now >= ne:
        timer["next_start"] = None
        timer["next_end"] = None


def find_next_round(session, now):
    for offset in PROBE_OFFSETS_SEC:
        probe_str = (now + timedelta(seconds=offset)).strftime(
            "%Y-%m-%d %H:%M:%S")
        payload = fetch_json(session, probe_str, level=3, tab="")
        if not payload:
            continue
        data = payload.get("data") or {}
        best = None

        for p in data.get("periods") or []:
            st, et = p.get("start_time"), p.get("end_time")
            if not st or not et:
                continue
            dt_s, dt_e = _parse_dt(st), _parse_dt(et)
            if dt_s and dt_e and dt_e > now:
                if best is None or dt_s < best[0]:
                    best = (dt_s, dt_e)

        for m in data.get("matches") or []:
            st, et = m.get("start_time"), m.get("end_time")
            if not st or not et:
                continue
            dt_s, dt_e = _parse_dt(st), _parse_dt(et)
            if dt_s and dt_e and dt_e > now:
                if best is None or dt_s < best[0]:
                    best = (dt_s, dt_e)

        if best is not None:
            return best

    return None, None


def refresh_next_round(session, now=None, force=False):
    now = now or _now()
    _promote_expired_next(now)
    timer = _rtimer()
    if timer["next_end"] is not None and not force:
        return
    ns, ne = find_next_round(session, now)
    if ns is not None:
        timer["next_start"] = ns
        timer["next_end"] = ne

# ------------------------------------------------------------------ wait
def _wait_until_end(session):
    width = 0
    try:
        while True:
            if _stop_event.is_set():
                return "stopped"
            now = _now()
            timer = _rtimer()
            ns = timer["next_start"]
            ne = timer["next_end"]

            if ns is None or ne is None:
                return "lost"

            target = ne + timedelta(seconds=ROUND_END_BUFFER_SECONDS)
            delta = (target - now).total_seconds()
            if delta <= 0:
                return "ended"

            if now >= ne:
                line = (f"  ⏳ round {ns.strftime('%H:%M:%S')}–"
                        f"{ne.strftime('%H:%M:%S')} finished, "
                        f"polling in {_fmt_secs(delta)}")
            elif now >= ns:
                line = (f"  ⏳ 🔴 LIVE {ns.strftime('%H:%M:%S')}–"
                        f"{ne.strftime('%H:%M:%S')} "
                        f"({_fmt_secs((ne - now).total_seconds())} left)")
            else:
                line = (f"  ⏳ next round {ns.strftime('%H:%M:%S')}–"
                        f"{ne.strftime('%H:%M:%S')} "
                        f"in {_fmt_secs((ns - now).total_seconds())}  "
                        f"(wake in {_fmt_secs(delta)})")

            if not _quiet():
                pad = max(0, width - len(line))
                print(f"\r{line}{' ' * pad}", end="", flush=True)
                width = len(line)
            sleep_for = min(10.0, max(1.0, delta / 8))
            if _stop_event.wait(sleep_for):
                return "stopped"
    finally:
        if width:
            print(f"\r{' ' * (width + 5)}\r", end="", flush=True)


def _wait_probing(session, fallback_secs):
    hard_end = time.monotonic() + fallback_secs
    last_probe = 0.0
    width = 0
    try:
        while time.monotonic() < hard_end:
            if _stop_event.is_set():
                return "stopped"
            now = _now()

            if time.monotonic() - last_probe >= PROBE_REFRESH_INTERVAL:
                last_probe = time.monotonic()
                refresh_next_round(session, now, force=True)
                if _stop_event.is_set():
                    return "stopped"
                if _rtimer()["next_end"] is not None:
                    return _wait_until_end(session)

            remaining = hard_end - time.monotonic()
            if not _quiet():
                line = (f"  🔍 searching for next round…  "
                        f"(re-probe in "
                        f"{_fmt_secs(min(remaining, PROBE_REFRESH_INTERVAL))})")
                pad = max(0, width - len(line))
                print(f"\r{line}{' ' * pad}", end="", flush=True)
                width = len(line)
            if _stop_event.wait(1):
                return "stopped"
    finally:
        if width:
            print(f"\r{' ' * (width + 5)}\r", end="", flush=True)
    return "fallback"


def wait_for_round_end(fallback_secs=CYCLE_INTERVAL):
    if _stop_event.is_set():
        return "stopped"
    session = get_session()
    now = _now()
    _promote_expired_next(now)

    if _rtimer()["next_end"] is None:
        refresh_next_round(session, now, force=True)

    if _stop_event.is_set():
        return "stopped"
    if _rtimer()["next_end"] is None:
        return _wait_probing(session, fallback_secs)

    return _wait_until_end(session)

# ------------------------------------------------------------------ cycle
def run_cycle(matches, done_rounds, attempts, skipped):
    session = get_session()
    period = _now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    payload = fetch_json(session, period, level=2, tab="results", label="[P1]")
    p1 = time.perf_counter() - t0

    if payload is None:
        print(f"{_tag()}  ⚠️  phase 1 failed ({p1:.1f}s)")
        return 0, []

    data = payload.get("data") or {}
    seen = set()
    rounds = []
    period_match_counts = {}
    for item in data.get("matches") or []:
        item_start = item.get("start_time")
        if item_start:
            period_match_counts[item_start] = period_match_counts.get(item_start, 0) + 1
    for period in data.get("periods") or []:
        period_start = period.get("start_time") or period.get("period")
        if period_start:
            count = (period.get("match_count") or period.get("matches_count")
                     or period.get("matchCount"))
            if count is not None:
                try:
                    period_match_counts[period_start] = int(count)
                except (TypeError, ValueError):
                    pass

    def _collect(items):
        for it in items or []:
            st = it.get("start_time")
            if st and st not in seen:
                seen.add(st)
                rounds.append({
                    "round_id":   it.get("round_id"),
                    "start_time": st,
                    "end_time":   it.get("end_time"),
                    "match_count": period_match_counts.get(st),
                })

    _collect(data.get("results"))

    if not rounds:
        print(f"{_tag()}  no rounds in data.results ({p1:.1f}s)")
        _collect(data.get("matches"))
        if rounds:
            print(f"{_tag()}  [debug] recovered {len(rounds)} rounds from data.matches")

    if not rounds:
        return 0, []

    new_rounds = [
        r for r in rounds
        if r["start_time"] not in done_rounds
        and r["start_time"] not in skipped
    ]

    if not new_rounds:
        print(f"{_tag()}  {len(rounds)} rounds seen, 0 new  (phase 1: {p1:.1f}s)")
        return 0, []

    print(f"{_tag()}  {len(rounds)} rounds seen, {len(new_rounds)} to fetch  "
          f"(phase 1: {p1:.1f}s)")

    t0 = time.perf_counter()
    added = 0
    new_rows = []
    league_state = (_cid(), _out_csv(), _quiet())
    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS,
        initializer=_set_league,
        initargs=league_state,
    ) as pool:
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
                        new_rows.append(m)
                added += new_here
                end_dt = _parse_dt(rnd["end_time"])
                now = _now()
                final_status = any(
                    str(m.get("status") or "").strip().lower()
                    in {"ft", "finished", "final", "ended", "complete", "completed"}
                    for m in ms
                )
                after_end_buffer = (
                    end_dt is not None
                    and now >= end_dt + timedelta(seconds=ROUND_END_BUFFER_SECONDS)
                )
                match_count = rnd.get("match_count")
                count_matches = (
                    match_count is None
                    or len(ms) == match_count
                )
                if end_dt is not None and end_dt <= now and final_status:
                    done_rounds.add(st)
                    attempts.pop(st, None)
                    print(f"{_tag()}    round {rnd['round_id']:<5} {st}  "
                          f"{dt*1000:>5.0f}ms  +{new_here:>3} new")
                elif after_end_buffer and count_matches:
                    done_rounds.add(st)
                    attempts.pop(st, None)
                    print(f"{_tag()}    round {rnd['round_id']:<5} {st}  "
                          f"{dt*1000:>5.0f}ms  +{new_here:>3} new")
                else:
                    print(f"{_tag()}    round {rnd['round_id']:<5} {st}  "
                          f"+{new_here} new (deferred — round not final)")
            else:
                n = attempts.get(st, 0) + 1
                attempts[st] = n
                tag = "FAIL" if not ok else "EMPTY"
                if n >= MAX_ROUND_ATTEMPTS:
                    skipped.add(st)
                    print(f"{_tag()}    round {rnd['round_id']:<5} {st}  {tag}  "
                          f"→ skipped after {n} attempts")
                else:
                    print(f"{_tag()}    round {rnd['round_id']:<5} {st}  {tag}  "
                          f"(attempt {n}/{MAX_ROUND_ATTEMPTS})")
    p2 = time.perf_counter() - t0

    if added == 0:
        print(f"{_tag()}  phase 2: no new matches  ({p2:.1f}s)")
    return added, new_rows


def do_cycle(matches, done_rounds, attempts, skipped):
    t0 = time.perf_counter()
    try:
        added, new_rows = run_cycle(matches, done_rounds, attempts, skipped)
    except Exception as e:
        print(f"{_tag()}  ❌ cycle error: {e}")
        added = 0
        new_rows = []
    elapsed = time.perf_counter() - t0
    if added > 0:
        try:
            append_csv_rows(new_rows)
            print(f"{_tag()}  ✅ +{added} new (total {len(matches)})  "
                  f"cycle {elapsed:.1f}s  → {_out_csv()}")
        except Exception as e:
            print(f"{_tag()}  ⚠️  write error: {e}")
    else:
        print(f"{_tag()}  💤 no new data  (cycle {elapsed:.1f}s)")
    return added

# ------------------------------------------------------------------ run one
def run_single(args):
    out_csv = args.csv or Path(f"results_{args.competition}.csv")
    _set_league(
        args.competition,
        out_csv,
        quiet=getattr(args, "quiet", False),
    )

    matches = {}
    done_rounds = set()
    attempts = {}
    skipped = set()
    cycle = 0

    try:
        matches, done_rounds = load_csv()
        _log(f"📊 Loaded {_out_csv()}")
        _log(f"   competition       : {_cid()} "
             f"({COMPETITIONS.get(_cid(), '?')})")
        _log(f"   matches           : {len(matches)}")
        _log(f"   completed rounds  : {len(done_rounds)}")
        _log(f"   fallback interval : {CYCLE_INTERVAL}s")
        _log(f"   round buffer      : {ROUND_END_BUFFER_SECONDS}s after end_time")
        _log("   Ctrl+C to stop\n")

        if not _stop_event.is_set():
            cycle += 1
            _log(f"── cycle {cycle} @ {fmt_now()}  [startup] ──")
            do_cycle(matches, done_rounds, attempts, skipped)

        while not _stop_event.is_set():
            reason = wait_for_round_end(fallback_secs=CYCLE_INTERVAL)
            if _stop_event.is_set() or reason == "stopped":
                break
            cycle += 1
            _log(f"── cycle {cycle} @ {fmt_now()}  "
                 f"[trigger: {reason}] ──")
            do_cycle(matches, done_rounds, attempts, skipped)

    except KeyboardInterrupt:
        _stop_event.set()
        _log("\n⏹  Ctrl+C — saving final snapshot...")
    finally:
        try:
            write_csv_compact(matches)
        except Exception as e:
            _log(f"  ⚠️  save error: {e}")
        _log(f"   Matches : {len(matches)}")
        _log(f"   Rounds  : {len(done_rounds)} completed, "
             f"{len(skipped)} skipped")
        _log(f"   File    : {_out_csv()}")
        _log("   Bye 👋")

# ------------------------------------------------------------------ all mode
def run_all(args):
    """Run all 4 leagues concurrently as threads in one process."""
    _stop_event.clear()
    threads = []
    for cid in COMPETITIONS:
        t = threading.Thread(
            target=run_single,
            args=(argparse.Namespace(competition=cid, csv=None, quiet=True),),
            name=f"league-{cid}",
            daemon=False,
        )
        threads.append(t)
        t.start()
        print(f"  → {cid}:{COMPETITIONS[cid]:<8} thread started")

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n⏹  Ctrl+C — stopping all leagues …")
        _stop_event.set()
        for t in threads:
            t.join(timeout=5)

# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", default="all",
                help="1=English 2=Spanish 3=Kenyan 4=Italian, or 'all' (default)")
    ap.add_argument("--csv", type=Path, default=None,
                    help="output csv (default: results_<cid>.csv)")
    args = ap.parse_args()

    if args.competition == "all":
        run_all(args)
        return

    args.competition = int(args.competition)
    if args.competition not in COMPETITIONS:
        ap.error(f"unknown competition {args.competition}")
    _stop_event.clear()
    run_single(args)


if __name__ == "__main__":
    main()