#!/usr/bin/env python3
"""
odds.py — Odibets virtuals odds scraper (multi-league).

Usage:
    uv run odds.py --competition 1            # English only
    uv run odds.py --competition all          # all 4 leagues in threads
    uv run odds.py --competition 2 --workers 8

Writes  odds_<cid>.csv  by default.
"""

import argparse
import csv
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

import requests

# ================================================================ constants
BASE = "https://odibets.com/pxy2/virtuals"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://odibets.com/virtuals",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/158.0.0.0 Safari/537.36"),
    "x-requested-with": "XMLHttpRequest",
}

COMPETITIONS = {1: "English", 2: "Spanish", 3: "Kenyan", 4: "Italian"}

_tls = threading.local()
_stop_event = threading.Event()


def _set_league(cid: int, csv_path: str, quiet: bool = False):
    _tls.competition_id = cid
    _tls.csv_path = csv_path
    _tls.quiet = quiet
    _tls.tag = f"[L{cid}] " if quiet else ""


def _cid() -> int:
    return getattr(_tls, "competition_id", 1)


def _csv_path() -> str:
    return getattr(_tls, "csv_path", "odds.csv")


def _quiet() -> bool:
    return getattr(_tls, "quiet", False)


def _tag() -> str:
    return getattr(_tls, "tag", "")


SUB_TYPES = [
    "1X2", "GG", "DC",
    "TG15", "TG25", "TG35",
    "1X2OU15", "1X2OU25", "1X2OU35", "1X2OU45", "1X2OU55",
    "CS", "HS", "DCH", "H1X2", "1X2G", "DR", "FTS",
    "HGG", "HX1", "HX2", "MG",
    "T1G", "T1OU15", "T2G", "T2OU15",
    "TFG", "TG", "TGOE",
]

CSV_FIELDS = [
    "scraped_at", "round_start", "round_id", "competition_id",
    "home", "away", "market", "selection", "odd",
]

LIVE_SECONDS     = 35
SKIP_THRESHOLD   = 10
WINDOW_LEAD      = 85

# ================================================================ time
_FIXED_OFFSET_HOURS = {
    "Africa/Nairobi": 3,
    "UTC": 0,
}


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


def now_in(tz):
    z = _resolve_tz(tz)
    return datetime.now(z) if z else datetime.now()


def parse_dt(s, tz=None):
    z = _resolve_tz(tz)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=z) if z else dt
        except (ValueError, TypeError):
            continue
    return None


def fmt_hms(seconds):
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# ================================================================ display
class Display:
    def __init__(self, width=130, tag="", quiet=False):
        self.lock = threading.RLock()
        self.timer = ""
        self.progress = ""
        self.width = width
        self.tag = f"[{tag}] " if tag else ""
        self.quiet = quiet

    def _truncate_visual(self, s):
        out, w = [], 0
        for ch in s:
            cp = ord(ch)
            cw = 2 if (
                0x1F300 <= cp <= 0x1FAFF
                or 0x2600 <= cp <= 0x27BF
            ) else 1

            if w + cw > self.width:
                break

            out.append(ch)
            w += cw

        return "".join(out)

    def _render(self):
        if self.quiet:
            return

        parts = [part for part in (self.timer, self.progress) if part]
        line = self._truncate_visual(self.tag + " │ ".join(parts))
        print(f"\033[2K\r{line}", end="", flush=True)

    def set_timer(self, txt):
        with self.lock:
            self.timer = txt
            self._render()

    def set_progress(self, txt):
        with self.lock:
            self.progress = txt
            self._render()

    def log(self, msg):
        with self.lock:
            if self.quiet:
                print(f"{self.tag}{msg}")
            else:
                print(f"\033[2K\r{self.tag}{msg}")

    def finish(self):
        with self.lock:
            if not self.quiet:
                print("\033[2K\r", end="", flush=True)

# ================================================================ HTTP
def fetch_raw(period, sub_type, level, competition_id):
    params = {
        "competition_id": str(competition_id),
        "tab": "",
        "period": period,
        "sub_type_id": sub_type,
        "level": level,
        "platform": "desktop",
        "view": "odileague",
        "ua": HEADERS["user-agent"],
        "resource": "virtuals",
    }
    t0 = time.perf_counter()
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=20)
    elapsed = time.perf_counter() - t0
    r.raise_for_status()
    return r.json(), elapsed


def scrape_one(period, sub_type, level, competition_id):
    data, elapsed = fetch_raw(period, sub_type, level, competition_id)
    d = data.get("data", data)
    matches = d.get("matches") if isinstance(d, dict) else None
    return sub_type, matches or [], elapsed


def merge_matches(results, matches):
    for m in matches:
        key = (m.get("home_team") or m.get("home"),
               m.get("away_team") or m.get("away"))
        for mk in m.get("markets", []):
            mname = mk.get("odd_type") or mk.get("sub_type_id") or "?"
            dst = results.setdefault(key, {}).setdefault(mname, {})
            for oc in mk.get("outcomes", []):
                oname = oc.get("outcome_name") or oc.get("outcome_key")
                oval = oc.get("odd_value")
                if oname and oval:
                    dst[oname] = oval


def scrape_round(period, level, workers, competition_id):
    results = {}
    errors = []
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(scrape_one, period, st, level, competition_id): st
                for st in SUB_TYPES}
        for fut in as_completed(futs):
            st = futs[fut]
            try:
                _, matches, _ = fut.result()
                merge_matches(results, matches)
            except Exception as e:
                errors.append((st, str(e)))

    return results, time.perf_counter() - t0, errors


def has_odds(results):
    return any(mk for m in results.values() for mk in m.values())


def scrape_with_retries(period, level, workers, competition_id,
                        attempts=1, delay=2.0):
    last = ({}, 0.0, [])
    for i in range(attempts):
        if _stop_event.is_set():
            break
        last = scrape_round(period, level, workers, competition_id)
        if has_odds(last[0]):
            return last
        if i < attempts - 1:
            if _stop_event.wait(delay):
                break
    return last


def acquire_periods(level, tz, competition_id, max_probe=240):
    base = now_in(tz).replace(second=0, microsecond=0)
    for offset in range(0, max_probe + 1):
        if _stop_event.is_set():
            return []
        ts = base + timedelta(minutes=offset)
        period = ts.strftime("%Y-%m-%d %H:%M:%S")
        try:
            data, _ = fetch_raw(period, "1X2", level, competition_id)
        except (requests.RequestException, ValueError):
            continue
        d = data.get("data", data)
        periods = d.get("periods") if isinstance(d, dict) else None
        if periods:
            return periods
    return []

# ================================================================ CSV
def load_existing_rounds(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    rounds = set()
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rs = row.get("round_start")
            if rs:
                rounds.add(rs)
    return rounds


def append_to_csv(path, tz, round_start, round_id, competition_id, results):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    scraped_at = now_in(tz).strftime("%Y-%m-%d %H:%M:%S")
    n = 0
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not exists:
            w.writeheader()
        for (home, away), markets in results.items():
            for mname, outs in markets.items():
                for sel, odd in outs.items():
                    w.writerow({
                        "scraped_at":     scraped_at,
                        "round_start":    round_start,
                        "round_id":       round_id,
                        "competition_id": competition_id,
                        "home": home, "away": away,
                        "market": mname, "selection": sel, "odd": odd,
                    })
                    n += 1
    return n

# ================================================================ monitor
def monitor_round(start_dt, tz, live_seconds, display):
    while True:
        if _stop_event.is_set():
            return
        remaining = (start_dt - now_in(tz)).total_seconds()
        if remaining <= 0:
            break
        display.set_timer(f"⏱ kick-off in {fmt_hms(remaining)}")
        time.sleep(0.2)

    if _stop_event.is_set():
        return

    display.log("🔴 LIVE  match started")
    while True:
        if _stop_event.is_set():
            return
        elapsed = (now_in(tz) - start_dt).total_seconds()
        if elapsed >= live_seconds:
            break
        display.set_timer(f"🔴 LIVE  {elapsed:04.1f}s / {live_seconds}s")
        time.sleep(0.1)

    display.set_timer("")
    display.log(f"🔴 LIVE  {live_seconds}s / {live_seconds}s  ✓ finished")

# ================================================================ worker
def scrape_worker(rounds, target_s, level, workers, tz, csv_path,
                  competition_id, display, processed, lock):
    total = len(rounds)
    saved = 0
    sels = 0

    for i, (s, rid) in enumerate(rounds, 1):
        if _stop_event.is_set():
            break
        is_target = (s == target_s)
        dt = parse_dt(s, tz)

        if is_target and dt is not None:
            while True:
                if _stop_event.is_set():
                    return
                secs = (dt - now_in(tz)).total_seconds()
                if secs <= WINDOW_LEAD:
                    break
                display.set_progress(
                    f"📊 waiting for target window "
                    f"({secs - WINDOW_LEAD:.0f}s, {i}/{total})"
                )
                time.sleep(min(5, max(1, secs - WINDOW_LEAD)))

        marker = " ⭐" if is_target else ""
        display.set_progress(
            f"📊 {i}/{total} {s}{marker}  [{saved}✓ {sels}sel]"
        )

        attempts = 3 if is_target else 1
        try:
            results, wall, errors = scrape_with_retries(
                s, level, workers, competition_id,
                attempts=attempts, delay=2.0,
            )
        except Exception as e:
            display.log(f"  ✗ {s} failed: {e}")
            continue

        if has_odds(results):
            n = append_to_csv(csv_path, tz, s, rid, competition_id, results)
            n_sel = sum(len(o) for m in results.values() for o in m.values())
            display.log(f"  ✅ {s}  saved {n} rows / {n_sel} sels "
                        f"[{wall*1000:.0f} ms]")
            saved += 1
            sels += n_sel
            with lock:
                processed.add(s)
        else:
            display.log(f"  ⚠ {s} empty (retry next cycle)")

    display.set_progress(f"📊 done: {saved}/{total} rounds  [{sels} sels]")

# ================================================================ run one league
def run_single(args):
    cid      = args.competition
    tag      = f"{cid}:{COMPETITIONS.get(cid, '?')}"
    csv_path = args.csv or f"odds_{cid}.csv"
    quiet    = getattr(args, "quiet", False)

    _set_league(cid, csv_path, quiet=quiet)
    csv_path = _csv_path()

    display   = Display(
        tag=f"L{_cid()}" if _quiet() else "",
        quiet=_quiet(),
    )
    processed = load_existing_rounds(csv_path)
    lock      = threading.Lock()

    print(f"{_tag()}Odibets odds – {tag}")
    print(f"{_tag()}  csv={csv_path}  tz={args.tz}  level={args.level}  "
          f"workers={args.workers}  cycles="
          f"{'∞' if args.rounds == 0 else args.rounds}")
    print(f"{_tag()}  loaded {len(processed)} already-scraped rounds\n")

    cycles = 0
    try:
        while not _stop_event.is_set():
            cycle_start = now_in(args.tz)
            stamp = now_in(args.tz).strftime("%H:%M:%S")
            display.log(f"[{stamp}] acquiring session …")

            periods = acquire_periods(args.level, args.tz, _cid())
            if _stop_event.is_set():
                break
            if not periods:
                display.log("  ⚠ no session; retry in 15s")
                _stop_event.wait(15)
                continue

            rounds = []
            for p in periods:
                s = (p.get("period") or p.get("start")
                     or p.get("start_time"))
                rid = p.get("round_id") or p.get("id")
                if not s or rid is None:
                    continue
                dt = parse_dt(s, args.tz)
                if dt is None:
                    continue
                rounds.append((dt, s, rid))
            rounds.sort()

            now = now_in(args.tz)
            cutoff = now + timedelta(seconds=SKIP_THRESHOLD)
            pending = []
            skipped_close = 0
            for dt, s, rid in rounds:
                if s in processed:
                    continue
                if dt <= cutoff:
                    skipped_close += 1
                    continue
                pending.append((dt, s, rid))

            display.log(f"  batch: {len(rounds)} rounds  "
                        f"pending: {len(pending)}  "
                        f"skipped-close: {skipped_close}")

            if not pending:
                last_dt = rounds[-1][0] if rounds else None
                if last_dt is None:
                    display.log("  empty session; sleep 30s")
                    _stop_event.wait(30)
                    continue

                session_end = last_dt + timedelta(seconds=LIVE_SECONDS)
                remaining0 = (session_end - now).total_seconds()
                if remaining0 > 0:
                    display.log(
                        f"  ⏳ session complete; waiting for last round to end "
                        f"({fmt_hms(remaining0)})"
                    )
                    while True:
                        if _stop_event.is_set():
                            break
                        remaining = (session_end - now_in(args.tz)).total_seconds()
                        if remaining <= 0:
                            break
                        display.set_timer(
                            f"⏱ session ends in {fmt_hms(remaining)}  "
                            f"({last_dt.strftime('%H:%M:%S')})"
                        )
                        _stop_event.wait(0.5)
                    display.set_timer("")
                    display.finish()
                continue

            # pick nearest UPCOMING round as target (fix: upcoming[0])
            upcoming = [(dt, s, rid) for dt, s, rid in rounds if dt > cutoff]
            if not upcoming:
                display.log("  no upcoming rounds; sleep 30s")
                _stop_event.wait(30)
                continue
            target_dt, target_s, target_rid = upcoming[0]

            all_scrape = [(s, rid) for _, s, rid in pending]

            if _stop_event.is_set():
                break

            scraper = threading.Thread(
                target=scrape_worker,
                args=(all_scrape, target_s, args.level, args.workers,
                      args.tz, csv_path, _cid(), display, processed, lock),
                daemon=True,
            )
            scraper.start()

            monitor_round(target_dt, args.tz, LIVE_SECONDS, display)

            if scraper.is_alive():
                display.log("⏳ waiting for scraper to finish …")
            while scraper.is_alive():
                if _stop_event.is_set():
                    scraper.join(timeout=3)
                    break
                elapsed = (now_in(args.tz) - cycle_start).total_seconds()
                display.set_timer(
                    f"🔄 cycle {fmt_hms(elapsed)} — scraper running"
                )
                _stop_event.wait(0.5)

            scraper.join()
            display.set_timer("")
            display.finish()
            cycles += 1

            if args.rounds and cycles >= args.rounds:
                display.log(f"  reached --rounds {args.rounds}, stopping")
                break

    except KeyboardInterrupt:
        _stop_event.set()
    finally:
        display.finish()
        print(f"\n{_tag()}👋 {tag} stopped after {cycles} cycle(s)")
        print(f"{_tag()}total rounds in CSV: {len(processed)}")
        print(f"{_tag()}file: {csv_path}")

# ================================================================ all mode
def run_all(args):
    """Run all 4 leagues concurrently as threads in one process."""
    _stop_event.clear()
    threads = []

    for cid in COMPETITIONS:
        t = threading.Thread(
            target=run_single,
            args=(argparse.Namespace(
                competition=cid,
                csv=None,
                level=args.level,
                workers=args.workers,
                tz=args.tz,
                rounds=args.rounds,
                quiet=True,
            ),),
            name=f"odds-league-{cid}",
            daemon=False,
        )
        threads.append(t)
        t.start()
        print(f"  → {cid}:{COMPETITIONS[cid]:<8} thread started", flush=True)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n⏹  Ctrl+C — stopping all leagues …", flush=True)
        _stop_event.set()
        for t in threads:
            t.join(timeout=5)

# ================================================================ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", default="all",
                    help="1=English 2=Spanish 3=Kenyan 4=Italian, or 'all' (default)")
    ap.add_argument("--csv", default=None,
                    help="output csv (default: odds_<cid>.csv)")
    ap.add_argument("--level", type=int, default=4)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--tz", default="Africa/Nairobi")
    ap.add_argument("--rounds", type=int, default=0,
                    help="stop after N monitor cycles (0 = infinite)")
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