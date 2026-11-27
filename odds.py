#!/usr/bin/env python3
"""
Odibets Virtuals – monitor the nearest round while scraping odds for all others

Cycle flow:
  1. Acquire list of visible round timestamps.
  2. Skip rounds within SKIP_THRESHOLD (default 10 s) — they're about to start.
  3. Pick the soonest remaining round as the MONITOR target.
  4. Spawn a scraper thread that walks EVERY pending round in order:
       • the target first (waits for its betting window if needed, retries),
       • then all others (one fast attempt each — empty ones retried next cycle).
     Each successful round is appended to CSV immediately.
  5. In the main thread, monitor the target: live countdown → 35 s live timer.
  6. When the match finishes, wait for the scraper to finish, then re-acquire.

Usage:
    python scrape.py                    # infinite
    python scrape.py --rounds 3         # 3 cycles then stop
    python scrape.py --csv v.csv --workers 10
"""

import argparse
import csv
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    ZoneInfo = None
    ZoneInfoNotFoundError = None

import requests


class TimezoneError(RuntimeError):
    pass


# ================================================================ constants
BASE = "https://odibets.com/pxy2/virtuals"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://odibets.com/virtuals",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/157.0.0.0 Safari/537.36"),
    "x-requested-with": "XMLHttpRequest",
}

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
    "scraped_at", "round_start", "round_id",
    "home", "away", "market", "selection", "odd",
]

LIVE_SECONDS = 35
SKIP_THRESHOLD = 10      # skip rounds within this many seconds of starting
WINDOW_LEAD = 85         # betting window opens this many seconds before start


# ================================================================ time
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # Python < 3.9
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

# Fallback offsets (hours from UTC) for zones with NO DST.
# Only used if the IANA database is unavailable on this machine.
# Nairobi is UTC+3 year-round, so this is exactly correct.
_FIXED_OFFSET_HOURS = {
    "Africa/Nairobi": 3,
    "Africa/Kampala": 3,
    "Africa/Dar_es_Salaam": 3,
    "UTC": 0,
}


def _resolve_tz(tz):
    """Return a tzinfo for the given zone name, or None for naive/local time.

    Order of preference:
      1. zoneinfo.ZoneInfo (real IANA rules; needs system tzdata on Linux/macOS,
         or the `tzdata` package on Windows).
      2. A fixed-offset timezone from the built-in table (for zones without DST).
      3. None -> naive local time (last resort).
    """
    if not tz:
        return None

    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz)
        except (ZoneInfoNotFoundError, KeyError, OSError):
            pass  # tzdata missing -> try fallback below

    hours = _FIXED_OFFSET_HOURS.get(tz)
    if hours is not None:
        return timezone(timedelta(hours=hours))

    # Unknown zone and no tzdata -> naive local time
    return None


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
    """Single-line live dashboard: timer │ scraper-progress."""

    def __init__(self, width=130):
        self.lock = threading.RLock()
        self.timer = ""
        self.progress = ""
        self.width = width

    @staticmethod
    def _vwidth(s):
        w = 0
        for ch in s:
            cp = ord(ch)
            if 0x1F300 <= cp <= 0x1FAFF or 0x2600 <= cp <= 0x27BF:
                w += 2
            else:
                w += 1
        return w

    def _truncate_visual(self, s):
        out, w = [], 0
        for ch in s:
            cp = ord(ch)
            cw = 2 if (0x1F300 <= cp <= 0x1FAFF or 0x2600 <= cp <= 0x27BF) else 1
            if w + cw > self.width:
                break
            out.append(ch)
            w += cw
        return "".join(out)

    def _render(self):
        parts = []
        if self.timer:
            parts.append(self.timer)
        if self.progress:
            parts.append(self.progress)
        line = self._truncate_visual(" │ ".join(parts))
        # \033[2K = erase whole line, \r = return to col 0
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
            print(f"\033[2K\r{msg}")

    def finish(self):
        with self.lock:
            print("\033[2K\r", end="", flush=True)


# ================================================================ HTTP
def fetch_raw(period, sub_type, level):
    params = {
        "competition_id": "1",
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


def scrape_one(period, sub_type, level):
    data, elapsed = fetch_raw(period, sub_type, level)
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


def scrape_round(period, level, workers):
    results = {}
    errors = []
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(scrape_one, period, st, level): st
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


def scrape_with_retries(period, level, workers, attempts=1, delay=2.0):
    last = ({}, 0.0, [])
    for i in range(attempts):
        last = scrape_round(period, level, workers)
        if has_odds(last[0]):
            return last
        if i < attempts - 1:
            time.sleep(delay)
    return last


def acquire_periods(level, tz, max_probe=240):
    base = now_in(tz).replace(second=0, microsecond=0)
    for offset in range(0, max_probe + 1):
        ts = base + timedelta(minutes=offset)
        period = ts.strftime("%Y-%m-%d %H:%M:%S")
        try:
            data, _ = fetch_raw(period, "1X2", level)
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


def append_to_csv(path, tz, round_start, round_id, results):
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
                        "scraped_at": scraped_at,
                        "round_start": round_start,
                        "round_id": round_id,
                        "home": home, "away": away,
                        "market": mname, "selection": sel, "odd": odd,
                    })
                    n += 1
    return n

def idle_countdown(display, next_dt, tz, max_seconds=30):
    """Live countdown to the next round; returns after max_seconds
    or as soon as the round becomes imminent."""
    end = time.time() + max_seconds
    while time.time() < end:
        remaining = (next_dt - now_in(tz)).total_seconds()
        if remaining <= SKIP_THRESHOLD:
            break
        display.set_timer(
            f"⏱ next round in {fmt_hms(remaining)}  "
            f"({next_dt.strftime('%H:%M:%S')})"
        )
        time.sleep(0.3)
    display.set_timer("")
    display.finish()

# ================================================================ monitor
def monitor_round(start_dt, tz, live_seconds, display):
    """Countdown to kick-off, then 35 s live timer — plain ticks, no bar."""
    while True:
        remaining = (start_dt - now_in(tz)).total_seconds()
        if remaining <= 0:
            break
        display.set_timer(f"⏱ kick-off in {fmt_hms(remaining)}")
        time.sleep(0.2)

    display.log("🔴 LIVE  match started")
    while True:
        elapsed = (now_in(tz) - start_dt).total_seconds()
        if elapsed >= live_seconds:
            break
        display.set_timer(f"🔴 LIVE  {elapsed:04.1f}s / {live_seconds}s")
        time.sleep(0.1)

    display.set_timer("")
    display.log(f"🔴 LIVE  {live_seconds}s / {live_seconds}s  ✓ finished")


# ================================================================ scraper thread
def scrape_worker(rounds, target_s, level, workers, tz, csv_path,
                  display, processed, lock):
    total = len(rounds)
    saved = 0
    sels = 0

    for i, (s, rid) in enumerate(rounds, 1):
        is_target = (s == target_s)
        dt = parse_dt(s, tz)

        # for the target, wait until its betting window is open
        if is_target and dt is not None:
            while True:
                secs = (dt - now_in(tz)).total_seconds()
                if secs <= WINDOW_LEAD:
                    break
                display.set_progress(
                    f"📊 waiting for target window "
                    f"({secs - WINDOW_LEAD:.0f}s, {i}/{total})"
                )
                time.sleep(min(5, secs - WINDOW_LEAD))

        marker = " ⭐" if is_target else ""
        display.set_progress(
            f"📊 {i}/{total} {s}{marker}  [{saved}✓ {sels}sel]"
        )

        attempts = 3 if is_target else 1
        try:
            results, wall, errors = scrape_with_retries(
                s, level, workers, attempts=attempts, delay=2.0
            )
        except Exception as e:
            display.log(f"  ✗ {s} failed: {e}")
            continue

        if has_odds(results):
            n = append_to_csv(csv_path, tz, s, rid, results)
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


# ================================================================ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="odds.csv")
    ap.add_argument("--level", type=int, default=4)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--tz", default="Africa/Nairobi")
    ap.add_argument("--rounds", type=int, default=0,
                    help="stop after N monitor cycles (0 = infinite)")
    args = ap.parse_args()

    display = Display()
    processed = load_existing_rounds(args.csv)
    lock = threading.Lock()

    print(f"Odibets Virtuals – monitor + scrape")
    print(f"  csv={args.csv}  tz={args.tz}  level={args.level}  "
          f"workers={args.workers}  cycles="
          f"{'∞' if args.rounds == 0 else args.rounds}")
    print(f"  loaded {len(processed)} already-scraped rounds\n")

    cycles = 0
    try:
        while True:
            cycle_start = now_in(args.tz)
            stamp = now_in(args.tz).strftime("%H:%M:%S")
            display.log(f"[{stamp}] acquiring session …")

            periods = acquire_periods(args.level, args.tz)
            if not periods:
                display.log("  ⚠ no session; retry in 15s")
                time.sleep(15)
                continue

            # build (dt, start, rid) list
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

            # filter: skip processed + skip rounds within SKIP_THRESHOLD
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
                # every round in this batch is already scraped — do not
                # re-acquire until the last one has finished
                last_dt = rounds[-1][0] if rounds else None
                if last_dt is None:
                    display.log("  empty session; sleep 30s")
                    time.sleep(30)
                    continue

                session_end = last_dt + timedelta(seconds=LIVE_SECONDS)
                remaining0 = (session_end - now).total_seconds()
                if remaining0 > 0:
                    display.log(
                        f"  ⏳ session complete; waiting for last round to end "
                        f"({fmt_hms(remaining0)})"
                    )
                    while True:
                        remaining = (session_end - now_in(args.tz)).total_seconds()
                        if remaining <= 0:
                            break
                        display.set_timer(
                            f"⏱ session ends in {fmt_hms(remaining)}  "
                            f"({last_dt.strftime('%H:%M:%S')})"
                        )
                        time.sleep(0.5)
                    display.set_timer("")
                    display.finish()
                continue

             # pick the nearest UPCOMING round as the monitor target
            now = now_in(args.tz)
            cutoff = now + timedelta(seconds=SKIP_THRESHOLD)
            upcoming = [(dt, s, rid) for dt, s, rid in rounds if dt > cutoff]

            if not upcoming:
                display.log("  no upcoming rounds; sleep 30s")
                time.sleep(30)
                continue
            target_dt, target_s, target_rid = rounds[0]     # was pending[0]

            # still scrape all pending rounds (scraper skips far ones internally)
            all_scrape = [(s, rid) for _, s, rid in pending]

            # spawn scraper
            scraper = threading.Thread(
                target=scrape_worker,
                args=(all_scrape, target_s, args.level, args.workers,
                      args.tz, args.csv, display, processed, lock),
                daemon=True,
            )
            scraper.start()

                       # monitor in main thread
            monitor_round(target_dt, args.tz, LIVE_SECONDS, display)

            if scraper.is_alive():
                display.log("⏳ waiting for scraper to finish …")

            # keep a live cycle timer running until the scraper is done
            while scraper.is_alive():
                elapsed = (now_in(args.tz) - cycle_start).total_seconds()
                display.set_timer(f"🔄 cycle {fmt_hms(elapsed)} — scraper running")
                time.sleep(0.5)

            scraper.join()
            display.set_timer("")
            display.finish()
            cycles += 1

    except KeyboardInterrupt:
        pass

    display.finish()
    print(f"\n👋 stopped after {cycles} cycle(s)")
    print(f"total rounds in CSV: {len(processed)}")
    print(f"file: {args.csv}")


if __name__ == "__main__":
    try:
        main()
    except TimezoneError as e:
        raise SystemExit(f"Error: {e}") from None