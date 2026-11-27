#!/usr/bin/env python3
"""Launch and supervise the Odibets mobile scraping system."""

import argparse
import atexit
import csv
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parent
STOP = threading.Event()
RESTART_WINDOW = 300
MAX_RESTARTS = 5
LOG_ROTATE_BYTES = 50 * 1024 * 1024

_WAKE_LOCKED = False
_restart_history = defaultdict(lambda: deque())
_log_handles = {}
_processes = {}
_intentional = set()
_scraper_commands = {}


def _acquire_wake_lock():
    global _WAKE_LOCKED
    try:
        result = subprocess.run(
            ["termux-wake-lock"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _WAKE_LOCKED = result.returncode == 0
    except FileNotFoundError:
        print("[power] Termux wake-lock unavailable", flush=True)
    except OSError as exc:
        print(f"[power] wake-lock failed: {exc}", flush=True)


def _release_wake_lock():
    global _WAKE_LOCKED
    if not _WAKE_LOCKED:
        return
    try:
        subprocess.run(
            ["termux-wake-lock", "-u"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, OSError):
        pass
    _WAKE_LOCKED = False


def python_command(script, *args):
    uv = shutil.which("uv")
    if uv:
        return [uv, "run", "python", str(script), *map(str, args)]
    return [sys.executable, str(script), *map(str, args)]


def _open_log(name):
    log_path = ROOT / f"{name}.log"
    handle = open(log_path, "a", encoding="utf-8", buffering=1)
    _log_handles[name] = handle
    return handle


def _close_log(name):
    handle = _log_handles.pop(name, None)
    if handle is not None:
        handle.close()


def start_scraper(name):
    if name in _processes and _processes[name].poll() is None:
        return _processes[name]
    handle = _open_log(name)
    try:
        process = subprocess.Popen(
            _scraper_commands[name],
            cwd=ROOT,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
    except OSError as exc:
        handle.close()
        _log_handles.pop(name, None)
        print(f"[supervisor] failed to start {name}: {exc}", flush=True)
        return None
    _processes[name] = process
    print(f"[supervisor] started {name} (PID {process.pid})", flush=True)
    return process


def stop_process(name, timeout=5):
    process = _processes.get(name)
    if process is None:
        return
    _intentional.add(name)
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    _intentional.discard(name)


def restart_scraper(name, reason):
    now = time.monotonic()
    history = _restart_history[name]
    while history and now - history[0] > RESTART_WINDOW:
        history.popleft()
    if len(history) >= MAX_RESTARTS:
        print(
            f"[supervisor] CRITICAL: {name} exceeded {MAX_RESTARTS} "
            f"restarts in {RESTART_WINDOW}s; not restarting ({reason})",
            file=sys.stderr,
            flush=True,
        )
        return
    history.append(now)
    stop_process(name)
    _close_log(name)
    print(f"[supervisor] restarting {name}: {reason}", flush=True)
    start_scraper(name)


def stop_processes(timeout=8):
    STOP.set()
    for name in list(_processes):
        process = _processes[name]
        if process.poll() is None:
            _intentional.add(name)
            process.terminate()

    deadline = time.monotonic() + timeout
    for name, process in list(_processes.items()):
        if process.poll() is not None:
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            process.wait(timeout=max(0.1, remaining))
        except subprocess.TimeoutExpired:
            pass

    for process in _processes.values():
        if process.poll() is None:
            process.kill()
            process.wait()
    for name in list(_log_handles):
        _close_log(name)
    _release_wake_lock()


def rotate_odds_csvs(max_age_hours=6):
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    fields = (
        "scraped_at", "round_start", "round_id", "competition_id",
        "home", "away", "market", "selection", "odd",
    )

    odds_process = _processes.get("odds")
    if odds_process is not None:
        stop_process("odds")

    try:
        for path in sorted(ROOT.glob("odds*.csv")):
            if not path.exists() or path.stat().st_size == 0:
                continue
            tmp = path.with_name(path.name + ".rotate.tmp")
            kept = 0
            try:
                with path.open("r", newline="", encoding="utf-8") as source:
                    reader = csv.DictReader(source)
                    source_fields = reader.fieldnames or list(fields)
                    with tmp.open("w", newline="", encoding="utf-8") as target:
                        writer = csv.DictWriter(
                            target,
                            fieldnames=source_fields,
                            extrasaction="ignore",
                        )
                        writer.writeheader()
                        for row in reader:
                            scraped = row.get("scraped_at")
                            keep = True
                            if scraped:
                                try:
                                    when = datetime.strptime(
                                        scraped, "%Y-%m-%d %H:%M:%S"
                                    )
                                    keep = when >= cutoff
                                except (TypeError, ValueError):
                                    keep = True
                            if keep:
                                writer.writerow(row)
                                kept += 1
                os.replace(tmp, path)
                print(
                    f"[maintenance] rotated {path.name}: retained {kept} rows",
                    flush=True,
                )
            except (OSError, UnicodeError, csv.Error) as exc:
                print(f"[maintenance] could not rotate {path.name}: {exc}",
                      file=sys.stderr, flush=True)
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass
    finally:
        if odds_process is not None:
            start_scraper("odds")


def rotate_logs():
    for name in ("odds", "results"):
        log_path = ROOT / f"{name}.log"
        try:
            if log_path.stat().st_size < LOG_ROTATE_BYTES:
                continue
        except FileNotFoundError:
            continue
        except OSError as exc:
            print(f"[maintenance] cannot inspect {name}.log: {exc}",
                  file=sys.stderr, flush=True)
            continue

        stop_process(name)
        _close_log(name)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = ROOT / f"{name}.log.{stamp}.bak"
        try:
            os.replace(log_path, backup)
        except OSError as exc:
            print(f"[maintenance] cannot archive {name}.log: {exc}",
                  file=sys.stderr, flush=True)
            continue
        print(f"[maintenance] archived {name}.log to {backup.name}",
              flush=True)
        start_scraper(name)


def database_is_healthy(db_path):
    if not db_path.exists():
        return False
    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
        try:
            result = conn.execute("PRAGMA quick_check").fetchone()
            if not result or result[0] != "ok":
                return False
            conn.execute("SELECT 1 FROM competition LIMIT 1")
            return True
        finally:
            conn.close()
    except sqlite3.Error:
        return False


def startup_database(db_path):
    needs_rebuild = (
        not db_path.exists()
        or (time.time() - db_path.stat().st_mtime) > 600
        or not database_is_healthy(db_path)
    )
    if not needs_rebuild:
        print("[db] existing database is recent and healthy", flush=True)
        return

    print("[db] rebuilding database", flush=True)
    command = python_command(ROOT / "db.py", "--keep")
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            message = (result.stderr or "").strip()
            print(
                f"[db] startup update failed"
                f"{(': ' + message) if message else ''}",
                file=sys.stderr,
                flush=True,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"[db] startup update failed: {exc}", file=sys.stderr, flush=True)


def run_database_update():
    try:
        result = subprocess.run(
            python_command(ROOT / "db.py", "--keep"),
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            message = (result.stderr or "").strip()
            print(
                f"[db] update failed"
                f"{(': ' + message) if message else ''}",
                file=sys.stderr,
                flush=True,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"[db] update failed: {exc}", file=sys.stderr, flush=True)


def _handle_signal(signum, frame):
    STOP.set()


def main():
    parser = argparse.ArgumentParser(
        description="Run the Odibets odds, results, database, and dashboard"
    )
    parser.add_argument("--competition", default="all")
    parser.add_argument("--profile", choices=("full", "mobile", "low"),
                        default="mobile")
    parser.add_argument("--level", type=int, default=4)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--db-interval", type=float, default=30.0)
    parser.add_argument("--dashboard-interval", type=float, default=5.0)
    parser.add_argument("--no-dashboard", action="store_true")
    args = parser.parse_args()

    _acquire_wake_lock()
    atexit.register(_release_wake_lock)
    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    odds_args = [
        "--competition", args.competition,
        "--profile", args.profile,
        "--level", str(args.level),
    ]
    if args.workers is not None:
        odds_args.extend(["--workers", str(args.workers)])

    _scraper_commands["odds"] = python_command(ROOT / "odds.py", *odds_args)
    _scraper_commands["results"] = python_command(
        ROOT / "results.py",
        "--competition", args.competition,
    )

    db_path = ROOT / "odibets.db"
    startup_database(db_path)

    start_scraper("odds")
    start_scraper("results")

    if not args.no_dashboard:
        _processes["dashboard"] = subprocess.Popen(
            python_command(
                ROOT / "ui.py",
                "--db", str(db_path),
                "--interval", str(args.dashboard_interval),
            ),
            cwd=ROOT,
        )
        print(
            f"[supervisor] started dashboard (PID {_processes['dashboard'].pid})",
            flush=True,
        )

    next_db_update = time.monotonic() + args.db_interval
    next_odds_rotation = time.monotonic() + 3600

    try:
        while not STOP.is_set():
            now = time.monotonic()

            for name in ("odds", "results"):
                process = _processes.get(name)
                if process is not None and process.poll() is not None:
                    code = process.returncode
                    _close_log(name)
                    if name not in _intentional:
                        restart_scraper(name, f"unexpected exit with code {code}")

            rotate_logs()

            if now >= next_db_update:
                run_database_update()
                next_db_update = time.monotonic() + args.db_interval
            if now >= next_odds_rotation:
                rotate_odds_csvs(max_age_hours=6)
                next_odds_rotation = time.monotonic() + 3600

            dashboard = _processes.get("dashboard")
            if dashboard is not None and dashboard.poll() is not None:
                _processes.pop("dashboard", None)

            if STOP.wait(1):
                break
    finally:
        stop_processes()
        atexit.unregister(_release_wake_lock)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
