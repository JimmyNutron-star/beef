#!/usr/bin/env python3
"""Run the odds and results scrapers as isolated processes."""

import csv
import io
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import db


SCRAPERS = (
    ("odds.py", "odds.log"),
    ("results.py", "results.log"),
)

ODDS_CSV_FIELDS = (
    "scraped_at", "round_start", "round_id",
    "competition_id", "home", "away", "market", "selection", "odd",
)

ERROR_MARKERS = (
    "error", "failed", "failure", "exception", "traceback", "fail",
    "warning", "warn",
    "⚠", "❌", "✗",
)


def _file_signature(path):
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_size, stat.st_mtime_ns, stat.st_ino


def _row_signature(row):
    return tuple(sorted(row.items()))


class CsvDatabaseWriter:
    def __init__(self, conn, odds_paths, results_paths):
        self.conn = conn
        self.odds_paths = self._as_paths(odds_paths)
        self.results_paths = self._as_paths(results_paths)
        self.odds_offsets = {path: 0 for path in self.odds_paths}
        self.odds_file_ids = {path: None for path in self.odds_paths}
        self.result_versions = {path: {} for path in self.results_paths}
        self.completed = {}
        self.pending = {}
        self.errors = {}
        self._load_existing_csvs()

    @staticmethod
    def _as_paths(paths):
        if isinstance(paths, (str, Path)):
            paths = (paths,)
        return tuple(Path(path) for path in paths)

    @staticmethod
    def _read_csv(path):
        if not path.exists() or path.stat().st_size == 0:
            return []
        with path.open("r", newline="", encoding="utf-8") as csv_file:
            return list(csv.DictReader(csv_file))

    @staticmethod
    def _result_key(row):
        return row.get("parent_match_id") or _row_signature(row)

    def _load_existing_csvs(self):
        odds_saved = 0
        results_saved = 0
        for path in self.odds_paths:
            signature = _file_signature(path)
            rows = self._read_csv(path)
            if signature != _file_signature(path):
                raise RuntimeError(
                    f"{path.name} changed while initializing the database"
                )
            if rows:
                odds_saved += db.save_odds_rows(self.conn, rows)
            self.odds_offsets[path] = signature[0] if signature else 0
            self.odds_file_ids[path] = signature[2] if signature else None
            self.completed[path] = signature

        for path in self.results_paths:
            signature = _file_signature(path)
            rows = self._read_csv(path)
            if signature != _file_signature(path):
                raise RuntimeError(
                    f"{path.name} changed while initializing the database"
                )
            if rows:
                results_saved += db.save_result_rows(self.conn, rows)
            self.result_versions[path] = {
                self._result_key(row): _row_signature(row)
                for row in rows
            }
            self.completed[path] = signature

        print(
            f"[db] ready; loaded {odds_saved} odds rows and "
            f"{results_saved} result rows from {len(self.odds_paths)} odds "
            f"and {len(self.results_paths)} results CSVs",
            flush=True,
        )

    def _read_odds_delta(self, path, signature):
        size, _, file_id = signature
        offset = self.odds_offsets[path]
        previous_file_id = self.odds_file_ids[path]
        replaced = (
            previous_file_id is not None
            and file_id != previous_file_id
            and file_id != 0
        )
        if size < offset or replaced or offset == 0:
            return self._read_csv(path), size, True

        with path.open("rb") as csv_file:
            csv_file.seek(offset)
            data = csv_file.read()
        if not data:
            return [], size, True

        last_newline = data.rfind(b"\n")
        if last_newline < 0:
            return [], offset, False

        complete_data = data[:last_newline + 1]
        text = complete_data.decode("utf-8")
        reader = csv.DictReader(
            io.StringIO(text, newline=""),
            fieldnames=ODDS_CSV_FIELDS,
        )
        new_offset = offset + len(complete_data)
        return list(reader), new_offset, new_offset == size

    def _sync_odds(self, path, signature):
        rows, new_offset, complete = self._read_odds_delta(path, signature)
        saved = db.save_odds_rows(self.conn, rows) if rows else 0
        self.odds_offsets[path] = new_offset
        self.odds_file_ids[path] = signature[2]
        if saved:
            print(
                f"[db] {path.name}: stored {saved} new odds rows",
                flush=True,
            )
        return complete

    def _sync_results(self, path):
        rows = self._read_csv(path)
        updates = []
        versions = self.result_versions[path].copy()
        for row in rows:
            key = self._result_key(row)
            signature = _row_signature(row)
            if versions.get(key) != signature:
                updates.append(row)
                versions[key] = signature

        saved = db.save_result_rows(self.conn, updates) if updates else 0
        self.result_versions[path] = versions
        if saved:
            print(
                f"[db] {path.name}: stored {saved} new or updated result rows",
                flush=True,
            )

    def sync_once(self, force=False):
        for path in self.odds_paths + self.results_paths:
            try:
                signature = _file_signature(path)
                if signature is None:
                    continue
                odds_file = path in self.odds_offsets
                needs_odds_tail = (
                    odds_file and signature[0] > self.odds_offsets[path]
                )
                if signature == self.completed.get(path) and not needs_odds_tail:
                    self.pending.pop(path, None)
                    continue
                if not force and self.pending.get(path) != signature:
                    self.pending[path] = signature
                    continue

                if odds_file:
                    complete = self._sync_odds(path, signature)
                else:
                    self._sync_results(path)
                    complete = True

                if _file_signature(path) != signature:
                    self.pending[path] = _file_signature(path)
                    continue
                if complete:
                    self.completed[path] = signature
                    self.pending.pop(path, None)
                else:
                    self.pending[path] = signature
                self.errors.pop(path, None)
            except (OSError, UnicodeError, csv.Error, sqlite3.Error, ValueError) as e:
                message = str(e)
                if self.errors.get(path) != message:
                    print(
                        f"[db] {path.name} sync failed; will retry: {message}",
                        file=sys.stderr,
                        flush=True,
                    )
                self.errors[path] = message


def _is_error_line(line):
    lowered = line.lower()
    return any(marker in lowered for marker in ERROR_MARKERS)


def _forward_stream(name, stream, log_file, log_lock, terminal_lock,
                    stream_kind):
    for line in stream:
        with log_lock:
            log_file.write(line)
            log_file.flush()

        if line:
            is_error = stream_kind == "stderr" or _is_error_line(line)
            target = (
                sys.stderr if is_error else sys.stdout
            )
            output = f"[{name}] {line}" if line.strip() else line
            encoding = target.encoding or "utf-8"
            output = output.encode(
                encoding, errors="backslashreplace"
            ).decode(encoding, errors="replace")
            with terminal_lock:
                target.write(output)
                target.flush()


def _start_reader(name, stream, log_file, log_lock, terminal_lock,
                  stream_kind):
    reader = threading.Thread(
        target=_forward_stream,
        args=(name, stream, log_file, log_lock, terminal_lock, stream_kind),
        daemon=True,
    )
    reader.start()
    return reader


def stop_processes(processes, grace_seconds):
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if all(process.poll() is not None for process in processes.values()):
            return
        time.sleep(0.2)

    for process in processes.values():
        if process.poll() is None:
            process.terminate()

    for process in processes.values():
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _odds_csv_paths(root, odds_args):
    paths = [root / "odds.csv"]
    paths.extend(root / f"odds_{cid}.csv" for cid in range(1, 5))

    competition = "all"
    custom_csv = None
    index = 0
    while index < len(odds_args):
        arg = odds_args[index]
        if arg.startswith("--competition="):
            competition = arg.split("=", 1)[1]
        elif arg == "--competition" and index + 1 < len(odds_args):
            competition = odds_args[index + 1]
            index += 1
        elif arg.startswith("--csv="):
            custom_csv = arg.split("=", 1)[1]
        elif arg == "--csv" and index + 1 < len(odds_args):
            custom_csv = odds_args[index + 1]
            index += 1
        index += 1

    if custom_csv and competition != "all":
        custom_path = Path(custom_csv)
        paths.append(custom_path if custom_path.is_absolute() else root / custom_path)
    return tuple(dict.fromkeys(paths))


def main():
    if any(arg in ("-h", "--help") for arg in sys.argv[1:]):
        print(
            "Usage: python main.py [--db PATH] [odds.py options]\n"
            "Runs odds.py and results.py independently. Optional arguments "
            "are passed to odds.py only.\n"
            "CSV files: odds_1.csv..odds_4.csv and "
            "results_1.csv..results_4.csv (legacy CSVs are also read)\n"
            "Database: odibets.db by default\n"
            "Logs: odds.log and results.log; tagged scraper progress and "
            "errors are shown live"
        )
        return 0

    root = Path(__file__).resolve().parent
    db_path = root / "odibets.db"
    odds_args = []
    args = sys.argv[1:]
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--db":
            if index + 1 >= len(args):
                print("--db requires a path", file=sys.stderr)
                return 2
            db_path = Path(args[index + 1])
            if not db_path.is_absolute():
                db_path = root / db_path
            index += 2
        elif arg.startswith("--db="):
            db_path = Path(arg.split("=", 1)[1])
            if not db_path.is_absolute():
                db_path = root / db_path
            index += 1
        else:
            odds_args.append(arg)
            index += 1

    scripts = [(root / script, root / log) for script, log in SCRAPERS]
    missing = [str(script) for script, _ in scripts if not script.is_file()]
    if missing:
        print(f"Cannot start scrapers; missing: {', '.join(missing)}", file=sys.stderr)
        return 1

    db_conn = None
    try:
        db.initialize_live_database(db_path)
        db_conn = db.get_conn(db_path)
        odds_paths = _odds_csv_paths(root, odds_args)
        results_paths = (
            root / "results.csv",
            *(root / f"results_{cid}.csv" for cid in range(1, 5)),
        )
        csv_writer = CsvDatabaseWriter(
            db_conn, odds_paths, results_paths
        )
        print(
            f"[db] writing live data from {len(odds_paths)} odds and "
            f"{len(results_paths)} results CSVs to {db_path}",
            flush=True,
        )
    except (OSError, sqlite3.Error, csv.Error, UnicodeError, ValueError,
            RuntimeError) as e:
        print(f"Database initialization failed: {e}", file=sys.stderr)
        if db_conn is not None:
            db_conn.close()
        return 1

    child_env = os.environ.copy()
    child_env["PYTHONIOENCODING"] = "utf-8"
    child_env["PYTHONUNBUFFERED"] = "1"

    processes = {}
    log_files = {}
    readers = []
    terminal_lock = threading.Lock()
    try:
        for script, log_path in scripts:
            log_file = open(log_path, "a", encoding="utf-8", buffering=1)
            log_files[script.name] = log_file
            command = [sys.executable, str(script)]
            if script.name == "odds.py":
                command.extend(odds_args)
            processes[script.name] = subprocess.Popen(
                command,
                cwd=root,
                env=child_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            log_lock = threading.Lock()
            readers.extend((
                _start_reader(
                    script.name, processes[script.name].stdout,
                    log_file, log_lock, terminal_lock, stream_kind="stdout",
                ),
                _start_reader(
                    script.name, processes[script.name].stderr,
                    log_file, log_lock, terminal_lock, stream_kind="stderr",
                ),
            ))
            print(
                f"Started {script.name} (PID {processes[script.name].pid}); "
                f"output: {log_path.name}",
                flush=True,
            )
    except OSError as e:
        print(f"Could not start scraper: {e}", file=sys.stderr)
        stop_processes(processes, grace_seconds=2)
        for reader in readers:
            reader.join()
        for log_file in log_files.values():
            log_file.close()
        db_conn.close()
        return 1

    try:
        reported = set()
        while True:
            csv_writer.sync_once()
            active = False
            for script, log_path in scripts:
                process = processes[script.name]
                code = process.poll()
                if code is None:
                    active = True
                elif script.name not in reported:
                    print(
                        f"{script.name} exited with status {code}; "
                        f"see {log_path.name}",
                        flush=True,
                    )
                    reported.add(script.name)

            if not active:
                break
            time.sleep(0.25)
        csv_writer.sync_once(force=True)
    except KeyboardInterrupt:
        print("\nStopping scrapers; waiting for them to save and exit...", flush=True)
        stop_processes(processes, grace_seconds=10)
        csv_writer.sync_once(force=True)
        return 130
    finally:
        for reader in readers:
            reader.join()
        for log_file in log_files.values():
            log_file.close()
        db_conn.close()

    return 1 if any(process.returncode != 0 for process in processes.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
