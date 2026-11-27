**BEEF termux version
12-Hour Scraper Reliability Testing Guide
Overview**

This guide explains how to test the scraper's ability to run reliably for 12 hours on Android/Termux without actually waiting 12 hours.

The tests use time compression to simulate long-running conditions.

Estimated testing time: 45 minutes.

Testing Checklist
#	Test	Duration
1	Android wake-lock	5 minutes
2	Automatic process restart	30 seconds
3	Restart limit protection	1 minute
4	Log rotation	2 minutes
5	CSV rotation	3 minutes
6	Database performance	30 seconds
7	Network recovery	3 minutes
8	Memory and file descriptor leaks	20 minutes

Run all tests in order.

Requirements

Before starting, make sure the following are installed:

Termux
Termux:API Android application
Python
uv
tmux
Project dependencies

Install the required Termux packages:

bash
pkg update && pkg upgrade
pkg install python tmux termux-api


Verify wake-lock availability:

bash
command -v termux-wake-lock

Test 1: Android Wake-Lock

Duration: 5 minutes

Objective

Verify that Android does not suspend the Python process when the phone screen is locked.

Step 1: Stop existing processes
bash
pkill -f main.py || true

Step 2: Create the wake-lock test
bash
cat > /tmp/wake_test.py <<'EOF'
import time

t0 = time.time()

for i in range(30):
    print(
        f"{i:02d} elapsed={time.time()-t0:6.1f}s",
        flush=True
    )
    time.sleep(10)
EOF

Step 3: Start the test
bash
termux-wake-lock
python /tmp/wake_test.py | tee /tmp/wake_test.log

Step 4: Lock the phone
Lock the screen.
Leave the phone untouched for 5 minutes.
Unlock the phone after 5 minutes.
Step 5: Check results
bash
cat /tmp/wake_test.log
termux-wake-unlock


PASS criteria:

30 log entries are generated.
Each entry is approximately 10 seconds apart.
Total elapsed time is approximately 300 seconds.

FAIL criteria:

Fewer than 30 entries.
Several entries have nearly identical elapsed times.
If the test fails
Install Termux:API:
bash
pkg install termux-api

Make sure the separate Termux:API Android application is installed.
Disable battery optimization:

Settings → Apps → Termux → Battery → Unrestricted.

On Xiaomi, Huawei and Oppo devices, disable additional battery-saving and autostart restrictions.

Do not proceed until this test passes.

Test 2: Automatic Process Restart

Duration: 30 seconds

Objective

Verify that the supervisor automatically restarts odds.py when it crashes.

Step 1: Start the supervisor
bash
tmux new -s test


Inside tmux:

bash
uv run main.py --no-dashboard 2>&1 | tee /tmp/main_test.log


Detach using:

text
Ctrl+B, then D

Step 2: Kill the odds scraper
bash
pkill -f "odds.py"
sleep 3

Step 3: Inspect logs
bash
grep -E "supervisor|restart" /tmp/main_test.log | tail -5


PASS criteria:

The logs should show:

text
[supervisor] started odds (PID xxxxx)
[supervisor] restarting odds: unexpected exit with code -15
[supervisor] started odds (PID yyyyy)


The new PID must be different from the original PID.

FAIL criteria:

The process remains stopped.
The supervisor exits.
No new PID appears.
Test 3: Restart Limit Protection

Duration: 1 minute

Objective

Verify that the supervisor stops restarting a scraper after five restarts within 300 seconds.

Step 1: Kill the scraper repeatedly
bash
for i in {1..6}; do
    echo "kill #$i"
    pkill -f "odds.py" || true
    sleep 3
done

sleep 2

Step 2: Inspect logs
bash
tail -8 /tmp/main_test.log


PASS criteria:

You should see a message similar to:

text
[supervisor] CRITICAL: odds exceeded 5 restarts in 300s; not restarting


The odds.py process should no longer be running.

FAIL criteria:

The process continues restarting indefinitely.
The restart limit is ignored.
Step 3: Stop the test
bash
tmux kill-session -t test

Test 4: Log Rotation

Duration: 2 minutes

Objective

Verify that oversized logs are archived and the scraper restarts correctly.

Step 1: Pre-fill the log
bash
dd if=/dev/zero bs=1M count=51 2>/dev/null | tr '\0' 'X' >> odds.log


Verify:

bash
ls -lh odds.log


The file should be approximately 51 MB.

Step 2: Start the supervisor
bash
tmux new -s logtest


Inside tmux:

bash
uv run main.py --no-dashboard 2>&1 | tee /tmp/logtest.log


Detach:

text
Ctrl+B, then D

Step 3: Wait and inspect
bash
sleep 5
ls -lh odds.log*


PASS criteria:

An archived file named odds.log.TIMESTAMP.bak exists.
A new, smaller odds.log is created.
The scraper restarts successfully.

Check logs:

bash
grep -E "maintenance|supervisor" /tmp/logtest.log


FAIL criteria:

No backup file is created.
The scraper does not restart.
Test 5: CSV Rotation

Duration: 3 minutes

Objective

Verify that CSV records older than six hours are removed while recent records are preserved.

Step 1: Stop running processes
bash
tmux kill-session -t logtest 2>/dev/null || true

pkill -f main.py || true
pkill -f odds.py || true
pkill -f results.py || true

Step 2: Backdate old CSV records

Run:

bash
python - <<'EOF'
import csv
import datetime
import os

src = "odds_1.csv"
dst = "odds_1.csv.new"

cutoff = datetime.datetime.now() - datetime.timedelta(hours=7)

kept = dropped = 0

with open(src, newline="") as f_in, open(dst, "w", newline="") as f_out:
    r = csv.DictReader(f_in)
    w = csv.DictWriter(f_out, fieldnames=r.fieldnames)
    w.writeheader()

    for i, row in enumerate(r):
        if i % 10 == 0:
            w.writerow(row)
            kept += 1
        else:
            row["scraped_at"] = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            w.writerow(row)
            dropped += 1

os.replace(dst, src)

print(f"Kept: {kept}")
print(f"Backdated: {dropped}")
EOF

Step 3: Run CSV rotation
bash
python - <<'EOF'
import main

main.rotate_odds_csvs(max_age_hours=6)
EOF

Step 4: Verify results
bash
wc -l odds_1.csv odds_2.csv


PASS criteria:

Old records are removed from odds_1.csv.
Recent records remain.
odds_1.csv becomes significantly smaller.
odds_2.csv remains unchanged.

FAIL criteria:

No records are removed.
The CSV is emptied.
Recent records are incorrectly removed.
Test 6: Database Performance

Duration: 30 seconds

Objective

Verify that database imports process only new CSV rows instead of repeatedly importing the entire dataset.

The expected behavior is:

First run: imports new records.
Second run: detects no new records and finishes almost immediately.
Step 1: Stop other processes
bash
pkill -f main.py || true
pkill -f odds.py || true
pkill -f results.py || true

Step 2: Run the first import
bash
time uv run python db.py --keep

Step 3: Run the second import
bash
time uv run python db.py --keep


PASS criteria:

Second execution finishes in less than one second.
CSV files report zero new rows.

Example:

text
results_1.csv: 0 rows
odds_1.csv: 0 rows


FAIL criteria:

Second execution takes 10–30 seconds.
Existing records are repeatedly processed.
Step 4: Verify database offsets
bash
python - <<'EOF'
import sqlite3

conn = sqlite3.connect("odibets.db")

for row in conn.execute(
    "SELECT path, byte_offset FROM import_state ORDER BY path"
):
    print(row)
EOF


There should be one entry per imported CSV file, with offsets approximately matching their current file sizes.

Step 5: Perform a scale test

Create a CSV containing 50,000 records:

bash
python - <<'EOF'
import csv
import datetime

now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open("odds_9.csv", "w", newline="") as f:
    w = csv.writer(f)

    w.writerow([
        "scraped_at", "round_start", "round_id",
        "competition_id", "home", "away",
        "market", "selection", "odd"
    ])

    for i in range(50000):
        w.writerow([
            now, "2099-01-01 00:00:00",
            999999 + i, 1, "A", "B",
            "1X2", "Home Team wins", "2.00"
        ])
EOF


Import:

bash
time uv run python db.py --keep | grep odds_9


Run again:

bash
time uv run python db.py --keep | grep odds_9


Clean up:

bash
rm odds_9.csv


PASS criteria:

First import: approximately 3–8 seconds.
Second import: less than 0.5 seconds.
Test 7: Network Recovery

Duration: 3 minutes

Objective

Verify that the scrapers recover automatically after losing internet connectivity.

Step 1: Start the supervisor
bash
tmux new -s nettest


Inside tmux:

bash
uv run main.py --no-dashboard 2>&1 | tee /tmp/nettest.log


Detach:

text
Ctrl+B, then D

Step 2: Allow the scraper to stabilize
bash
sleep 30

Step 3: Simulate network failure

On your Android phone:

Turn airplane mode ON.
Wait two minutes.
Turn airplane mode OFF.
Step 4: Allow recovery
bash
sleep 60

Step 5: Check logs
bash
grep -cE "phase 1 failed|FAILED|request failed" /tmp/nettest.log

grep -cE "acquiring session|cycle [0-9]+ @|✅" /tmp/nettest.log


PASS criteria:

Failure messages appear during the outage.
New session-acquisition messages appear after connectivity returns.
New scraper cycles resume.
The latest log entries are recent.

FAIL criteria:

Scrapers remain stuck after internet connectivity returns.
No new cycles appear.

If the test fails, inspect the retry logic in odds.py, particularly the acquire_periods failure counter.

Test 8: Memory and File Descriptor Leak

Duration: 20 minutes

Objective

Check whether memory usage, open file descriptors or database WAL files grow continuously.

Step 1: Start the supervisor
bash
tmux new -s soakt


Inside tmux:

bash
uv run main.py --no-dashboard --db-interval 2 2>&1 | tee /tmp/soak.log


Detach:

text
Ctrl+B, then D


The shorter database interval increases the frequency of database updates to expose potential problems faster.

Step 2: Monitor system resources

Run:

bash
for i in {1..10}; do
    date +%H:%M:%S

    ps -o pid,rss,vsz,nlwp,cmd -p $(pgrep -f "main.py") 2>/dev/null

    echo "FDs: $(ls /proc/$(pgrep -f main.py)/fd 2>/dev/null | wc -l)"

    ls -lh odibets.db odibets.db-wal 2>/dev/null

    sleep 120
done

Step 3: Evaluate results

PASS criteria:

RSS memory remains within approximately ±20% of its starting value.
File descriptor count remains stable.
Database WAL stays below approximately 20 MB.
Main database grows gradually as expected.

FAIL criteria:

Memory continuously increases.
File descriptor count continuously increases.
WAL exceeds approximately 200 MB.
Step 4: Stop processes
bash
pkill -f main.py || true
pkill -f odds.py || true
pkill -f results.py || true

Full Preflight: Before the Overnight Run

Only proceed after all eight tests pass.

Step 1: Stop stale processes
bash
pkill -f main.py || true
pkill -f odds.py || true
pkill -f results.py || true
pkill -f ui.py || true

Step 2: Verify Termux:API
bash
command -v termux-wake-lock || pkg install termux-api

Step 3: Acquire wake-lock
bash
termux-wake-lock

Step 4: Check available storage
bash
df -h ~ | tail -1


Requirement: At least 1 GB of free storage.

Step 5: Start the overnight scraper

Create a persistent tmux session:

bash
tmux new -s odibets


Inside tmux:

bash
uv run main.py --no-dashboard --db-interval 30 2>&1 | tee -a main.log


Detach:

text
Ctrl+B, then D


The scraper should now continue running in the background.

Morning Verification

In the morning, reconnect to the session:

bash
tmux attach -t odibets

Step 1: Check successful scraper cycles
bash
grep -c "✅" main.log


Expected: Greater than zero.

Step 2: Check critical errors
bash
grep -cE "CRITICAL|exceeded" main.log


Expected: Zero.

Step 3: Check recent CSV data
bash
tail -3 results_1.csv


Verify that timestamps are recent.

Step 4: Check database records
bash
python -c "import sqlite3; print(sqlite3.connect('odibets.db').execute('SELECT COUNT(*), MAX(scraped_at) FROM odd_snapshot').fetchone())"


Verify:

The database contains records.
The latest scraped_at timestamp is recent.
Known Issues to Review Before Production

These are three potential issues identified during the original code review.

1. Dashboard Is Not Automatically Restarted

If ui.py crashes, the supervisor removes it from its process tracking map but does not restart it.

Impact: The scraper may continue collecting data while the dashboard remains unavailable.

Recommended fix:

Add dashboard restart functionality using the existing supervisor restart mechanism.
Alternatively, accept that the dashboard is optional and scraper data collection takes priority.
2. Wake-Lock May Remain Active After a Hard Crash

The following command in run.sh:

bash
trap 'termux-wake-unlock' EXIT


Only runs when the shell exits normally.

A SIGKILL or Android OOM kill can prevent cleanup.

Recommended fix:

Manage the wake-lock from supervise.sh.
Acquire it before starting the supervision loop.
Release it when the supervisor exits.
Avoid relying exclusively on atexit or shell EXIT traps for crash cleanup.
3. Dashboard May Not Clearly Report a Dead Scraper

The dashboard's idle-cache logic can display an idle status when new odds stop arriving.

Although stale data eventually triggers a re-render, the dashboard may not clearly identify the scraper as dead.

Recommended improvement:

Display an explicit scraper health status.
Distinguish between normal inactivity and a crashed scraper.
Show the last successful scraper cycle timestamp.
Final Acceptance Checklist

Complete this checklist before starting the real 12-hour run.

Test 1: Wake-lock survives screen-off testing.
Test 2: Supervisor restarts a killed scraper.
Test 3: Restart limit stops repeated crashes.
Test 4: Oversized logs rotate successfully.
Test 5: Old CSV records are removed correctly.
Test 6: Database delta imports are fast.
Test 7: Network recovery works.
Test 8: Memory and file descriptors remain stable.
At least 1 GB free disk space.
Termux:API installed and working.
Wake-lock acquired.
Scraper running inside tmux.
Overnight logs enabled.
Final Acceptance Criteria

All eight tests must pass before the production overnight run.

Once everything passes, start the scraper using:

bash
tmux new -s odibets
uv run main.py --no-dashboard --db-interval 30 2>&1 | tee -a main.log


Detach with Ctrl+B, then D.

Reattach in the morning using:

bash
tmux attach -t odibets


Goal: A stable 12-hour scraper session with automatic recovery, controlled restarts, reliable database imports, proper log rotation and no significant memory or file descriptor leaks.
