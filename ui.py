#!/usr/bin/env python3
"""
dashboard.py — live terminal dashboard for odibets.db

Reads the database (read-only) while db.py or the live scraper process writes to it.
Shows:
    * overall stats + odds-per-minute rate
    * scraper activity (from DB writes, not log files)
    * per-league progress table
    * latest 8 odds and latest 8 results

Usage:
    uv run dashboard.py
    uv run dashboard.py --db other.db --interval 1.5
"""

import argparse
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE / "odibets.db"

COMPETITIONS = {1: "English", 2: "Spanish", 3: "Kenyan", 4: "Italian"}
ACTIVE_THRESHOLD_SEC = 60


# ------------------------------------------------------------------ helpers
def human_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _age_seconds(s):
    dt = _parse_ts(s)
    if dt is None:
        return None
    return (datetime.now() - dt).total_seconds()


def _fmt_age(sec):
    if sec is None:
        return "—"
    if sec < 60:
        return f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec/60:.0f}m"
    return f"{sec/3600:.1f}h"


# ------------------------------------------------------------------ queries
def fetch_stats(conn):
    stats = {}

    stats["matches"]    = conn.execute("SELECT COUNT(*) FROM match").fetchone()[0]
    stats["odds"]       = conn.execute("SELECT COUNT(*) FROM odd_snapshot").fetchone()[0]
    stats["rounds"]     = conn.execute("SELECT COUNT(*) FROM round").fetchone()[0]
    stats["with_odds"]  = conn.execute(
        "SELECT COUNT(DISTINCT match_id) FROM odd_snapshot"
    ).fetchone()[0]
    stats["with_result"] = conn.execute(
        "SELECT COUNT(*) FROM match_result "
        "WHERE result IS NOT NULL AND result != ''"
    ).fetchone()[0]
    stats["with_both"] = conn.execute("""
        SELECT COUNT(*) FROM match m
        JOIN match_result r ON r.match_id = m.match_id
        WHERE EXISTS (SELECT 1 FROM odd_snapshot o WHERE o.match_id = m.match_id)
    """).fetchone()[0]

    stats["latest_odd"] = conn.execute(
        "SELECT MAX(scraped_at) FROM odd_snapshot"
    ).fetchone()[0]

    # odds in the last 60s (relative to the most recent scrape)
    stats["odds_last_min"] = 0
    last = _parse_ts(stats["latest_odd"])
    if last:
        cutoff = (last - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
        stats["odds_last_min"] = conn.execute(
            "SELECT COUNT(*) FROM odd_snapshot WHERE scraped_at >= ?",
            (cutoff,),
        ).fetchone()[0]

    # latest result timestamp (via round end)
    stats["latest_result"] = conn.execute("""
        SELECT MAX(rd.round_start) FROM match_result r
        JOIN match m ON m.match_id = r.match_id
        JOIN round rd ON rd.round_key = m.round_key
        WHERE r.result IS NOT NULL AND r.result != ''
    """).fetchone()[0]

    # per league
    leagues = []
    for row in conn.execute("""
        SELECT c.competition_id, c.name,
               COUNT(DISTINCT m.match_id),
               COUNT(DISTINCT CASE WHEN o.odd_id IS NOT NULL THEN m.match_id END),
               COUNT(DISTINCT CASE
                   WHEN r.result IS NOT NULL AND r.result != ''
                   THEN m.match_id END),
               COUNT(DISTINCT rd.round_key),
               COUNT(DISTINCT CASE
                   WHEN o.odd_id IS NOT NULL
                   AND r.result IS NOT NULL AND r.result != ''
                   THEN m.match_id END),
               MAX(rd.round_start)
        FROM competition c
        LEFT JOIN round rd       ON rd.competition_id = c.competition_id
        LEFT JOIN match m        ON m.round_key = rd.round_key
        LEFT JOIN odd_snapshot o ON o.match_id = m.match_id
        LEFT JOIN match_result r ON r.match_id = m.match_id
        GROUP BY c.competition_id, c.name
        ORDER BY c.competition_id
    """):
        leagues.append({
            "id": row[0], "name": row[1],
            "matches": row[2], "with_odds": row[3],
            "with_result": row[4],             "rounds": row[5],
            "complete": row[6],
            "last_round": row[7],
        })
    stats["leagues"] = leagues

    # latest odds (most recent 8 rows)
    stats["recent_odds"] = conn.execute("""
        SELECT o.scraped_at, c.name, ht.name, at.name,
               mk.name, o.selection, o.odd
        FROM odd_snapshot o
        JOIN match m  ON m.match_id = o.match_id
        JOIN round rd ON rd.round_key = m.round_key
        JOIN competition c ON c.competition_id = rd.competition_id
        JOIN team ht ON ht.team_id = m.home_team_id
        JOIN team at ON at.team_id = m.away_team_id
        JOIN market mk ON mk.market_id = o.market_id
        ORDER BY o.odd_id DESC
        LIMIT 8
    """).fetchall()

    # latest results (most recent 8)
    stats["recent_results"] = conn.execute("""
        SELECT rd.round_start, c.name, ht.name, at.name, r.result
        FROM match_result r
        JOIN match m  ON m.match_id = r.match_id
        JOIN round rd ON rd.round_key = m.round_key
        JOIN competition c ON c.competition_id = rd.competition_id
        JOIN team ht ON ht.team_id = m.home_team_id
        JOIN team at ON at.team_id = m.away_team_id
        WHERE r.result IS NOT NULL AND r.result != ''
        ORDER BY rd.round_start DESC, m.match_id DESC
        LIMIT 8
    """).fetchall()

    return stats


# ------------------------------------------------------------------ panels
def make_header(stats, db_path):
    size = human_size(db_path.stat().st_size) if db_path.exists() else "—"
    now = datetime.now().strftime("%H:%M:%S")
    rate = stats["odds_last_min"]
    latest = stats["latest_odd"] or "—"

    t = Text()
    t.append("● ", style="bold green")
    t.append("Odibets Live", style="bold white")
    t.append("  │  ", style="dim")
    t.append(now, style="bold cyan")
    t.append("  │  DB ", style="dim")
    t.append(size, style="white")
    t.append("  │  ", style="dim")
    t.append(f"{stats['odds']:,}", style="bold green")
    t.append(" odds ", style="dim")
    t.append(f"(+{rate}/min)", style="bold green" if rate else "dim")
    t.append("  │  ", style="dim")
    t.append(f"{stats['matches']:,}", style="bold white")
    t.append(" matches ", style="dim")
    t.append(f"{stats['with_both']:,}", style="bold yellow")
    t.append(" complete", style="dim")
    t.append("  │  latest ", style="dim")
    t.append(latest[11:19] if latest and latest != "—" else "—", style="dim")
    return Panel(t, style="green", padding=(0, 1))


def make_scrapers(stats):
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()

    odds_age   = _age_seconds(stats["latest_odd"])
    result_age = _age_seconds(stats["latest_result"])

    def marker(age):
        if age is None:
            return Text("○ no data", style="dim red")
        if age < ACTIVE_THRESHOLD_SEC:
            return Text(f"● active ({_fmt_age(age)} ago)", style="green")
        return Text(f"◐ idle ({_fmt_age(age)} ago)", style="yellow")

    grid.add_row("odds.py    ", marker(odds_age))
    grid.add_row("results.py ", marker(result_age))
    return Panel(grid, title="[bold]Scrapers[/]",
                 title_align="left", border_style="cyan")


def make_leagues(stats):
    t = Table(expand=True, show_header=True, header_style="bold cyan")
    t.add_column("League", style="bold white")
    t.add_column("Matches", justify="right")
    t.add_column("Odds",    justify="right", style="green")
    t.add_column("Results", justify="right", style="yellow")
    t.add_column("Complete", justify="right", style="magenta")
    t.add_column("Rounds",  justify="right")
    t.add_column("Last Round", style="dim")

    tot = dict(matches=0, odds=0, results=0, complete=0, rounds=0)
    for lg in stats["leagues"]:
        complete = lg["complete"]
        last = (lg["last_round"] or "")[11:19] if lg["last_round"] else "—"
        t.add_row(
            f"{lg['id']}: {lg['name']}",
            str(lg["matches"]),
            str(lg["with_odds"]),
            str(lg["with_result"]),
            str(complete),
            str(lg["rounds"]),
            last,
        )
        tot["matches"]  += lg["matches"]
        tot["odds"]     += lg["with_odds"]
        tot["results"]  += lg["with_result"]
        tot["complete"] += complete
        tot["rounds"]   += lg["rounds"]

    t.add_section()
    t.add_row(
        Text("TOTAL", style="bold"),
        Text(str(tot["matches"]),  style="bold"),
        Text(str(tot["odds"]),     style="bold green"),
        Text(str(tot["results"]),  style="bold yellow"),
        Text(str(tot["complete"]), style="bold magenta"),
        Text(str(tot["rounds"]),   style="bold"),
        "",
    )
    return Panel(t, title="[bold]Per-League Progress[/]",
                 title_align="left", border_style="cyan")


def make_recent_odds(stats):
    t = Table(expand=True, show_header=True, header_style="bold magenta")
    t.add_column("Time", style="dim", width=8)
    t.add_column("Lg", style="cyan", width=4)
    t.add_column("Match", style="white")
    t.add_column("Market", style="dim")
    t.add_column("Sel", style="dim")
    t.add_column("Odd", justify="right", style="bold green", width=6)

    if not stats["recent_odds"]:
        t.add_row("", "", Text("(no odds yet)", style="dim"), "", "", "")
    else:
        for ts, cname, home, away, market, sel, odd in stats["recent_odds"]:
            t.add_row(
                (ts or "")[11:19], cname[:3],
                f"{home} v {away}", market, sel,
                f"{odd:.2f}",
            )
    return Panel(t, title="[bold]Latest Odds[/]",
                 title_align="left", border_style="magenta")


def make_recent_results(stats):
    t = Table(expand=True, show_header=True, header_style="bold blue")
    t.add_column("Round", style="dim", width=6)
    t.add_column("Lg", style="cyan", width=4)
    t.add_column("Match", style="white")
    t.add_column("Result", justify="center", style="bold yellow", width=8)

    if not stats["recent_results"]:
        t.add_row("", "", Text("(no results yet)", style="dim"), "")
    else:
        for round_start, cname, home, away, result in stats["recent_results"]:
            t.add_row(
                (round_start or "")[11:16], cname[:3],
                f"{home} v {away}", result,
            )
    return Panel(t, title="[bold]Latest Results[/]",
                 title_align="left", border_style="blue")


# ------------------------------------------------------------------ layout
def build_layout(stats, db_path):
    layout = Layout()
    layout.split_column(
        Layout(name="header",   size=3),
        Layout(name="scrapers", size=5),
        Layout(name="leagues",  size=11),
        Layout(name="bottom"),
    )
    layout["bottom"].split_row(
        Layout(name="odds"),
        Layout(name="results"),
    )
    layout["header"].update(make_header(stats, db_path))
    layout["scrapers"].update(make_scrapers(stats))
    layout["leagues"].update(make_leagues(stats))
    layout["odds"].update(make_recent_odds(stats))
    layout["results"].update(make_recent_results(stats))
    return layout


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--interval", type=float, default=5.0)
    args = ap.parse_args()

    console = Console()
    if not args.db.exists():
        console.print(f"[red]Database not found: {args.db}[/]")
        console.print("Run [bold]uv run db.py[/] first.")
        return 1

    if console.height < 20 or console.width < 60:
        console.print(f"[red]Terminal too small: {console.width}x{console.height}[/]")
        console.print("Need at least [bold]60x20[/].")
        return 1

    conn = sqlite3.connect(str(args.db))
    conn.execute("PRAGMA query_only = ON")  # read-only connection

    previous_layout = None
    previous_latest = None
    previous_rendered = 0.0

    def render():
        nonlocal previous_layout, previous_latest, previous_rendered
        try:
            stats = fetch_stats(conn)
            latest = stats["latest_odd"]
            now = time.monotonic()
            stale = latest is None or _age_seconds(latest) is None or \
                _age_seconds(latest) > 180
            if (previous_layout is not None
                    and latest == previous_latest
                    and now - previous_rendered < 60
                    and not stale):
                return previous_layout
            previous_layout = build_layout(stats, args.db)
            previous_latest = latest
            previous_rendered = now
            return previous_layout
        except sqlite3.Error as e:
            return Panel(f"[red]DB error: {e}[/]", border_style="red")

    try:
        with Live(render(), console=console,
                  screen=True, auto_refresh=False) as live:
            while True:
                time.sleep(args.interval)
                live.update(render(), refresh=True)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())