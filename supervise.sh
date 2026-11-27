#!/data/data/com.termux/files/usr/bin/bash
set -uo pipefail

while true; do
    ./run.sh uv run main.py --competition all --profile mobile || true
    echo "[supervise] restarting in 30s…"
    sleep 30
done
