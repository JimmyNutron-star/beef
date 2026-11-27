#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

termux-wake-lock
trap 'termux-wake-unlock' EXIT

exec nice -n 19 "$@"
