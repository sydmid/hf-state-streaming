#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Starting High-Frequency Tick-to-Trade Latency & Jitter Profiler..."
python3 "${SCRIPT_DIR}/simulate_e2e.py" 100000 100000
