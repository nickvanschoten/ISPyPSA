#!/usr/bin/env bash
# Pass 1 power-sector dashboard launcher (Linux/macOS).
# Run from any directory; resolves to the script's location automatically.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -x ".venv/bin/python" ]; then
    echo
    echo "ERROR: Virtual environment not found at .venv/bin/python"
    echo
    echo "Install dependencies first by running from this directory:"
    echo "    uv sync"
    echo
    exit 1
fi

if command -v uv >/dev/null 2>&1; then
    exec uv run streamlit run analysis/dashboard/frontier_dashboard.py
else
    echo "Note: 'uv' not on PATH; launching via the venv directly."
    exec .venv/bin/python -m streamlit run analysis/dashboard/frontier_dashboard.py
fi
