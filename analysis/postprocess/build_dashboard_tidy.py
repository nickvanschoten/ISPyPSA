"""Regenerate the dashboard's tidy contract CSVs from the validated production frontier.

The frontier dashboard reads two tidy artifacts by fixed name:

  - frontier_points.tidy.csv          — the 7×5 frontier rows
  - compositions_NONCONTRACT.tidy.csv — capacity-by-carrier, incl. the 2025 baseline

This is their **canonical producer**. It rebuilds both from the validated
production artifacts (`prodfinal_7x5_frontier.csv`, the per-trajectory
`compositions_NONCONTRACT_c*.csv`, and `existing_capacity_2025.csv`) so the
dashboard binds to the post-fix, fully-converged 7×5 menu and never the
superseded interim files. Re-run after any frontier re-extraction.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

_FRONTIER_DIR = Path(__file__).resolve().parents[1] / "outputs" / "frontier"
_SWEEPS = ["c0", "c40", "c80", "c150", "c250", "c350", "c550"]


def _write_frontier_tidy() -> Path:
    """The frontier tidy is the production 7×5 frontier verbatim (same schema)."""
    src = pd.read_csv(_FRONTIER_DIR / "prodfinal_7x5_frontier.csv")
    out = _FRONTIER_DIR / "frontier_points.tidy.csv"
    src.to_csv(out, index=False)
    return out


def _load_milestone_compositions() -> pd.DataFrame:
    """Concatenate the 7 per-trajectory composition diagnostics (2030–2050)."""
    frames = [
        pd.read_csv(_FRONTIER_DIR / f"compositions_NONCONTRACT_{s}.csv")
        for s in _SWEEPS
    ]
    return pd.concat(frames, ignore_index=True)


def _broadcast_2025_baseline() -> pd.DataFrame:
    """The actual 2025 existing fleet, replicated under each sweep_id so every
    trajectory shares the same existing-fleet starting column."""
    base = pd.read_csv(_FRONTIER_DIR / "existing_capacity_2025.csv")
    out = []
    for sweep_id in _SWEEPS:
        rows = base[["year", "carrier", "capacity_gw"]].copy()
        rows.insert(0, "sweep_id", sweep_id)
        out.append(rows)
    return pd.concat(out, ignore_index=True)


def _write_compositions_tidy() -> Path:
    """Milestone compositions + the broadcast 2025 baseline, in dashboard schema."""
    tidy = pd.concat(
        [_broadcast_2025_baseline(), _load_milestone_compositions()],
        ignore_index=True,
    )
    tidy = tidy[["sweep_id", "year", "carrier", "capacity_gw"]].sort_values(
        ["sweep_id", "year", "carrier"]
    )
    out = _FRONTIER_DIR / "compositions_NONCONTRACT.tidy.csv"
    tidy.to_csv(out, index=False)
    return out


def main() -> None:
    frontier = _write_frontier_tidy()
    compositions = _write_compositions_tidy()
    print(f"frontier tidy     -> {frontier}")
    print(f"compositions tidy -> {compositions}")


if __name__ == "__main__":
    main()
