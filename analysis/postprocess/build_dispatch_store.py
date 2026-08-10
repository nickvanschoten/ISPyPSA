"""Pre-extract 30-minute dispatch-by-carrier (+ demand) for the dashboard's
granular Generation view.

The solved `.nc` networks carry full 30-minute dispatch, but each costs ~50 s to
load — far too slow for interactive cell-switching. This pre-extracts one
trajectory's five milestone years to a compact long-format parquet the dashboard
reads instantly:

  columns: [sweep_id, carbon_price, year, snapshot, carrier, mw]

Demand is stored as `carrier == "Demand"` so the dashboard can overlay it as a
line on the stacked-generation area. Run once per trajectory (parallel-safe);
re-run after any re-solve.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from analysis.postprocess.extract_dispatch_timeseries import (
    extract_dispatch_timeseries,
    find_run_dir,
)


def _long_dispatch(dispatch: dict, year: int) -> pd.DataFrame:
    """One year's dispatch-by-carrier + storage + demand + emissions, melted to
    long [year, snapshot, carrier, mw]. Generation carriers and `Storage`
    (battery discharge) are positive bands; `Storage charge` is negative (below
    the axis, OpenElectricity-style); `Demand` is a line; `CO2 t/h` is the
    per-snapshot emissions rate (a tCO2e/h value carried in the `mw` column for
    the linked emissions panel — the dashboard handles these named series
    specially, never stacking them with generation)."""
    wide = dispatch["dispatch_by_carrier"].copy()
    storage_discharge = dispatch["storage_dispatch"]
    storage_charge = dispatch["storage_charge"]
    if not storage_discharge.empty:
        wide["Storage"] = storage_discharge.sum(axis=1)
    if not storage_charge.empty:
        wide["Storage charge"] = -storage_charge.sum(axis=1)
    wide["Demand"] = dispatch["demand"]
    wide["CO2 t/h"] = dispatch["emissions_rate_t_per_h"]
    wide.index.name = "snapshot"
    long = wide.reset_index().melt(
        id_vars="snapshot", var_name="carrier", value_name="mw"
    )
    long.insert(0, "year", year)
    return long


def _extract_trajectory(
    runs_dir: Path, run_id: str, archetype: str, years: list[int]
) -> pd.DataFrame:
    """Concatenate the long dispatch frames across a trajectory's milestone years."""
    frames = []
    for year in years:
        run_dir = find_run_dir(runs_dir, run_id, archetype, year)
        dispatch = extract_dispatch_timeseries(run_dir)
        frames.append(_long_dispatch(dispatch, year))
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--sweep-id", required=True)
    ap.add_argument("--carbon-price", type=float, required=True)
    ap.add_argument(
        "--years", type=int, nargs="+", default=[2030, 2035, 2040, 2045, 2050]
    )
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument(
        "--runs-dir", type=Path, default=Path("analysis/benchmarks/runs_myopic")
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("analysis/outputs/frontier/dispatch"),
    )
    args = ap.parse_args()

    out = _extract_trajectory(args.runs_dir, args.run_id, args.archetype, args.years)
    out.insert(0, "carbon_price", args.carbon_price)
    out.insert(0, "sweep_id", args.sweep_id)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    path = args.out_dir / f"dispatch_30min_{args.sweep_id}.parquet"
    out.to_parquet(path, index=False)
    print(
        f"{args.sweep_id}: {len(out)} rows ({out['carrier'].nunique()} series) -> {path}"
    )


if __name__ == "__main__":
    main()
