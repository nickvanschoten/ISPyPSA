"""Pre-extract per-corridor transmission flow for the dashboard's Transmission tab.

Mirrors `build_dispatch_store.py`'s approach: the solved `.nc` networks carry
full 30-minute per-link flow, but loading one costs ~50 s — too slow for
interactive cell-switching. This extracts, per solved ccx cell (carbon price x
year), a 30-minute flow series and a daily aggregate to parquet the dashboard
reads instantly.

Transmission in ISPyPSA is modelled as Links, not Lines. Each of the 16 NEM
flow paths (`ispypsa_inputs/flow_paths.csv`) maps to one or more links named
`{flow_path}_existing` and `{flow_path}_exp_{year}`; corridor flow is the sum
of `p0` across those links, and corridor rating is asymmetric via `p_min_pu`
(e.g. `p_nom_opt=795, p_min_pu=-1.654` -> forward 795 MW, reverse 1315 MW).

Outputs per cell, under `--out-dir/{cell_id}/`:
  flows_30min.parquet  — [snapshot, corridor, flow_mw (signed), utilisation]
  flows_daily.parquet  — [date, corridor, flow_mean_mw, flow_min_mw,
                          flow_max_mw, util_max, util_p95, congested_hours]
  corridors.parquet    — [corridor, node_from, node_to, rating_fwd_mw,
                          rating_rev_mw, expanded_mw]
Plus one manifest at `--out-dir/cells.csv`:
  [cell_id, carbon_price, year, run_dir, fidelity]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pypsa

_CONGESTED_UTILISATION_THRESHOLD = 0.98


# ---------- snapshot helpers (mirrors extract_dispatch_timeseries) ----------


def _period_snapshots(network: pypsa.Network, year: int) -> list:
    return [s for s in network.snapshots if s[0] == year]


def _flatten_snapshots(snaps: list) -> pd.DatetimeIndex:
    return pd.DatetimeIndex([s[1] for s in snaps])


# ---------- corridor-topology helpers ----------


def _read_flow_paths(run_dir: Path) -> pd.DataFrame:
    return pd.read_csv(run_dir / "ispypsa_inputs" / "flow_paths.csv")


def _corridor_link_names(link_index: pd.Index, corridor: str) -> list[str]:
    is_existing = link_index == f"{corridor}_existing"
    is_expansion = link_index.str.startswith(f"{corridor}_exp_")
    return link_index[is_existing | is_expansion].tolist()


def _corridor_ratings_row(links: pd.DataFrame, corridor: str, node_from: str, node_to: str) -> dict:
    subset = links.loc[_corridor_link_names(links.index, corridor)]
    is_expansion = subset.index.str.contains("_exp_")
    return {
        "corridor": corridor,
        "node_from": node_from,
        "node_to": node_to,
        "rating_fwd_mw": (subset["p_nom_opt"] * subset["p_max_pu"]).sum(),
        "rating_rev_mw": (subset["p_nom_opt"] * subset["p_min_pu"].abs()).sum(),
        "expanded_mw": subset.loc[is_expansion, "p_nom_opt"].sum(),
    }


def _corridor_ratings(network: pypsa.Network, flow_paths: pd.DataFrame) -> pd.DataFrame:
    rows = [
        _corridor_ratings_row(network.links, r.flow_path, r.node_from, r.node_to)
        for r in flow_paths.itertuples()
    ]
    return pd.DataFrame(rows)


# ---------- flow + utilisation helpers ----------


def _corridor_flow_mw(network: pypsa.Network, corridor: str, snaps: list, flat_idx: pd.DatetimeIndex) -> pd.Series:
    names = _corridor_link_names(network.links.index, corridor)
    flow = network.links_t.p0.loc[snaps, names].sum(axis=1)
    flow.index = flat_idx
    return flow


def _direction_aware_utilisation(flow_mw: pd.Series, rating_fwd: float, rating_rev: float) -> pd.Series:
    rating = np.where(flow_mw.to_numpy() >= 0, rating_fwd, rating_rev)
    return flow_mw.abs() / rating


def _long_flows(network: pypsa.Network, flow_paths: pd.DataFrame, ratings: pd.DataFrame, snaps: list, flat_idx: pd.DatetimeIndex) -> pd.DataFrame:
    frames = []
    for r in ratings.itertuples():
        flow_mw = _corridor_flow_mw(network, r.corridor, snaps, flat_idx)
        utilisation = _direction_aware_utilisation(flow_mw, r.rating_fwd_mw, r.rating_rev_mw)
        frames.append(pd.DataFrame({"snapshot": flat_idx, "corridor": r.corridor, "flow_mw": flow_mw.to_numpy(), "utilisation": utilisation.to_numpy()}))
    return pd.concat(frames, ignore_index=True)


def _daily_aggregate(long_flows: pd.DataFrame, period_hours: float) -> pd.DataFrame:
    tagged = long_flows.copy()
    tagged["date"] = tagged["snapshot"].dt.date
    tagged["congested"] = tagged["utilisation"] >= _CONGESTED_UTILISATION_THRESHOLD
    daily = tagged.groupby(["date", "corridor"]).agg(
        flow_mean_mw=("flow_mw", "mean"),
        flow_min_mw=("flow_mw", "min"),
        flow_max_mw=("flow_mw", "max"),
        util_max=("utilisation", "max"),
        util_p95=("utilisation", lambda s: s.quantile(0.95)),
        congested_hours=("congested", "sum"),
    )
    daily["congested_hours"] = daily["congested_hours"] * period_hours
    return daily.reset_index()


# ---------- public interface ----------


def extract_corridor_flows(run_dir: Path, year: int) -> dict[str, pd.DataFrame]:
    """Load the solved NetCDF and return per-corridor flow/utilisation tables
    for one cell: `flows_30min`, `flows_daily`, `corridors`."""
    network = pypsa.Network(str(run_dir / "outputs" / "capacity_expansion.nc"))
    flow_paths = _read_flow_paths(run_dir)
    ratings = _corridor_ratings(network, flow_paths)
    snaps = _period_snapshots(network, year)
    flat_idx = _flatten_snapshots(snaps)
    period_hours = network.snapshot_weightings.loc[snaps, "generators"].iloc[0]
    flows_30min = _long_flows(network, flow_paths, ratings, snaps, flat_idx)
    return {
        "flows_30min": flows_30min,
        "flows_daily": _daily_aggregate(flows_30min, period_hours),
        "corridors": ratings,
    }


# ---------- CLI: batch-extract the whole ccx grid ----------


def _cell_id(sweep_id: str, year: int) -> str:
    return f"{sweep_id}_{year}"


def _extract_and_write_cell(runs_dir: Path, out_dir: Path, run_id_prefix: str, sweep_id: str, carbon_price: float, year: int, archetype: str) -> dict:
    run_dir = runs_dir / f"{run_id_prefix}_{year}__{archetype}"
    tables = extract_corridor_flows(run_dir, year)
    cell_dir = out_dir / _cell_id(sweep_id, year)
    cell_dir.mkdir(parents=True, exist_ok=True)
    tables["flows_30min"].to_parquet(cell_dir / "flows_30min.parquet", index=False)
    tables["flows_daily"].to_parquet(cell_dir / "flows_daily.parquet", index=False)
    tables["corridors"].to_parquet(cell_dir / "corridors.parquet", index=False)
    return {
        "cell_id": _cell_id(sweep_id, year),
        "carbon_price": carbon_price,
        "year": year,
        "run_dir": str(run_dir),
        "fidelity": "solved",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-ids", nargs="+", default=["c0", "c40", "c80", "c150", "c250", "c350", "c550"])
    ap.add_argument("--run-id-prefix-template", default="ccx_{sweep_id}")
    ap.add_argument("--carbon-prices", nargs="+", type=float, default=[0, 40, 80, 150, 250, 350, 550])
    ap.add_argument("--years", nargs="+", type=int, default=[2030, 2035, 2040, 2045, 2050])
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument("--runs-dir", type=Path, default=Path("analysis/benchmarks/runs_myopic"))
    ap.add_argument("--out-dir", type=Path, default=Path("analysis/outputs/frontier_ccx/corridors"))
    args = ap.parse_args()

    manifest_rows = []
    for sweep_id, carbon_price in zip(args.sweep_ids, args.carbon_prices):
        run_id_prefix = args.run_id_prefix_template.format(sweep_id=sweep_id)
        for year in args.years:
            row = _extract_and_write_cell(args.runs_dir, args.out_dir, run_id_prefix, sweep_id, carbon_price, year, args.archetype)
            manifest_rows.append(row)
            print(f"{row['cell_id']}: extracted -> {args.out_dir / row['cell_id']}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(manifest_rows).to_csv(args.out_dir / "cells.csv", index=False)
    print(f"manifest -> {args.out_dir / 'cells.csv'} ({len(manifest_rows)} cells)")


if __name__ == "__main__":
    main()
