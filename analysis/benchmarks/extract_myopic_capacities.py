"""Post-hoc extraction of per-period capacity build from a myopic sequence.

Fixes the path-mismatch in run_myopic.py's inline _extract_built_capacities:
actual NetCDF lives at runs_myopic/<sub_run_id>_<year>__cost_optimal/outputs/
where <sub_run_id> already contains the year (e.g. nsw_6p_myopic_2025).

Usage:
    uv run python analysis/benchmarks/extract_myopic_capacities.py nsw_6p_myopic
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import pypsa

BENCH = Path(__file__).parent
RUNS_MYOPIC = BENCH / "runs_myopic"
RECORDS = BENCH / "records"


def _coalesce_fuel(fuel: str) -> str:
    if fuel in ("Wind", "Solar"): return "wind_solar"
    if fuel in ("Black Coal", "Brown Coal"): return "coal"
    if fuel in ("Gas", "Hyblend"): return "gas"
    if fuel in ("Hydrogen",): return "hydrogen"
    if fuel in ("Biomass",): return "biomass"
    if fuel in ("Water",): return "hydro"
    if fuel == "Unserved Energy": return "_drop"
    if fuel == "Battery" or fuel is None: return "storage"
    return "other"


def _per_period_active_capacity_gw(run_id: str, year: int) -> dict:
    nc = RUNS_MYOPIC / f"{run_id}_{year}_{year}__cost_optimal" / "outputs" / "capacity_expansion.nc"
    if not nc.exists():
        return {}
    n = pypsa.Network(nc)
    gens = n.generators[["bus", "carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    gens = gens[gens["bus"] != "bus_for_custom_constraint_gens"]
    storage = n.storage_units[["carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    storage["carrier"] = storage["carrier"].fillna("Storage")
    components = pd.concat([gens.drop(columns=["bus"]), storage])
    components["fuel_group"] = components["carrier"].apply(_coalesce_fuel)
    components = components[components["fuel_group"] != "_drop"]
    active = components[
        (components["build_year"].fillna(0) <= year)
        & (components["build_year"].fillna(0) + components["lifetime"].fillna(0) > year)
    ]
    return (active.groupby("fuel_group")["p_nom_opt"].sum() / 1000.0).to_dict()


def _annual_generation_twh(run_id: str, year: int) -> float | None:
    nc = RUNS_MYOPIC / f"{run_id}_{year}_{year}__cost_optimal" / "outputs" / "capacity_expansion.nc"
    if not nc.exists():
        return None
    n = pypsa.Network(nc)
    weightings = n.snapshot_weightings["generators"]
    load_t = n.loads_t.p_set.sum(axis=1)
    return float((load_t * weightings).sum() / 1e6)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id", help="Sequence run id, e.g. nsw_6p_myopic")
    args = ap.parse_args()

    seq = json.loads((RECORDS / f"{args.run_id}.json").read_text())
    per_period = seq.get("per_period", {})
    print(f"\n{args.run_id} per-period active capacity (GW) and annual TWh:")
    print(f"{'year':>6} | {'wall (s)':>8} | {'gen (TWh)':>10} | {'wind+sol':>9} {'gas':>7} {'coal':>7} {'storage':>8} {'hydro':>7} {'biomass':>8} {'hydrogen':>9}")
    print("-" * 110)
    for y in sorted(int(k) for k in per_period.keys()):
        per_period[str(y)]["active_capacity_gw"] = _per_period_active_capacity_gw(args.run_id, y)
        per_period[str(y)]["annual_consumption_twh"] = _annual_generation_twh(args.run_id, y)
        cap = per_period[str(y)]["active_capacity_gw"]
        gen = per_period[str(y)]["annual_consumption_twh"]
        wall = per_period[str(y)].get("per_period_wall_s", 0)
        print(f"{y:>6} | {wall:>8.0f} | {gen or 0:>10.1f} | "
              f"{cap.get('wind_solar',0):>9.1f} {cap.get('gas',0):>7.1f} "
              f"{cap.get('coal',0):>7.1f} {cap.get('storage',0):>8.1f} "
              f"{cap.get('hydro',0):>7.1f} {cap.get('biomass',0):>8.1f} "
              f"{cap.get('hydrogen',0):>9.1f}")
    seq["per_period"] = per_period
    (RECORDS / f"{args.run_id}.json").write_text(json.dumps(seq, indent=2, default=str))
    print(f"\nUpdated {RECORDS / f'{args.run_id}.json'}")


if __name__ == "__main__":
    main()
