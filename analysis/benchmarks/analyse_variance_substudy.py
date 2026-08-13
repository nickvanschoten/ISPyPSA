"""Phase 8.1 variance sub-study analysis.

Loads N PDLP-1e-3 runs on the same 3-week LP and reports:
  - Per-carrier capacity range, mean, stdev across runs
  - Objective spread
  - Wall-clock spread
  - Comparison to Test 1 v2 PDLP solution
  - Comparison to Phase 7 sample 1 outlier values
"""
import json
import statistics
from pathlib import Path

import pandas as pd
import pypsa

BENCH = Path("analysis/benchmarks")
ROOT = Path("analysis")

# Phase 7 sample 1 outlier values (from phase7_granular/capacity_gw.csv rows 31-40)
PHASE7_SAMPLE_1 = {
    "Wind": 39.719,
    "Solar": 43.036,
    "Gas": 7.886,
    "Water": 6.751,
    "Biomass": 0.522,
    "Black Coal": 3.900,
    "Brown Coal": 1.160,
    "Hyblend": 0.400,
    "Liquid Fuel": 0.103,
}
PHASE7_SAMPLE_1_STORAGE_GW = 28.634


def _disaggregated_capacity(nc_path: Path, year: int = 2040) -> dict[str, float]:
    """Return GW per (carrier or Storage:carrier) for active components at year."""
    n = pypsa.Network(nc_path)
    g = n.generators[["bus", "carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    g = g[g["bus"] != "bus_for_custom_constraint_gens"]
    g = g[(g["build_year"].fillna(0) <= year)
          & (g["build_year"].fillna(0) + g["lifetime"].fillna(0) > year)]
    s = n.storage_units[["carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    s["carrier"] = s["carrier"].fillna("Battery")
    s = s[(s["build_year"].fillna(0) <= year)
          & (s["build_year"].fillna(0) + s["lifetime"].fillna(0) > year)]
    gen_cap = (g.groupby("carrier")["p_nom_opt"].sum() / 1000).to_dict()
    sto_cap = {f"Storage:{c}": v for c, v in (s.groupby("carrier")["p_nom_opt"].sum() / 1000).to_dict().items()}
    return {**gen_cap, **sto_cap}


def main():
    run_ids = [f"p81vs_pdlp_r{i}" for i in range(1, 6)]
    rows = []
    capacities_per_run = {}
    for rid in run_ids:
        rec_path = BENCH / "records" / f"{rid}_2040.json"
        nc_path = BENCH / "runs_myopic" / f"{rid}_2040__cost_optimal" / "outputs" / "capacity_expansion.nc"
        if not rec_path.exists() or not nc_path.exists():
            print(f"SKIP {rid}: record or NetCDF missing")
            continue
        rec = json.loads(rec_path.read_text())
        net = pypsa.Network(nc_path)
        cap = _disaggregated_capacity(nc_path)
        capacities_per_run[rid] = cap
        rows.append({
            "run_id": rid,
            "wall_s": rec.get("wall_clock_s"),
            "solve_s": rec.get("solve_s"),
            "highs_run_time_s": rec.get("highs_run_time_s"),
            "pdlp_iterations": rec.get("pdlp_iterations"),
            "gap_rel": rec.get("pdlp_final_gap_rel"),
            "pinf_rel": rec.get("pdlp_final_pinf_rel"),
            "dinf_rel": rec.get("pdlp_final_dinf_rel"),
            "objective_pypsa": net.objective,
        })

    if not rows:
        print("No runs completed yet.")
        return

    print("=" * 100)
    print("Phase 8.1 PDLP variance sub-study — 5 runs of PDLP-1e-3 on identical 3-week LP")
    print("=" * 100)
    print(f"\nCompleted runs: {len(rows)} / 5")

    print(f"\n{'Run':<18} {'Wall (s)':>10} {'Solve (s)':>10} {'HiGHS (s)':>10} "
          f"{'PDLP iters':>12} {'gap_rel':>10} {'Objective ($)':>18}")
    print("-" * 100)
    for r in rows:
        print(f"{r['run_id']:<18} {r['wall_s']:>10.1f} {r['solve_s']:>10.1f} "
              f"{r['highs_run_time_s'] or 0:>10.1f} {r['pdlp_iterations'] or 0:>12d} "
              f"{r['gap_rel'] or 0:>10.2e} {r['objective_pypsa']:>18,.0f}")

    # Variance stats per carrier
    all_carriers = set()
    for cap in capacities_per_run.values():
        all_carriers.update(cap.keys())
    all_carriers = sorted(all_carriers, key=lambda c: -max(cap.get(c, 0) for cap in capacities_per_run.values()))

    print(f"\n{'Component':<25} {'Min':>10} {'Max':>10} {'Range':>10} {'Mean':>10} {'Stdev':>10} {'Stdev %':>8} {'P7 sample 1':>12}")
    print("-" * 100)
    for c in all_carriers:
        vals = [cap.get(c, 0.0) for cap in capacities_per_run.values()]
        if max(vals) < 0.01:
            continue
        vmin = min(vals); vmax = max(vals); vmean = statistics.mean(vals)
        vstd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        vrng = vmax - vmin
        rel = (vstd / vmean * 100) if vmean > 0.01 else 0
        p7 = PHASE7_SAMPLE_1.get(c, PHASE7_SAMPLE_1_STORAGE_GW if c.startswith("Storage") else None)
        p7_str = f"{p7:.2f}" if p7 is not None else "—"
        print(f"{c:<25} {vmin:>10.4f} {vmax:>10.4f} {vrng:>10.4f} {vmean:>10.4f} {vstd:>10.4f} {rel:>7.3f}% {p7_str:>12}")

    # Objective spread
    objs = [r["objective_pypsa"] for r in rows]
    if len(objs) > 1:
        obj_min = min(objs); obj_max = max(objs); obj_mean = statistics.mean(objs)
        print(f"\nObjective spread: ${obj_min:,.0f} – ${obj_max:,.0f}")
        print(f"  Range: ${obj_max - obj_min:,.0f} ({(obj_max - obj_min)/obj_mean*100:.4f}% of mean)")

    # Compare against Phase 7 sample 1 outlier
    if len(rows) >= 1:
        print(f"\nPhase 7 sample 1 outlier check:")
        print(f"  Solar: P7={PHASE7_SAMPLE_1['Solar']:.2f} GW; runs min-max = "
              f"{min(c.get('Solar', 0) for c in capacities_per_run.values()):.2f}-"
              f"{max(c.get('Solar', 0) for c in capacities_per_run.values()):.2f} GW "
              f"({'NO run reproduces outlier' if max(c.get('Solar', 0) for c in capacities_per_run.values()) < 30 else 'AT LEAST ONE run near outlier'})")
        print(f"  Wind:  P7={PHASE7_SAMPLE_1['Wind']:.2f} GW; runs min-max = "
              f"{min(c.get('Wind', 0) for c in capacities_per_run.values()):.2f}-"
              f"{max(c.get('Wind', 0) for c in capacities_per_run.values()):.2f} GW")
        all_storage = []
        for cap in capacities_per_run.values():
            sto = sum(v for k, v in cap.items() if k.startswith("Storage"))
            all_storage.append(sto)
        print(f"  Total storage: P7={PHASE7_SAMPLE_1_STORAGE_GW:.2f} GW; "
              f"runs min-max = {min(all_storage):.2f}-{max(all_storage):.2f} GW")


if __name__ == "__main__":
    main()
