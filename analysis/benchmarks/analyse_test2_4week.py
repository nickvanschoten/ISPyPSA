"""Phase 8.1 Test 2 — compare 4-week LP solutions (PDLP + Gurobi) vs Test 1 v2 (3-week)."""
import json
from pathlib import Path

import pandas as pd
import pypsa

BENCH = Path("analysis/benchmarks")


def _disaggregated_capacity(nc_path: Path, year: int = 2040) -> dict[str, float]:
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


def _show_run(label: str, nc_path: Path, rec_path: Path, is_pdlp: bool = False):
    if not nc_path.exists() or not rec_path.exists():
        print(f"\n[{label}] MISSING: {nc_path.exists()=}, {rec_path.exists()=}")
        return None, None
    rec = json.loads(rec_path.read_text())
    net = pypsa.Network(nc_path)
    cap = _disaggregated_capacity(nc_path)
    print(f"\n[{label}]")
    print(f"  Status:          {rec.get('model_status')}")
    print(f"  Objective:       ${net.objective:,.0f}")
    print(f"  Total wall (s):  {rec.get('wall_clock_s', 0):.1f}")
    print(f"  Solve stage (s): {rec.get('solve_s', 0):.1f}")
    if is_pdlp:
        print(f"  HiGHS time (s):  {rec.get('highs_run_time_s')}")
        print(f"  PDLP iterations: {rec.get('pdlp_iterations')}")
        print(f"  Final gap_rel:   {rec.get('pdlp_final_gap_rel')}")
        print(f"  Final pinf_rel:  {rec.get('pdlp_final_pinf_rel')}")
        print(f"  Final dinf_rel:  {rec.get('pdlp_final_dinf_rel')}")
    else:
        print(f"  Gurobi time (s): {rec.get('gurobi_solver_time_s')}")
        print(f"  Barrier iters:   {rec.get('gurobi_barrier_iterations')}")
        print(f"  Barrier time s:  {rec.get('gurobi_barrier_time_s')}")
        print(f"  Crossover iters: {rec.get('gurobi_iterations')}")
    print(f"  LP rows/cols/nz: {rec.get('lp_rows')} / {rec.get('lp_cols')} / {rec.get('lp_nonzeros')}")
    print(f"  Peak RSS (GiB):  {rec.get('peak_rss_gib'):.2f}")
    print(f"  Load served (TWh): {rec.get('annual_generation_mwh_by_period', {}).get('2040', 0)/1e6:.2f}")
    return cap, rec


def main():
    # Test 2 (4-week)
    print("=" * 100)
    print("Phase 8.1 Test 2 — 4-week LP (rep_weeks=[42,33] + named peak winter + named peak summer)")
    print("=" * 100)
    t2_pdlp_cap, t2_pdlp_rec = _show_run(
        "Test 2 PDLP-1e-3 (4-week)",
        BENCH / "runs_myopic/p81t2_pdlp_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t2_pdlp_2040.json",
        is_pdlp=True,
    )
    t2_gurobi_cap, t2_gurobi_rec = _show_run(
        "Test 2 Gurobi (4-week)",
        BENCH / "runs_myopic/p81t2_gurobi_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t2_gurobi_2040.json",
        is_pdlp=False,
    )
    # Test 1 v2 reference (3-week)
    print("\n--- Reference: Test 1 v2 (3-week, same authoritative-PHES LP minus week 33) ---")
    t1_pdlp_cap, t1_pdlp_rec = _show_run(
        "Test 1 v2 PDLP-1e-3 (3-week)",
        BENCH / "runs_myopic/p81t1_pdlp_v2_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t1_pdlp_v2_2040.json",
        is_pdlp=True,
    )
    t1_gurobi_cap, t1_gurobi_rec = _show_run(
        "Test 1 v2 Gurobi (3-week)",
        BENCH / "runs_myopic/p81t1_gurobi_v2_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t1_gurobi_v2_2040.json",
        is_pdlp=False,
    )

    # Side-by-side
    if not (t2_pdlp_cap or t2_gurobi_cap):
        return
    print(f"\n{'='*100}\nCapacity comparison (GW)\n{'='*100}")
    cols = []
    if t2_pdlp_cap is not None: cols.append(("T2 PDLP 4wk", t2_pdlp_cap))
    if t2_gurobi_cap is not None: cols.append(("T2 Gurobi 4wk", t2_gurobi_cap))
    if t1_pdlp_cap is not None: cols.append(("T1 PDLP 3wk", t1_pdlp_cap))
    if t1_gurobi_cap is not None: cols.append(("T1 Gurobi 3wk", t1_gurobi_cap))
    all_carriers = set()
    for _, c in cols:
        all_carriers.update(c.keys())
    all_carriers = sorted(all_carriers, key=lambda x: -max(c.get(x, 0) for _, c in cols))

    header = f"{'Component':<25}" + "".join(f"{lab:>15}" for lab, _ in cols)
    print(header)
    print("-" * len(header))
    for c in all_carriers:
        if max(col.get(c, 0) for _, col in cols) < 0.01:
            continue
        row = f"{c:<25}" + "".join(f"{col.get(c, 0):>15.3f}" for _, col in cols)
        print(row)

    # Test 2 PDLP vs Test 1 v2 PDLP (direction of movement: 3-week → 4-week)
    if t2_pdlp_cap and t1_pdlp_cap:
        print(f"\n--- Direction of movement: 3-week → 4-week under PDLP ---")
        print(f"{'Component':<25} {'3-week':>10} {'4-week':>10} {'d GW':>10} {'d %':>8}")
        for c in all_carriers:
            v3 = t1_pdlp_cap.get(c, 0); v4 = t2_pdlp_cap.get(c, 0)
            if max(v3, v4) < 0.01:
                continue
            delta = v4 - v3
            rel = (delta/v3*100) if v3 > 0.01 else 0
            mat = " **>5%**" if abs(rel) > 5 and max(v3, v4) > 0.1 else ""
            print(f"  {c:<23} {v3:>10.3f} {v4:>10.3f} {delta:+10.3f} {rel:+7.2f}%{mat}")


if __name__ == "__main__":
    main()
