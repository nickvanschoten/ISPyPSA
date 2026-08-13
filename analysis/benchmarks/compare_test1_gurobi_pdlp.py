"""Phase 8.1 Test 1 — compare Gurobi BarConvTol=1e-3 vs PDLP-1e-3 on same LP.

Both runs are on the same (stub-PHES) cost_optimal_2040 LP. The apples-to-apples
question: how do the two solvers' capacity decisions and objective values
compare at matched tolerance?

Phase 7 PDLP variance envelope (from outputs/phase7_granular/) is also pulled
for informational comparison, with the caveat that the Phase 7 LP differed
(authoritative PHES routing, not the stub).
"""
import json
from pathlib import Path

import pandas as pd
import pypsa

BENCH = Path("analysis/benchmarks")
ROOT = Path("analysis")


def _active_capacity_gw(nc_path: Path, year: int = 2040) -> dict[str, float]:
    n = pypsa.Network(nc_path)
    gens = n.generators[["bus", "carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    gens = gens[gens["bus"] != "bus_for_custom_constraint_gens"]
    storage = n.storage_units[["carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    storage["carrier"] = storage["carrier"].fillna("Battery")
    parts = pd.concat([gens.drop(columns=["bus"]), storage])
    active = parts[
        (parts["build_year"].fillna(0) <= year)
        & (parts["build_year"].fillna(0) + parts["lifetime"].fillna(0) > year)
    ]
    cap = (active.groupby("carrier")["p_nom_opt"].sum() / 1000.0).to_dict()
    return {k: v for k, v in cap.items() if k != "Unserved Energy"}


def _annual_load_twh(nc_path: Path) -> float:
    n = pypsa.Network(nc_path)
    w = n.snapshot_weightings["generators"]
    return float((n.loads_t.p_set.sum(axis=1) * w).sum() / 1e6)


def _phase7_variance(year: int = 2040, archetype: str = "cost_optimal") -> list[dict]:
    df = pd.read_csv(ROOT / "outputs/phase7_granular/capacity_gw.csv")
    sub = df[(df["archetype_id"] == archetype) & (df["year"] == year)].reset_index(drop=True)
    if sub.empty:
        return []
    # 10 carriers per sample → 3 samples = 30 rows
    samples = []
    for i in range(0, len(sub), 10):
        block = sub.iloc[i:i+10]
        samples.append(dict(zip(block["technology"], block["capacity_gw"])))
    return samples


def main():
    gnc = BENCH / "runs_myopic" / "p81t1_gurobi_2040__cost_optimal" / "outputs" / "capacity_expansion.nc"
    pnc = BENCH / "runs_myopic" / "p81t1_pdlp_2040__cost_optimal" / "outputs" / "capacity_expansion.nc"
    grec = BENCH / "records" / "p81t1_gurobi_2040.json"
    prec = BENCH / "records" / "p81t1_pdlp_2040.json"

    print("=" * 90)
    print("Phase 8.1 Test 1 — Gurobi BarConvTol=1e-3 vs PDLP-1e-3 (same stub-PHES LP)")
    print("=" * 90)

    if not gnc.exists():
        print(f"MISSING Gurobi NetCDF: {gnc}")
        return
    g_cap = _active_capacity_gw(gnc)
    g_load = _annual_load_twh(gnc)
    g_rec = json.loads(grec.read_text())
    print(f"\n--- Gurobi result ---")
    print(f"  Status:           {g_rec['model_status']}")
    print(f"  Objective:        ${g_rec['objective_value']:,.0f}")
    print(f"  Total wall (s):   {g_rec['wall_clock_s']:.1f}")
    print(f"  Solve stage (s):  {g_rec['solve_s']:.1f}")
    print(f"  Gurobi time (s):  {g_rec['gurobi_solver_time_s']:.1f}")
    print(f"  Barrier iters:    {g_rec['gurobi_barrier_iterations']}")
    print(f"  Barrier time (s): {g_rec['gurobi_barrier_time_s']:.1f}")
    print(f"  Crossover iters:  {g_rec.get('gurobi_iterations')}")
    print(f"  Peak RSS (GiB):   {g_rec['peak_rss_gib']:.2f}")
    print(f"  Annual load (TWh):{g_load:.2f}")

    if not pnc.exists():
        print(f"\nMISSING PDLP NetCDF: {pnc}  (still running? compare what we have)")
        print(f"\nGurobi capacity by carrier (GW):")
        for c, v in sorted(g_cap.items(), key=lambda x: -x[1]):
            if v > 0.01:
                print(f"  {c:25s}  {v:8.3f}")
        return

    p_cap = _active_capacity_gw(pnc)
    p_load = _annual_load_twh(pnc)
    p_rec = json.loads(prec.read_text())
    print(f"\n--- PDLP-1e-3 result ---")
    print(f"  Status:           {p_rec['model_status']}")
    print(f"  Objective:        ${p_rec['objective_value']:,.0f}" if p_rec.get('objective_value') else f"  Objective:        n/a")
    print(f"  Total wall (s):   {p_rec['wall_clock_s']:.1f}")
    print(f"  Solve stage (s):  {p_rec['solve_s']:.1f}")
    print(f"  PDLP iters:       {p_rec.get('pdlp_iterations')}")
    print(f"  Final gap_rel:    {p_rec.get('pdlp_final_gap_rel')}")
    print(f"  Final pinf_rel:   {p_rec.get('pdlp_final_pinf_rel')}")
    print(f"  Final dinf_rel:   {p_rec.get('pdlp_final_dinf_rel')}")
    print(f"  HiGHS time (s):   {p_rec.get('highs_run_time_s')}")
    print(f"  Peak RSS (GiB):   {p_rec['peak_rss_gib']:.2f}")
    print(f"  Annual load (TWh):{p_load:.2f}")

    print(f"\n--- Objective comparison ---")
    if g_rec.get('objective_value') and p_rec.get('objective_value'):
        delta = p_rec['objective_value'] - g_rec['objective_value']
        rel = delta / g_rec['objective_value'] * 100
        print(f"  Gurobi:           ${g_rec['objective_value']:,.0f}")
        print(f"  PDLP-1e-3:        ${p_rec['objective_value']:,.0f}")
        print(f"  Difference:       ${delta:+,.0f}  ({rel:+.4f}%)")
        print(f"  (Gurobi is lower-cost = closer to true optimum)" if delta > 0 else f"  (PDLP found lower cost)")

    print(f"\n--- Capacity comparison (active GW at 2040) ---")
    carriers = sorted(set(g_cap.keys()) | set(p_cap.keys()), key=lambda c: -(g_cap.get(c, 0) + p_cap.get(c, 0)))
    p7_samples = _phase7_variance()
    print(f"\n{'Carrier':<18} {'Gurobi':>10} {'PDLP-1e-3':>10} {'d GW':>10} {'d %':>8} {'P7 var min-max':>18} {'Material?':<10}")
    print("-" * 95)
    for c in carriers:
        gv = g_cap.get(c, 0.0)
        pv = p_cap.get(c, 0.0)
        if max(gv, pv) < 0.01:
            continue
        delta = pv - gv
        rel = (delta / gv * 100) if gv > 0.01 else float("inf") if pv > 0.01 else 0
        p7_vals = [s.get(c, 0.0) for s in p7_samples if c in s or any(s.get(c, 0) > 0 for s in p7_samples)]
        p7_str = f"{min(p7_vals):.2f}-{max(p7_vals):.2f}" if p7_vals else "n/a"
        material = "**>5%**" if abs(rel) > 5 and max(gv, pv) > 0.1 else ""
        rel_str = f"{rel:+7.2f}%" if abs(rel) < 1e6 else "  n/a"
        print(f"{c:<18} {gv:>10.3f} {pv:>10.3f} {delta:+10.3f} {rel_str:>8} {p7_str:>18} {material}")

    print(f"\n--- Annual consumption ---")
    print(f"  Gurobi:  {g_load:.2f} TWh    PDLP-1e-3:  {p_load:.2f} TWh    Δ: {p_load - g_load:+.2f} TWh")


if __name__ == "__main__":
    main()
