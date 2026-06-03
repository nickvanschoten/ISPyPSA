"""Phase 8.1 Test 3 — 8760 LP analysis.

Compares the 8760-hour full-year PDLP-1e-3 solution against the 3-week
(Test 1 v2) and 4-week (Test 2) PDLP baselines.
"""
import json
from pathlib import Path

import pandas as pd
import pypsa

BENCH = Path("mvp_pass1_power/bench")


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


def _show_run(label: str, nc_path: Path, rec_path: Path):
    if not nc_path.exists() or not rec_path.exists():
        print(f"\n[{label}] MISSING: nc={nc_path.exists()}, rec={rec_path.exists()}")
        return None, None
    rec = json.loads(rec_path.read_text())
    net = pypsa.Network(nc_path)
    cap = _disaggregated_capacity(nc_path)
    print(f"\n[{label}]")
    print(f"  Status:          {rec.get('model_status')}")
    print(f"  Objective:       ${net.objective:,.0f}")
    print(f"  Total wall (s):  {rec.get('wall_clock_s', 0):.1f}")
    print(f"  Solve stage (s): {rec.get('solve_s', 0):.1f}")
    print(f"  HiGHS time (s):  {rec.get('highs_run_time_s')}")
    print(f"  PDLP iterations: {rec.get('pdlp_iterations')}")
    print(f"  Final gap_rel:   {rec.get('pdlp_final_gap_rel')}")
    print(f"  Final pinf_rel:  {rec.get('pdlp_final_pinf_rel')}")
    print(f"  Final dinf_rel:  {rec.get('pdlp_final_dinf_rel')}")
    print(f"  LP rows/cols/nz: {rec.get('lp_rows')} / {rec.get('lp_cols')} / {rec.get('lp_nonzeros')}")
    print(f"  Peak RSS (GiB):  {rec.get('peak_rss_gib'):.2f}")
    print(f"  Snapshots:       {len(net.snapshots)}")
    print(f"  Load served (TWh): {rec.get('annual_generation_mwh_by_period', {}).get('2040', 0)/1e6:.2f}")
    return cap, rec


def main():
    print("=" * 100)
    print("Phase 8.1 Test 3 — 8760 LP (full-year hourly, no rep-weeks) — PDLP-1e-3")
    print("=" * 100)
    t3_cap, t3_rec = _show_run(
        "Test 3 PDLP-1e-3 8760 (hourly, full year)",
        BENCH / "runs_myopic/p81t3_pdlp_8760_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t3_pdlp_8760_2040.json",
    )

    print("\n--- References ---")
    t2_cap, t2_rec = _show_run(
        "Test 2 PDLP-1e-3 (4-week)",
        BENCH / "runs_myopic/p81t2_pdlp_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t2_pdlp_2040.json",
    )
    t1_cap, t1_rec = _show_run(
        "Test 1 v2 PDLP-1e-3 (3-week)",
        BENCH / "runs_myopic/p81t1_pdlp_v2_2040__cost_optimal/outputs/capacity_expansion.nc",
        BENCH / "records/p81t1_pdlp_v2_2040.json",
    )

    if not t3_cap:
        print("\nTest 3 still running or failed; partial comparison only.")
        return

    print(f"\n{'='*100}\nCapacity comparison (GW)\n{'='*100}")
    cols = [("T3 PDLP 8760", t3_cap)]
    if t2_cap: cols.append(("T2 PDLP 4wk", t2_cap))
    if t1_cap: cols.append(("T1 PDLP 3wk", t1_cap))
    allc = set()
    for _, c in cols: allc.update(c.keys())
    allc = sorted(allc, key=lambda x: -max(c.get(x, 0) for _, c in cols))

    header = f"{'Component':<25}" + "".join(f"{lab:>15}" for lab, _ in cols)
    print(header); print("-" * len(header))
    for c in allc:
        if max(col.get(c, 0) for _, col in cols) < 0.01: continue
        row = f"{c:<25}" + "".join(f"{col.get(c, 0):>15.3f}" for _, col in cols)
        print(row)

    if t2_cap and t1_cap:
        print(f"\n--- Direction of movement: 3-week → 4-week → 8760 (PDLP) ---")
        print(f"{'Component':<25} {'3-week':>10} {'4-week':>10} {'8760':>10} {'3w->4w d%':>10} {'4w->8760 d%':>12}")
        for c in allc:
            v1 = t1_cap.get(c, 0); v2 = t2_cap.get(c, 0); v3 = t3_cap.get(c, 0)
            if max(v1, v2, v3) < 0.01: continue
            d12 = (v2 - v1) / v1 * 100 if v1 > 0.01 else 0
            d23 = (v3 - v2) / v2 * 100 if v2 > 0.01 else 0
            print(f"  {c:<23} {v1:>10.3f} {v2:>10.3f} {v3:>10.3f} {d12:+9.2f}% {d23:+11.2f}%")


if __name__ == "__main__":
    main()
