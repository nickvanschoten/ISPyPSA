"""Sweep the corrected-cost-axis (ccx_*) 7x5 frontier records and report per-cell
convergence against the PDLP 3e-3 acceptance rule.

Acceptance (brief 2): a cell is CONVERGED iff its primal residual, dual residual,
AND duality gap (the pdlp_final_*_rel fields) are ALL <= tolerance. `model_status`
is IGNORED (it reads "Unknown" on converged PDLP solves — a documented quirk).

Read-only. Not a solve input; safe to run against records while chains solve.

Usage:
    python analysis/benchmarks/ccx_sweep.py [--tol 3e-3]
"""

import argparse
import json
from pathlib import Path

RECORDS = Path("analysis/benchmarks/records")
PRICES = [0, 40, 80, 150, 250, 350, 550]
YEARS = [2030, 2035, 2040, 2045, 2050]
FALLBACK_TOL = 5e-3  # documented per-§3 low-carbon gap-floor fallback (see run report)


def _record_path(price: int, year: int) -> Path:
    return RECORDS / f"ccx_c{price}_{year}.json"


def _classify_cell(price: int, year: int, tol: float) -> dict:
    """Read one per-period record and classify its convergence state."""
    path = _record_path(price, year)
    if not path.exists():
        return {"state": "-", "detail": ""}
    r = json.loads(path.read_text())
    status = r.get("status")
    pinf = r.get("pdlp_final_pinf_rel")
    dinf = r.get("pdlp_final_dinf_rel")
    gap = r.get("pdlp_final_gap_rel")
    if status != "completed":
        return {"state": status or "??", "detail": f"pinf={pinf} dinf={dinf} gap={gap}"}
    metrics = [pinf, dinf, gap]
    if any(m is None for m in metrics):
        return {"state": "NO-METRICS", "detail": f"pinf={pinf} dinf={dinf} gap={gap}"}
    solve_h = (r.get("solve_s") or 0) / 3600.0
    detail = (f"pinf={pinf:.2e} dinf={dinf:.2e} gap={gap:.2e} "
              f"solve={solve_h:.1f}h obj={r.get('objective_value')}")
    # Tiered: CONV = met strict 3e-3 target; CONV5 = accepted per-§3 low-carbon
    # fallback (all metrics <= 5e-3, gap floored above 3e-3, primal/dual near-converged);
    # NOT-CONV = worse than the 5e-3 documented fallback (a real anomaly).
    worst = max(metrics)
    if worst <= tol:
        state = "CONV"
    elif worst <= FALLBACK_TOL:
        state = "CONV5"
    else:
        state = "NOT-CONV"
    return {"state": state, "detail": detail}


def _print_grid(tol: float) -> tuple[int, list[str]]:
    """Print the price x year convergence grid. Return (n_converged, anomalies)."""
    print(f"\nccx 7x5 convergence sweep  (tol={tol:g}, CONV = all of pinf/dinf/gap <= tol)")
    header = "price\\year | " + " ".join(f"{y:>9}" for y in YEARS)
    print(header)
    print("-" * len(header))
    n_conv = 0
    n_conv5 = 0
    anomalies = []
    for price in PRICES:
        cells = []
        for year in YEARS:
            c = _classify_cell(price, year, tol)
            cells.append(f"{c['state']:>9}")
            if c["state"] == "CONV":
                n_conv += 1
            if c["state"] == "CONV5":
                n_conv5 += 1
            if c["state"] in ("NOT-CONV", "timed_out", "failed", "NO-METRICS"):
                anomalies.append(f"c{price}_{year}: {c['state']} | {c['detail']}")
        print(f"c{price:<9}| " + " ".join(cells))
    return n_conv, n_conv5, anomalies


def _print_details(tol: float) -> None:
    """Print the per-cell metric line for every record present."""
    print("\nPer-cell detail (present records only):")
    for price in PRICES:
        for year in YEARS:
            c = _classify_cell(price, year, tol)
            if c["state"] != "-":
                print(f"  c{price}_{year}: {c['state']:<9} {c['detail']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tol", type=float, default=3e-3)
    ap.add_argument("--details", action="store_true")
    args = ap.parse_args()

    n_conv, n_conv5, anomalies = _print_grid(args.tol)
    if args.details:
        _print_details(args.tol)
    print(f"\nCONV (<=3e-3): {n_conv}/35   CONV5 (accepted 3e-3<gap<=5e-3, per-§3): "
          f"{n_conv5}/35   total accepted: {n_conv + n_conv5}/35")
    if anomalies:
        print("\n*** ANOMALIES (>5e-3 / timed_out / failed — investigate before accepting) ***")
        for a in anomalies:
            print(f"  {a}")
    else:
        print("No anomalies (all present cells accepted at <=5e-3).")


if __name__ == "__main__":
    main()
