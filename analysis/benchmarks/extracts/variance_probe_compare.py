"""PDLP tolerance-variance probe for Phase 7.2.2 (diagnostic 1).

The pre-3-week dashboard framing warned of large PDLP solution variance,
anchored on a single-week observation of a cost_optimal 2050 solar swing
(~75 -> 27 GW) between solver settings. This script re-tests that sensitivity
under the 3-week production sampling by comparing the cost_optimal 2050 fleet
solved at the production tolerance (1e-3) against a tighter re-solve (1e-4).

Both networks are myopic single-period 2050 solves on identical inputs; only
pdlp_optimality_tolerance / primal / dual feasibility tolerances differ.

Result (recorded 2026-05-27): the two solutions agree to within ~0.5 % on VRE
capacity and +0.037 % on objective — i.e. under 3-week sampling the solution
is tolerance-stable and the old large-swing framing does not reproduce.
The 1e-4 solve cost ~42 min / 132k PDLP iterations vs ~6 min at 1e-3, so 1e-3
is the right production tolerance.
"""

import pypsa
import warnings
import logging

warnings.filterwarnings("ignore")
logging.getLogger("pypsa").setLevel(logging.ERROR)

RUNS = "analysis/benchmarks/runs_myopic"
NC_1E3 = f"{RUNS}/nem_3week_v1_cost_optimal_2050__cost_optimal/outputs/capacity_expansion.nc"
NC_1E4 = f"{RUNS}/probe_1e4_cost_optimal_2050__cost_optimal/outputs/capacity_expansion.nc"


def _fleet(nc):
    n = pypsa.Network(nc)
    gcap = n.generators.groupby("carrier").p_nom_opt.sum() / 1e3
    scap = (
        n.storage_units.groupby("carrier").p_nom_opt.sum() / 1e3
        if len(n.storage_units)
        else None
    )
    return n.objective, gcap, scap


def main():
    o3, g3, s3 = _fleet(NC_1E3)
    o4, g4, s4 = _fleet(NC_1E4)
    print(f"objective  1e-3={o3:,.0f}  1e-4={o4:,.0f}  delta={100*(o4-o3)/o3:+.3f}%")
    print(f"\n{'CAPACITY GW':24} {'1e-3':>8} {'1e-4':>8} {'delta':>8}")
    for c in sorted(set(g3.index) | set(g4.index)):
        if c in ("Unserved Energy", ""):
            continue
        a, b = g3.get(c, 0), g4.get(c, 0)
        if max(a, b) < 0.05:
            continue
        print(f"{c:24} {a:8.2f} {b:8.2f} {b-a:+8.2f}")
    print("-- storage --")
    idx = set(s3.index if s3 is not None else []) | set(s4.index if s4 is not None else [])
    for c in sorted(idx):
        a = s3.get(c, 0) if s3 is not None else 0
        b = s4.get(c, 0) if s4 is not None else 0
        print(f"{c:24} {a:8.2f} {b:8.2f} {b-a:+8.2f}")


if __name__ == "__main__":
    main()
