"""Definitive INFEASIBLE-vs-UNBOUNDED discriminator for the 2045 model.

The objective audit PROVED the LP is not cost-unbounded (all obj coeffs > 0,
vars >= 0). The hardened barrier then DIVERGED with primal infeasibility stuck
~7.3e4 -> the signature of an infeasible (not unbounded, not merely
ill-conditioned) model. Gurobi's INF_OR_UNBD status (4) is ambiguous because of
presolve dual reductions; DualReductions=0 forces a definitive verdict.
"""

from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network

run = Path("analysis/benchmarks/runs_myopic/pilot_nem_150_v75_r2_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
print("building 2045 model...", flush=True)
network = build_pypsa_network(pf, ts)
print("solving with DualReductions=0 (definitive INF vs UNBD)...", flush=True)

status, condition = network.optimize.solve_model(
    solver_name="gurobi",
    solver_options={
        "DualReductions": 0,   # forces INFEASIBLE (3) or UNBOUNDED (5), not 4
        "Method": 2,
        "Crossover": 0,
        "NumericFocus": 2,
    },
)
gm = network.model.solver_model
code = gm.Status
verdict = {2: "OPTIMAL", 3: "INFEASIBLE", 5: "UNBOUNDED"}.get(code, f"status_{code}")
print(f"\n=== DEFINITIVE VERDICT: {verdict}  (gurobi status code {code}) ===", flush=True)
print(f"linopy status={status} condition={condition}", flush=True)
print("DONE", flush=True)
