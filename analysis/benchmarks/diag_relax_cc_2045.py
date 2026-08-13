"""Empirical discriminator: is the 2045 infeasibility in the custom constraints
or the base model? Build WITHOUT any custom-constraint tables and solve.

Reasoning predicts BASE model: every custom constraint is <= with an unbounded
expansion-capacity relaxation valve, so they're structurally satisfiable. If the
relaxed model is STILL infeasible, the cause is the base model (most likely the
fixed carried capacity violating a base limit). DualReductions=0 -> clean verdict.
"""

from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network

run = Path("analysis/benchmarks/runs_myopic/pilot_nem_150_v75_r2_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
dropped = [k for k in pf if k.startswith("custom_constraints")]
pf_relaxed = {k: v for k, v in pf.items() if not k.startswith("custom_constraints")}
print(f"dropped custom-constraint tables: {dropped}", flush=True)
print("building RELAXED model (no custom constraints)...", flush=True)
network = build_pypsa_network(pf_relaxed, ts)
print("solving relaxed (DualReductions=0)...", flush=True)
status, cond = network.optimize.solve_model(
    solver_name="gurobi",
    solver_options={"DualReductions": 0, "Method": 2, "Crossover": 0, "NumericFocus": 2},
)
gm = network.model.solver_model
verdict = {2: "OPTIMAL", 3: "INFEASIBLE", 5: "UNBOUNDED"}.get(gm.Status, f"status_{gm.Status}")
print(f"\n=== RELAXED (no custom constraints) VERDICT: {verdict} (gurobi status {gm.Status}) ===", flush=True)
if status == "ok":
    print(f"objective={network.objective:.4e}", flush=True)
print("  OPTIMAL    -> infeasibility WAS in the custom constraints", flush=True)
print("  INFEASIBLE -> infeasibility is in the BASE model (carried capacity / base limits / balance)", flush=True)
print("DONE", flush=True)
