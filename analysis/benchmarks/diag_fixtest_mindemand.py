"""Fix-confirmation: if the must-run floors (p_min_pu>0) are the cause of the
min-demand-hour overgeneration infeasibility, then setting p_min_pu=0 (no hard
must-run floor -- the standard capacity-expansion-LP treatment, since min-load is
a unit-commitment concept) should flip the same model to FEASIBLE.
"""

from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network
from ispypsa.pypsa_build.custom_constraints import _add_custom_constraints

run = Path("analysis/benchmarks/runs_myopic/diag_mindemand_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
print("building min-demand model...", flush=True)
network = build_pypsa_network(pf, ts)

n_mustrun = int((network.generators["p_min_pu"] > 0).sum())
print(f"generators with must-run floor (p_min_pu>0): {n_mustrun}", flush=True)
network.generators["p_min_pu"] = 0.0
print("set all p_min_pu=0; rebuilding linopy model + re-adding custom constraints...", flush=True)
network.optimize.create_model(multi_investment_periods=True)
if "custom_constraints_rhs" in pf:
    _add_custom_constraints(network, pf["custom_constraints_rhs"], pf["custom_constraints_lhs"])

status, cond = network.optimize.solve_model(
    solver_name="gurobi", solver_options={"Method": 1, "DualReductions": 0}
)
code = network.model.solver_model.Status
print(f"\n=== p_min_pu=0 result: status={status} cond={cond} gurobi={code} (2=OPTIMAL,3=INFEASIBLE) ===", flush=True)
if code == 2:
    print("FEASIBLE -> CONFIRMED: the must-run floors caused the overgeneration infeasibility.", flush=True)
    try:
        print(f"objective={network.objective:.4e}", flush=True)
    except Exception:
        pass
else:
    print("still infeasible -> must-run is not the (sole) cause; reconsider.", flush=True)
print("DONE", flush=True)
