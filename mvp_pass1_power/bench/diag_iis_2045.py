"""Definitive localization: compute the IIS (Irreducible Infeasible Subset) of the
full 2045 model. Names the exact constraints AND variable bounds that together
cannot be satisfied. Tries linopy's mapping first (readable names), falls back to
raw gurobipy ConstrName/VarName.
"""

from collections import Counter
from pathlib import Path

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network

run = Path("mvp_pass1_power/bench/runs_myopic/diag_mindemand_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
print("building full 2045 model...", flush=True)
network = build_pypsa_network(pf, ts)
print("solving (DualReductions=0) to confirm infeasible...", flush=True)
status, cond = network.optimize.solve_model(
    solver_name="gurobi",
    solver_options={"Method": 1, "DualReductions": 0, "NumericFocus": 3},  # dual simplex: robust, clean infeasible status
)
gm = network.model.solver_model
print(f"gurobi status={gm.Status} (3=INFEASIBLE, 5=UNBOUNDED)  linopy={status}/{cond}", flush=True)

if gm.Status in (3, 4, 12):
    print(f"computing IIS (status {gm.Status})...", flush=True)
    gm.computeIIS()
    import re
    iis_labels = [
        int(re.search(r"\d+", c.ConstrName).group())
        for c in gm.getConstrs()
        if c.IISConstr and re.search(r"\d+", c.ConstrName)
    ]
    print(f"\n=== IIS constraints: {len(iis_labels)} ===", flush=True)
    # map linopy constraint label -> group name
    label_to_group = {}
    for cname in network.model.constraints:
        labs = network.model.constraints[cname].labels.values
        for lab in labs.ravel():
            if lab >= 0:
                label_to_group[int(lab)] = cname
    grp = Counter(label_to_group.get(l, "UNKNOWN") for l in iis_labels)
    print("IIS by linopy constraint GROUP:", flush=True)
    for g, n in grp.most_common():
        print(f"  {g}: {n}", flush=True)
    # for the dominant group(s), show which coords (snapshots/buses) are involved
    for cname in network.model.constraints:
        con = network.model.constraints[cname]
        labs = con.labels
        hit = [l for l in iis_labels if label_to_group.get(l) == cname]
        if hit and len(hit) <= 6000 and grp[cname] >= 3:
            mask = labs.isin(hit)
            coords = labs.where(mask, drop=True)
            print(f"  -- {cname}: coords present = {dict((d, list(map(str, coords[d].values))[:6]) for d in coords.dims)}", flush=True)
    # IIS variable bounds — which variable bounds are in the conflict
    iis_lb = [(v.VarName, v.LB) for v in gm.getVars() if v.IISLB]
    iis_ub = [(v.VarName, v.UB) for v in gm.getVars() if v.IISUB]
    print(f"\n=== IIS variable bounds: {len(iis_lb)} LB + {len(iis_ub)} UB ===", flush=True)
    print("  LB-in-IIS examples:", iis_lb[:10], flush=True)
    print("  UB-in-IIS examples:", iis_ub[:10], flush=True)
else:
    print(f"NOT infeasible (status {gm.Status}) -> no IIS to compute", flush=True)
print("DONE", flush=True)
