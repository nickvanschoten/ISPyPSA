"""Step 3: re-solve the (provably bounded) 2045 model with HARDENED numerics to
confirm the 'unbounded' verdict was a conditioning artifact, and to obtain a
finite optimum. Builds from the saved 2045 pypsa_friendly (same carried tranche
+ custom constraints), solves with Gurobi NumericFocus=3 + aggressive scaling.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network

run = Path("analysis/benchmarks/runs_myopic/pilot_nem_150_v75_r2_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
print("building 2045 model...", flush=True)
network = build_pypsa_network(pf, ts)
print("model built; solving with hardened numerics...", flush=True)

status, condition = network.optimize.solve_model(
    solver_name="gurobi",
    solver_options={
        "NumericFocus": 3,   # maximum numerical care
        "ScaleFlag": 2,      # aggressive (geometric-mean) scaling
        "Method": 2,         # barrier
        "Crossover": 0,      # interior solution is sufficient for the verdict
        "BarConvTol": 1e-4,
        "BarHomogeneous": 1,
    },
)
print(f"\n=== SOLVE RESULT: status={status} condition={condition} ===", flush=True)

if status == "ok":
    try:
        print(f"objective = {network.objective:.6e}", flush=True)
    except Exception as e:
        print("objective n/a:", e, flush=True)
    # annual generation sanity (weighted dispatch)
    w = network.snapshot_weightings["generators"]
    p = network.generators_t.p.clip(lower=0)
    twh = (p.mul(w, axis=0)).sum().sum() / 1e6
    print(f"annual generation = {twh:.1f} TWh (sane full-NEM ~ 200-250)", flush=True)
    network.export_to_netcdf(str(run / "outputs" / "capacity_expansion_gurobi_hardened.nc"))
    print("saved hardened solution NetCDF.", flush=True)
else:
    print("HARDENED SOLVE DID NOT RETURN ok -> deeper numerical issue than expected.", flush=True)
print("DONE", flush=True)
