"""Diagnostic: audit the ACTUAL linopy objective coefficients of the built 2045
model (the one Gurobi calls unbounded). Resolves the paradox:
  - any negative objective coefficient  -> that's the negative direction (pin it)
  - all coefficients >= 0               -> LP can't be cost-unbounded => numerical
Build-only (no solve). Also flags free (no-lower-bound) cost-bearing variables.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network

run = Path("analysis/benchmarks/runs_myopic/pilot_nem_150_v75_r2_2045__cost_optimal")
pf = read_csvs(run / "pypsa_friendly")
ts = run / "pypsa_friendly" / "capacity_expansion_timeseries"
print("building 2045 model (incl. custom constraints)...", flush=True)
network = build_pypsa_network(pf, ts)
m = network.model
print("model built.", flush=True)

# --- map variable label -> (group name, coord string) ---
vflat = m.variables.flat
print("variables.flat columns:", list(vflat.columns), flush=True)
coord_cols = [c for c in vflat.columns if c not in ("labels", "lower", "upper", "mask")]
lab2name = {}
lab2bounds = {}
for _, r in vflat.iterrows():
    lab = r["labels"]
    desc = " ".join(f"{c}={r[c]}" for c in coord_cols if c in r and pd.notna(r.get(c)))
    lab2name[lab] = desc
    if "lower" in r and "upper" in r:
        lab2bounds[lab] = (r["lower"], r["upper"])

# --- objective coefficients ---
oflat = m.objective.flat
print("objective.flat columns:", list(oflat.columns), "| terms:", len(oflat), flush=True)
ccol = "coeffs" if "coeffs" in oflat.columns else [c for c in oflat.columns if "coef" in c.lower()][0]
vcol = "vars" if "vars" in oflat.columns else [c for c in oflat.columns if c.lower() in ("vars", "labels", "_term")][0]

neg = oflat[oflat[ccol] < 0]
print(f"\n=== NEGATIVE objective coefficients: {len(neg)} ===", flush=True)
agg = {}
for _, r in neg.iterrows():
    nm = lab2name.get(r[vcol], f"label{r[vcol]}")
    grp = nm.split(" ")[0] if nm else "?"
    agg.setdefault(grp, []).append((nm, r[ccol]))
for grp, items in agg.items():
    print(f"  group {grp}: {len(items)} negative-coeff vars; examples:", flush=True)
    for nm, c in items[:6]:
        lb_ub = lab2bounds.get([k for k, v in lab2name.items() if v == nm][0], ("?", "?")) if nm in lab2name.values() else ("?", "?")
        print(f"    {nm}: coeff={c:.4f}", flush=True)

print(f"\nobjective coeff range: min={oflat[ccol].min():.4f} max={oflat[ccol].max():.4f}", flush=True)

# --- free (no-lower-bound) variables that ALSO carry an objective coefficient ---
obj_labels = set(oflat[vcol])
free_costed = []
for _, r in vflat.iterrows():
    lab = r["labels"]
    lo = r.get("lower", 0)
    if pd.notna(lo) and lo < -1e30 and lab in obj_labels:
        free_costed.append(lab2name.get(lab, str(lab)))
print(f"\nfree (lower=-inf) variables carrying an objective coefficient: {len(free_costed)}", flush=True)
for nm in free_costed[:10]:
    print(f"    {nm}", flush=True)
print("\nDONE", flush=True)
