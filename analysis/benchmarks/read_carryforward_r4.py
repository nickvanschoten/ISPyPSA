"""Out-year carry-forward / free-baseload read for the completed r4 $150 chain.
Per period: generation mix + renewable share. At each out-year: carried capacity
(carrier breakdown) + carried-FOSSIL free-baseload check (CF + whether priced).
"""

import pypsa
import pandas as pd
import numpy as np

BASE = "analysis/benchmarks/runs_myopic/pilot_nem_150_v75_r4_{y}__cost_optimal/outputs/capacity_expansion.nc"
REN = {"Wind", "Solar", "Biomass", "Hydro", "Water", "Pumped Hydro"}
FOSSIL = {"Black Coal", "Brown Coal", "Gas", "Liquid Fuel"}
YEARS = [2030, 2035, 2040, 2045, 2050]

print("=== per-period generation mix + renewable share ($150 chain, fixed) ===", flush=True)
for y in YEARS:
    n = pypsa.Network(BASE.format(y=y))
    w = n.snapshot_weightings["generators"]
    p = n.generators_t.p.clip(lower=0)
    gen = (p.mul(w, axis=0)).sum().groupby(n.generators.carrier).sum() / 1e6
    tot = gen.sum()
    ren = gen[[c for c in gen.index if c in REN]].sum()
    foss = {c: round(gen.get(c, 0), 1) for c in FOSSIL if gen.get(c, 0) > 0.05}
    print(f"  {y}: total={tot:.1f} TWh | renewable={100*ren/tot:.1f}% | fossil_TWh={foss}", flush=True)

    if y in (2045, 2050):
        g = n.generators.copy()
        hours = float(w.sum())
        energy = (p.mul(w, axis=0)).sum()
        g["cf"] = energy / (g.p_nom_opt.replace(0, np.nan) * hours)
        carried = g[(g.build_year >= 2030) & (g.build_year < y) & (~g.p_nom_extendable)]
        print(f"     carried into {y}: {len(carried)} gens, {carried.p_nom.sum()/1000:.1f} GW; "
              f"by carrier(GW)={carried.groupby('carrier')['p_nom'].sum().div(1000).round(2).to_dict()}", flush=True)
        cf = carried[carried.carrier.isin(FOSSIL)]
        if len(cf):
            print(f"     carried FOSSIL: {len(cf)} gens, {cf.p_nom.sum()/1000:.2f} GW, "
                  f"CF range=[{cf.cf.min():.2f},{cf.cf.max():.2f}], mean CF={cf.cf.mean():.2f} "
                  f"(high CF + would-be-$0 = free baseload symptom)", flush=True)
        else:
            print(f"     carried FOSSIL: NONE (at $150, 2030-{y-5} builds are all-renewable -> "
                  f"free-baseload test is vacuous here; $0 trajectory is the real test)", flush=True)
    del n
print("DONE", flush=True)
