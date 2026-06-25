"""$0 chain carry-forward read: the strongest carried-fossil free-baseload test.
At $0 carbon fossil baseloads economically, so CF alone won't distinguish a bug;
the decisive check is carried-fossil marginal_cost == fresh-fossil marginal_cost
(the carry-forward preserves the carried unit's true cost; the bug would zero it).
"""

import pypsa
import pandas as pd
import numpy as np

BASE = "mvp_pass1_power/bench/runs_myopic/prod2026_c0_{y}__cost_optimal/outputs/capacity_expansion.nc"
REN = {"Wind", "Solar", "Biomass", "Hydro", "Water", "Pumped Hydro"}
FOSSIL = {"Black Coal", "Brown Coal", "Gas", "Liquid Fuel"}
YEARS = [2030, 2035, 2040, 2045, 2050]

print("=== $0 chain: per-period renewable share ===", flush=True)
for y in YEARS:
    n = pypsa.Network(BASE.format(y=y))
    w = n.snapshot_weightings["generators"]
    p = n.generators_t.p.clip(lower=0)
    gen = (p.mul(w, axis=0)).sum().groupby(n.generators.carrier).sum() / 1e6
    tot = gen.sum()
    ren = gen[[c for c in gen.index if c in REN]].sum()
    foss = {c: round(gen.get(c, 0), 1) for c in FOSSIL if gen.get(c, 0) > 0.05}
    print(f"  {y}: total={tot:.1f} TWh | renewable={100*ren/tot:.1f}% | fossil_TWh={foss}", flush=True)

    if y == 2050:
        g = n.generators.copy()
        hours = float(w.sum())
        energy = (p.mul(w, axis=0)).sum()
        g["cf"] = energy / (g.p_nom_opt.replace(0, np.nan) * hours)
        g["carried"] = (g.build_year >= 2030) & (g.build_year < 2050) & (~g.p_nom_extendable)
        mct = n.generators_t.marginal_cost
        mc_mean = mct.mean() if mct.shape[1] else pd.Series(dtype=float)
        g["mc_t"] = mc_mean.reindex(g.index)
        g["mc_static"] = pd.to_numeric(g["marginal_cost"], errors="coerce")
        g["mc"] = g["mc_t"].fillna(g["mc_static"])
        print("\n  === 2050 FREE-BASELOAD CHECK: carried vs fresh/existing FOSSIL ===", flush=True)
        for c in sorted(FOSSIL):
            sub = g[g.carrier == c]
            if not len(sub):
                continue
            car, oth = sub[sub.carried], sub[~sub.carried]
            cm = car["mc"].mean() if len(car) else float("nan")
            om = oth["mc"].mean() if len(oth) else float("nan")
            ccf = car["cf"].mean() if len(car) else float("nan")
            ocf = oth["cf"].mean() if len(oth) else float("nan")
            print(f"    {c:<12} carried n={len(car)} mc={cm:.1f} cf={ccf:.2f} | "
                  f"fresh/exist n={len(oth)} mc={om:.1f} cf={ocf:.2f}", flush=True)
        print("  (carried mc ~= fresh mc => priced correctly; carried mc ~0 while fresh>0 => free-baseload bug)", flush=True)
    del n
print("DONE", flush=True)
