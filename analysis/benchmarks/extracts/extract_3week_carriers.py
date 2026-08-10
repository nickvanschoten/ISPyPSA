import pypsa, json, warnings
import pandas as pd
warnings.filterwarnings("ignore")
import logging; logging.getLogger("pypsa").setLevel(logging.ERROR)

ARCH = ["cost_optimal","rapid_coal_phaseout","gas_fleet_maintained",
        "storage_led","fossil_incumbent","nuclear_baseload"]
YEARS = [2025,2030,2035,2040,2045,2050]
BASE = "mvp_pass1_power/bench/runs_myopic"

def extract(arch, year):
    nc = f"{BASE}/nem_3week_v1_{arch}_{year}__{arch}/outputs/capacity_expansion.nc"
    n = pypsa.Network(nc)
    w = n.snapshot_weightings.generators
    # Generation TWh by carrier (generators)
    gen = n.generators_t.p.mul(w, axis=0).sum()  # MWh
    gen_by = gen.groupby(n.generators.carrier).sum() / 1e6  # TWh
    # Storage dispatch (discharge) TWh
    if len(n.storage_units):
        sd = n.storage_units_t.p.clip(lower=0).mul(w, axis=0).sum()
        sd_by = sd.groupby(n.storage_units.carrier).sum() / 1e6
    else:
        sd_by = pd.Series(dtype=float)
    # Capacity GW by carrier
    gcap = n.generators.groupby("carrier").p_nom_opt.sum() / 1e3
    if len(n.storage_units):
        scap = n.storage_units.groupby("carrier").p_nom_opt.sum() / 1e3
    else:
        scap = pd.Series(dtype=float)
    # Load served
    load_twh = n.loads_t.p_set.mul(w, axis=0).sum().sum() / 1e6
    return {
        "gen_twh": {k: round(v,2) for k,v in gen_by.items() if abs(v)>0.005},
        "storage_discharge_twh": {k: round(v,2) for k,v in sd_by.items() if abs(v)>0.005},
        "gen_cap_gw": {k: round(v,3) for k,v in gcap.items() if abs(v)>0.001},
        "storage_cap_gw": {k: round(v,3) for k,v in scap.items() if abs(v)>0.001},
        "load_twh": round(load_twh,1),
        "n_snapshots": len(n.snapshots),
    }

out = {}
for a in ARCH:
    out[a] = {}
    for y in YEARS:
        out[a][str(y)] = extract(a,y)
        print(f"done {a} {y}", flush=True)

json.dump(out, open("mvp_pass1_power/bench/extracts/extract_3week_carriers.json","w"), indent=2)
print("WROTE _extract_72.json")
