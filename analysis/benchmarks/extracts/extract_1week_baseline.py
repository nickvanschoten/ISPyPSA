import pypsa, json, warnings, os
warnings.filterwarnings("ignore")
import logging; logging.getLogger("pypsa").setLevel(logging.ERROR)

ARCH = ["cost_optimal","rapid_coal_phaseout","gas_fleet_maintained",
        "storage_led","fossil_incumbent","nuclear_baseload"]
YEARS = [2025,2030,2035,2040,2045,2050]
BASE = "mvp_pass1_power/bench/runs_myopic"
PREFIX = "20260526_161705"

out = {}
missing = []
for a in ARCH:
    out[a] = {}
    for y in YEARS:
        nc = f"{BASE}/{PREFIX}__{a}_{y}__{a}/outputs/capacity_expansion.nc"
        if not os.path.exists(nc):
            missing.append(f"{a}_{y}")
            continue
        n = pypsa.Network(nc)
        w = n.snapshot_weightings.generators
        gcap = (n.generators.groupby("carrier").p_nom_opt.sum()/1e3)
        scap = (n.storage_units.groupby("carrier").p_nom_opt.sum()/1e3) if len(n.storage_units) else None
        gen = (n.generators_t.p.mul(w,axis=0).sum().groupby(n.generators.carrier).sum()/1e6)
        load = n.loads_t.p_set.mul(w,axis=0).sum().sum()/1e6
        out[a][str(y)] = {
            "n_snapshots": len(n.snapshots),
            "load_twh": round(load,1),
            "gas_cap_gw": round(float(gcap.get("Gas",0)),3),
            "wind_cap_gw": round(float(gcap.get("Wind",0)),3),
            "solar_cap_gw": round(float(gcap.get("Solar",0)),3),
            "battery_cap_gw": round(float(scap.get("Battery",0)),3) if scap is not None else 0,
            "gas_gen_twh": round(float(gen.get("Gas",0)),2),
            "coal_gen_twh": round(float(gen.get("Black Coal",0)+gen.get("Brown Coal",0)),2),
            "wind_gen_twh": round(float(gen.get("Wind",0)),2),
            "solar_gen_twh": round(float(gen.get("Solar",0)),2),
            "nuclear_cap_gw": round(float(gcap.get("Nuclear",0)),3),
        }
        print(f"done {a} {y} snaps={len(n.snapshots)}", flush=True)

json.dump(out, open("mvp_pass1_power/bench/extracts/extract_1week_baseline.json","w"), indent=2)
print("MISSING:", missing)
