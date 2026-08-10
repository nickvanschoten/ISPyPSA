"""Per-cell physical interrogation of the ccx frontier for the ISP-alignment
crux checks (brief §4): USE, VRE curtailment, gas CCS-vs-unabated split (capacity +
dispatch + CF), storage power (storage_units + stores), coal/biomass capacity+dispatch,
dispatch-based renewable share. One tidy row per (carbon price x year). Read-only.

Usage:
    python analysis/benchmarks/ccx_interrogate.py            # all present cells
    python analysis/benchmarks/ccx_interrogate.py --prices 0 550 --years 2050
"""

import argparse
import os
from pathlib import Path

os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")

import pandas as pd
import pypsa

RUNS = Path("analysis/benchmarks/runs_myopic")
OUT = Path("analysis/outputs/frontier_ccx")
PRICES = [0, 40, 80, 150, 250, 350, 550]
YEARS = [2030, 2035, 2040, 2045, 2050]
_VRE = {"Wind", "Solar"}
_RENEWABLE = {"Wind", "Solar", "Water", "Biomass"}


def _nc(price: int, year: int) -> Path:
    return RUNS / f"ccx_c{price}_{year}__cost_optimal" / "outputs" / "capacity_expansion.nc"


def _active_gens(n: pypsa.Network, year: int) -> pd.DataFrame:
    g = n.generators[n.generators["bus"] != "bus_for_custom_constraint_gens"].copy()
    return g[g["build_year"] + g["lifetime"] > year]


def _disp_twh(n: pypsa.Network, names: pd.Index, w: pd.Series) -> pd.Series:
    cols = [c for c in names if c in n.generators_t.p.columns]
    return (n.generators_t.p[cols].clip(lower=0).multiply(w, axis=0)).sum() / 1e6


def _cap_gw(df: pd.DataFrame) -> float:
    return float(df["p_nom_opt"].sum()) / 1e3


def _gas_split(active: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    gas = active[active["carrier"] == "Gas"]
    if "isp_technology_type" in gas.columns:
        is_ccs = gas["isp_technology_type"].astype(str).str.contains("CCS", na=False)
    else:
        is_ccs = pd.Series(False, index=gas.index)
    return gas[is_ccs], gas[~is_ccs]


def _vre_curtail_pct(n: pypsa.Network, active: pd.DataFrame, w: pd.Series) -> float:
    vre = active[active["carrier"].isin(_VRE)]
    cols = [c for c in vre.index if c in n.generators_t.p_max_pu.columns]
    if not cols:
        return float("nan")
    avail = n.generators_t.p_max_pu[cols].multiply(active.loc[cols, "p_nom_opt"], axis=1)
    avail_mwh = avail.multiply(w, axis=0).sum().sum()
    disp_mwh = n.generators_t.p[cols].clip(lower=0).multiply(w, axis=0).sum().sum()
    return 100.0 * (avail_mwh - disp_mwh) / avail_mwh if avail_mwh > 0 else float("nan")


def _storage(n: pypsa.Network, year: int) -> dict:
    su = n.storage_units
    su_gw = float(su.loc[su["build_year"] + su["lifetime"] > year, "p_nom_opt"].sum()) / 1e3 if len(su) else 0.0
    store_gwh = float(n.stores["e_nom_opt"].sum()) / 1e3 if len(n.stores) and "e_nom_opt" in n.stores else 0.0
    return {"storage_units_gw": round(su_gw, 3), "stores_energy_gwh": round(store_gwh, 1)}


def _interrogate(price: int, year: int) -> dict | None:
    nc = _nc(price, year)
    if not nc.exists():
        return None
    n = pypsa.Network(nc)
    w = n.snapshot_weightings["generators"]
    active = _active_gens(n, year)
    disp = _disp_twh(n, active.index, w)
    by_carrier = disp.groupby(active["carrier"]).sum()
    total = float(by_carrier.sum())
    ren = float(by_carrier[[c for c in by_carrier.index if c in _RENEWABLE]].sum())
    use = active[active["carrier"] == "Unserved Energy"]
    use_twh = float(_disp_twh(n, use.index, w).sum()) if len(use) else 0.0
    ccs, unab = _gas_split(active)
    ccs_disp = float(_disp_twh(n, ccs.index, w).sum())
    unab_disp = float(_disp_twh(n, unab.index, w).sum())
    ccs_gw, unab_gw = _cap_gw(ccs), _cap_gw(unab)
    coal = active[active["carrier"].isin(["Black Coal", "Brown Coal"])]
    bio = active[active["carrier"] == "Biomass"]
    row = {
        "carbon_price": price, "year": year,
        "total_gen_twh": round(total, 2),
        "use_twh": round(use_twh, 4),
        "use_pct": round(100.0 * use_twh / total, 5) if total else 0.0,
        "renewable_share_dispatch_pct": round(100.0 * ren / total, 1) if total else 0.0,
        "vre_curtail_pct": round(_vre_curtail_pct(n, active, w), 1),
        "coal_cap_gw": round(_cap_gw(coal), 2),
        "coal_disp_twh": round(float(_disp_twh(n, coal.index, w).sum()), 2),
        "gas_ccs_cap_gw": round(ccs_gw, 2),
        "gas_ccs_disp_twh": round(ccs_disp, 2),
        "gas_ccs_cf_pct": round(100.0 * ccs_disp * 1e6 / (ccs_gw * 1e3 * 8760), 1) if ccs_gw > 0.01 else 0.0,
        "gas_unabated_cap_gw": round(unab_gw, 2),
        "gas_unabated_disp_twh": round(unab_disp, 2),
        "gas_unabated_cf_pct": round(100.0 * unab_disp * 1e6 / (unab_gw * 1e3 * 8760), 1) if unab_gw > 0.01 else 0.0,
        "biomass_cap_gw": round(_cap_gw(bio), 3),
        "biomass_disp_twh": round(float(_disp_twh(n, bio.index, w).sum()), 3),
        "wind_cap_gw": round(_cap_gw(active[active["carrier"] == "Wind"]), 2),
        "solar_cap_gw": round(_cap_gw(active[active["carrier"] == "Solar"]), 2),
        "water_cap_gw": round(_cap_gw(active[active["carrier"] == "Water"]), 2),
    }
    row.update(_storage(n, year))
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prices", type=int, nargs="+", default=PRICES)
    ap.add_argument("--years", type=int, nargs="+", default=YEARS)
    args = ap.parse_args()
    rows = []
    for price in args.prices:
        for year in args.years:
            try:
                r = _interrogate(price, year)
            except Exception as e:
                print(f"  c{price}_{year}: ERROR {e}", flush=True)
                continue
            if r is None:
                continue
            rows.append(r)
            print(f"  c{price}_{year}: USE={r['use_pct']:.4f}% curtail={r['vre_curtail_pct']}% "
                  f"coal={r['coal_disp_twh']}TWh gasCCS={r['gas_ccs_cap_gw']}GW@{r['gas_ccs_cf_pct']}%CF "
                  f"gasUnab={r['gas_unabated_cap_gw']}GW@{r['gas_unabated_cf_pct']}%CF "
                  f"bio={r['biomass_cap_gw']}GW storU={r['storage_units_gw']}GW", flush=True)
    if rows:
        df = pd.DataFrame(rows).sort_values(["carbon_price", "year"]).reset_index(drop=True)
        OUT.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUT / "interrogation_ccx.tidy.csv", index=False)
        print(f"\nWrote {len(df)} cells -> {OUT / 'interrogation_ccx.tidy.csv'}")


if __name__ == "__main__":
    main()
