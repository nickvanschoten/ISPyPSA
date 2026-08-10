"""Tier-1 gas/AEMO discrimination: per (carbon price x year) pull bulk-grid
capacity by carrier + the EXISTING(ECAA)-vs-NEW gas split (by build_year), to
separate a static gas-preference from a dynamic (myopic-chaining) gas-lean, and
to scrutinise existing-gas retirement. Static network read only (n.generators),
so fast. Read-only. Writes gas_vintage_ccx.tidy.csv.
"""
import os
from pathlib import Path
os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")
import pandas as pd
import pypsa

RUNS = Path("analysis/benchmarks/runs_myopic")
OUT = Path("analysis/outputs/frontier_ccx")
PRICES = [0, 40, 80, 150, 250, 350, 550]
YEARS = [2030, 2035, 2040, 2045, 2050]


def _cell(price: int, year: int) -> dict | None:
    nc = RUNS / f"ccx_c{price}_{year}__cost_optimal" / "outputs" / "capacity_expansion.nc"
    if not nc.exists():
        return None
    n = pypsa.Network(nc)
    g = n.generators[n.generators["bus"] != "bus_for_custom_constraint_gens"].copy()
    g = g[g["build_year"] + g["lifetime"] > year]  # active
    su = n.storage_units
    su = su[su["build_year"] + su["lifetime"] > year] if len(su) else su
    def capgw(df, mask):
        return round(float(df.loc[mask, "p_nom_opt"].sum()) / 1e3, 2)
    gas = g[g["carrier"] == "Gas"]
    is_ccs = gas["isp_technology_type"].astype(str).str.contains("CCS", na=False) if "isp_technology_type" in gas else pd.Series(False, index=gas.index)
    existing_gas = gas[gas["build_year"] < 2030]     # ECAA / pre-2030 = existing fleet
    new_gas = gas[gas["build_year"] >= 2030]
    return {
        "carbon_price": price, "year": year,
        "wind": capgw(g, g["carrier"] == "Wind"),
        "solar": capgw(g, g["carrier"] == "Solar"),
        "hydro": capgw(g, g["carrier"] == "Water"),
        "storage": round(float(su["p_nom_opt"].sum()) / 1e3, 2) if len(su) else 0.0,
        "coal": capgw(g, g["carrier"].isin(["Black Coal", "Brown Coal"])),
        "gas_total": capgw(gas, gas.index.notna()),
        "gas_existing": capgw(existing_gas, existing_gas.index.notna()),
        "gas_new": capgw(new_gas, new_gas.index.notna()),
        "gas_ccs": capgw(gas, is_ccs),
        "gas_unabated": capgw(gas, ~is_ccs),
    }


def main() -> None:
    rows = [c for p in PRICES for y in YEARS if (c := _cell(p, y)) is not None]
    df = pd.DataFrame(rows)
    # bulk-grid subset shares (wind+solar+storage+hydro+gas+coal)
    df["bulk_total"] = df[["wind", "solar", "storage", "hydro", "gas_total", "coal"]].sum(axis=1)
    for c in ["wind", "solar", "storage", "hydro", "gas_total", "coal"]:
        df[f"{c}_share"] = (100 * df[c] / df["bulk_total"]).round(1)
    df["ws_share"] = (df["wind_share"] + df["solar_share"]).round(1)
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "gas_vintage_ccx.tidy.csv", index=False)

    def piv(v):
        return df.pivot(index="year", columns="carbon_price", values=v)
    print("=== GAS capacity GW (total) by year x price ===")
    print(piv("gas_total").round(1).to_string())
    print("\n=== EXISTING gas GW (build_year<2030, ECAA) — retirement trajectory ===")
    print(piv("gas_existing").round(1).to_string())
    print("\n=== NEW gas GW (build>=2030: CCS + unabated) ===")
    print(piv("gas_new").round(1).to_string())
    print("\n=== gas SHARE % of bulk-grid capacity ===")
    print(piv("gas_total_share").round(1).to_string())
    print("\n=== wind+solar SHARE % (AEMO: 55/~110-total 2030 .. 127/~200 2050) ===")
    print(piv("ws_share").round(1).to_string())
    print("\n=== storage SHARE % ===")
    print(piv("storage_share").round(1).to_string())
    print("\n=== absolute wind+solar GW (AEMO grid-scale: 55 @2030 -> 127 @2050) ===")
    df["ws"] = df["wind"] + df["solar"]
    print(piv("ws").round(1).to_string())
    print(f"\nwrote {len(df)} cells -> {OUT/'gas_vintage_ccx.tidy.csv'}")


if __name__ == "__main__":
    main()
