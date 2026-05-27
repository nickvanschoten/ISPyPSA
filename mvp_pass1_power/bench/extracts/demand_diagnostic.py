"""Demand gap diagnostic for Phase 7.2.2 (hold item A).

Question: the 3-week production runs show NEM 2050 LP load ~276 TWh, below
an AEMO Step Change operational headline of ~310 TWh. Is the gap a 3-week
annualisation artefact, or a definitional / input-data difference?

This script sums the raw v6.0 (isp_2024 `example` dataset) Step Change demand
trace at full half-hourly resolution, by `demand_type` and `poe`, for each
milestone year. ISPyPSA feeds POE50 OPSO_MODELLING into the LP
(see src/ispypsa/translator/buses.py: poe="POE50", demand_type="OPSO_MODELLING").

Interpretation key:
  OPSO_MODELLING        operational demand sent-out, rooftop-PV netted off
                        (what the grid must supply) — THIS is the LP input.
  PV_TOT                total behind-the-meter rooftop PV generation.
  OPSO_MODELLING_PVLITE underlying consumption = OPSO + PV_TOT
                        (demand as if rooftop PV did not exist).

Compare the true full-year OPSO (8760 h) against the 3-week-annualised LP
load to isolate the annualisation bias from the definitional gap.
"""

import pandas as pd

TRACE = (
    "mvp_pass1_power/data/traces/isp_2024/demand/"
    "scenario=Step Change/reference_year=2018/data_0.parquet"
)
MILESTONES = [2025, 2030, 2035, 2040, 2045, 2050]


def _load_trace():
    df = pd.read_parquet(TRACE)
    # Financial-year label: Jul-start (a July+ timestamp belongs to FY ending
    # the following calendar year).
    df["fy"] = df["datetime"].dt.year + (df["datetime"].dt.month >= 7).astype(int)
    return df


def _annual_twh(df, fy, poe, demand_type):
    sub = df[(df.fy == fy) & (df.poe == poe) & (df.demand_type == demand_type)]
    # Half-hourly MW → energy: each step is 0.5 h, sum(MW) * 0.5 = MWh.
    return sub["value"].sum() * 0.5 / 1e6


def main():
    df = _load_trace()
    print("Full-year NEM demand by type (TWh), POE50, all 12 subregions:")
    print(f"{'FY':>6} {'OPSO':>8} {'PV_TOT':>8} {'PVLITE':>8}")
    for fy in MILESTONES:
        opso = _annual_twh(df, fy, "POE50", "OPSO_MODELLING")
        pv = _annual_twh(df, fy, "POE50", "PV_TOT")
        pvlite = _annual_twh(df, fy, "POE50", "OPSO_MODELLING_PVLITE")
        print(f"{fy:>6} {opso:>8.1f} {pv:>8.1f} {pvlite:>8.1f}")


if __name__ == "__main__":
    main()
