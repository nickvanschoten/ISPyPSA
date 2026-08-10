"""Builds the central-case east-coast GPG gas supply curve CSV.

Emits gas_supply_curve_central.csv in this directory: one row per tranche per
financial year (ending-year label), columns tranche, financial_year, cap_pj,
adder_$/gj. Adders are real ~Jun-2025 AUD/GJ premiums above the IASR baseline
gas prices already in ISPyPSA generator marginal costs; caps are PJ/year of
gas available to gas-powered generation (GPG) NEM-wide.

Parameterisation basis (full derivation and citations in GAS_SUPPLY_CURVE.md):

- existing_market: GPG supply at the IASR baseline price. 110 PJ/yr = top of
  AEMO 2026 GSOO Step Change GPG band (actuals 97-137 PJ 2021-2025), tapering
  to 90 PJ/yr by FY2045 with southern 2P decline (Longford closes 2033).
- diverted_lng_spot: ~85 PJ/yr expected LNG spot sales (2026 GSOO) plus
  incremental Surat/Cooper CSG, at the oil-linked netback premium.
- new_basin_development: Narrabri (~70 PJ) + Beetaloo-east (~50-100 PJ) + Qld
  2C conversion, phasing 2031-2036, at Rystad 2035 marginal cost of supply
  ($13-14/GJ) plus transport.
- lng_imports: Port Kembla FSRU (500 TJ/d ~ 180 PJ/yr, operational from 2027)
  at JKM netback + regas; a second terminal (Geelong/Outer Harbor) from 2033.
- redirected_export_csg: export-scale CSG available domestically as foundation
  LNG contracts roll off from ~2036, at new-well CSG cost + transport
  (~import/export parity).
- scarcity_backstop: uncapped tail at +$18/GJ (~$30/GJ delivered, anchored
  well below the $40/GJ 2022 administered price cap) so the LP prices extreme
  volumes instead of going infeasible.
"""

from pathlib import Path

import pandas as pd

FIRST_FY = 2025
LAST_FY = 2055

# (tranche, adder $/GJ, {fy: cap_pj} anchors interpolated linearly between,
# held flat after the last anchor; None = uncapped).
TRANCHES = [
    ("existing_market", 0.0, {2025: 110.0, 2035: 110.0, 2045: 90.0}),
    ("diverted_lng_spot", 2.0, {2025: 50.0, 2026: 100.0}),
    ("new_basin_development", 4.0, {2025: 0.0, 2030: 0.0, 2036: 150.0}),
    ("lng_imports", 6.0, {2025: 0.0, 2026: 0.0, 2027: 180.0, 2032: 180.0, 2033: 360.0}),
    ("redirected_export_csg", 7.0, {2025: 0.0, 2035: 0.0, 2036: 350.0}),
    ("scarcity_backstop", 18.0, None),
]


def _interpolate_caps(anchors: dict[int, float] | None) -> pd.Series:
    years = pd.Index(range(FIRST_FY, LAST_FY + 1), name="financial_year")
    if anchors is None:
        return pd.Series(float("nan"), index=years)
    caps = pd.Series(anchors, index=years).interpolate(method="index")
    return caps.ffill()


def build_curve() -> pd.DataFrame:
    rows = []
    for tranche, adder, anchors in TRANCHES:
        caps = _interpolate_caps(anchors)
        for financial_year, cap_pj in caps.items():
            rows.append(
                {
                    "tranche": tranche,
                    "financial_year": financial_year,
                    "cap_pj": round(cap_pj, 1) if pd.notna(cap_pj) else None,
                    "adder_$/gj": adder,
                }
            )
    return (
        pd.DataFrame(rows)
        .sort_values(["financial_year", "adder_$/gj"])
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    curve = build_curve()
    out = Path(__file__).parent / "gas_supply_curve_central.csv"
    curve.to_csv(out, index=False)
    print(f"Wrote {len(curve)} rows to {out}")
    milestone_years = [2025, 2030, 2035, 2040, 2045, 2050]
    capped = curve.dropna(subset=["cap_pj"])
    totals = capped[capped["financial_year"].isin(milestone_years)]
    print(totals.groupby("financial_year")["cap_pj"].sum().rename("total_capped_pj"))
