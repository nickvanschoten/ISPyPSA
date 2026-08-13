"""Builds the central-case NEM biomass feedstock supply curve CSV.

Emits biomass_supply_curve_central.csv in this directory: one row per tranche
per financial year (ending-year label), columns tranche, financial_year,
cap_pj, adder_$/gj. Adders are real AUD/GJ premiums above the IASR baseline
biomass price (~$0.62/GJ, the residue-tier value) already in ISPyPSA generator
marginal costs; caps are PJ/year of feedstock available to biomass-fired
generation NEM-wide. When this curve is configured the flat $6/GJ feedstock
re-price pre-pass stands down, so tranche 1 genuinely buys residue-priced
feedstock and the curve carries the whole cost shape.

Parameterisation basis (full derivation and citations in
BIOMASS_SUPPLY_CURVE.md):

- existing_industry_byproduct: on-site bagasse cogeneration, landfill gas and
  wood-processing waste feeding today's ~0.9 GW bioelectricity fleet
  (~1-3.2 TWh/yr = ~15-50 PJ feedstock); genuine ~$0/GJ byproducts, so they
  buy at the IASR residue baseline. 30 PJ/yr = midpoint of the actuals band.
- collected_residues: forestry / sawmill / agricultural residues collected and
  hauled (~100 km viability radius), at CSIRO-Boeing delivered residue prices
  ($110-125/t = ~$6/GJ delivered). Volume is the power sector's residual claim
  after biomethane (AEMO/ACIL Step Change ~250 PJ by the 2030s) and SAF claim
  the same pools, ramping with supply-chain mobilisation.
- energy_crops: short-rotation-coppice plantings per Crawford et al. (2016) —
  zero commercial deployment today, first material volumes late 2030s, power's
  share ~120 PJ by 2050 of the ~530 PJ theoretical SRC expansion.
- imported_pellet_backstop: uncapped tail at +$15/GJ (~$16/GJ delivered, top
  of IRENA's internationally-traded-pellet tier) so the LP prices extreme
  volumes instead of going infeasible. Australia has no pellet-import supply
  chain, so this is a true scarcity price, not an expected market.
"""

from pathlib import Path

import pandas as pd

FIRST_FY = 2025
LAST_FY = 2055

# (tranche, adder $/GJ, {fy: cap_pj} anchors interpolated linearly between,
# held flat after the last anchor; None = uncapped).
TRANCHES = [
    ("existing_industry_byproduct", 0.0, {2025: 30.0}),
    ("collected_residues", 5.5, {2025: 5.0, 2030: 25.0, 2040: 60.0, 2050: 90.0}),
    ("energy_crops", 10.0, {2025: 0.0, 2035: 0.0, 2040: 30.0, 2050: 120.0}),
    ("imported_pellet_backstop", 15.0, None),
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
    out = Path(__file__).parent / "biomass_supply_curve_central.csv"
    curve.to_csv(out, index=False)
    print(f"Wrote {len(curve)} rows to {out}")
    milestone_years = [2025, 2030, 2035, 2040, 2045, 2050]
    capped = curve.dropna(subset=["cap_pj"])
    totals = capped[capped["financial_year"].isin(milestone_years)]
    print(totals.groupby("financial_year")["cap_pj"].sum().rename("total_capped_pj"))
