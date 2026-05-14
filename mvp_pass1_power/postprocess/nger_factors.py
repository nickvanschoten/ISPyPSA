"""NGER emission-factor cross-walk to ISPyPSA carriers.

Source: National Greenhouse Accounts Factors 2024 (DCCEEW, July 2024 edition),
https://www.dcceew.gov.au/sites/default/files/documents/national-greenhouse-account-factors-2024.pdf

Underlying legal basis: National Greenhouse and Energy Reporting (Measurement)
Determination 2008 (Schedule 1).

All factors are Scope 1 (direct combustion), in kg CO2-e per GJ on a Gross
Calorific Value basis. CO2 / CH4 / N2O are reported separately (in CO2-e units
already, i.e. multiplied by AR5 GWP-100); the "combined" column is their sum.

Cross-walk decisions for ISPyPSA's carrier set:
  - "Black Coal"   -> NGA "Bituminous coal" (Table 4). Sub-bituminous gives the
    same combined EF (90.24), so the cross-walk is unambiguous for the AEMO ISP
    fleet.
  - "Brown Coal"   -> NGA "Brown coal (lignite)" (Table 4). One-to-one.
  - "Gas"          -> NGA "Natural gas distributed in a pipeline" (Table 5).
                       This covers CCGT, OCGT, GPG. Coal seam methane has the
                       same combined EF; pipeline blending is the dominant case.
  - "Liquid Fuel"  -> NGA "Diesel oil" (Table 8). ISPyPSA's IASR fleet uses
    diesel for liquid-fuelled OCGT.
  - "Biomass"      -> NGA "Primary solid biomass fuels other than those
    mentioned in the items above" (Table 4). CO2 is biogenic (zero); CH4 + N2O
    combustion residuals only.
  - "Hydrogen"     -> Zero combustion CO2-e by definition (pure H2 combustion
    produces only water). Upstream emissions sit on whichever sector produces
    the hydrogen and are out of scope here.
  - "Biomethane"   -> NGA "Biomethane" (Table 5). Biogenic CO2, small CH4/N2O.
  - "Hyblend"      -> Linear blend of natural gas and hydrogen by H2 fraction.
                       ISPyPSA's gpg_emissions_reduction_h2 table provides the
                       per-year H2 fraction for the relevant generators. Handled
                       in emissions.py by scaling the gas EF by (1 - h2_frac).
  - "Wind", "Solar", "Water" -> zero combustion EF.
  - "Storage" (BESS, pumped hydro): not a combustion fuel — no Scope 1.
    Round-trip losses are an internal-to-electricity inefficiency, accounted
    in the source method's per-MWh-delivered cost.

Process emissions for electricity: NGA has no separate process-emission factor
for electricity generation (the only stationary-energy process emissions in
NGER are calcination, soda ash etc.). All combustion emissions are categorised
as energy emissions (combustion). Hence process_emissions_kg_CO2e_per_GJ = 0
for every fuel here.

Multi-pollutant note: NGA reports CO2, CH4 and N2O separately (already in
CO2-e). The simple-msm contract supports per-pollutant decomposition; we expose
each separately under pollutant ids "CO2", "CH4_CO2e", "N2O_CO2e". CH4 and N2O
are reported in CO2-e units because that is how NGA publishes them; converting
back to physical mass would require the inverse of AR5 GWP-100 (CH4: 28, N2O:
265), and there is no analytic gain since the orchestrator's bound metric is
total CO2-e.
"""

import pandas as pd

# Scope 1 combustion emission factors, kg CO2-e/GJ (GCV basis).
# Each row: (CO2, CH4_in_CO2e, N2O_in_CO2e). NGA Factors 2024 Tables 4, 5, 8.
_NGER_FACTORS_KG_CO2E_PER_GJ = {
    # ISPyPSA carrier  : (CO2, CH4, N2O, NGA table, NGA fuel name)
    "Black Coal":    (90.0, 0.04, 0.2,   "Table 4", "Bituminous coal"),
    "Brown Coal":    (93.5, 0.02, 0.3,   "Table 4", "Brown coal (lignite)"),
    "Gas":           (51.4, 0.1,  0.03,  "Table 5", "Natural gas distributed in a pipeline"),
    "Liquid Fuel":   (69.9, 0.1,  0.2,   "Table 8", "Diesel oil"),
    "Biomass":       (0.0,  0.8,  1.0,   "Table 4", "Primary solid biomass fuels"),
    "Hydrogen":      (0.0,  0.0,  0.0,   "—",       "Pure H2 combustion (zero Scope 1)"),
    "Biomethane":    (0.0,  0.1,  0.03,  "Table 5", "Biomethane"),
    # Hyblend handled separately — composed of Gas + Hydrogen in per-year ratio.
    # Renewables / hydro / storage default to zero across all pollutants.
    "Wind":          (0.0,  0.0,  0.0,   "—",       "Non-combustion"),
    "Solar":         (0.0,  0.0,  0.0,   "—",       "Non-combustion"),
    "Water":         (0.0,  0.0,  0.0,   "—",       "Non-combustion"),
    "Storage":       (0.0,  0.0,  0.0,   "—",       "No primary fuel"),
}


def nger_factor_table() -> pd.DataFrame:
    """Return the NGER Scope 1 emission factor cross-walk as a DataFrame.

    One row per ISPyPSA carrier. Columns:
      carrier, co2_kg_per_gj, ch4_co2e_kg_per_gj, n2o_co2e_kg_per_gj,
      total_co2e_kg_per_gj, nga_table, nga_fuel_name.
    """
    rows = []
    for carrier, (co2, ch4, n2o, table, fuel) in _NGER_FACTORS_KG_CO2E_PER_GJ.items():
        rows.append({
            "carrier": carrier,
            "co2_kg_per_gj": co2,
            "ch4_co2e_kg_per_gj": ch4,
            "n2o_co2e_kg_per_gj": n2o,
            "total_co2e_kg_per_gj": co2 + ch4 + n2o,
            "nga_table": table,
            "nga_fuel_name": fuel,
        })
    return pd.DataFrame(rows)


def hyblend_factor(h2_fraction: float) -> dict:
    """Return the effective combustion EF for a Hyblend gas mix.

    h2_fraction is the H2 share by energy content (consistent with how
    ISPyPSA's gpg_emissions_reduction_h2 table is denominated). The remaining
    fraction is natural gas.
    """
    gas_co2, gas_ch4, gas_n2o = (51.4, 0.1, 0.03)
    return {
        "co2_kg_per_gj":          (1 - h2_fraction) * gas_co2,
        "ch4_co2e_kg_per_gj":     (1 - h2_fraction) * gas_ch4,
        "n2o_co2e_kg_per_gj":     (1 - h2_fraction) * gas_n2o,
        "total_co2e_kg_per_gj":   (1 - h2_fraction) * (gas_co2 + gas_ch4 + gas_n2o),
    }
