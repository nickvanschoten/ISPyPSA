"""Storage-led archetype.

Pathway: coal retired by 2035; all new gas (including CCS) ruled out; storage
deployment held at 1.25× AEMO Step Change projection at each milestone year.
The LP must meet reliability through renewable overbuild, accelerated
long-duration storage, hydrogen and biomass.

Primary structural differentiator vs cost_optimal is the no-gas exclusion.
The 1.25× storage mandate provides modest acceleration relative to AEMO's
projected trajectory while staying within realistic supply-chain and
grid-integration limits.

Storage power-capacity floors (NEM-wide, 1.25× AEMO Step Change projection;
commit B will wire as PyPSA custom constraints):
  2025: ≥  3,445 MW (AEMO projects  2,756 MW)
  2030: ≥ 33,926 MW (AEMO projects 27,141 MW)
  2035: ≥ 38,619 MW (AEMO projects 30,895 MW)
  2040: ≥ 38,433 MW (AEMO projects 30,747 MW)
  2045: ≥ 41,784 MW (AEMO projects 33,427 MW)
  2050: ≥ 40,350 MW (AEMO projects 32,280 MW)

Levers (commit A — coal + gas exclusion only):
  1. Clip every coal closure_year to 2035.
  2. Drop all gas new-entrants: OCGT (small GT), OCGT (large GT), CCGT, any
     row where technology_type contains "CCS", or fuel_type is "Gas".
     Hydrogen and biomass rows remain.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2035)
    ispypsa_tables["ecaa_generators"] = ecaa

    ne = ispypsa_tables["new_entrant_generators"]
    drop_gas_tech = ne["fuel_type"] == "Gas"
    drop_ccs_tech = ne["technology_type"].str.contains("CCS", na=False)
    ispypsa_tables["new_entrant_generators"] = ne[~(drop_gas_tech | drop_ccs_tech)].copy()

    return ispypsa_tables
