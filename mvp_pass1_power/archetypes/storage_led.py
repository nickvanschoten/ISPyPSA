"""Storage-led archetype.

Pathway: coal retired by 2035; all new gas (including CCS) ruled out. The LP
must meet reliability through renewable overbuild, long-duration storage,
hydrogen and biomass. This is the most demanding reliability archetype for the
storage and hydrogen sectors.

Levers:
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
