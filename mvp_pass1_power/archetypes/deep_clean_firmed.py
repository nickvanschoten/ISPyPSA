"""Deep-clean-firmed archetype.

Pathway: aggressive coal retirement and zero new unabated thermal, but firming
allowed via CCGT-with-CCS, biomass and hydrogen. This is the "ambitious
decarbonisation with reliability" archetype.

Levers:
  1. Force every coal unit to retire by 2035 (closure_year capped).
  2. Drop new-entrant unabated gas (OCGT small/large, CCGT without CCS).
     Keep CCGT-with-CCS, biomass and hydrogen as firming options.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2035)
    ispypsa_tables["ecaa_generators"] = ecaa

    ne = ispypsa_tables["new_entrant_generators"]
    drop_tech = ["OCGT (small GT)", "OCGT (large GT)", "CCGT"]
    ispypsa_tables["new_entrant_generators"] = ne[~ne["technology_type"].isin(drop_tech)].copy()

    return ispypsa_tables
