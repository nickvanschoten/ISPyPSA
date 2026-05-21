"""Fast-fossil-exit archetype.

Pathway: coal retired by 2030; no new unabated gas peakers or mid-merit, so
the LP fills the coal gap with renewables, storage, and firming-capable
low-carbon options (CCGT-CCS, hydrogen, biomass).

Levers:
  1. Clip every coal closure_year to 2030.
  2. Drop OCGT (small GT), OCGT (large GT), CCGT from new_entrant_generators.
     CCGT with CCS, hydrogen and biomass remain as firming options.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2030)
    ispypsa_tables["ecaa_generators"] = ecaa

    ne = ispypsa_tables["new_entrant_generators"]
    drop_tech = ["OCGT (small GT)", "OCGT (large GT)", "CCGT"]
    ispypsa_tables["new_entrant_generators"] = ne[~ne["technology_type"].isin(drop_tech)].copy()

    return ispypsa_tables
