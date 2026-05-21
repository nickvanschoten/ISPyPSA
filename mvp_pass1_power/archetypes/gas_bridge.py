"""Gas-bridge archetype.

Pathway: coal retired by 2030, but new gas (OCGT, CCGT) remains available as
a bridge technology. The LP chooses between gas plant and storage to fill the
coal gap, reflecting a transition where gas infrastructure investment is still
acceptable during the 2025–2035 window.

Levers:
  1. Clip every coal closure_year to 2030.
  2. No changes to new_entrant_generators — all IASR new-entrant options remain.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2030)
    ispypsa_tables["ecaa_generators"] = ecaa

    return ispypsa_tables
