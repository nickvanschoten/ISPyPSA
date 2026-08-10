"""Rapid-coal-phaseout archetype.

Pathway: every coal unit retires by 2030. Gas (and every other new-entrant
option) remains available; the LP chooses whether to use it on cost grounds.
No deployment mandates are applied beyond the coal closure clip — this
archetype tests the cost of an accelerated coal exit on its own, distinct
from gas_fleet_maintained which additionally floors gas capacity at the
2030/2035 milestone years.

Levers:
  1. Clip every coal closure_year to 2030.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2030)
    ispypsa_tables["ecaa_generators"] = ecaa

    return ispypsa_tables
