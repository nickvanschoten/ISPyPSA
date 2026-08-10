"""Gas-fleet-maintained archetype.

Pathway: coal retired by 2030, AND total NEM gas capacity is held at or above
12,500 MW at both the 2030 and 2035 milestone years. Beyond 2035 no mandate
applies because AEMO's published Step Change trajectory naturally exceeds
12,500 MW (12,640 MW @ 2040, 14,188 MW @ 2045+).

Semantic: tests the cost of holding gas fleet stable through the transition
decade rather than allowing the retirement-and-rebuild pattern AEMO's
projection contains. The mandate concentrates in 2030-2035 where AEMO
projects natural gas capacity decline; later years rely on AEMO's natural
trajectory.

AEMO Step Change projections for reference:
  2030: 11,884 MW (mandate binds at +616 MW)
  2035: 11,852 MW (mandate binds at +648 MW)
  2040: 12,640 MW (mandate non-binding)
  2045+: ≥14,188 MW (mandate non-binding)

Levers:
  1. Clip every coal closure_year to 2030.
  2. PyPSA custom_constraints floor: total NEM gas capacity ≥ 12,500 MW at
     2030 and 2035 (sum over all gas Generator components — existing
     non-retired + new entrants active at the milestone year).
"""

from mvp_pass1_power.archetypes._capacity_floor import add_capacity_floor

_GAS_FLOOR_MW_BY_YEAR = {2030: 12_500, 2035: 12_500}


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2030)
    ispypsa_tables["ecaa_generators"] = ecaa

    return add_capacity_floor(
        ispypsa_tables,
        config,
        constraint_prefix="gas_floor",
        floors_by_year=_GAS_FLOOR_MW_BY_YEAR,
        new_entrant_table="new_entrant_generators",
        new_entrant_id_col="generator",
        new_entrant_predicate=lambda row: row.get("fuel_type") == "Gas",
        existing_table="ecaa_generators",
        existing_predicate=lambda row: row.get("fuel_type") == "Gas",
        term_type="generator_capacity",
    )
