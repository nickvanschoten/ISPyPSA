"""Storage-led archetype.

Pathway: coal retired by 2035; all new gas (including CCS) ruled out; storage
deployment held at 1.25× AEMO Step Change projection at each milestone year.
The LP must meet reliability through renewable overbuild, accelerated
long-duration storage, hydrogen and biomass.

Primary structural differentiator vs cost_optimal is the no-gas exclusion.
The 1.25× storage mandate provides modest acceleration relative to AEMO's
projected trajectory while staying within realistic supply-chain and
grid-integration limits.

Storage power-capacity floors (NEM-wide, 1.25× AEMO Step Change projection):
  2025: ≥  3,445 MW (AEMO projects  2,756 MW)
  2030: ≥ 33,926 MW (AEMO projects 27,141 MW)
  2035: ≥ 38,619 MW (AEMO projects 30,895 MW)
  2040: ≥ 38,433 MW (AEMO projects 30,747 MW)
  2045: ≥ 41,784 MW (AEMO projects 33,427 MW)
  2050: ≥ 40,350 MW (AEMO projects 32,280 MW)

Floor applies to NEM-wide storage power (StorageUnit p_nom) — covers all
existing + new entrant batteries and pumped storage. The 'storage_capacity'
custom_constraint term_type added in this commit wires the floor through to
the PyPSA StorageUnit_p_nom variable.

Levers:
  1. Clip every coal closure_year to 2035.
  2. Drop all gas new-entrants: OCGT (small GT), OCGT (large GT), CCGT, any
     row where technology_type contains "CCS", or fuel_type is "Gas".
     Hydrogen and biomass rows remain.
  3. PyPSA custom_constraints floor on storage power per milestone year.
"""

from mvp_pass1_power.archetypes._capacity_floor import add_capacity_floor

_STORAGE_FLOOR_MW_BY_YEAR = {
    2025:  3_445,
    2030: 33_926,
    2035: 38_619,
    2040: 38_433,
    2045: 41_784,
    2050: 40_350,
}


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2035)
    ispypsa_tables["ecaa_generators"] = ecaa

    ne = ispypsa_tables["new_entrant_generators"]
    drop_gas_tech = ne["fuel_type"] == "Gas"
    drop_ccs_tech = ne["technology_type"].str.contains("CCS", na=False)
    ispypsa_tables["new_entrant_generators"] = ne[~(drop_gas_tech | drop_ccs_tech)].copy()

    return add_capacity_floor(
        ispypsa_tables,
        config,
        constraint_prefix="storage_floor",
        floors_by_year=_STORAGE_FLOOR_MW_BY_YEAR,
        new_entrant_table="new_entrant_batteries",
        new_entrant_id_col="storage_name",
        new_entrant_predicate=lambda row: True,
        existing_table="ecaa_batteries",
        existing_predicate=lambda row: True,
        term_type="storage_capacity",
    )
