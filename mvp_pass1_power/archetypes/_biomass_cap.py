"""Phase 7.0 (b): biomass availability cap as a methodology improvement.

The Phase 6 production run surfaced biomass dispatch ~70× current Australian
consumption (135.9 TWh from 16.1 GW capacity at rapid_coal_phaseout 2050).
The IASR-default biomass economics ($0.66/GJ fuel cost, unconstrained
p_max_pu=1.0) make biomass structurally the cheapest firm-capacity option
whenever coal is forced out without a storage mandate. Phase 1's hypothesis
that NEM-wide multi-region diversity would mitigate the NSW-smoke-peak
biomass preference was rejected — diversity didn't help.

This module adds an Australian-scale biomass availability cap as a
PyPSA custom_constraint, applied to every archetype as a pre-pass. The
cap represents the real biomass fuel supply chain constraint that the
IASR baseline doesn't enforce.

Caps (NEM-wide biomass new-entrant capacity, MW):

  2025: 1,000      — ~current Australian biomass-for-electricity baseline
  2030: 1,500      — early-deployment growth
  2035: 2,000
  2040: 3,000
  2045: 4,000
  2050: 5,000      — ARENA Bioenergy Roadmap 2021 ambitious upper bound

At ~90 % CF the 2050 cap of 5 GW corresponds to ~39 TWh annual generation —
still optimistic relative to the ~5-15 TWh range AEMO/industry projections
suggest, but a defensible Pass-1 MVP ceiling. The cap binds the unrealistic
LP outcomes (16 GW in rapid_coal_phaseout, 12 GW in nuclear_baseload, 10 GW
in fossil_incumbent) while leaving room for archetype-specific
differentiation.

There is no existing biomass capacity in the IASR ECAA cache (verified at
the ecaa_generators templater output stage), so the cap applies only to
new-entrant biomass rows. 12 sub-region biomass new entrants (one per NEM
sub-region) sum into the LHS per milestone year.

Sources:
  - ARENA Bioenergy Roadmap 2021 — 4-7 GW upper-bound for bioenergy-for-
    electricity by 2050 across all bioenergy categories.
  - AEMO ISP 2024 Step Change technology projections — modest biomass
    deployment baseline (<1 GW capacity through 2050).
  - Clean Energy Council 2024 Australian Renewable Energy Investment
    Report — current Australian biomass-for-electricity capacity ~1 GW.

Documented Pass-1 limitations:
  1. This is a CAPACITY cap, not a strict fuel-availability cap. A
     methodologically rigorous approach would constrain annual generation
     (TWh) directly via a snapshot-weighted Generator.p sum constraint.
     The custom_constraints framework doesn't currently expose
     time-weighted generation summations as LHS terms; that's a
     framework extension for Pass 3.
  2. The cap is uniform across sub-regions. Real Australian biomass
     supply chain constraints differ by region (e.g. forestry residues
     concentrated in VIC/TAS, sugarcane bagasse in QLD). A regional
     cap is also a Pass-3 refinement.
  3. The numerical values are MVP-defensible upper bounds; the team
     conversation may revise these against more recent IASR or ARENA
     publications.

Applied as a pre-pass on every archetype via the registry's
_with_pre_passes wrapper so all six archetypes see consistent biomass
availability economics.
"""

from __future__ import annotations

import logging

import pandas as pd

from mvp_pass1_power.archetypes._capacity_floor import add_capacity_cap

log = logging.getLogger(__name__)


_BIOMASS_CAP_MW_BY_YEAR = {
    2025: 1_000,
    2030: 1_500,
    2035: 2_000,
    2040: 3_000,
    2045: 4_000,
    2050: 5_000,
}


def apply(ispypsa_tables: dict[str, pd.DataFrame], config) -> dict[str, pd.DataFrame]:
    """Add a biomass-capacity ceiling per milestone year to custom_constraints."""
    return add_capacity_cap(
        ispypsa_tables,
        config,
        constraint_prefix="biomass_cap",
        caps_by_year=_BIOMASS_CAP_MW_BY_YEAR,
        new_entrant_table="new_entrant_generators",
        new_entrant_id_col="generator",
        new_entrant_predicate=lambda row: row.get("fuel_type") == "Biomass",
        existing_table="ecaa_generators",
        existing_predicate=lambda row: row.get("fuel_type") == "Biomass",
        term_type="generator_capacity",
    )
