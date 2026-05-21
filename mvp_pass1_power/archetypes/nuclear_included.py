"""Nuclear-included archetype.

Pathway: IASR coal schedule unchanged. Advanced nuclear is injected into
new_entrant_generators (one row per NEM sub-region) and new_entrant_build_costs
(one technology row, constant cost). The LP may then choose nuclear alongside
all other IASR new-entrant options.

Nuclear parameters (CSIRO GenCost 2023-24):
  - Build cost: 9,000,000 AUD/MW (constant — no cost reduction trajectory)
  - VOM:        10 AUD/MWh
  - Heat rate:  0.0 GJ/MWh (combustion emissions = zero; nuclear fission)
  - Lifetime:   60 years
  - Min stable: 50 %

Structural template:
  CCGT rows (preferred), or H2/Hyblend if CCGT absent, are used to infer the
  set of sub_region_ids and carry over connection-cost metadata.
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_NUCLEAR_TECH = "Advanced Nuclear"
# ISPyPSA's translator only supports fuel-cost lookup for carriers in
# _CARRIER_TO_FUEL_COST_TABLES (Gas, Coal, …) or the hardcoded non_fuel_carriers
# list (Wind, Water, Solar). "Nuclear" appears in neither, so using it directly
# raises KeyError in create_pypsa_friendly_dynamic_marginal_costs. Setting
# fuel_type to "Water" routes nuclear through the non_fuel path (zero fuel cost,
# fully dispatchable), which is correct for the LP. The generator names and
# technology_type remain "Advanced Nuclear" for post-processing identification.
_NUCLEAR_FUEL = "Water"
_BUILD_COST_AUD_PER_MW = 9_000_000
_VOM = 10.0
_HEAT_RATE = 0.0
_MIN_STABLE = 50.0
_LIFETIME = 60

_TEMPLATE_TECH_PREFERENCE = ["CCGT", "Hydrogen Reciprocating Engine", "Hyblend Reciprocating Engine"]


def apply(ispypsa_tables, config):
    ne = _get_new_entrant_generators(ispypsa_tables)
    bc = _get_build_costs(ispypsa_tables)
    if ne is None or bc is None:
        return ispypsa_tables

    template_rows = _select_template_rows(ne)
    if template_rows.empty:
        log.warning("nuclear_included: no non-VRE thermal template rows found; skipping nuclear injection")
        return ispypsa_tables

    nuclear_rows = _build_nuclear_rows(template_rows, ne)
    ispypsa_tables["new_entrant_generators"] = pd.concat([ne, nuclear_rows], ignore_index=True)

    nuclear_bc_row = _build_nuclear_cost_row(bc)
    ispypsa_tables["new_entrant_build_costs"] = pd.concat([bc, nuclear_bc_row], ignore_index=True)

    log.info(f"nuclear_included: added Advanced Nuclear to sub-regions: {sorted(nuclear_rows['sub_region_id'].tolist())}")
    return ispypsa_tables


def _get_new_entrant_generators(ispypsa_tables):
    """Return new_entrant_generators or None if missing."""
    if "new_entrant_generators" not in ispypsa_tables:
        log.warning("nuclear_included: new_entrant_generators table missing; skipping")
        return None
    return ispypsa_tables["new_entrant_generators"]


def _get_build_costs(ispypsa_tables):
    """Return new_entrant_build_costs or None if missing."""
    if "new_entrant_build_costs" not in ispypsa_tables:
        log.warning("nuclear_included: new_entrant_build_costs table missing; skipping")
        return None
    return ispypsa_tables["new_entrant_build_costs"]


def _select_template_rows(ne):
    """Pick the best available thermal template rows, one per sub_region_id."""
    for tech in _TEMPLATE_TECH_PREFERENCE:
        candidates = ne[ne["technology_type"] == tech]
        if not candidates.empty:
            return candidates.drop_duplicates(subset=["sub_region_id"])
    return pd.DataFrame()


def _build_nuclear_rows(template_rows, ne):
    """Construct one nuclear new-entrant row per sub_region_id from template."""
    nuclear_rows = template_rows.copy()
    nuclear_rows["generator_name"] = _NUCLEAR_TECH
    nuclear_rows["generator"] = _NUCLEAR_TECH + " " + nuclear_rows["sub_region_id"]
    nuclear_rows["technology_type"] = _NUCLEAR_TECH
    nuclear_rows["fuel_type"] = _NUCLEAR_FUEL
    nuclear_rows["heat_rate_gj/mwh"] = _HEAT_RATE
    nuclear_rows["vom_$/mwh_sent_out"] = _VOM
    nuclear_rows["minimum_stable_level_%"] = _MIN_STABLE
    nuclear_rows["lifetime"] = _LIFETIME
    nuclear_rows["rez_id"] = np.nan
    nuclear_rows["fuel_cost_mapping"] = np.nan
    if "connection_cost_technology" in nuclear_rows.columns:
        nuclear_rows["connection_cost_technology"] = "CCGT"
    return nuclear_rows


def _build_nuclear_cost_row(bc):
    """Construct a new_entrant_build_costs row for Advanced Nuclear."""
    year_cols = [c for c in bc.columns if c.endswith("_$/mw")]
    cost_values = {col: _BUILD_COST_AUD_PER_MW for col in year_cols}
    return pd.DataFrame([{"technology": _NUCLEAR_TECH, **cost_values}])
