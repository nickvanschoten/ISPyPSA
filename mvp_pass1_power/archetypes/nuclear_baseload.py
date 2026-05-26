"""Nuclear-baseload archetype.

Pathway: IASR coal schedule unchanged. Advanced nuclear is injected into
new_entrant_generators (one row per NEM sub-region) and new_entrant_build_costs
(one technology row, constant cost). The LP may then choose nuclear alongside
all other IASR new-entrant options, subject to a phased deployment floor that
anchors to the Coalition 2024 nuclear policy reference (AEMO does not model
nuclear).

Phased deployment floor (commit B will wire as PyPSA custom constraints):
  2025-2040: no nuclear mandate (LP unconstrained)
  2045: ≥ 2,000 MW total nuclear capacity NEM-wide
  2050: ≥ 4,000 MW total nuclear capacity NEM-wide

Documented methodological limitation: under myopic period decomposition the
LP can treat nuclear capacity as commissionable in the mandate year (instant
deployment). Real nuclear construction lead time is ~10 years. The cost
trajectory under this archetype therefore represents the structural cost of
"nuclear-included" rather than a realistic deployment forecast.

Nuclear parameters (CSIRO GenCost 2024-25 Final, July 2025):
  - Build cost: 31,100,000 AUD/MW (SMR, derived from UAMPS CFPP USD 9.3bn ref;
    GenCost p.ix). Constant — no cost-reduction trajectory in MVP.
  - VOM:        10 AUD/MWh (indicative; IEA "Projected Costs of Generating
    Electricity 2020" nuclear O&M ~USD 9-15/MWh)
  - Heat rate:  0.0 GJ/MWh (nuclear fuel cost not represented in IASR fuel-cost
    tables; modelled as a "free" carrier in the LP — capital + VOM only)
  - Lifetime:   60 years (GenCost assumption)
  - Min stable: 53 % (GenCost 2024-25 baseload CF range floor, p.51)

Carrier: "Nuclear" is a first-class carrier from ISPyPSA's perspective. It is
listed in src/ispypsa/translator/generators.py:non_fuel_carriers alongside
Wind/Solar/Water so the translator routes zero-fuel-cost generators correctly.
Post-processing (mvp_pass1_power/postprocess/nger_factors.py) maps Nuclear to
zero Scope 1 combustion emissions.

Structural template:
  CCGT rows (preferred), or H2/Hyblend if CCGT absent, are used to infer the
  set of sub_region_ids and carry over connection-cost metadata.
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_NUCLEAR_TECH = "Advanced Nuclear"
_NUCLEAR_FUEL = "Nuclear"
# CSIRO GenCost 2024-25 Final report (July 2025), Table B.2.
# Capital cost is held constant across the modelling horizon; GenCost's
# central case shows essentially no cost-reduction trajectory for SMR.
_BUILD_COST_AUD_PER_MW = 31_100_000
_VOM = 10.0
_HEAT_RATE = 0.0
_MIN_STABLE = 53.0
_LIFETIME = 60

_TEMPLATE_TECH_PREFERENCE = ["CCGT", "Hydrogen Reciprocating Engine", "Hyblend Reciprocating Engine"]


def apply(ispypsa_tables, config):
    ne = _get_new_entrant_generators(ispypsa_tables)
    bc = _get_build_costs(ispypsa_tables)
    if ne is None or bc is None:
        return ispypsa_tables

    template_rows = _select_template_rows(ne)
    if template_rows.empty:
        log.warning("nuclear_baseload: no non-VRE thermal template rows found; skipping nuclear injection")
        return ispypsa_tables

    nuclear_rows = _build_nuclear_rows(template_rows, ne)
    ispypsa_tables["new_entrant_generators"] = pd.concat([ne, nuclear_rows], ignore_index=True)

    nuclear_bc_row = _build_nuclear_cost_row(bc)
    ispypsa_tables["new_entrant_build_costs"] = pd.concat([bc, nuclear_bc_row], ignore_index=True)

    log.info(f"nuclear_baseload: added Advanced Nuclear to sub-regions: {sorted(nuclear_rows['sub_region_id'].tolist())}")
    return ispypsa_tables


def _get_new_entrant_generators(ispypsa_tables):
    """Return new_entrant_generators or None if missing."""
    if "new_entrant_generators" not in ispypsa_tables:
        log.warning("nuclear_baseload: new_entrant_generators table missing; skipping")
        return None
    return ispypsa_tables["new_entrant_generators"]


def _get_build_costs(ispypsa_tables):
    """Return new_entrant_build_costs or None if missing."""
    if "new_entrant_build_costs" not in ispypsa_tables:
        log.warning("nuclear_baseload: new_entrant_build_costs table missing; skipping")
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
