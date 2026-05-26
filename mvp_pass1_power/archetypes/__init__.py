"""Archetype registry — maps archetype id to a mutation function.

Each archetype mutation function takes (ispypsa_tables: dict[str, DataFrame],
config) and returns a (possibly mutated) ispypsa_tables dict. Mutations
typically edit `new_entrant_generators`, append rows to `custom_constraints_lhs`
/ `custom_constraints_rhs`, or tweak `expected_closure_years` to enforce the
archetype's structural pathway.

Design choice: archetypes act at the ISPyPSA-input layer (CSVs between templater
and translator) rather than the PyPSA-friendly layer. This keeps mutations
expressed in ISP-domain units (technology names, REZ ids, financial-year shares)
rather than PyPSA bus/generator names.

Production archetypes (Phase 2 six-archetype catalogue):
  cost_optimal         — unmodified Step Change baseline
  rapid_coal_phaseout  — coal retired by 2030; gas remains available (no mandate)
  gas_fleet_maintained — coal retired by 2030; gas ≥ 12,500 MW @ 2030 & 2035
  storage_led          — coal retired by 2035; no gas; storage ≥ 1.25× AEMO per year
  fossil_incumbent     — coal life +10y; constrained renewable build (structural upper bound)
  nuclear_baseload     — Coalition 2024 phased nuclear ≥ 2,000 MW @ 2045, ≥ 4,000 MW @ 2050

Deployment mandates anchor against AEMO's published 2024 ISP Step Change
projections rather than the team's own cost_optimal output, so the archetypes
read as alternative policy pathways relative to a public authoritative source.
"""

from ._pumped_storage_fix import apply as _pumped_storage_fix_apply
from ._maintenance_overlay import apply as _maintenance_overlay_apply
from ._repowering import apply as _repowering_apply
from ._biomass_cap import apply as _biomass_cap_apply
from .cost_optimal import apply as cost_optimal_apply
from .rapid_coal_phaseout import apply as rapid_coal_phaseout_apply
from .gas_fleet_maintained import apply as gas_fleet_maintained_apply
from .storage_led import apply as storage_led_apply
from .fossil_incumbent import apply as fossil_incumbent_apply
from .nuclear_baseload import apply as nuclear_baseload_apply

PRODUCTION_ARCHETYPES = [
    "cost_optimal",
    "rapid_coal_phaseout",
    "gas_fleet_maintained",
    "storage_led",
    "fossil_incumbent",
    "nuclear_baseload",
]


def _with_pre_passes(archetype_fn):
    """Wrap an archetype mutation with four cross-archetype pre-passes:

      1. Pumped-storage fix — re-route Wivenhoe / Shoalhaven / Borumba / Snowy 2.0
         from ecaa_generators to ecaa_batteries so they are modelled as PyPSA
         StorageUnits, not unconstrained Water-carrier generators.
         See _pumped_storage_fix.py for the data sources.

      2. Option B ageing-fleet maintenance overlay — adds a per-row ageing
         premium to fom_$/kw/annum on ECAA thermal generators in their final
         years of operation, anchored against published refurbishment cost
         references. See _maintenance_overlay.py for sources and methodology.

      3. EOL renewable repowering — extends ECAA wind / solar closure_year by
         20 years and adds an annualised repowering capex premium to
         fom_$/kw/annum. Addresses the 2045 wind capacity dip observed in
         prior production runs. See _repowering.py for sources, methodology,
         and Pass-1 limitations.

      4. Phase 7.0 biomass availability cap — adds a per-milestone-year
         NEM-wide biomass capacity ceiling as a PyPSA custom_constraint.
         Methodology refinement after Phase 6 surfaced biomass dispatch
         ~70x current Australian usage. ARENA Bioenergy Roadmap 2021 +
         AEMO ISP 2024 baseline. See _biomass_cap.py.

    Pre-passes run on EVERY archetype so all six see consistent baseline
    accounting before per-archetype mutations are applied.
    """

    def wrapped(ispypsa_tables, config):
        ispypsa_tables = _pumped_storage_fix_apply(ispypsa_tables, config)
        ispypsa_tables = _maintenance_overlay_apply(ispypsa_tables, config)
        ispypsa_tables = _repowering_apply(ispypsa_tables, config)
        ispypsa_tables = _biomass_cap_apply(ispypsa_tables, config)
        return archetype_fn(ispypsa_tables, config)

    wrapped.__name__ = f"{archetype_fn.__name__}_with_pre_passes"
    return wrapped


APPLY_ARCHETYPE = {
    "cost_optimal":         _with_pre_passes(cost_optimal_apply),
    "rapid_coal_phaseout":  _with_pre_passes(rapid_coal_phaseout_apply),
    "gas_fleet_maintained": _with_pre_passes(gas_fleet_maintained_apply),
    "storage_led":          _with_pre_passes(storage_led_apply),
    "fossil_incumbent":     _with_pre_passes(fossil_incumbent_apply),
    "nuclear_baseload":     _with_pre_passes(nuclear_baseload_apply),
}
