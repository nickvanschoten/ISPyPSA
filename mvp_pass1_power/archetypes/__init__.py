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


def _with_pumped_storage_fix(archetype_fn):
    """Wrap an archetype mutation to first re-route the four NEM pumped storage
    facilities (Wivenhoe, Shoalhaven, Borumba, Snowy 2.0) from ecaa_generators
    to ecaa_batteries — i.e. model them as PyPSA StorageUnits, not unconstrained
    Water-carrier generators. See _pumped_storage_fix.py for the data sources."""

    def wrapped(ispypsa_tables, config):
        ispypsa_tables = _pumped_storage_fix_apply(ispypsa_tables, config)
        return archetype_fn(ispypsa_tables, config)

    wrapped.__name__ = f"{archetype_fn.__name__}_with_pumped_storage_fix"
    return wrapped


APPLY_ARCHETYPE = {
    "cost_optimal":         _with_pumped_storage_fix(cost_optimal_apply),
    "rapid_coal_phaseout":  _with_pumped_storage_fix(rapid_coal_phaseout_apply),
    "gas_fleet_maintained": _with_pumped_storage_fix(gas_fleet_maintained_apply),
    "storage_led":          _with_pumped_storage_fix(storage_led_apply),
    "fossil_incumbent":     _with_pumped_storage_fix(fossil_incumbent_apply),
    "nuclear_baseload":     _with_pumped_storage_fix(nuclear_baseload_apply),
}
