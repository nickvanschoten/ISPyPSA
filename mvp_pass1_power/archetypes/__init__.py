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

Production archetypes (use these for Pass 1 runs):
  cost_optimal     — unmodified Step Change baseline
  fast_fossil_exit — coal by 2030, no new unabated gas
  gas_bridge       — coal by 2030, gas available as bridge
  storage_led      — coal by 2035, no new gas at all
  fossil_incumbent — extended coal + constrained renewable build
  nuclear_included — IASR schedule + Advanced Nuclear option

Legacy archetypes (kept for backward compatibility, not in production catalogue):
  renewables_led   — superseded by fast_fossil_exit / storage_led
  deep_clean_firmed — superseded by fast_fossil_exit
"""

from .cost_optimal import apply as cost_optimal_apply
from .fast_fossil_exit import apply as fast_fossil_exit_apply
from .gas_bridge import apply as gas_bridge_apply
from .storage_led import apply as storage_led_apply
from .fossil_incumbent import apply as fossil_incumbent_apply
from .nuclear_included import apply as nuclear_included_apply
from .renewables_led import apply as renewables_led_apply
from .deep_clean_firmed import apply as deep_clean_firmed_apply

PRODUCTION_ARCHETYPES = [
    "cost_optimal",
    "fast_fossil_exit",
    "gas_bridge",
    "storage_led",
    "fossil_incumbent",
    "nuclear_included",
]

APPLY_ARCHETYPE = {
    "cost_optimal":     cost_optimal_apply,
    "fast_fossil_exit": fast_fossil_exit_apply,
    "gas_bridge":       gas_bridge_apply,
    "storage_led":      storage_led_apply,
    "fossil_incumbent": fossil_incumbent_apply,
    "nuclear_included": nuclear_included_apply,
    # Legacy — not in PRODUCTION_ARCHETYPES but still callable.
    "renewables_led":   renewables_led_apply,
    "deep_clean_firmed": deep_clean_firmed_apply,
}
