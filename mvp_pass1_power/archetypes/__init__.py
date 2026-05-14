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
"""

from .cost_optimal import apply as cost_optimal_apply
from .renewables_led import apply as renewables_led_apply
from .fossil_incumbent import apply as fossil_incumbent_apply
from .deep_clean_firmed import apply as deep_clean_firmed_apply

APPLY_ARCHETYPE = {
    "cost_optimal": cost_optimal_apply,
    "renewables_led": renewables_led_apply,
    "fossil_incumbent": fossil_incumbent_apply,
    "deep_clean_firmed": deep_clean_firmed_apply,
}
