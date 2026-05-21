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

from ._pumped_storage_fix import apply as _pumped_storage_fix_apply
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
    "cost_optimal":     _with_pumped_storage_fix(cost_optimal_apply),
    "fast_fossil_exit": _with_pumped_storage_fix(fast_fossil_exit_apply),
    "gas_bridge":       _with_pumped_storage_fix(gas_bridge_apply),
    "storage_led":      _with_pumped_storage_fix(storage_led_apply),
    "fossil_incumbent": _with_pumped_storage_fix(fossil_incumbent_apply),
    "nuclear_included": _with_pumped_storage_fix(nuclear_included_apply),
    # Legacy — not in PRODUCTION_ARCHETYPES but still callable.
    "renewables_led":    _with_pumped_storage_fix(renewables_led_apply),
    "deep_clean_firmed": _with_pumped_storage_fix(deep_clean_firmed_apply),
}
