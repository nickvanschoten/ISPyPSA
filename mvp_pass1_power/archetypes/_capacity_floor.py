"""Helper for archetype mandate constraints — per-milestone-year capacity floors.

Translates a user-facing mandate spec (a dict of {year: floor_MW}) into PyPSA
custom_constraints_lhs / custom_constraints_rhs rows. The constraint at year T
is:

    sum(p_nom of all matching extendable assets active at T) >= floor[T] - existing_at[T]

where:
  - "matching" is defined by caller-supplied predicates over the templater
    tables (new_entrant_generators / new_entrant_batteries, ecaa_generators /
    ecaa_batteries);
  - "active at T" means build_year <= T < build_year + lifetime (PyPSA's
    multi-investment-period convention);
  - the constant contribution from existing (non-extendable) assets is moved
    to the RHS so the LHS contains only decision variables, satisfying the
    existing custom_constraints framework which accepts only Generator /
    StorageUnit / Link variables, not constants.

Mandate years that don't appear in config.temporal.capacity_expansion.investment_periods
are skipped with a warning — the LP only has a capacity-state at those years.

Mandate years where existing capacity already meets or exceeds the floor are
skipped with an info log (constraint would be trivially satisfied).

For Generator-component matches use term_type='generator_capacity'; for
StorageUnit-component matches (batteries / pumped storage) use 'storage_capacity'.
The 'storage_capacity' term_type is wired into the translator and pypsa_build
custom-constraint layers as StorageUnit.p_nom.
"""

from __future__ import annotations

import logging
from typing import Callable

import pandas as pd

log = logging.getLogger(__name__)


def add_capacity_floor(
    ispypsa_tables: dict[str, pd.DataFrame],
    config,
    constraint_prefix: str,
    floors_by_year: dict[int, float],
    new_entrant_table: str,
    new_entrant_id_col: str,
    new_entrant_predicate: Callable[[pd.Series], bool],
    existing_table: str | None,
    existing_predicate: Callable[[pd.Series], bool] | None,
    term_type: str,
) -> dict[str, pd.DataFrame]:
    """Append per-year capacity floor rows to custom_constraints_lhs/rhs."""
    periods = _investment_periods(config)
    binding = _filter_to_known_periods(floors_by_year, periods, constraint_prefix)
    if not binding:
        return ispypsa_tables

    lhs_rows, rhs_rows = [], []
    for year, floor_mw in binding.items():
        existing_mw = _existing_capacity_at_year(ispypsa_tables, existing_table, existing_predicate, year)
        residual = floor_mw - existing_mw
        if residual <= 0:
            log.info(f"{constraint_prefix}_{year}: existing {existing_mw:.0f} MW already meets floor {floor_mw:.0f} MW; skipping")
            continue
        ne_terms = _new_entrant_active_terms(ispypsa_tables, new_entrant_table, new_entrant_id_col, new_entrant_predicate, periods, year)
        if ne_terms.empty:
            log.warning(f"{constraint_prefix}_{year}: residual {residual:.0f} MW required but no matching new entrants — constraint will be infeasible if applied")
            continue
        lhs_rows.append(_build_lhs(f"{constraint_prefix}_{year}", ne_terms, term_type))
        rhs_rows.append(_build_rhs(f"{constraint_prefix}_{year}", residual))

    return _append_constraint_rows(ispypsa_tables, lhs_rows, rhs_rows)


def _investment_periods(config) -> list[int]:
    """Read investment periods from config; tests can pass None and get []."""
    if config is None:
        return []
    return list(config.temporal.capacity_expansion.investment_periods)


def _filter_to_known_periods(floors_by_year: dict[int, float], periods: list[int], prefix: str) -> dict[int, float]:
    """Drop mandate years that aren't in the model's investment periods."""
    unknown = sorted(y for y in floors_by_year if y not in periods)
    if unknown:
        log.warning(f"{prefix}: mandate years {unknown} not in investment_periods {periods}; skipping those years")
    return {y: floors_by_year[y] for y in floors_by_year if y in periods}


def _existing_capacity_at_year(
    ispypsa_tables: dict[str, pd.DataFrame],
    existing_table: str | None,
    existing_predicate: Callable[[pd.Series], bool] | None,
    year: int,
) -> float:
    """Sum maximum_capacity_mw of existing assets matching predicate and active at year."""
    if existing_table is None or existing_table not in ispypsa_tables:
        return 0.0
    df = ispypsa_tables[existing_table]
    if df.empty:
        return 0.0
    active = df[df.apply(existing_predicate, axis=1) & (df["closure_year"] > year)]
    return float(active["maximum_capacity_mw"].fillna(0).sum())


def _new_entrant_active_terms(
    ispypsa_tables: dict[str, pd.DataFrame],
    new_entrant_table: str,
    id_col: str,
    predicate: Callable[[pd.Series], bool],
    periods: list[int],
    year: int,
) -> pd.DataFrame:
    """Return DataFrame with one row per (matching new entrant) × (build_year active at year)."""
    if new_entrant_table not in ispypsa_tables:
        return pd.DataFrame(columns=[id_col, "build_year", "lifetime"])
    df = ispypsa_tables[new_entrant_table]
    matching = df[df.apply(predicate, axis=1)]
    expanded = _expand_to_build_years(matching, id_col, periods)
    return expanded[(expanded["build_year"] <= year) & (expanded["build_year"] + expanded["lifetime"] > year)]


def _expand_to_build_years(matching: pd.DataFrame, id_col: str, periods: list[int]) -> pd.DataFrame:
    """One row per (asset, investment_period) — mirrors the translator's expansion."""
    rows = []
    for _, row in matching.iterrows():
        for build_year in periods:
            rows.append({id_col: row[id_col], "build_year": build_year, "lifetime": row["lifetime"]})
    return pd.DataFrame(rows, columns=[id_col, "build_year", "lifetime"])


def _build_lhs(constraint_id: str, terms: pd.DataFrame, term_type: str) -> pd.DataFrame:
    """LHS row per term: variable_name = '{id_col_value}_{build_year}'."""
    id_col = [c for c in terms.columns if c not in ("build_year", "lifetime")][0]
    term_ids = terms[id_col].astype(str) + "_" + terms["build_year"].astype(str)
    return pd.DataFrame({
        "constraint_id": constraint_id,
        "term_type": term_type,
        "term_id": term_ids.tolist(),
        "coefficient": 1.0,
    })


def _build_rhs(constraint_id: str, residual_mw: float) -> pd.DataFrame:
    """Single RHS row: floor minus already-committed existing capacity."""
    return pd.DataFrame([{"constraint_id": constraint_id, "constraint_type": ">=", "rhs": residual_mw}])


def _append_constraint_rows(
    ispypsa_tables: dict[str, pd.DataFrame],
    lhs_rows: list[pd.DataFrame],
    rhs_rows: list[pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Concat new constraint rows onto the existing custom_constraints tables."""
    if not lhs_rows:
        return ispypsa_tables
    ispypsa_tables["custom_constraints_lhs"] = _concat_with_existing(ispypsa_tables.get("custom_constraints_lhs"), lhs_rows)
    ispypsa_tables["custom_constraints_rhs"] = _concat_with_existing(ispypsa_tables.get("custom_constraints_rhs"), rhs_rows)
    return ispypsa_tables


def _concat_with_existing(existing: pd.DataFrame | None, new_rows: list[pd.DataFrame]) -> pd.DataFrame:
    """Concat caller-supplied rows onto an existing template DataFrame."""
    parts = [df for df in [existing] if df is not None and not df.empty]
    parts.extend(new_rows)
    return pd.concat(parts, ignore_index=True)
