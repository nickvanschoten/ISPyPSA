"""Recursive-dynamic capacity roll-forward for the myopic chain.

The myopic driver in `run_myopic.py` historically solved each milestone year
as an independent static greenfield problem (see PHASE8_RECONNAISSANCE.md for
the survey). The persistence probe established that locking 2040 capacity
into a 2045 solve shifts emissions ~+17% and renewable share ~-7.7 pp, so the
greenfield chain is not realisable. This module implements the missing
write-back: each year's newly-built tranche becomes part of the next year's
*existing* fleet, accumulating across the chain.

Three correctness traps the design clears (per the commission's sharpening):

1. **Current-year carbon price on carried rows.** Carried rows carry physical
   characteristics (heat_rate, residual_co2, captured_co2 — vintage-invariant
   per IASR) but reference the *base-tech* marginal_cost mapping. The
   year-(t+1) translator regenerates the marginal_cost parquet at year-(t+1)'s
   `config.carbon_pricing.carbon_price`, so a 2030-built CCGT dispatched in
   2045 pays 2045's carbon adder, not 2030's.

2. **Storage carries capacity, not state-of-charge.** Carried batteries get
   `p_nom_extendable=False` with the solved `p_nom_opt` and original
   `max_hours`; SOC is re-solved fresh by year (t+1) under PyPSA's existing
   `cyclic_state_of_charge=True` convention (see translator/storage.py:101).

3. **Additive vintage accumulation across the full chain.** Each year's
   tranche is persisted to its own file (`tranches/<year>.parquet`) and
   `load_tranches(before_year=...)` concatenates ALL surviving prior tranches.
   By 2050 a chain has up to four prior tranches; each is named after the
   originating new-entrant row (e.g. `wind_high_cwo_2030`,
   `wind_high_cwo_2035`) so the same base technology built in two different
   years yields two distinct carried rows that both persist subject to
   retirement.

Retirement: a carried row's PyPSA `lifetime` is the IASR new-entrant lifetime
(annuitisation lifetime == technical/economic operating life — they are the
same physical quantity, not a convenience coincidence). PyPSA's
`multi_investment_periods=True` active-assets check (build_year <= period
< build_year + lifetime) retires expired vintages at the model layer.
**Dependency flag**: if a future IASR ever splits annuitisation lifetime from
technical operating lifetime, this module's `extract_new_built_tranche` would
need the technical-life column; today they are the same field and inheriting
it is correct.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pypsa

_TRANCHE_SUBDIR = "tranches"
_GEN_FILENAME = "generators.parquet"
_BAT_FILENAME = "batteries.parquet"
_P_NOM_OPT_THRESHOLD_MW = 1.0


def _tranche_dir_for_year(tranches_root: Path, year: int) -> Path:
    return tranches_root / str(year)


def _extract_new_built(
    component: pd.DataFrame,
    pf_rows: pd.DataFrame,
    year: int,
    existing_names: set[str],
) -> pd.DataFrame:
    """Carry-forward rows for one component, sourced from the build-year
    pypsa_friendly table so the FULL attribute set is preserved.

    The build-year pypsa_friendly row is the source of truth (it has every
    physically-meaningful column — heat_rate, capture_rate, residual_co2,
    captured_co2, fuel_cost_mapping, vom, ...). We start from it and override
    only the three things that change on carry-forward: `p_nom` ← solved
    `p_nom_opt`, `p_nom_extendable` → False, `capital_cost` → 0.

    This deliberately does NOT copy from `network.<component>`: the PyPSA
    component table drops the `isp_*` metadata, so copying it silently zeroed
    heat_rate/capture_rate/residual on every carried row — which made carried
    fossil/CCS units mis-priced in the next solve and mis-counted (zero fuel,
    zero emissions) at extraction. Sourcing from pypsa_friendly preserves the
    whole class, not just the one observed (capture-rate) symptom.

    `existing_names` (the re-templated ECAA fleet) is excluded from the carry.
    The retirement seam (`make_existing_reducible`) turns the existing fleet
    into a downward capacity decision — `p_nom_extendable=True` with the
    period as `build_year` — so an extendable/build-year filter alone
    misattributes a retained existing unit as a new build and carries it, where
    it collides with next period's re-templated ECAA row (the duplicate-index
    halt). Only genuine new-entrant builds carry; the existing fleet reappears
    every period from IASR and must never be carried.
    """
    built_mask = (
        (component["p_nom_extendable"])
        & (component["build_year"] == year)
        & (component["p_nom_opt"] > _P_NOM_OPT_THRESHOLD_MW)
        & (~component.index.astype(str).isin(existing_names))
    )
    if "bus" in component.columns:
        built_mask &= component["bus"] != "bus_for_custom_constraint_gens"
    names = list(component.index[built_mask])
    carried = pf_rows.reindex(names).copy()  # FULL pypsa_friendly attribute set
    carried["p_nom"] = component.loc[names, "p_nom_opt"]
    carried["p_nom_extendable"] = False
    carried["capital_cost"] = 0.0
    return carried.reset_index()  # 'name' back as a column


def extract_new_built_tranche(nc_path: Path, year: int) -> dict[str, pd.DataFrame]:
    """Read a year-T solved NetCDF and return its newly-built tranche.

    Carried rows preserve the full physically-meaningful attribute set by
    sourcing from the run's build-year `pypsa_friendly` tables (siblings of the
    NetCDF), not from the attribute-stripped PyPSA component tables. The
    re-templated existing (ECAA) fleet — read from the run's `ispypsa_inputs`
    siblings — is excluded so retirement-extendable existing units are not
    misattributed as new builds. SOC is intentionally NOT carried for storage —
    `cyclic_state_of_charge=True` re-solves it fresh per year. Returns
    `generators` + `batteries` DataFrames in pypsa_friendly column shape, ready
    to concat into year-(T+1)'s dict.
    """
    network = pypsa.Network(nc_path)
    run_root = Path(nc_path).resolve().parents[1]
    pf_gens = pd.read_csv(run_root / "pypsa_friendly" / "generators.csv").set_index(
        "name"
    )
    pf_bats = pd.read_csv(run_root / "pypsa_friendly" / "batteries.csv").set_index(
        "name"
    )
    existing_gens = _read_existing_fleet_names(
        run_root, "ecaa_generators.csv", "generator"
    )
    existing_bats = _read_existing_fleet_names(
        run_root, "ecaa_batteries.csv", "storage_name"
    )
    return {
        "generators": _extract_new_built(
            network.generators, pf_gens, year, existing_gens
        ),
        "batteries": _extract_new_built(
            network.storage_units, pf_bats, year, existing_bats
        ),
    }


def _read_existing_fleet_names(
    run_root: Path, filename: str, name_col: str
) -> set[str]:
    """The re-templated existing (ECAA) fleet names for one component — the set
    the carry-forward must never carry (they reappear every period from IASR)."""
    path = run_root / "ispypsa_inputs" / filename
    return set(pd.read_csv(path)[name_col].astype(str))


def _extract_new_built_generators(
    network: pypsa.Network,
    pf_gens: pd.DataFrame,
    year: int,
    existing_names: set[str] = frozenset(),
) -> pd.DataFrame:
    """Back-compat thin wrapper (pf_gens indexed by name)."""
    return _extract_new_built(network.generators, pf_gens, year, existing_names)


def _extract_new_built_storage_units(
    network: pypsa.Network,
    pf_bats: pd.DataFrame,
    year: int,
    existing_names: set[str] = frozenset(),
) -> pd.DataFrame:
    """Back-compat thin wrapper (pf_bats indexed by name)."""
    return _extract_new_built(network.storage_units, pf_bats, year, existing_names)


def save_tranche(
    tranche: dict[str, pd.DataFrame], tranches_root: Path, year: int
) -> None:
    """Persist a year-T tranche under `tranches_root/<year>/`."""
    out_dir = _tranche_dir_for_year(tranches_root, year)
    out_dir.mkdir(parents=True, exist_ok=True)
    tranche["generators"].to_parquet(out_dir / _GEN_FILENAME, index=False)
    tranche["batteries"].to_parquet(out_dir / _BAT_FILENAME, index=False)


def _load_one_tranche(year_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        "generators": pd.read_parquet(year_dir / _GEN_FILENAME),
        "batteries": pd.read_parquet(year_dir / _BAT_FILENAME),
    }


def _list_tranche_years(tranches_root: Path) -> list[int]:
    if not tranches_root.exists():
        return []
    years = []
    for sub in sorted(tranches_root.iterdir()):
        if sub.is_dir() and sub.name.isdigit():
            years.append(int(sub.name))
    return sorted(years)


def load_tranches(tranches_root: Path, before_year: int) -> dict[str, pd.DataFrame]:
    """Concatenate all surviving tranches with build_year < `before_year`.

    Retirement filter: drop rows where `build_year + lifetime <= before_year`.
    Mirrors PyPSA's active_assets logic (period < build_year + lifetime) so a
    carried row that survives this filter will also be active under PyPSA's
    `multi_investment_periods=True` check at year `before_year`.
    """
    parts_gen: list[pd.DataFrame] = []
    parts_bat: list[pd.DataFrame] = []
    for year in _list_tranche_years(tranches_root):
        if year >= before_year:
            continue
        one = _load_one_tranche(_tranche_dir_for_year(tranches_root, year))
        parts_gen.append(one["generators"])
        parts_bat.append(one["batteries"])
    if not parts_gen and not parts_bat:
        return {"generators": pd.DataFrame(), "batteries": pd.DataFrame()}
    gens = pd.concat(parts_gen, ignore_index=True) if parts_gen else pd.DataFrame()
    bats = pd.concat(parts_bat, ignore_index=True) if parts_bat else pd.DataFrame()
    gens = _filter_retired(gens, before_year)
    bats = _filter_retired(bats, before_year)
    return {"generators": gens, "batteries": bats}


def _filter_retired(df: pd.DataFrame, period: int) -> pd.DataFrame:
    if df.empty or "build_year" not in df.columns or "lifetime" not in df.columns:
        return df
    surviving = df["build_year"] + df["lifetime"] > period
    return df.loc[surviving].reset_index(drop=True)


def inject_carried_tranches(
    pypsa_friendly: dict[str, pd.DataFrame],
    carried: dict[str, pd.DataFrame],
) -> dict[str, int | float]:
    """Append carried rows to the in-memory pypsa_friendly dict.

    CRITICAL: `build_pypsa_network` consumes the in-memory pypsa_friendly
    dict, NOT the CSV on disk. Mutating only a CSV would silently no-op the
    write-back (the original probe hit this bug — see
    PHASE8_RECONNAISSANCE.md). This function mutates the in-memory
    DataFrames in place; the caller is responsible for any CSV rewrite for
    inspectability.
    """
    diag = {
        "carried_generators": 0,
        "carried_batteries": 0,
        "carried_generator_mw": 0.0,
        "carried_battery_mw": 0.0,
    }
    gens_in = carried.get("generators", pd.DataFrame())
    if not gens_in.empty:
        aligned = _align_to_target_columns(gens_in, pypsa_friendly["generators"])
        pypsa_friendly["generators"] = pd.concat(
            [pypsa_friendly["generators"], aligned], ignore_index=True
        )
        diag["carried_generators"] = int(len(aligned))
        diag["carried_generator_mw"] = float(aligned["p_nom"].sum())
    bats_in = carried.get("batteries", pd.DataFrame())
    if not bats_in.empty and "batteries" in pypsa_friendly:
        aligned = _align_to_target_columns(bats_in, pypsa_friendly["batteries"])
        pypsa_friendly["batteries"] = pd.concat(
            [pypsa_friendly["batteries"], aligned], ignore_index=True
        )
        diag["carried_batteries"] = int(len(aligned))
        diag["carried_battery_mw"] = float(aligned["p_nom"].sum())
    return diag


def _align_to_target_columns(
    incoming: pd.DataFrame, target: pd.DataFrame
) -> pd.DataFrame:
    """Drop incoming columns the target schema does not know, fill missing
    target columns from incoming where present, else NaN."""
    out = pd.DataFrame()
    for col in target.columns:
        if col in incoming.columns:
            out[col] = incoming[col].values
        else:
            out[col] = pd.NA
    return out


# ---------------------------------------------------------------------------
# Capacity cap/floor cumulative-scope fix (the carry-forward × per-period-cap
# seam). The mvp `_capacity_floor` caps/floors (biomass_cap, the storage/nuclear/
# gas floors) list ONLY the current period's new-entrant variables
# (`{base}_{current_year}`), because they expand over config.investment_periods,
# which is a single year in the myopic chain. Earlier vintages, injected here as
# `{base}_{earlier_year}`, escape — the leak that let biomass reach ~2x its
# intended ceiling. We re-impose the cumulative bound by netting the carried
# (fixed, non-extendable) capacity off the RHS, matched to each constraint by
# shared base-name. Capacity (`p_nom`/`e_nom`) constraints only — output/flow
# security constraints (attribute `p`) are untouched. Call AFTER injection.
# ---------------------------------------------------------------------------

_CAPACITY_ATTRS = ("p_nom", "e_nom")


def _strip_vintage(name: str) -> str:
    """Drop a trailing `_YYYY` vintage suffix so a generator's vintages share a
    base (`biomass_nq_2030` / `biomass_nq_2050` -> `biomass_nq`)."""
    return re.sub(r"_\d{4}$", "", str(name))


def _carried_capacity_rows(
    pypsa_friendly: dict[str, pd.DataFrame], component: str, current_year: int
) -> pd.DataFrame:
    """Carried (earlier-vintage, non-extendable) rows of the component a constraint
    sums — `generators` for Generator terms, `batteries` for StorageUnit terms."""
    key = "batteries" if component == "StorageUnit" else "generators"
    df = pypsa_friendly.get(key)
    if df is None or df.empty or "build_year" not in df.columns:
        return pd.DataFrame(columns=["name", "p_nom"])
    carried = df[(~df["p_nom_extendable"]) & (df["build_year"] < current_year)]
    return carried[["name", "p_nom"]]


def adjust_capacity_caps_for_carried(
    pypsa_friendly: dict[str, pd.DataFrame], current_year: int
) -> dict[str, float]:
    """Net carried capacity off every capacity cap/floor RHS so it bounds the
    CUMULATIVE active fleet (carried + new), not just the current period's vintage.

    Returns {constraint_name: carried_MW_netted}. Mutates
    `custom_constraints_rhs` in place. A no-op when there are no capacity
    constraints or no matching carried capacity (e.g. the first chain year)."""
    lhs = pypsa_friendly.get("custom_constraints_lhs")
    rhs = pypsa_friendly.get("custom_constraints_rhs")
    if lhs is None or rhs is None or lhs.empty or rhs.empty:
        return {}
    capacity_terms = lhs[lhs["attribute"].isin(_CAPACITY_ATTRS)]
    adjustments: dict[str, float] = {}
    for (cname, component), terms in capacity_terms.groupby(
        ["constraint_name", "component"]
    ):
        bases = {_strip_vintage(v) for v in terms["variable_name"]}
        carried = _carried_capacity_rows(pypsa_friendly, component, current_year)
        carried_mw = float(
            carried.loc[carried["name"].map(_strip_vintage).isin(bases), "p_nom"].sum()
        )
        if carried_mw <= 0:
            continue
        mask = rhs["constraint_name"] == cname
        rhs.loc[mask, "rhs"] = (rhs.loc[mask, "rhs"] - carried_mw).clip(lower=0.0)
        adjustments[cname] = adjustments.get(cname, 0.0) + carried_mw
    return adjustments
