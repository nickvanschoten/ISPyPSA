"""Translate IASR table column names from older workbook versions into the
canonical v7.4 schema.

The team's policy is that templater code is **version-naive with respect to
column names**: every consumer of an IASR table dict expects v7.4 column
conventions. The job of this module is to translate older versions (currently
v6.0) into v7.4 form at a single chokepoint — the cache-build step in
`local_cache.build_local_cache`.

Scope limit (deliberate):
    Only pure column **renames** belong here — cases where v6.0 and v7.4
    columns mean the same thing and just have different headers. Structural
    differences (column count changes, new identifier columns, restructured
    cost panels, consolidated tables) are NOT handled here; those are dealt
    with via templater logic changes and version-aware table-name dispatch.
    See `iasr-schema-normalisation-design` for the rationale.

The transform map is keyed by **table name** (the IASR table key as
produced by isp-workbook-parser). For each table, the value is a
`{v6.0 column name: v7.4 column name}` dict. Tables not present in the map
are passed through unchanged.

Adding a new rename: confirm the columns are semantically the same in both
versions, then add an entry below with a one-line comment noting *what* the
column represents (so a future reader can audit the choice without diffing
two workbooks).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


# v6.0 → v7.4 table-name renames. These cover cases where AEMO renamed a
# table between versions but the semantic content is the same (or a
# v7.4-superset that the templater can still consume by selecting columns).
#
# Structural restructures (multiple v6.0 tables consolidated into one
# region-keyed v7.4 table, e.g. the H2 GPG emissions reduction tables) are
# NOT handled here — the data shapes differ enough that they need templater-
# side logic changes, not a simple file rename.
_V60_TO_V74_TABLE_RENAMES: dict[str, str] = {
    "initial_build_limits": "initial_resource_limits",
    # v7.4 consolidated the v6.0 `outages_2023-2024`, `full_outages_forecast`,
    # and `partial_outages_forecast` per-fuel-type outage tables into one
    # long-form `other_outages` table (with a Property column and per-year
    # value columns). The minimum-overlap mapping for the templater's
    # current single-value-per-property consumption is v6.0's
    # `outages_2023-2024_existing_generators` → v7.4's
    # `other_outages_existing_generators` (which `pivot_v74_other_outages_to_wide`
    # then collapses into a wide single-year shape for the templater).
    "outages_2023-2024_existing_generators": "other_outages_existing_generators",
}


# Per-table v6.0 → v7.4 column renames. Each rename below has been confirmed
# by comparing v6.0 and v7.4 caches built from the same templater contract:
# the column carries the same semantic content in both versions, AEMO just
# changed the header text between releases.
_V60_TO_V74_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "sub_regional_reference_nodes": {
        "NEM Region": "NEM region",
        "ISP Sub-region": "ISP sub-region",
        "Sub-region Reference Node": "Sub-regional reference node",
    },
    "regional_reference_nodes": {
        "NEM Region": "NEM region",
        "ISP Sub-region": "ISP sub-region",
        # v6.0 distinguished "Regional Reference Node" (in this table) from
        # "Sub-region Reference Node" (in sub_regional_reference_nodes).
        # v7.4 uses "Sub-regional reference node" in both tables.
        "Regional Reference Node": "Sub-regional reference node",
    },
    "renewable_energy_zones": {
        "NEM Region": "NEM region",
        "ISP Sub-region": "ISP sub-region",
        # NTNDP Zone and Regional Cost Zones existed in v6.0 but are not in v7.4
        # and are not referenced by templater code — no rename needed; the
        # columns are passed through and ignored by downstream code.
    },
    # NOTE: this table is also FILE-renamed to `initial_resource_limits.csv`
    # on disk (see `_V60_TO_V74_TABLE_RENAMES`). The column rename below is
    # keyed by the v6.0 table name because the rename happens before the
    # file rename.
    #
    # v6.0 carried both the resource limits (wind/solar capacity) and the
    # REZ-to-grid transmission limits in this single table. v7.x split them
    # into two tables — `initial_resource_limits` (resource cols only) +
    # `initial_transmission_limits` (transmission cols). For v6.0 caches
    # the transmission cols remain in the same file and the templater
    # falls back to reading them from `initial_resource_limits` when no
    # separate `initial_transmission_limits` table is present.
    "initial_build_limits": {
        "ISP Sub-region": "ISP sub-region",
        # Casing shifts on the REZ transmission limit columns:
        "REZ Transmission Network limit_Peak demand": "REZ transmission network limit_Peak demand",
        "REZ Transmission Network limit_Summer Typical": "REZ transmission network limit_Summer typical",
        "REZ Transmission Network limit_Winter Reference": "REZ transmission network limit_Winter reference",
        # v7.4 made the Tranche 1 suffix explicit (v6.0 left the first
        # tranche unsuffixed). Templater downstream uses the Tranche 1
        # form as canonical.
        "Indicative transmission expansion cost ($M/MW)": "Indicative transmission expansion cost ($M/MW)_Tranche 1",
    },
    # Class (c4c) — per-property ECAA tables that were already consolidated in
    # v6.0 but use a different identifier column name. v7.4 keys these tables
    # by per-unit `IASR ID` + power-station name; v6.0 keys them by the
    # power-station name only (under the header `Generator`). The rename
    # below brings the v6.0 identifier into v7.4-canonical form so the
    # templater's static property lookups work against both. Verified
    # 2026-05-24: v7.4 per-unit rows for a given Power Station all carry
    # the same value, so the post-rename lookup is unambiguous.
    "fixed_opex_existing_committed_anticipated_additional_generators": {
        "Generator": "Power Station",
    },
    "heat_rates_existing_committed_anticipated_additional_generators": {
        "Generator": "Power Station",
    },
    "variable_opex_existing_committed_anticipated_additional_generators": {
        # v7.4 uses "Power Station / Technology" for this table specifically
        # (the value can be either a station name or a technology label).
        "Generator": "Power Station / Technology",
    },
    # coal_minimum_stable_level granularity shift (per design call 2026-05-24).
    # v6.0 carried a single `Minimum Stable Level (MW)` value per generating
    # unit. v7.4 publishes three sub-columns:
    #   - `_IASR 2023 (Backcasting)` — historical operational behaviour
    #   - `_Typical Lowest Band` — operational behaviour narrower than the
    #     technical floor
    #   - `_Minimum Continuous Operating Level` — the technical floor below
    #     which the unit cannot operate
    # The capacity-expansion model needs the technical floor (third). v6.0's
    # single value is renamed to that canonical name so the templater uses
    # consistent column names regardless of source. The other two v7.4
    # sub-columns are passed through unused (documented in the methodology
    # tab when Phase 1 closes).
    "coal_minimum_stable_level": {
        "Generator Station": "Power Station",
        "Generating unit": "IASR ID",
        "Minimum Stable Level (MW)": "Minimum Stable Level (MW)_Minimum Continuous Operating Level",
    },
    # Outages property names (per design call 2026-05-24). v6.0 wrapped these
    # under a "Forced Outage Rate (%)_" / "Mean time to repair (hrs)_" prefix;
    # v7.4's `Property` column publishes them in canonical short form. Templater
    # column lookups updated to use the v7.4 names. Applied to the v6.0 file
    # before it's renamed to `other_outages_existing_generators.csv` (see
    # `_V60_TO_V74_TABLE_RENAMES`).
    "outages_2023-2024_existing_generators": {
        "Forced Outage Rate (%)_Full outage (% of time)": "Full outage (% of time)",
        "Forced Outage Rate (%)_Partial outage (% of time)": "Partial outage (% of time)",
        "Mean time to repair (hrs)_Full outage": "Full outage MTTR (hrs)",
        "Mean time to repair (hrs)_Partial outage": "Partial outage MTTR (hrs)",
    },
    # gpg_min_stable_level identifier columns — same casing/naming shift as
    # coal_minimum_stable_level. v6.0's `Generator Station` (station name)
    # and `Generating Unit` (per-unit IASR ID) → v7.4's `Power Station` +
    # `IASR ID`.
    "gpg_min_stable_level_existing_generators": {
        "Generator Station": "Power Station",
        "Generating Unit": "IASR ID",
    },
    # expected_closure_years uses different identifier columns between
    # versions: v6.0's `Generator name` / `DUID` → v7.4's `Power Station` /
    # `IASR ID`. Templater downstream looks up by Power Station (snakecased).
    "expected_closure_years": {
        "Generator name": "Power Station",
        "DUID": "IASR ID",
    },
    # new_entrants_summary: v6.0's `New entrants` first column carries the
    # technology label (e.g. "OCGT (small GT)"). v7.4 publishes the same
    # technology label under `Technology Type` and adds a `Power Station`
    # column with composite per-(sub-region × tech) names (e.g. "NNSW CCGT").
    # Renaming v6.0's `New entrants` → `Technology Type` keeps the templater
    # version-naive: it consistently uses Technology Type as the property-table
    # lookup key and as the build_costs.technology merge key.
    # Also renames the connection-cost zone identifier (`Connection cost_REZ/Region`
    # → `Connection cost_Region`) — v7.4 dropped the `REZ/` prefix.
    # v6.0's separate `Technology type` (lowercase 't') column held a broader
    # category label (e.g. "Wind" where the deployment label is
    # "Wind - offshore (fixed)") and would collide with `Technology Type`
    # under snake_case (both → `technology_type`). v7.4 doesn't carry the
    # category column at all. Renaming to `Technology category` keeps the
    # data accessible for any future use while preventing the snake-case
    # collision in storage.py and similar downstream consumers.
    "new_entrants_summary": {
        "New entrants": "Technology Type",
        "Technology type": "Technology category",
        "Connection cost_REZ/Region": "Connection cost_Region",
    },
    # === Sweep 2026-05-24: v6.0 → v7.4 mechanical identifier renames ===
    # Per-table audit confirms these are pure renames (v7.4 keeps the same
    # semantic content under different headers). Templater table_lookup
    # values updated to the v7.4 canonical names.
    "maintenance_new_entrants": {
        "Generator type": "Technology Type",
    },
    "seasonal_ratings_new_entrants": {
        "Generator type": "Technology Type",
    },
    "maximum_capacity_new_entrants": {
        "Generator type": "Technology Type",
    },
    "outages_new_entrants": {
        "Forced Outage Rate (%)_Full outage (% of time)": "Full outage (% of time)",
        "Forced Outage Rate (%)_Partial outage (% of time)": "Partial outage (% of time)",
        "Mean time to repair (hrs)_Full outage": "Full outage MTTR (hrs)",
        "Mean time to repair (hrs)_Partial outage": "Partial outage MTTR (hrs)",
    },
    "marginal_loss_factors_new_entrants": {
        "Generator": "IASR ID",
    },
    "lead_time_and_project_life": {
        # v6.0 carried footnote markers in the headers; v7.4 stripped some
        # but added a different one (`4,`) to Total lead time — canonical
        # form has no footnote text.
        "Economic life (years) 5": "Economic life (years)",
        "Technical life (years) 6": "Technical life (years)",
    },
    "locational_cost_factors": {
        # v7.4 added REZ-level granularity to the cost-zone identifier.
        "Cost zones": "Cost zone / REZ ID",
        # NOTE: the v6.0→v7.4 shift renamed "Equipment costs" to
        # "Equipment and installation costs", but we DON'T mirror that
        # rename here — `_calculate_and_merge_tech_specific_lcfs` aligns
        # `locational_cost_factors` columns to `technology_cost_breakdown_ratios`
        # via fuzzy matching, and `breakdown_ratios` keeps "Equipment costs"
        # in both versions. Renaming locational_cost_factors would break the
        # alignment for v6.0 (where the two were exact matches). v7.4's
        # in-built mismatch is handled by the templater's fuzzy matcher.
    },
    "technology_specific_lcfs": {
        # Same cost-zone identifier shift as locational_cost_factors. v7.4
        # also adds a `REZ name / Description` column (passes through unused).
        "Cost zones / Sub-region": "Cost zone / REZ ID",
    },
    # Same denominator shift as ECAA aux_load (% of nameplate → % of
    # generation). Per design call 2026-05-24: v7.4 framing adopted;
    # accuracy difference is small for typical load periods.
    "auxiliary_load_new_entrants": {
        "Auxiliary load (% of nameplate capacity)": "Auxiliary load (% of generation)",
    },
    # v6.0 coal_minimum_stable_level uses a flat layout (`Generating unit`,
    # `Minimum Stable Level (MW)`); v7.4 keys on IASR ID and adds the
    # `Minimum Continuous Operating Level` sub-column distinction (per design
    # call 2026-05-21). Forward-rename so the templater can use v7.4 form.
    "coal_minimum_stable_level": {
        "Generating unit": "IASR ID",
        "Minimum Stable Level (MW)": "Minimum Stable Level (MW)_Minimum Continuous Operating Level",
    },
    # connection_costs_other: v7.4 reformatted several technology column
    # headers (mechanical spacing / casing standardisation). Listed below
    # are the renames with clear v6.0 ↔ v7.4 analogues. Semantic-shift
    # cases (Hydrogen Reciprocating engines, Reciprocating engines, etc.)
    # are deliberately NOT in this map — they're surfaced for team review
    # because v7.4 may have removed/restructured rather than renamed.
    "connection_costs_other": {
        "Small OCGT2": "OCGT (small GT)",
        "Large OCGT": "OCGT (large GT)",
        # NB: capital S in "Battery Storage" — matches the casing used in
        # `Connection cost_Technology` cells in new_entrants_summary so the
        # post-melt key alignment succeeds without fuzzy-match dependency.
        "1 hr Battery Storage": "Battery Storage (1hr storage)",
        "2 hr Battery Storage": "Battery Storage (2hrs storage)",
        "4 hr Battery Storage": "Battery Storage (4hrs storage)",
        "8 hr Battery Storage": "Battery Storage (8hrs storage)",
        "BOTN- Cethana": "BOTN - Cethana",
        "Pumped Hydro (24 hrs storage)": "Pumped Hydro (24hrs storage)",
        "Pumped Hydro (48 hrs storage)": "Pumped Hydro (48hrs storage)",
    },
}


# v7.4-side normalisations: cases where v7.4 introduced a footnote/typo that
# the canonical form should not have. Applied regardless of source version
# (the rename is a no-op if the source column name doesn't appear).
# Per-(table, column) v6.0 → v7.4 cell-value renames. v6.0 summary tables
# carried tech-name lookup strings (e.g. "1 hr Battery Storage") that key
# into the v6.0 column headers of property tables. After renaming those
# column headers to v7.4 canonical form, the cell values in the summary
# tables also need updating so the post-melt joins succeed.
_V60_TO_V74_CELL_VALUE_RENAMES: dict[tuple[str, str], dict[str, str]] = {
    ("new_entrants_summary", "Connection cost_Technology"): {
        "Small OCGT2": "OCGT (small GT)",
        "Large OCGT": "OCGT (large GT)",
        "1 hr Battery Storage": "Battery Storage (1hr storage)",
        "2 hr Battery Storage": "Battery Storage (2hrs storage)",
        "4 hr Battery Storage": "Battery Storage (4hrs storage)",
        "8 hr Battery Storage": "Battery Storage (8hrs storage)",
        "Pumped Hydro (24 hrs storage)": "Pumped Hydro (24hrs storage)",
        "Pumped Hydro (48 hrs storage)": "Pumped Hydro (48hrs storage)",
        "BOTN- Cethana": "BOTN - Cethana",
    },
}


_V74_TO_CANONICAL_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "lead_time_and_project_life": {
        # v7.4 appended a "4," footnote text to this column header.
        "Total lead time (years)4,": "Total lead time (years)",
    },
    # v7.4 connection_costs_other uses inconsistent casing for battery tech
    # column headers ("Battery storage" lowercase for 1-4hrs, "Battery Storage"
    # capital S for 8hrs). new_entrants_summary uses capital S consistently in
    # `Connection cost_Technology` values. Standardise the column headers to
    # capital S so post-melt key alignment is clean.
    "connection_costs_other": {
        "Battery storage (1hr storage)": "Battery Storage (1hr storage)",
        "Battery storage (2hrs storage)": "Battery Storage (2hrs storage)",
        "Battery storage (4hrs storage)": "Battery Storage (4hrs storage)",
    },
    # v7.4 splits gas_prices into per-status tables with different first
    # column headers (`Generator` for existing, `New generating stations`
    # for new entrants). Templater concats them and expects a single
    # identifier column.
    "gas_prices_new_entrants": {
        "New generating stations": "Generator",
    },
}


def normalise_columns_to_v74(
    iasr_tables: dict[str, pd.DataFrame],
    source_version: str,
) -> dict[str, pd.DataFrame]:
    """Return `iasr_tables` with column names translated to v7.4 canonical form.

    Args:
        iasr_tables: dict of `{table_name: DataFrame}` as produced by
            `isp-workbook-parser` (or the equivalent CSV cache).
        source_version: workbook version string (e.g. "6.0", "7.4").

    Returns:
        New dict (same keys, same DataFrame instances for tables that needed
        no rename; renamed copies for tables that did). DataFrames are
        renamed in place via `pd.DataFrame.rename`, which returns a copy so
        the caller's originals are untouched.
    """
    if source_version.startswith("7."):
        return iasr_tables
    return {
        name: _rename_v60_columns(df, name) for name, df in iasr_tables.items()
    }


def _rename_v60_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    renames = _V60_TO_V74_COLUMN_RENAMES.get(table_name)
    if not renames:
        return df
    return df.rename(columns=renames)


# v6.0 → v7.4 generator summary consolidation.
#
# v6.0 has four per-status summary tables (existing/committed/anticipated/
# additional), each with a status-named first column that holds the human-
# readable power-station name. v7.4 consolidates them into one table with
# columns including `Power Station` (the name) + `Status` (the per-row
# status). The v6.0 tables already carry a `Status` column too, so the
# consolidation is mostly a concat + uniform-renaming of the first column.
_V60_GENERATOR_SUMMARY_SOURCES: dict[str, str] = {
    # source v6.0 table name → first-column header (the human name column).
    # Batteries are included here because v7.4's consolidated table folds
    # batteries in via a technology_type column rather than a separate
    # batteries summary table — matching that shape keeps storage.py
    # version-naive.
    "existing_generators_summary": "Existing generator",
    "committed_generators_summary": "Committed generator",
    "anticipated_projects_summary": "Anticipated projects",
    "additional_projects_summary": "Additional projects",
    "batteries_summary": "Batteries",
}
_V74_CONSOLIDATED_GENERATOR_SUMMARY = (
    "existing_committed_anticipated_additional_generator_summary"
)


_V74_MAX_CAPACITY_CONSOLIDATED = (
    "maximum_capacity_existing_committed_anticipated_additional_generators"
)
# v6.0 per-status maximum_capacity table → (status_label, identifier_column).
# v7.4 keys the consolidated table by `Power Station`; v6.0's tables use
# `Generator` (existing/committed/anticipated), `Project` (additional), or
# `Storage` (batteries). For the batteries table the per-row `Project status`
# column already encodes status — passing None as the status_label here
# signals to read it from that column instead of hardcoding.
_V60_MAX_CAPACITY_SOURCES: dict[str, tuple[str | None, str]] = {
    "maximum_capacity_existing_generators": ("Existing", "Generator"),
    "maximum_capacity_committed_generators": ("Committed", "Generator"),
    "maximum_capacity_anticipated_projects": ("Anticipated", "Generator"),
    "maximum_capacity_additional_projects": ("Additional", "Project"),
    "maximum_capacity_existing_committed_and_anticipated_batteries": (None, "Storage"),
}


def consolidate_v60_maximum_capacity_tables(cache_path: Path) -> None:
    """Concat v6.0's four per-status `maximum_capacity_*` tables into the v7.4
    consolidated form with explicit column alignment.

    Column alignment (per design call 2026-05-24):
    - Identifier: existing/committed/anticipated `Generator` and additional's
      `Project` both → `Power Station`.
    - `Policy`: kept as a separate column with null for non-additional rows
      (matches v7.4 structure where additional projects carry a policy ID).
    - `Commissioning date` and `Indicative commissioning date`: kept as
      separate columns — committed rows have the former populated;
      anticipated/additional rows have the latter populated. Preserves
      the confirmed-vs-indicative distinction the v6.0 data carried.
    - `Energy (MWh)` (v6.0, additional only) → `Storage Capacity (MWh)`
      (v7.4 naming). Null for non-additional statuses.
    - `Status`: derived from source table name ("Existing"/"Committed"/etc.)

    No-op if none of the source files are present (already consolidated).
    """
    frames = []
    for source_name, (status, id_col) in _V60_MAX_CAPACITY_SOURCES.items():
        source_path = cache_path / f"{source_name}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        df = df.rename(
            columns={id_col: "Power Station", "Energy (MWh)": "Storage Capacity (MWh)"}
        )
        if status is None:
            # Batteries table — status is per-row in the `Project status` column.
            df = df.rename(columns={"Project status": "Status"})
        else:
            df["Status"] = status
        frames.append(df)
    if not frames:
        return
    consolidated = pd.concat(frames, axis=0, ignore_index=True)
    consolidated.to_csv(
        cache_path / f"{_V74_MAX_CAPACITY_CONSOLIDATED}.csv", index=False
    )
    for source_name in _V60_MAX_CAPACITY_SOURCES:
        source_path = cache_path / f"{source_name}.csv"
        if source_path.exists():
            source_path.unlink()


def split_v74_maximum_capacity_commissioning_dates(cache_path: Path) -> None:
    """Split v7.4's single `Commissioning date` column into the two-column
    layout the templater downstream expects.

    v6.0 had `Commissioning date` (confirmed, committed rows) AND
    `Indicative commissioning date` (anticipated/additional rows) as two
    separate columns. v7.4 collapsed both into one `Commissioning date`,
    relying on `Status` to encode the distinction. To preserve the design
    of `_template_h2_gpg_emissions_reduction_factors`'s shared
    alternative-value lookup, we restore the two-column layout: for rows
    with `Status in {Anticipated, Additional}` the date moves into
    `Indicative commissioning date`. No-op if the table is absent or
    already has both columns.
    """
    csv_path = cache_path / f"{_V74_MAX_CAPACITY_CONSOLIDATED}.csv"
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    if "Indicative commissioning date" in df.columns:
        return
    df["Indicative commissioning date"] = pd.NA
    indicative_mask = df["Status"].isin(["Anticipated", "Additional"])
    df.loc[indicative_mask, "Indicative commissioning date"] = df.loc[
        indicative_mask, "Commissioning date"
    ]
    df.loc[indicative_mask, "Commissioning date"] = pd.NA
    df.to_csv(csv_path, index=False)


_V74_MLF_CONSOLIDATED = "marginal_loss_factors_existing_generators"
# v6.0 per-status MLF table → (status_label, identifier_column, value_column).
# Despite the v7.4 filename's "_existing_generators" suffix, it actually
# carries ALL ECAA statuses + batteries inside via a Status column — AEMO
# just kept the legacy name. v6.0's per-status tables are concat'd into that
# same shape so the templater stays version-naive.
_V60_MLF_SOURCES: dict[str, tuple[str | None, str, str]] = {
    "marginal_loss_factors_existing_generators": ("Existing", "Generator", "MLF"),
    "marginal_loss_factors_committed_generators": ("Committed", "Generator", "MLF"),
    "marginal_loss_factors_anticipated_projects": ("Anticipated", "Generator", "MLF"),
    "marginal_loss_factors_additional_projects": (
        "Additional",
        "Project",
        "MLF - Generation",
    ),
    "marginal_loss_factors_existing_committed_and_anticipated_batteries": (
        None,
        "Battery",
        "MLF - Generation",
    ),
}


def consolidate_v60_marginal_loss_factors_tables(cache_path: Path) -> None:
    """Concat v6.0's MLF per-status tables + batteries MLF into v7.4 form.

    Each source has a status-specific identifier column (Generator / Project /
    Battery) which is mapped to v7.4's `Power Station`. The v6.0 additional
    and batteries tables use `MLF - Generation` instead of `MLF` — same
    semantic, renamed to the canonical `MLF`. Hardcoded Status labels for
    the four per-status tables; the batteries table doesn't carry status
    info per row, so it gets the existing-template's "Existing" placeholder
    (it's filtered by technology_type downstream anyway).

    Writes to `marginal_loss_factors_existing_generators.csv` (v7.4
    canonical name despite holding all statuses) and deletes the sources.
    No-op if no source files present.
    """
    frames = []
    for source_name, (status, id_col, value_col) in _V60_MLF_SOURCES.items():
        source_path = cache_path / f"{source_name}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        renames = {id_col: "Power Station"}
        if value_col != "MLF":
            renames[value_col] = "MLF"
        df = df.rename(columns=renames)
        df["Status"] = status if status is not None else "Existing"
        frames.append(df)
    if not frames:
        return
    consolidated = pd.concat(frames, axis=0, ignore_index=True)
    consolidated.to_csv(cache_path / f"{_V74_MLF_CONSOLIDATED}.csv", index=False)
    for source_name in _V60_MLF_SOURCES:
        if source_name == _V74_MLF_CONSOLIDATED:
            continue  # don't delete the target we just wrote
        source_path = cache_path / f"{source_name}.csv"
        if source_path.exists():
            source_path.unlink()


_V74_OTHER_OUTAGES_SINGLE_YEAR_COLUMN = "2025-26"


def pivot_v74_other_outages_to_wide(cache_path: Path) -> None:
    """Generate the templater-expected forms from v7.4's long-form
    `other_outages_existing_generators` (rows keyed by Fuel type + Property,
    year columns 2025-26 through 2034-35).

    Produces:
    - `other_outages_existing_generators.csv`: pivoted wide form, one row
      per Fuel type with per-Property columns (single year value, 2025-26).
      Used by static generator-property merges (Partial Outage Derating
      Factor, MTTR full/partial).
    - `full_outages_forecast_existing_generators.csv`: time-series of
      "Full outage (% of time)" per Fuel type × year. Used by dynamic
      generator-property templater.
    - `partial_outages_forecast_existing_generators.csv`: time-series of
      "Partial outage (% of time)" per Fuel type × year.

    Per design call 2026-05-24: outages use v7.4's time series; the static
    pipeline uses a single representative year (2025-26) until time-varying
    PyPSA params are plumbed through.

    No-op if the long-form file is missing or already pivoted.
    """
    path = cache_path / "other_outages_existing_generators.csv"
    if not path.exists():
        return
    df = pd.read_csv(path)
    if "Property" not in df.columns:
        return  # already wide
    if _V74_OTHER_OUTAGES_SINGLE_YEAR_COLUMN not in df.columns:
        return  # no expected year column to pivot on
    year_cols = [
        c for c in df.columns if c not in ("Fuel type", "Property")
    ]
    # Time-series forecasts per Property.
    for property_value, target_name in [
        ("Full outage (% of time)", "full_outages_forecast_existing_generators"),
        ("Partial outage (% of time)", "partial_outages_forecast_existing_generators"),
    ]:
        subset = df[df["Property"] == property_value].drop(columns="Property")
        subset.to_csv(cache_path / f"{target_name}.csv", index=False)
    # Wide static form (single representative year per Property).
    wide = df.pivot_table(
        index="Fuel type",
        columns="Property",
        values=_V74_OTHER_OUTAGES_SINGLE_YEAR_COLUMN,
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    wide.to_csv(path, index=False)


_V60_TO_V74_BATTERY_PROPERTY_RENAMES: dict[str, str] = {
    # v6.0 published utility-battery values under `(utility)` qualifiers and
    # VPP-aggregated values under `(aggregated)`. v7.4 dropped both
    # qualifiers and publishes one canonical row per property. For ECAA
    # storage (utility batteries) the templater uses the `(utility)` values;
    # the `(aggregated)` row (VPPs only) is dropped during transformation.
    "Maximum power": "Maximum power",
    "Energy capacity": "Energy capacity",
    "Charge efficiency (utility)": "Charge efficiency",
    "Discharge efficiency (utility)": "Discharge efficiency",
    "Round trip efficiency (utility)": "Round trip efficiency",
    "Annual degradation (utility)": "Annual degradation",
    "Allowable max state of charge": "Allowable max state of charge",
    "Allowable min state of charge": "Allowable min state of charge",
}
# v6.0 stored battery energy capacity in MWh but values are actually hours
# of storage (e.g. 1, 2, 4, 8 for the four canonical battery durations).
# v7.4 corrected the label: `Energy capacity_Hours`. Mapping the v6.0 unit
# string explicitly here avoids embedding wrong-unit labels.
_V60_BATTERY_PROPERTY_UNIT_OVERRIDES: dict[str, str] = {
    "Energy capacity": "Hours",
}


def transform_v60_battery_properties_to_wide(cache_path: Path) -> None:
    """Transpose v6.0's long-format `battery_properties` into v7.4 wide form.

    v6.0 publishes one row per property with battery-type columns and a
    separate `Units` column. v7.4 publishes one row per battery type with
    property-unit-embedded column headers (`Maximum power_MW`, etc.).
    Per design call 2026-05-24: schema normalisation transforms forward
    (v6.0 → v7.4 shape), templater consumes v7.4 shape directly.

    Drops the `(aggregated)` round-trip-efficiency row used by VPPs only
    (templater downstream uses `(utility)` values for ECAA batteries).

    No-op if the table is missing or already in wide form (no `Property`
    column).
    """
    path = cache_path / "battery_properties.csv"
    if not path.exists():
        return
    df = pd.read_csv(path)
    if "Property" not in df.columns:
        return  # already in v7.4 wide form
    units = df.set_index("Property")["Units"].to_dict()
    df = df.set_index("Property").drop(columns="Units")
    # Drop the VPP-aggregated row; templater downstream uses utility values.
    df = df.drop(index="Round trip efficiency (aggregated)", errors="ignore")
    # Rename property labels to drop the (utility) qualifier.
    df = df.rename(index=_V60_TO_V74_BATTERY_PROPERTY_RENAMES)
    # Transpose: battery types become rows, properties become columns.
    wide = df.T.reset_index(names="Technology")
    # Embed units into column headers (`Charge efficiency_%`, `Maximum power_MW`).
    renames = {}
    for prop, unit in units.items():
        new_prop = _V60_TO_V74_BATTERY_PROPERTY_RENAMES.get(prop, prop)
        unit_to_embed = _V60_BATTERY_PROPERTY_UNIT_OVERRIDES.get(prop, unit)
        if new_prop in wide.columns and isinstance(unit_to_embed, str):
            renames[new_prop] = f"{new_prop}_{unit_to_embed}"
    wide = wide.rename(columns=renames)
    wide.to_csv(path, index=False)


# v6.0 per-scenario fuel-price table → (v7.4 Scenario value).
# v6.0 published one table per ISP scenario; v7.4 has a single consolidated
# table with a `Scenario` column. The v6.0 → v7.4 ISP scenario name shift:
#   - "Step Change" → "Step Change" (unchanged)
#   - "Progressive Change" → "Slower Growth"
#   - "Green Energy Exports" → "Accelerated Transition"
# (Thematic mapping verified 2026-05-24: v7.4 prices are ~2× v6.0 levels in
# early forecast years across all scenarios — an AEMO baseline update, not
# a scenario semantic shift. The within-version scenario ranking is preserved.)
_V60_TO_V74_FUEL_PRICE_SCENARIO: dict[str, str] = {
    "step_change": "Step Change",
    "progressive_change": "Slower Growth",
    "green_energy_exports": "Accelerated Transition",
}


def consolidate_v60_coal_prices_to_v74(cache_path: Path) -> None:
    """Combine v6.0's three per-scenario coal-price tables into v7.4's
    single `coal_fuel_price` form (Generator, Scenario, year columns).

    The v6.0 "Coal Price Scenario" column ("Central"/"Low"/"High") is a
    sub-scenario AEMO published within each ISP scenario; the templater
    uses only the row whose ISP scenario matches config. The new
    `Scenario` column carries the v7.4-canonical ISP scenario name
    derived from the source table.
    """
    frames = []
    for v60_suffix, v74_scenario in _V60_TO_V74_FUEL_PRICE_SCENARIO.items():
        source_path = cache_path / f"coal_prices_{v60_suffix}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        if "Coal Price Scenario" in df.columns:
            df = df.drop(columns="Coal Price Scenario")
        df["Scenario"] = v74_scenario
        frames.append(df)
    if not frames:
        return
    cols_leading = ["Generator", "Scenario"]
    consolidated = pd.concat(frames, ignore_index=True)
    other_cols = [c for c in consolidated.columns if c not in cols_leading]
    consolidated = consolidated[cols_leading + other_cols]
    consolidated.to_csv(cache_path / "coal_fuel_price.csv", index=False)
    for v60_suffix in _V60_TO_V74_FUEL_PRICE_SCENARIO:
        source_path = cache_path / f"coal_prices_{v60_suffix}.csv"
        if source_path.exists():
            source_path.unlink()


def consolidate_v60_gas_prices_to_v74(cache_path: Path) -> None:
    """Combine v6.0's three per-scenario gas-price tables into v7.4's
    consolidated form. Per design call 2026-05-24: v7.4 split gas prices
    into separate ECAA and new-entrant tables, but v6.0 didn't differentiate.
    For v6.0 we produce **both** v7.4 tables with identical content — the
    templater concats them downstream and the underlying data carries the
    same generator-to-fuel-cost mapping coverage.
    """
    frames = []
    for v60_suffix, v74_scenario in _V60_TO_V74_FUEL_PRICE_SCENARIO.items():
        source_path = cache_path / f"gas_prices_{v60_suffix}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        # v6.0 has an `Unnamed: 1` column header for the generator name;
        # rename to `Generator` to match v7.4 form.
        if "Unnamed: 1" in df.columns:
            df = df.rename(columns={"Unnamed: 1": "Generator"})
        # v6.0 has "Gas price scenario" column; the templater drops the
        # equivalent in v7.4. Overwrite with v7.4 canonical scenario.
        df["Gas price scenario"] = v74_scenario
        frames.append(df)
    if not frames:
        return
    consolidated = pd.concat(frames, ignore_index=True)
    cols_leading = ["Generator", "Gas price scenario"]
    other_cols = [c for c in consolidated.columns if c not in cols_leading]
    consolidated = consolidated[cols_leading + other_cols]
    consolidated.to_csv(cache_path / "gas_prices_existing_generators.csv", index=False)
    consolidated.to_csv(cache_path / "gas_prices_new_entrants.csv", index=False)
    for v60_suffix in _V60_TO_V74_FUEL_PRICE_SCENARIO:
        source_path = cache_path / f"gas_prices_{v60_suffix}.csv"
        if source_path.exists():
            source_path.unlink()


def consolidate_v60_biomass_prices_to_v74(cache_path: Path) -> None:
    """Combine v6.0's `biomass_prices` + `coal_and_biomass_price_consultant_scenario_mapping`
    into v7.4's `biomass_fuel_price` form (direct ISP-scenario rows, no
    consultant scenario layer).

    v6.0 published biomass prices keyed by AEMO consultant scenarios
    (Central/Low Price/High Price), then a separate mapping table from ISP
    scenarios to consultant scenarios (Step Change → Central, etc.).
    v7.4 collapsed both into a single table keyed by ISP scenario directly.
    Forward normalisation here applies the mapping to produce v7.4 form.
    """
    bp_path = cache_path / "biomass_prices.csv"
    mapping_path = cache_path / "coal_and_biomass_price_consultant_scenario_mapping.csv"
    if not bp_path.exists() or not mapping_path.exists():
        return
    biomass = pd.read_csv(bp_path)
    mapping_df = pd.read_csv(mapping_path).set_index(
        pd.read_csv(mapping_path).columns[0]
    )
    # First (and only) row maps each ISP scenario col → consultant scenario value.
    mapping_series = mapping_df.iloc[0]
    # Normalise the consultant labels to match the case used in biomass_prices.
    # v6.0 biomass_prices rows use "Central"/"Low Price"/"High Price"; mapping
    # uses "Central"/"Low price"/"High price" — fuzzy-tolerant via .str.lower().
    biomass["Price Scenario lower"] = biomass["Price Scenario"].str.lower()
    consultant_to_isp = {
        consultant.lower(): _V60_TO_V74_FUEL_PRICE_SCENARIO.get(
            _snakecase_isp(isp), isp
        )
        for isp, consultant in mapping_series.items()
    }
    biomass["Scenario"] = biomass["Price Scenario lower"].map(consultant_to_isp)
    biomass = biomass.dropna(subset=["Scenario"]).drop(
        columns=["Price Scenario", "Price Scenario lower"]
    )
    year_cols = [c for c in biomass.columns if c not in ("Biomass price", "Scenario")]
    biomass = biomass[["Biomass price", "Scenario"] + year_cols]
    biomass.to_csv(cache_path / "biomass_fuel_price.csv", index=False)
    bp_path.unlink()
    if mapping_path.exists():
        mapping_path.unlink()


def _snakecase_isp(isp_name: str) -> str:
    """Helper: convert ISP scenario name to snake_case (used in
    _V60_TO_V74_FUEL_PRICE_SCENARIO keys). E.g. "Step Change" → "step_change"."""
    return isp_name.strip().lower().replace(" ", "_")


# v6.0 per-scenario build_costs → (v6.0 ISP scenario, v7.4 IASR scenario,
# GenCost scenario label as it appears in v7.4's `GenCost Scenario` column).
_V60_TO_V74_BUILD_COSTS_SCENARIO = [
    ("build_costs_current_policies", "Slower Growth", "GenCost Current Policies"),
    ("build_costs_global_nze_post_2050", "Step Change", "GenCost Global NZE post 2050"),
    ("build_costs_global_nze_by_2050", "Accelerated Transition", "GenCost Global NZE by 2050"),
]


def consolidate_v60_build_costs_to_v74(cache_path: Path) -> None:
    """Combine v6.0's per-GenCost-scenario build_costs tables + pumped_hydro
    into v7.4's unified `build_costs` (Technology, GenCost Scenario, IASR
    Scenario, Source, year cols).

    Mapping (verified against v6.0's `build_costs_scenario_mapping`):
      Progressive Change → GenCost Current Policies (→ Slower Growth in v7.4)
      Step Change        → GenCost Global NZE post 2050 (→ Step Change)
      Green Energy Exports → GenCost Global NZE by 2050 (→ Accelerated Transition)

    Pumped hydro: v6.0 had a separate `build_costs_pumped_hydro` table; v7.4
    folded it into `build_costs`. We replicate it under all three IASR
    scenarios with "Not Applicable" GenCost label (matching v7.4 form).
    """
    frames = []
    for source_name, iasr_scenario, gencost_label in _V60_TO_V74_BUILD_COSTS_SCENARIO:
        source_path = cache_path / f"{source_name}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        df["IASR Scenario"] = iasr_scenario
        df["GenCost Scenario"] = gencost_label
        frames.append(df)
    phes_path = cache_path / "build_costs_pumped_hydro.csv"
    if phes_path.exists():
        phes = pd.read_csv(phes_path)
        for iasr_scenario in ["Slower Growth", "Step Change", "Accelerated Transition"]:
            replicated = phes.copy()
            replicated["IASR Scenario"] = iasr_scenario
            replicated["GenCost Scenario"] = "Not Applicable"
            frames.append(replicated)
    if not frames:
        return
    consolidated = pd.concat(frames, ignore_index=True)
    leading = ["Technology", "GenCost Scenario", "IASR Scenario", "Source"]
    other = [c for c in consolidated.columns if c not in leading]
    consolidated = consolidated[
        [c for c in leading if c in consolidated.columns] + other
    ]
    consolidated.to_csv(cache_path / "build_costs.csv", index=False)
    for source_name, _, _ in _V60_TO_V74_BUILD_COSTS_SCENARIO:
        source_path = cache_path / f"{source_name}.csv"
        if source_path.exists():
            source_path.unlink()
    if phes_path.exists():
        phes_path.unlink()
    mapping_path = cache_path / "build_costs_scenario_mapping.csv"
    if mapping_path.exists():
        mapping_path.unlink()


_V60_SEASONAL_RATINGS_PER_STATUS_TABLES = [
    "seasonal_ratings_existing_generators",
    "seasonal_ratings_committed_generators",
    "seasonal_ratings_anticipated_projects",
    "seasonal_ratings_additional_projects",
]
_V74_SEASONAL_RATINGS_CONSOLIDATED = (
    "seasonal_ratings_existing_committed_anticipated_additional_generators"
)


def consolidate_v60_seasonal_ratings_tables(cache_path: Path) -> None:
    """Concat v6.0's four per-status seasonal_ratings tables into v7.4's
    single consolidated form. Mirror of the max_capacity / MLF / generator
    summary consolidations earlier in this module.

    No-op if no per-status sources are present.
    """
    frames = []
    for source_name in _V60_SEASONAL_RATINGS_PER_STATUS_TABLES:
        source_path = cache_path / f"{source_name}.csv"
        if not source_path.exists():
            continue
        frames.append(pd.read_csv(source_path))
    if not frames:
        return
    consolidated = pd.concat(frames, axis=0, ignore_index=True)
    consolidated.to_csv(
        cache_path / f"{_V74_SEASONAL_RATINGS_CONSOLIDATED}.csv", index=False
    )
    for source_name in _V60_SEASONAL_RATINGS_PER_STATUS_TABLES:
        source_path = cache_path / f"{source_name}.csv"
        if source_path.exists():
            source_path.unlink()


def aggregate_v74_liquid_fuel_prices_to_v60_form(cache_path: Path) -> None:
    """v7.4 split `liquid_fuel_prices` into per-generator rows (Generator,
    Gas price scenario, year cols), differentiating prices by location.
    The templater downstream consumes a single representative price per
    scenario — v6.0's framing. Aggregate across the Generator dimension
    (mean per scenario+year) and rename columns to match v6.0 templater
    expectations. Per Phase 1 simplification 2026-05-24: per-generator
    liquid-fuel pricing is a future refinement; for the current modelling
    scope (limited liquid-fuel capacity in NEM), a fleet-average is a
    reasonable proxy.

    No-op if the file is already in v6.0 single-row form or absent.
    """
    path = cache_path / "liquid_fuel_prices.csv"
    if not path.exists():
        return
    df = pd.read_csv(path)
    if "Liquid fuel price" in df.columns:
        return  # already in v6.0 form
    if "Generator" not in df.columns or "Gas price scenario" not in df.columns:
        return  # unexpected shape — leave alone
    year_cols = [
        c for c in df.columns if c not in ("Generator", "Gas price scenario")
    ]
    df[year_cols] = df[year_cols].apply(pd.to_numeric, errors="coerce")
    aggregated = (
        df.groupby("Gas price scenario", as_index=False)[year_cols].mean()
    )
    aggregated.insert(0, "Liquid fuel price", "Liquid fuel")
    aggregated = aggregated.rename(
        columns={"Gas price scenario": "Liquid fuel price scenario"}
    )
    aggregated.to_csv(path, index=False)


def aggregate_v74_biomethane_prices_to_v60_form(cache_path: Path) -> None:
    """v7.4 publishes biomethane prices per-source (Landfill gas, Waste, Crop
    residue) × scenario; v6.0 had a single source per scenario. The templater
    downstream expects single-row-per-scenario shape. Average across sources
    per scenario+year. Per Phase 1 simplification (analogue of liquid fuel
    aggregation): a fleet-average per scenario is a reasonable proxy until
    Phase 2+ adds per-source biomethane modelling.

    No-op if already single-source or table absent.
    """
    path = cache_path / "biomethane_prices.csv"
    if not path.exists():
        return
    df = pd.read_csv(path)
    if "Biomethane price" not in df.columns or "Biomethane price scenario" not in df.columns:
        return
    if df["Biomethane price"].nunique() <= 1:
        return  # already single source
    year_cols = [
        c
        for c in df.columns
        if c not in ("Biomethane price", "Biomethane price scenario")
    ]
    df[year_cols] = df[year_cols].apply(pd.to_numeric, errors="coerce")
    aggregated = (
        df.groupby("Biomethane price scenario", as_index=False)[year_cols].mean()
    )
    aggregated.insert(0, "Biomethane price", "Biomethane (mean across sources)")
    aggregated.to_csv(path, index=False)


def consolidate_v60_h2_gpg_to_regional(cache_path: Path) -> None:
    """Combine v6.0's two named-generator H2 GPG emissions-reduction tables into
    v7.4's region-keyed `hydrogen_limit_for_gpg` shape.

    v6.0 published two tables (`gpg_emissions_reduction_h2_kogan`,
    `gpg_emissions_reduction_h2_sa_turbine`) each carrying a scenarios × years
    matrix of emissions-reduction percentages for one specific named gas peaker.
    v7.4 collapsed both into a single regional table — `hydrogen_limit_for_gpg`
    — keyed by (Region, Scenario) × years, with values applied uniformly to
    every gas peaker in the region.

    Per (c3) Reading 1 (design call 2026-05-24): each H2 GPG generator
    inherits its region's H2 fraction. For the v6.0 → v7.4 transform:

      - `gpg_emissions_reduction_h2_kogan` carries QLD's H2 trajectory
        (Kogan Creek is in QLD).
      - `gpg_emissions_reduction_h2_sa_turbine` carries SA's H2 trajectory.

    The first column of each v6.0 table has the generator-name header
    (`Kogan Gas` / `SA Hydrogen Turbine`) but actually holds scenario
    labels per row — that column gets renamed to `Scenario`. A `Region`
    column is added per source.

    No-op if neither v6.0 source file is present.
    """
    kogan_path = cache_path / "gpg_emissions_reduction_h2_kogan.csv"
    sa_path = cache_path / "gpg_emissions_reduction_h2_sa_turbine.csv"
    if not kogan_path.exists() and not sa_path.exists():
        return
    frames = []
    for source_path, header_to_rename, region in [
        (kogan_path, "Kogan Gas", "QLD"),
        (sa_path, "SA Hydrogen Turbine", "SA"),
    ]:
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        df = df.rename(columns={header_to_rename: "Scenario"})
        df["Region"] = region
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    leading_cols = ["Region", "Scenario"]
    other_cols = [c for c in combined.columns if c not in leading_cols]
    combined = combined[leading_cols + other_cols]
    combined.to_csv(cache_path / "hydrogen_limit_for_gpg.csv", index=False)
    if kogan_path.exists():
        kogan_path.unlink()
    if sa_path.exists():
        sa_path.unlink()


def expand_v60_auxiliary_load_to_per_generator(cache_path: Path) -> None:
    """Rebuild v6.0's per-technology auxiliary_load table into v7.4's per-generator
    shape, joining the per-tech values against the (already-consolidated) generator
    summary table.

    v6.0 published a flat lookup `Fuel/Technology type → Auxiliary load (% of
    nameplate capacity)`. v7.4 publishes per-generator values keyed by IASR ID
    + Power Station, framed as `% of generation` (different denominator —
    physically more accurate; the magnitude difference is small because aux
    loads are small percentages and capacity factors near unity for most
    relevant plants — see design call 2026-05-24 [[h2-gpg-constraint-semantic-shift-v60-to-v74]]).

    For v6.0 caches we expand the per-tech values by joining with the
    consolidated generator summary (post-(c1)) — each summary row's
    `Auxiliary load (%)` column holds a v6.0-shape tech code (e.g. "Black
    Coal NSW") that we look up in the per-tech aux_load table. The result
    is written as `auxiliary_load_existing_committed_anticipated_additional_generators.csv`
    in v7.4 shape (IASR ID, Power Station, Technology, Auxiliary load
    (% of generation)); the v6.0 per-tech file is then deleted.

    No-op if the v6.0 per-tech table is already in v7.4 form (i.e. already
    has a `Power Station` column).
    """
    aux_path = cache_path / "auxiliary_load_existing_committed_anticipated_additional_generators.csv"
    summary_path = cache_path / f"{_V74_CONSOLIDATED_GENERATOR_SUMMARY}.csv"
    if not aux_path.exists() or not summary_path.exists():
        return
    aux = pd.read_csv(aux_path)
    if "Power Station" in aux.columns:
        return  # already in v7.4 shape
    summary = pd.read_csv(summary_path)
    tech_to_aux = aux.set_index("Fuel/Technology type")[
        "Auxiliary load (% of nameplate capacity)"
    ].to_dict()
    expanded = pd.DataFrame(
        {
            "IASR ID": summary.get("IASR ID / DLT names"),
            "Power Station": summary["Power Station"],
            "Technology": summary.get("Technology Type"),
            "Auxiliary load (% of generation)": summary["Auxiliary load (%)"].map(
                tech_to_aux
            ),
        }
    )
    expanded.to_csv(aux_path, index=False)


def consolidate_v60_ecaa_generator_summaries(cache_path: Path) -> None:
    """Concat v6.0 per-status summary CSVs into the v7.4 consolidated form.

    Reads the four per-status CSVs from `cache_path`, renames each one's
    status-specific first column to `Power Station` (the v7.4 canonical
    name for the human-readable identifier), concatenates them, and writes
    a single `existing_committed_anticipated_additional_generator_summary.csv`.
    The original four files are removed so the cache contains exactly one
    summary table on disk (matching what a v7.4 cache build produces).

    No-op if none of the per-status files are present (e.g. running on a
    cache that has already been consolidated).
    """
    frames = []
    for source_name, name_col in _V60_GENERATOR_SUMMARY_SOURCES.items():
        source_path = cache_path / f"{source_name}.csv"
        if not source_path.exists():
            continue
        df = pd.read_csv(source_path)
        # `Technology type` (lowercase 't') in v6.0 per-status summaries →
        # `Technology Type` (capital 'T') to align with the v7.4 native
        # consolidated table and with the rename applied to new_entrants_summary.
        # Without this, the downstream concat of the ECAA consolidated table
        # against new_entrants_summary in templater/storage.py produces two
        # columns that both snake-case to `technology_type`, triggering an
        # AttributeError when filtering on storage_summaries['technology_type'].
        df = df.rename(columns={name_col: "Power Station", "Technology type": "Technology Type"})
        frames.append(df)
    if not frames:
        return
    consolidated = pd.concat(frames, axis=0, ignore_index=True)
    consolidated.to_csv(
        cache_path / f"{_V74_CONSOLIDATED_GENERATOR_SUMMARY}.csv", index=False
    )
    for source_name in _V60_GENERATOR_SUMMARY_SOURCES:
        source_path = cache_path / f"{source_name}.csv"
        if source_path.exists():
            source_path.unlink()


# v6.0 per-(ISP scenario suffix) connection-cost-forecast file groups.
# Each suffix maps to the v7.4 IASR scenario(s) it provides data for.
# step_change&green_energy_exports → two v7.4 scenarios share the same forecast values.
_V60_CONNECTION_COST_FORECAST_SCENARIO_SOURCES: list[tuple[str, list[str]]] = [
    ("step_change&green_energy_exports", ["Step Change", "Accelerated Transition"]),
    ("progressive_change", ["Slower Growth"]),
]


def consolidate_v60_connection_cost_forecasts_to_v74(cache_path: Path) -> None:
    """Combine v6.0's per-scenario wind/solar + non-REZ connection cost
    forecast tables into v7.4's single `connection_cost_forecast_wind_and_solar`
    form (REZ names, Scenario, Connection capacity (MVA), year columns).

    v6.0 published four files keyed by file-name scenario suffix:
      - connection_cost_forecast_wind_and_solar_{suffix}.csv   (REZ entries)
      - connection_cost_forecast_non_rez_{suffix}.csv          (Non-REZ entries)
    where suffix is `step_change&green_energy_exports` or `progressive_change`.

    v7.4 folded:
      - all of these into a single `connection_cost_forecast_wind_and_solar.csv`
        keyed by (REZ ID, REZ names, Scenario), with Non-REZ regions promoted
        to first-class REZ entries (N0-style IDs).
      - per-REZ Connection capacity (MVA) was dropped from the forecast table
        and lives in `connection_costs_for_wind_and_solar.csv` instead.

    Forward normalisation here:
      1. Concat all four v6.0 forecast tables into the v7.4 single-table form,
         renaming `Non-REZ name` → `REZ names` and duplicating the
         `step_change&green_energy_exports` rows so Step Change and Accelerated
         Transition each get their own copy.
      2. Keeps the `Connection capacity (MVA)` column inline — the templater
         consumes it from the forecast table for the per-MW division. (The v7.4
         normalisation step merges MVA in from `connection_costs_for_wind_and_solar`
         to produce the same shape.)

    No-op if no v6.0 source files are present.
    """
    frames = []
    for suffix, iasr_scenarios in _V60_CONNECTION_COST_FORECAST_SCENARIO_SOURCES:
        ws_path = cache_path / f"connection_cost_forecast_wind_and_solar_{suffix}.csv"
        nr_path = cache_path / f"connection_cost_forecast_non_rez_{suffix}.csv"
        per_suffix_frames = []
        if ws_path.exists():
            per_suffix_frames.append(pd.read_csv(ws_path))
        if nr_path.exists():
            nr = pd.read_csv(nr_path)
            nr = nr.rename(columns={"Non-REZ name": "REZ names"})
            per_suffix_frames.append(nr)
        if not per_suffix_frames:
            continue
        suffix_combined = pd.concat(per_suffix_frames, axis=0, ignore_index=True)
        for iasr_scenario in iasr_scenarios:
            replicated = suffix_combined.copy()
            replicated["Scenario"] = iasr_scenario
            frames.append(replicated)
    if not frames:
        return
    consolidated = pd.concat(frames, axis=0, ignore_index=True)
    leading = ["REZ names", "Scenario", "Connection capacity (MVA)"]
    other = [c for c in consolidated.columns if c not in leading]
    consolidated = consolidated[
        [c for c in leading if c in consolidated.columns] + other
    ]
    consolidated.to_csv(
        cache_path / "connection_cost_forecast_wind_and_solar.csv", index=False
    )
    for suffix, _ in _V60_CONNECTION_COST_FORECAST_SCENARIO_SOURCES:
        for prefix in (
            "connection_cost_forecast_wind_and_solar",
            "connection_cost_forecast_non_rez",
        ):
            source_path = cache_path / f"{prefix}_{suffix}.csv"
            if source_path.exists():
                source_path.unlink()


def strip_v74_rez_prefix_from_aug_cost_options(cache_path: Path) -> None:
    """v7.4 publishes `rez_augmentation_costs_*` tables with Option values
    prefixed by the REZ ID (e.g. `"DN1 Option 1"`); the corresponding
    `rez_augmentation_options_*` tables use the bare suffix (`"Option 1"`).
    The templater merges aug options + costs on `(id, option)` and the
    mismatch silently drops nearly all rows — leaving `rez_transmission_expansion_costs`
    empty, which means PyPSA links from REZs to sub-regions become
    non-extendable. VRE build is then capped at the existing REZ
    transmission limits and the LP fills load-bus capacity gaps with
    biomass instead.

    Strip the leading `"{REZ ID} "` from Option values in v7.4 cost tables
    so the merge with the options table succeeds. The aug_options tables
    are left untouched.

    No-op if cost files are absent or already in stripped form.
    """
    for csv_path in cache_path.glob("rez_augmentation_costs_*.csv"):
        df = pd.read_csv(csv_path)
        rez_col = "REZ / Constraint ID" if "REZ / Constraint ID" in df.columns else None
        if rez_col is None or "Option" not in df.columns:
            continue
        has_prefix = df.apply(
            lambda r: isinstance(r["Option"], str)
            and isinstance(r[rez_col], str)
            and r["Option"].startswith(r[rez_col] + " "),
            axis=1,
        )
        if not has_prefix.any():
            continue
        df.loc[has_prefix, "Option"] = df.loc[has_prefix].apply(
            lambda r: r["Option"][len(r[rez_col]) + 1 :],
            axis=1,
        )
        df.to_csv(csv_path, index=False)


def merge_v74_connection_capacity_into_forecast(cache_path: Path) -> None:
    """v7.4 publishes per-REZ `Connection capacity (MVA)` only in
    `connection_costs_for_wind_and_solar.csv`; the forecast table itself
    carries just year-column values in $. The templater needs MVA inline
    on the forecast table to compute $/MW. Merge it here so the templater
    sees the same shape regardless of source version.

    No-op if the forecast already has `Connection capacity (MVA)` (v6.0
    consolidation path) or if either source file is absent.
    """
    forecast_path = cache_path / "connection_cost_forecast_wind_and_solar.csv"
    initial_path = cache_path / "connection_costs_for_wind_and_solar.csv"
    if not forecast_path.exists() or not initial_path.exists():
        return
    forecast = pd.read_csv(forecast_path)
    if "Connection capacity (MVA)" in forecast.columns:
        return
    initial = pd.read_csv(initial_path)
    if "Connection capacity (MVA)" not in initial.columns:
        return
    capacity = initial[["REZ names", "Connection capacity (MVA)"]].drop_duplicates(
        subset=["REZ names"]
    )
    merged = forecast.merge(capacity, on="REZ names", how="left")
    leading = ["REZ names", "Scenario", "Connection capacity (MVA)"]
    other = [c for c in merged.columns if c not in leading]
    merged = merged[[c for c in leading if c in merged.columns] + other]
    merged.to_csv(forecast_path, index=False)


# v7.4 ECAA tables that publish one row per generating unit (IASR ID), which
# downstream templater code expects to see at per-power-station granularity
# (one row per Power Station). Aggregation strategy:
#   summary table:          first values per Power Station (unit properties
#                           are identical across all units of the same plant)
#   maximum_capacity table: sum `Installed capacity (MW)` across units, first
#                           values for other columns
#   property tables (heat_rates, fixed_opex, variable_opex, auxiliary_load,
#                    marginal_loss_factors, seasonal_ratings): first values
_V74_ECAA_PER_UNIT_TABLES_FIRST_AGG = [
    "existing_committed_anticipated_additional_generator_summary",
    "heat_rates_existing_committed_anticipated_additional_generators",
    "fixed_opex_existing_committed_anticipated_additional_generators",
    "variable_opex_existing_committed_anticipated_additional_generators",
    "auxiliary_load_existing_committed_anticipated_additional_generators",
    "marginal_loss_factors_existing_generators",
    "seasonal_ratings_existing_committed_anticipated_additional_generators",
]


def aggregate_v74_ecaa_units_to_power_stations(cache_path: Path) -> None:
    """Collapse v7.4 per-unit ECAA rows to per-Power-Station.

    v6.0 published one row per power station with totalised capacity. v7.4
    publishes one row per generating unit (IASR ID = e.g. BW01, BW02, BW03,
    BW04 for Bayswater units 1–4), with property values typically identical
    across all units of the same plant. The templater downstream is built
    around the per-plant convention — duplicate "generator" identifiers
    (snake_cased Power Station) propagate through to PyPSA and break the
    network build.

    This function forward-normalises v7.4 to v6.0's per-plant granularity:
    groupby Power Station and take first values for properties, summing
    only `Installed capacity (MW)` in the max_capacity table where total
    plant capacity is the meaningful figure.

    No-op if the table is missing or already per-plant.
    """
    for table_name in _V74_ECAA_PER_UNIT_TABLES_FIRST_AGG:
        csv_path = cache_path / f"{table_name}.csv"
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path)
        if "Power Station" not in df.columns:
            continue
        if df["Power Station"].duplicated().sum() == 0:
            continue
        df = _aggregate_first_by_power_station(df)
        df.to_csv(csv_path, index=False)

    cap_path = (
        cache_path
        / "maximum_capacity_existing_committed_anticipated_additional_generators.csv"
    )
    if cap_path.exists():
        df = pd.read_csv(cap_path)
        if "Power Station" in df.columns and df["Power Station"].duplicated().sum() > 0:
            df = _aggregate_capacity_by_power_station(df)
            df.to_csv(cap_path, index=False)


def _aggregate_first_by_power_station(df: pd.DataFrame) -> pd.DataFrame:
    other_cols = [c for c in df.columns if c != "Power Station"]
    aggregated = df.groupby("Power Station", as_index=False, sort=False)[
        other_cols
    ].first()
    return aggregated[list(df.columns)]


def _aggregate_capacity_by_power_station(df: pd.DataFrame) -> pd.DataFrame:
    sum_cols = [c for c in ("Installed capacity (MW)",) if c in df.columns]
    first_cols = [c for c in df.columns if c not in sum_cols + ["Power Station"]]
    agg_spec = {c: "first" for c in first_cols}
    agg_spec.update({c: "sum" for c in sum_cols})
    aggregated = (
        df.groupby("Power Station", as_index=False, sort=False).agg(agg_spec)
    )
    return aggregated[list(df.columns)]


# Tables that share the v7.4 per-power-station ECAA generator inventory.
# Filtering by Power Station must be consistent across all of them.
_V74_ECAA_TABLES_FOR_TRACE_FILTER = _V74_ECAA_PER_UNIT_TABLES_FIRST_AGG + [
    "maximum_capacity_existing_committed_anticipated_additional_generators",
]


def filter_v74_ecaa_to_trace_coverage(
    cache_path: Path, trace_directory: Path
) -> None:
    """Filter v7.4 ECAA VRE generators to the set with matched trace data.

    The v7.4 IASR includes new VRE projects (committed/anticipated/additional
    policy-supported) that don't exist in the 2024-era trace dataset shipped
    via `isp-trace-parser` (currently the only supported dataset_src is
    `isp_2024`). Without trace data, the translator's
    `_aggregate_wind_solar_traces` raises ValueError on any missing snapshot.

    This function reads the project trace parquet files under
    `<trace_directory>/isp_2024/project/`, extracts the set of project names
    with traces available, and drops VRE rows (Technology Type matching
    Wind|Solar) from the v7.4 ECAA tables whose Power Station isn't in the
    set. Non-VRE generators (coal, gas, hydro, etc.) are untouched.

    A drop manifest `dropped_generators_iasr_2025-26.csv` is written into the
    cache directory listing every dropped row with Power Station, Technology
    Type, Status, Region, and Sub-region. The drop list is also emitted as a
    WARNING log.

    No-op if `trace_directory` is None, doesn't exist, or contains no project
    parquet files. This keeps the filter quiet for callers using v6.0 source
    or running without trace data.

    Phase 2 trigger: when Open-ISP/isp-trace-parser releases support for
    2026 traces (currently tracked in issues #33-#43 — metadata parsing,
    generator name mappings, demand data changes, REZ traces), this filter
    should be revisited to allow `isp_2025`/`isp_2026` dataset_src.
    """
    project_dir = trace_directory / "isp_2024" / "project"
    zone_dir = trace_directory / "isp_2024" / "zone"
    project_files = list(project_dir.rglob("*.parquet")) if project_dir.exists() else []
    zone_files = list(zone_dir.rglob("*.parquet")) if zone_dir.exists() else []
    if not project_files and not zone_files:
        return

    trace_projects = (
        _collect_trace_names(project_files, "project") if project_files else set()
    )
    trace_zones = _collect_trace_names(zone_files, "zone") if zone_files else set()

    dropped_frames = []
    summary_path = (
        cache_path / "existing_committed_anticipated_additional_generator_summary.csv"
    )
    if summary_path.exists() and trace_projects:
        summary = pd.read_csv(summary_path)
        drop_mask = _vre_rows_without_trace(summary, trace_projects)
        if drop_mask.any():
            dropped_ecaa = summary.loc[
                drop_mask, _trace_filter_manifest_columns(summary)
            ].copy()
            dropped_ecaa["reason"] = (
                "no 2025-26 project trace data available via isp-trace-parser"
            )
            dropped_frames.append(dropped_ecaa)
            dropped_power_stations = set(summary.loc[drop_mask, "Power Station"])
            for table_name in _V74_ECAA_TABLES_FOR_TRACE_FILTER:
                csv_path = cache_path / f"{table_name}.csv"
                if not csv_path.exists():
                    continue
                df = pd.read_csv(csv_path)
                if "Power Station" not in df.columns:
                    continue
                filtered = df[~df["Power Station"].isin(dropped_power_stations)]
                filtered.to_csv(csv_path, index=False)
            logging.warning(
                f"Dropped {len(dropped_ecaa)} v7.4 ECAA VRE generators with "
                f"no 2025-26 project trace coverage: "
                f"{sorted(dropped_power_stations)}"
            )

    ne_path = cache_path / "new_entrants_summary.csv"
    if ne_path.exists() and trace_zones:
        ne = pd.read_csv(ne_path)
        ne_drop_mask = _vre_new_entrant_rows_without_zone_trace(ne, trace_zones)
        if ne_drop_mask.any():
            dropped_ne = ne.loc[
                ne_drop_mask, _trace_filter_manifest_columns(ne)
            ].copy()
            dropped_ne["reason"] = (
                "no 2025-26 zone trace data available via isp-trace-parser"
            )
            dropped_frames.append(dropped_ne)
            ne_filtered = ne[~ne_drop_mask]
            ne_filtered.to_csv(ne_path, index=False)
            dropped_rez_ids = sorted(set(ne.loc[ne_drop_mask, "REZ ID"]))
            logging.warning(
                f"Dropped {len(dropped_ne)} v7.4 new-entrant VRE rows in REZ "
                f"IDs with no trace coverage: {dropped_rez_ids}"
            )

    if dropped_frames:
        manifest_path = cache_path / "dropped_generators_iasr_2025-26.csv"
        pd.concat(dropped_frames, ignore_index=True).to_csv(manifest_path, index=False)


def _collect_trace_names(parquet_files: list[Path], column: str) -> set[str]:
    frames = [pd.read_parquet(f, columns=[column]) for f in parquet_files]
    return set(pd.concat(frames, ignore_index=True)[column].dropna().unique())


def _vre_rows_without_trace(
    summary: pd.DataFrame, trace_projects: set[str]
) -> pd.Series:
    is_vre = summary["Technology Type"].str.contains(
        "Wind|Solar", case=False, na=False
    )
    is_ecaa = summary["Status"].isin(
        ["Existing", "Committed", "Anticipated", "Additional policy-supported project"]
    )
    has_no_trace = ~summary["Power Station"].isin(trace_projects)
    return is_vre & is_ecaa & has_no_trace


def _vre_new_entrant_rows_without_zone_trace(
    new_entrants: pd.DataFrame, trace_zones: set[str]
) -> pd.Series:
    """New-entrant VRE rows (Wind/Solar Technology Type) whose REZ ID isn't
    in the trace zone coverage set. Non-VRE rows (CCGT, batteries, etc.) and
    rows with `Not Applicable` REZ ID are kept unchanged.
    """
    if "REZ ID" not in new_entrants.columns:
        return pd.Series([False] * len(new_entrants), index=new_entrants.index)
    is_vre = new_entrants["Technology Type"].str.contains(
        "Wind|Solar", case=False, na=False
    )
    has_rez_id = new_entrants["REZ ID"].notna() & (
        new_entrants["REZ ID"] != "Not Applicable"
    )
    has_no_trace = ~new_entrants["REZ ID"].isin(trace_zones)
    return is_vre & has_rez_id & has_no_trace


def _trace_filter_manifest_columns(summary: pd.DataFrame) -> list[str]:
    preferred = ["Power Station", "Technology Type", "Status", "Region", "Sub-region"]
    return [c for c in preferred if c in summary.columns]
