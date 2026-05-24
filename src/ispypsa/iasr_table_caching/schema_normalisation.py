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
    # technology label (e.g. "OCGT (small GT)") used as the new entrant
    # identifier; v7.4 carries the same identifier under `Power Station`.
    # Renaming here keeps the templater version-naive.
    # Also renames the connection-cost zone identifier (`Connection cost_REZ/Region`
    # → `Connection cost_Region`) — v7.4 dropped the `REZ/` prefix.
    "new_entrants_summary": {
        "New entrants": "Power Station",
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
}


# v7.4-side normalisations: cases where v7.4 introduced a footnote/typo that
# the canonical form should not have. Applied regardless of source version
# (the rename is a no-op if the source column name doesn't appear).
_V74_TO_CANONICAL_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "lead_time_and_project_life": {
        # v7.4 appended a "4," footnote text to this column header.
        "Total lead time (years)4,": "Total lead time (years)",
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
    """Pivot v7.4's long-form `other_outages_existing_generators` (rows keyed
    by Fuel type + Property, year columns 2025-26 through 2034-35) into the
    wide single-value-per-(fuel_type, property) form the templater consumes.

    Per design call 2026-05-24: the deliverable uses v7.4's time series in
    principle, but the current static-generator-property templater pipeline
    consumes scalar values per (Fuel type, Property). For Phase 1 we pick
    the first published year (2025-26) and document the simplification in
    the methodology tab; full year-by-year support is a follow-up that
    requires plumbing time-varying outage parameters through to PyPSA.

    The extrapolation policy ("hold 2034-35 constant through 2050-51") only
    becomes relevant when the templater starts emitting the full time
    series; with single-year extraction it's a no-op.

    No-op if the file is missing or already in wide form.
    """
    path = cache_path / "other_outages_existing_generators.csv"
    if not path.exists():
        return
    df = pd.read_csv(path)
    if "Property" not in df.columns:
        return  # already wide
    if _V74_OTHER_OUTAGES_SINGLE_YEAR_COLUMN not in df.columns:
        return  # no expected year column to pivot on
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
        df = df.rename(columns={name_col: "Power Station"})
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
