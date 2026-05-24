from pathlib import Path

import pandas as pd
from isp_workbook_parser import Parser

from ..feature_flags import FEATURE_FLAGS
from ..templater.mappings import (
    _ACTIONABLE_ISP_PROJECTS_TABLES,
    _FLOW_PATH_AGUMENTATION_TABLES,
    _FLOW_PATH_AUGMENTATION_COST_TABLES,
    _GENERATOR_PROPERTIES,
    _PREPATORY_ACTIVITIES_TABLES,
    _REZ_AUGMENTATION_COST_TABLES,
    _REZ_CONNECTION_AGUMENTATION_TABLES,
    _REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES,
    actionable_isp_projects_tables,
    flow_path_augmentation_cost_tables,
    flow_path_augmentation_tables,
    fuel_price_tables,
    generator_property_tables,
    generator_storage_summary_tables,
    prepatory_activities_tables,
    rez_augmentation_cost_tables,
    rez_connection_prepatory_activities_tables,
)

# Repo-tracked parser-config overrides for workbook versions the installed
# `isp-workbook-parser` doesn't ship configs for. Keys are the workbook
# version strings (as reported by the Change Log sheet); values are paths
# to a YAML config directory shaped like isp-workbook-parser's per-version
# subdirectories. See parser_configs/{version}/README.md for provenance.
_PARSER_CONFIG_OVERRIDES_ROOT = Path(__file__).parent / "parser_configs"
_PARSER_CONFIG_OVERRIDES = {
    "7.4": _PARSER_CONFIG_OVERRIDES_ROOT / "7.4",
}


def _build_required_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Return the list of IASR table names to extract for a given workbook version.

    Table-name dispatch between IASR 2024 v6.0 and IASR 2025 v7.3:
      - `initial_build_limits` (v6.0) → `initial_resource_limits` (v7.3): renamed
        and restructured (column range B:Y → B:O). The downstream templater code
        will be updated separately for the structural change.
      - `gpg_emissions_reduction_h2_kogan` + `_sa_turbine` (v6.0, two tables) →
        `hydrogen_limit_for_gpg` (v7.3, single consolidated table). The split→
        consolidation is handled in the templater layer in a follow-up patch.

    The `use_new_table_format` feature flag still gates the minimal-template path
    (network geography + transmission only) used by ongoing upstream refactors.
    """
    if FEATURE_FLAGS["use_new_table_format"]:
        _NETWORK_REQUIRED_TABLES = [
            "sub_regional_reference_nodes",
            "renewable_energy_zones",
            "flow_path_transfer_capability",
            "initial_transmission_limits",
        ]
        return _NETWORK_REQUIRED_TABLES

    is_v7 = iasr_workbook_version.startswith("7.")
    _initial_limits_table = "initial_resource_limits" if is_v7 else "initial_build_limits"
    _h2_gpg_tables = (
        ["hydrogen_limit_for_gpg"]
        if is_v7
        else ["gpg_emissions_reduction_h2_kogan", "gpg_emissions_reduction_h2_sa_turbine"]
    )
    _flow_path_augmentation_tables = flow_path_augmentation_tables(iasr_workbook_version)
    _flow_path_augmentation_cost_tables = flow_path_augmentation_cost_tables(iasr_workbook_version)
    _prepatory_activities_tables = prepatory_activities_tables(iasr_workbook_version)
    _actionable_isp_projects_tables = actionable_isp_projects_tables(iasr_workbook_version)
    _rez_augmentation_cost_tables = rez_augmentation_cost_tables(iasr_workbook_version)
    _rez_connection_prepatory_activities_tables = rez_connection_prepatory_activities_tables(iasr_workbook_version)
    if True:
        # Generator property tables (maximum_capacity, seasonal_ratings, etc.)
        # are version-dispatched: v6.0 has per-status tables; v7.x has
        # consolidated existing+committed+anticipated+additional + per-property
        # outage restructure. Fuel-price tables likewise restructured.
        _GENERATOR_PROPERTY_TABLES = generator_property_tables(iasr_workbook_version)
        _FUEL_PRICE_TABLES = fuel_price_tables(iasr_workbook_version)
        if is_v7:
            # v7.x consolidates new-entrant cost tables. Build costs are one
            # table now (scenario as a column, not as a suffix). Pumped hydro
            # build costs and per-policy build-cost tables also folded in.
            # Connection cost forecasts dropped the scenario suffix entirely.
            _NEW_ENTRANTS_COST_TABLES = [
                "build_costs",
                "connection_costs_for_wind_and_solar",
                "connection_costs_other",
                "connection_cost_forecast_wind_and_solar",
                "connection_cost_forecast_other",
            ]
        else:
            _NEW_ENTRANTS_COST_TABLES = [
                "coal_and_biomass_price_consultant_scenario_mapping",
                "biomass_prices",
                "build_costs_scenario_mapping",
                "build_costs_current_policies",
                "build_costs_global_nze_by_2050",
                "build_costs_global_nze_post_2050",
                "build_costs_pumped_hydro",
                "connection_costs_for_wind_and_solar",
                "connection_costs_other",
                "connection_cost_forecast_wind_and_solar_progressive_change",
                "connection_cost_forecast_wind_and_solar_step_change&green_energy_exports",
                "connection_cost_forecast_non_rez_progressive_change",
                "connection_cost_forecast_non_rez_step_change&green_energy_exports",
            ]
        _NETWORK_REQUIRED_TABLES = [
            "sub_regional_reference_nodes",
            "regional_topology_representation",
            "regional_reference_nodes",
            "renewable_energy_zones",
            "flow_path_transfer_capability",
            "interconnector_transfer_capability",
            _initial_limits_table,
        ]
        if is_v7:
            # v7.x split v6.0's `initial_build_limits` into resource limits
            # (wind/solar capacity, kept as `initial_resource_limits`) and a
            # separate `initial_transmission_limits` carrying REZ-to-grid
            # transmission network limits + expansion costs.
            _NETWORK_REQUIRED_TABLES.append("initial_transmission_limits")
        _NETWORK_REQUIRED_TABLES = (
            _NETWORK_REQUIRED_TABLES
            + _flow_path_augmentation_tables
            + _flow_path_augmentation_cost_tables
            + _prepatory_activities_tables
            + _actionable_isp_projects_tables
            + _REZ_CONNECTION_AGUMENTATION_TABLES
            + _rez_augmentation_cost_tables
            + _rez_connection_prepatory_activities_tables
        )
        _GENERATORS_STORAGE_REQUIRED_SUMMARY_TABLES = generator_storage_summary_tables(
            iasr_workbook_version
        )
        # Common static property tables. Fuel-price names dispatched via
        # `fuel_price_tables(version)` above; the remaining names below
        # (closure years, locational cost factors, LCFs, etc.) appear stable
        # across versions.
        _GENERATORS_REQUIRED_PROPERTY_TABLES = [
            "expected_closure_years",
            "coal_minimum_stable_level",
            *_FUEL_PRICE_TABLES,
            *_h2_gpg_tables,
            "gpg_emissions_reduction_biomethane",
            "locational_cost_factors",
            "technology_cost_breakdown_ratios",
            "lead_time_and_project_life",
            "technology_specific_lcfs",
        ] + _GENERATOR_PROPERTY_TABLES
        _BATTERY_REQUIRED_PROPERTY_TABLES = ["battery_properties"]
        if is_v7:
            # v7.x restructured the policy table set: renamed several to
            # `_target` form (dropped `_trajectory`), split NSW roadmap storage
            # into separate energy / power capacity trajectories, added LRET,
            # regional carbon budget, SA renewable generation target, and the
            # NSW roadmap min-VRE-generation target. Capacity Investment Scheme
            # tables are no longer in the workbook in v7.x (folded elsewhere).
            _POLICY_REQUIRED_TABLES = [
                "vic_renewable_target",
                "vic_storage_target",
                "vic_offshore_wind_target",
                "nsw_roadmap_storage_energy_capacity_trajectory",
                "nsw_roadmap_storage_power_capacity_trajectory",
                "nsw_roadmap_min_vre_generation_target",
                "tas_renewable_target_trajectory",
                "tas_renewable_energy_target",
                "lret_target",
                "regional_carbon_budget_trajectory",
                "sa_renewable_generation_target",
                # Note: QLD renewable target dropped from v7.x policy tables;
                # capacity_investment_scheme_* and powering_australia_plan tables
                # were also removed.
            ]
        else:
            _POLICY_REQUIRED_TABLES = [
                "vic_renewable_target_trajectory",
                "qld_renewable_target_trajectory",
                "powering_australia_plan_trajectory",
                "capacity_investment_scheme_renewable_trajectory",
                "capacity_investment_scheme_storage_trajectory",
                "nsw_roadmap_storage_trajectory",
                "vic_storage_target_trajectory",
                "vic_offshore_wind_target_trajectory",
                "nsw_roadmap_renewable_trajectory",
                "tas_renewable_target_trajectory",
            ]
        return (
            _NETWORK_REQUIRED_TABLES
            + _GENERATORS_STORAGE_REQUIRED_SUMMARY_TABLES
            + _GENERATORS_REQUIRED_PROPERTY_TABLES
            + _BATTERY_REQUIRED_PROPERTY_TABLES
            + _NEW_ENTRANTS_COST_TABLES
            + _POLICY_REQUIRED_TABLES
        )


REQUIRED_TABLES = _build_required_tables()


def required_tables_for_version(iasr_workbook_version: str) -> list[str]:
    """Public accessor used by callers that need the version-specific table list
    (e.g. cache invalidation, schema checks)."""
    return _build_required_tables(iasr_workbook_version)


def build_local_cache(
    cache_path: Path | str, workbook_path: Path | str, iasr_workbook_version: str
) -> None:
    """Uses `isp-workbook-parser` to build a local cache of parsed workbook CSVs

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.iasr_table_caching import build_local_cache

        Build the local cache of parsed workbook CSVs.
        >>> build_local_cache(
        ...     cache_path=Path("parsed_workbook_cache"),
        ...     workbook_path=Path("path/to/ISP_Workbook.xlsx"),
        ...     iasr_workbook_version="6.0"
        ... )

    Args:
        cache_path: Path that should be created for the local cache
        workbook_path: Path to an ISP Assumptions Workbook that is supported by
            `isp-workbook-parser`
        iasr_workbook_version: str specifying the version of the work being used.

    Returns:
        None
    """
    parser_config_override = _PARSER_CONFIG_OVERRIDES.get(iasr_workbook_version)
    if parser_config_override is not None:
        # The installed `isp-workbook-parser` doesn't ship a config dir for this
        # version (e.g. v7.4); point it at the repo-tracked override.
        workbook = Parser(Path(workbook_path), user_config_directory_path=parser_config_override)
    else:
        workbook = Parser(Path(workbook_path))
    if workbook.workbook_version != iasr_workbook_version:
        raise ValueError(
            "The IASR workbook provided does not match the version "
            "specified in the config."
        )
    tables_to_get = _build_required_tables(iasr_workbook_version)
    # When we're running with a repo-tracked config override (e.g. v7.4 cloned
    # from v7.5), the per-table `end_row` values drift one or two rows from
    # the actual workbook content because AEMO adjusted row counts between
    # releases. Disable the strict end-row config check so the parser extracts
    # data up to whatever it actually finds. Functional correctness is
    # unaffected — the parser still uses the config's sheet_name and header
    # offsets to identify table content.
    config_checks = parser_config_override is None
    workbook.save_tables(cache_path, tables=tables_to_get, config_checks=config_checks)
    # Translate older-version column headers to v7.4 canonical form. Cached
    # CSVs on disk become version-independent so downstream readers (templater,
    # tests) never see v6.0 column names.
    _normalise_cached_csvs_to_v74(Path(cache_path), tables_to_get, iasr_workbook_version)
    return None


def _normalise_cached_csvs_to_v74(
    cache_path: Path, table_names: list[str], source_version: str
) -> None:
    """Rewrite cached CSVs with v7.4 canonical column headers + filenames.

    For all versions, applies a parser-quirk fix: when the parser's multi-row
    header concatenates an empty top row with a value row, columns end up
    with a leading underscore (e.g. `_IASR ID`). Strip those.

    For v6.0 we also (a) rename columns in-place using the per-table rename
    map, then (b) rename files on disk to their v7.4 canonical names.
    Tables not in either map are left alone (no disk I/O).
    """
    from .schema_normalisation import (
        _V60_TO_V74_COLUMN_RENAMES,
        _V60_TO_V74_TABLE_RENAMES,
        _V74_TO_CANONICAL_COLUMN_RENAMES,
        consolidate_v60_ecaa_generator_summaries,
        consolidate_v60_h2_gpg_to_regional,
        consolidate_v60_marginal_loss_factors_tables,
        consolidate_v60_maximum_capacity_tables,
        expand_v60_auxiliary_load_to_per_generator,
        pivot_v74_other_outages_to_wide,
        split_v74_maximum_capacity_commissioning_dates,
        transform_v60_battery_properties_to_wide,
    )

    _strip_leading_underscore_columns(cache_path, table_names)
    # v7.4-side column normalisations (apply regardless of source — no-op when
    # the source column name isn't present).
    for table_name, renames in _V74_TO_CANONICAL_COLUMN_RENAMES.items():
        csv_path = cache_path / f"{table_name}.csv"
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path)
        df = df.rename(columns=renames)
        df.to_csv(csv_path, index=False)

    if source_version.startswith("7."):
        # v7.4 carries a single `Commissioning date` column on the consolidated
        # max_capacity table; downstream code expects the v6.0 two-column
        # layout (confirmed vs indicative) which we restore here so the
        # templater stays version-naive.
        split_v74_maximum_capacity_commissioning_dates(cache_path)
        # v7.4 `other_outages_existing_generators` is long-form
        # (Fuel type × Property × year columns); templater consumes scalars,
        # so we pivot to wide-form picking the 2025-26 year. See design call
        # 2026-05-24 — full time-series consumption is Phase 1+ work.
        pivot_v74_other_outages_to_wide(cache_path)
        return

    for table_name in table_names:
        csv_path = cache_path / f"{table_name}.csv"
        if not csv_path.exists():
            continue
        column_renames = _V60_TO_V74_COLUMN_RENAMES.get(table_name)
        if column_renames:
            df = pd.read_csv(csv_path)
            df = df.rename(columns=column_renames)
            df.to_csv(csv_path, index=False)
        new_table_name = _V60_TO_V74_TABLE_RENAMES.get(table_name)
        if new_table_name:
            csv_path.rename(cache_path / f"{new_table_name}.csv")

    # Class (c1): concat v6.0's four per-status ECAA summary CSVs into the
    # v7.4 single-consolidated form, then delete the originals. Templater
    # downstream always reads the consolidated form.
    consolidate_v60_ecaa_generator_summaries(cache_path)
    # Class (c4a): same pattern for the per-status maximum_capacity tables,
    # with explicit column alignment (Generator/Project → Power Station,
    # Energy (MWh) → Storage Capacity (MWh), Status column derived).
    consolidate_v60_maximum_capacity_tables(cache_path)
    # Same concat pattern for the marginal_loss_factors per-status + batteries
    # tables (v7.4 keeps the legacy `_existing_generators` filename despite
    # the consolidated table carrying all statuses).
    consolidate_v60_marginal_loss_factors_tables(cache_path)
    # Class (c4d): expand v6.0's per-tech auxiliary_load lookup table into
    # v7.4's per-generator shape by joining with the (just-consolidated)
    # generator summary. Must run after `consolidate_v60_ecaa_generator_summaries`.
    expand_v60_auxiliary_load_to_per_generator(cache_path)
    # Class (c3): combine v6.0's two named-generator H2 GPG tables into
    # v7.4's region-keyed `hydrogen_limit_for_gpg` shape.
    consolidate_v60_h2_gpg_to_regional(cache_path)
    # battery_properties shape: transpose v6.0 long-form to v7.4 wide-form,
    # embed units into column headers.
    transform_v60_battery_properties_to_wide(cache_path)


def _strip_leading_underscore_columns(
    cache_path: Path, table_names: list[str]
) -> None:
    """Strip leading underscores from cached CSV column headers.

    `isp_workbook_parser` joins multi-row headers with `_`. When the top row
    of a header pair is blank for some columns (e.g. identifier columns under
    a seasonal-label-only top row in v7.4 seasonal_ratings), the joined name
    ends up with a leading underscore. This is a parser artifact, not real
    schema, so we strip it once at cache-load time.
    """
    for table_name in table_names:
        csv_path = cache_path / f"{table_name}.csv"
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path)
        renames = {
            col: col.lstrip("_")
            for col in df.columns
            if isinstance(col, str) and col.startswith("_")
        }
        if renames:
            df = df.rename(columns=renames)
            df.to_csv(csv_path, index=False)


def list_cache_files(cache_path, iasr_workbook_version: str = "6.0"):
    """Return the list of cache file paths produced by `build_local_cache`.

    Reflects post-normalisation file names (always v7.4 canonical), so callers
    that check cache freshness see the same names regardless of which source
    workbook produced the cache.
    """
    from .schema_normalisation import (
        _V60_GENERATOR_SUMMARY_SOURCES,
        _V60_MAX_CAPACITY_SOURCES,
        _V60_MLF_SOURCES,
        _V60_TO_V74_TABLE_RENAMES,
        _V74_CONSOLIDATED_GENERATOR_SUMMARY,
        _V74_MAX_CAPACITY_CONSOLIDATED,
        _V74_MLF_CONSOLIDATED,
    )

    files = _build_required_tables(iasr_workbook_version)
    if not iasr_workbook_version.startswith("7."):
        # Apply 1:1 table renames.
        files = [_V60_TO_V74_TABLE_RENAMES.get(name, name) for name in files]
        # Collapse the four per-status summary entries into the single
        # consolidated entry that exists on disk after normalisation.
        per_status = set(_V60_GENERATOR_SUMMARY_SOURCES)
        if any(name in per_status for name in files):
            files = [name for name in files if name not in per_status]
            files.append(_V74_CONSOLIDATED_GENERATOR_SUMMARY)
        # Same collapse for the per-status maximum_capacity tables.
        per_status_max_cap = set(_V60_MAX_CAPACITY_SOURCES)
        if any(name in per_status_max_cap for name in files):
            files = [name for name in files if name not in per_status_max_cap]
            files.append(_V74_MAX_CAPACITY_CONSOLIDATED)
        # Same collapse for the marginal_loss_factors per-status + batteries
        # tables (target name keeps v7.4's legacy `_existing_generators`).
        per_status_mlf = set(_V60_MLF_SOURCES) - {_V74_MLF_CONSOLIDATED}
        if any(name in per_status_mlf for name in files):
            files = [name for name in files if name not in per_status_mlf]
            if _V74_MLF_CONSOLIDATED not in files:
                files.append(_V74_MLF_CONSOLIDATED)
    return [cache_path / Path(file + ".csv") for file in files]
