import pandas as pd

from .helpers import _snakecase_string
from .lists import (
    _ALL_GENERATOR_STORAGE_TYPES,
    _CONDENSED_GENERATOR_TYPES,
    _ECAA_BATTERY_TYPES,
    _ECAA_GENERATOR_TYPES,
    _ISP_SCENARIOS,
)

_NEM_REGION_IDS = pd.Series(
    {
        "Queensland": "QLD",
        "New South Wales": "NSW",
        "Victoria": "VIC",
        "South Australia": "SA",
        "Tasmania": "TAS",
    },
    name="nem_region_id_mapping",
)

_SINGLE_REGION_ID = "NEM"

_NEM_SUB_REGION_IDS = pd.Series(
    {
        "Northern Queensland": "NQ",
        "Central Queensland": "CQ",
        "Gladstone Grid": "GG",
        "Southern Queensland": "SQ",
        "Northern New South Wales": "NNSW",
        "Central New South Wales": "CNSW",
        "Southern New South Wales": "SNSW",
        "Sydney, Newcastle, Wollongong": "SNW",
        # v7.4 split the single "Victoria" subregion into three (MEL/SEV/WNV) and
        # added "Northern South Australia". "Victoria" is kept for the regional
        # reference-node table (the VIC region). Without the three VIC + NSA
        # entries, these fall through as descriptive names (or fuzzy-match the
        # stale "Victoria"->VIC) and no longer match the demand store's codes.
        "Victoria": "VIC",
        "Greater Melbourne and Geelong": "MEL",
        "South East Victoria": "SEV",
        "West and North Victoria": "WNV",
        "Central South Australia": "CSA",
        "Northern South Australia": "NSA",
        "South East South Australia": "SESA",
        "Tasmania": "TAS",
    },
    name="nem_region_id_mapping",
)

_HVDC_FLOW_PATHS = pd.DataFrame(
    {
        "node_from": ["NNSW", "VIC", "TAS"],
        "node_to": ["SQ", "CSA", "VIC"],
        "flow_path": ["Terranora", "Murraylink", "Basslink"],
    }
)

_GENERATOR_PROPERTIES = {
    "maximum_capacity": _ALL_GENERATOR_STORAGE_TYPES,
    "seasonal_ratings": _ALL_GENERATOR_STORAGE_TYPES,
    "maintenance": ["existing_generators", "new_entrants"],
    "fixed_opex": _CONDENSED_GENERATOR_TYPES,
    "variable_opex": _CONDENSED_GENERATOR_TYPES,
    "marginal_loss_factors": _ALL_GENERATOR_STORAGE_TYPES,
    "auxiliary_load": _CONDENSED_GENERATOR_TYPES,
    "heat_rates": _CONDENSED_GENERATOR_TYPES,
    "outages_2023-2024": ["existing_generators"],
    "long_duration_outages": ["existing_generators"],
    "outages": ["new_entrants"],
    "full_outages_forecast": ["existing_generators"],
    "partial_outages_forecast": ["existing_generators"],
    "gpg_min_stable_level": ["existing_generators", "new_entrants"],
    "coal_prices": list(map(_snakecase_string, _ISP_SCENARIOS)),
    "gas_prices": list(map(_snakecase_string, _ISP_SCENARIOS)),
}

_ECAA_GENERATOR_NEW_COLUMN_MAPPING = {
    "partial_outage_derating_factor_%": "forced_outage_rate_partial_outage_%_of_time",
    "commissioning_date": "generator",
    "closure_year": "generator",
    # v7.4 publishes per-generator auxiliary_load values; v6.0 had a per-tech
    # lookup that the schema normalisation layer expands to per-generator.
    # In both versions the templater looks up by Power Station, so seed the
    # summary's `auxiliary_load_%` column with the generator name (= Power
    # Station after the (c1) consolidation rename).
    "auxiliary_load_%": "generator",
}

_NEW_ENTRANT_GENERATOR_NEW_COLUMN_MAPPING = {
    "partial_outage_derating_factor_%": "forced_outage_rate_partial_outage_%_of_time",
    # "maximum_capacity_mw": "generator_name",
    # "unit_capacity_mw": "generator_name",
    "lifetime": "generator_name",
    "minimum_stable_level_%": "technology_type",
    # v7.4 dropped the per-row Summer/Winter rating (MW) columns from
    # new_entrants_summary (v6.0 stored tech-name lookup strings there, not
    # actual MW values). The downstream static-property lookup against
    # seasonal_ratings_new_entrants uses these columns as the fuzzy-match
    # key — so `technology_type` (present in both versions, same tech-name
    # values) serves as the seed for all three rating columns.
    "summer_peak_rating_%": "technology_type",
    "summer_rating_mw": "technology_type",
    "winter_rating_mw": "technology_type",
    "technology_specific_lcf_%": "regional_build_cost_zone",
}

_ECAA_STORAGE_NEW_COLUMN_MAPPING = {
    "maximum_capacity_mw": "storage_name",
    "energy_capacity_mwh": "storage_name",
    "storage_duration_hours": "storage_name",
    "round_trip_efficiency_%": "fom_$/kw/annum",
    "charging_efficiency_%": "fom_$/kw/annum",
    "discharging_efficiency_%": "fom_$/kw/annum",
    "lifetime": "storage_name",
    "commissioning_date": "storage_name",
    "closure_year": "storage_name",
    "isp_resource_type": "fom_$/kw/annum",
}

_NEW_STORAGE_NEW_COLUMN_MAPPING = {
    # v7.5 renamed new-entrant batteries from the generic tech label to
    # per-subregion names ("NQ Battery - 1h"), so the static-property columns
    # must be seeded from `technology_type` (the generic "Battery Storage
    # (Nhr storage)") — which is what the property tables are keyed on — rather
    # than `storage_name`. In v7.4 the two were identical, so no regression.
    "maximum_capacity_mw": "technology_type",
    "storage_duration_hours": "technology_type",
    "round_trip_efficiency_%": "technology_type",
    "charging_efficiency_%": "technology_type",
    "discharging_efficiency_%": "technology_type",
    "lifetime": "technology_type",
    "technology_specific_lcf_%": "regional_build_cost_zone",
    "isp_resource_type": "technology_type",
}

"""
_NEW_COLUMN_MAPPING dicts define new/additional columns to be added to the corresponding
ECAA or new entrant generator summary tables. Keys are the name of the column to be added
(corresponding to the table/column name of the data being added) and values are the name
of the column in the existing summary table that holds the required data mapping for merging
in the new column.
"""

_ECAA_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "maximum_capacity_mw": dict(
        # v7.4 canonical: single consolidated max_capacity table. v6.0's four
        # per-status tables are concatenated into this form by schema
        # normalisation, with Generator/Project both mapped to Power Station.
        table="maximum_capacity_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Installed capacity (MW)",
    ),
    "commissioning_date": dict(
        table="maximum_capacity_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Commissioning date",
        # Confirmed (Committed) dates land in `Commissioning date`; indicative
        # (Anticipated/Additional) dates land in `Indicative commissioning date`.
        # Schema normalisation preserves this two-column layout across both
        # v6.0 (concat populates the right column per source) and v7.4
        # (single date column gets split by Status).
        alternative_values=["Indicative commissioning date"],
    ),
    "maintenance_duration_%": dict(
        table="maintenance_existing_generators",
        table_lookup="Generator type",
        table_value="Proportion of time out (%)",
    ),
    "minimum_load_mw": dict(
        table="coal_minimum_stable_level",
        # v7.4 canonical: keyed by per-unit IASR ID, value uses the technical
        # floor sub-column. v6.0's single value is renamed to match by schema
        # normalisation.
        table_lookup="IASR ID",
        table_value="Minimum Stable Level (MW)_Minimum Continuous Operating Level",
    ),
    "fom_$/kw/annum": dict(
        table="fixed_opex_existing_committed_anticipated_additional_generators",
        # v7.4 canonical: lookup by Power Station. v6.0's `Generator` header
        # is renamed to `Power Station` by the schema normalisation layer.
        table_lookup="Power Station",
        table_value="Fixed OPEX ($/kW/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        table="variable_opex_existing_committed_anticipated_additional_generators",
        # v7.4 keys this table by "Power Station / Technology" (the cell can
        # be either a station name or a technology label). Normalised from
        # v6.0's plain `Generator` header.
        table_lookup="Power Station / Technology",
        table_value="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        table="heat_rates_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        # v7.4 canonical: single MLF table named (misleadingly)
        # `marginal_loss_factors_existing_generators` but actually carrying all
        # ECAA statuses plus batteries via a `Status` column. Schema
        # normalisation consolidates v6.0's per-status + batteries MLF tables
        # into this same shape; the `MLF - Generation` v6.0 value column for
        # additional + batteries is normalised to `MLF`.
        table="marginal_loss_factors_existing_generators",
        table_lookup="Power Station",
        table_value="MLF",
    ),
    "auxiliary_load_%": dict(
        table="auxiliary_load_existing_committed_anticipated_additional_generators",
        # v7.4 canonical: per-generator (Power Station) lookup, value framed
        # as % of generation. Denominator change vs v6.0's "% of nameplate
        # capacity" is small for thermal plants (capacity factors near unity
        # in load periods) — methodology tab documents the shift; the
        # templater consumes whichever value the cache holds.
        table_lookup="Power Station",
        table_value="Auxiliary load (% of generation)",
    ),
    # v7.4 canonical: outages live in `other_outages_existing_generators`,
    # which schema normalisation pivots to a single-year wide form (2025-26
    # column extracted from v7.4's per-year time series; v6.0's single
    # value column-renamed to match). Full time-series consumption is
    # Phase 1+ work — see design call 2026-05-24.
    "partial_outage_derating_factor_%": dict(
        table="other_outages_existing_generators",
        table_lookup="Fuel type",
        table_value="Partial Outage Derating Factor (%)",
        generator_status="Existing",
    ),
    "mean_time_to_repair_full_outage": dict(
        table="other_outages_existing_generators",
        table_lookup="Fuel type",
        table_value="Full outage MTTR (hrs)",
        generator_status="Existing",
    ),
    "mean_time_to_repair_partial_outage": dict(
        table="other_outages_existing_generators",
        table_lookup="Fuel type",
        table_value="Partial outage MTTR (hrs)",
        generator_status="Existing",
    ),
}
""""
Existing, committed, anticipated and additional summary table columns mapped to
corresponding IASR tables and lookup information that can be used to retrieve values.

    `table`: IASR table name or a list of table names.
    `table_lookup`: Column in the table that acts as a key for merging into the summary
    `alternative_lookups`: A list of alternative key columns, e.g. "Project" as an
        alternative to  "Generator" in the additional projects table. If a lookup value
        is NA in the `table_lookup` column, it will be replaced by a lookup value from
        this list in the order specified.
    `table_value`: Column in the table that corresponds to the data to be merged in
    `alternative_values`: As for `alternative_lookups`, but for the data values in the
        table, e.g. "MLF - Generation" instead of "MLF" in the additional projects table
    `new_col_name`: The name that will be used to rename the column in the summary table
"""

_NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP = {
    "summer_peak_rating_%": dict(
        table="seasonal_ratings_new_entrants",
        # v7.4 canonical: `Technology Type`; v6.0's `Generator type` is
        # renamed at cache load to match.
        table_lookup="Technology Type",
        table_value="Summer Peak (% of nameplate)",
    ),
    "summer_rating_mw": dict(
        table="seasonal_ratings_new_entrants",
        table_lookup="Technology Type",
        table_value="Summer Typical (% of nameplate)",
        new_col_name="summer_typical_rating_%",
    ),
    "winter_rating_mw": dict(
        table="seasonal_ratings_new_entrants",
        table_lookup="Technology Type",
        table_value="Winter (% of nameplate)",
        new_col_name="winter_rating_%",
    ),
    # "maximum_capacity_mw": dict(
    #     table="maximum_capacity_new_entrants",
    #     table_lookup="Generator type",
    #     table_value="Total plant size (MW)",
    # ),
    # "unit_capacity_mw": dict(
    #     table="maximum_capacity_new_entrants",
    #     table_lookup="Generator type",
    #     table_value="Unit size (MW)",
    # ),
    "maintenance_duration_%": dict(
        table="maintenance_new_entrants",
        # v7.4 canonical: `Technology Type` (was v6.0 `Generator type`,
        # renamed at cache load).
        table_lookup="Technology Type",
        table_value="Proportion of time out (%)",
    ),
    "fom_$/kw/annum": dict(
        table="fixed_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Fixed OPEX ($/kW sent out/year)",
    ),
    "vom_$/mwh_sent_out": dict(
        table="variable_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Variable OPEX ($/MWh sent out)",
    ),
    "heat_rate": dict(
        table="heat_rates_new_entrants",
        table_lookup="Technology",
        table_value="Heat rate (GJ/MWh)",
        new_col_name="heat_rate_gj/mwh",
    ),
    "mlf": dict(
        table="marginal_loss_factors_new_entrants",
        # v7.4 canonical: `IASR ID` (was v6.0 `Generator`; renamed at cache load).
        table_lookup="IASR ID",
        table_value="MLF",
    ),
    "auxiliary_load_%": dict(
        table="auxiliary_load_new_entrants",
        table_lookup="Generator",
        # v7.4 canonical: framing changed from "% of nameplate" to "% of
        # generation" — small accuracy difference for typical load periods.
        table_value="Auxiliary load (% of generation)",
    ),
    "partial_outage_derating_factor_%": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        table_value="Partial Outage Derating Factor (%)",
    ),
    "mean_time_to_repair_full_outage": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        # v7.4 canonical column names (v6.0 wrapped in "Mean time to repair (hrs)_"
        # prefix; renamed at cache load).
        table_value="Full outage MTTR (hrs)",
    ),
    "mean_time_to_repair_partial_outage": dict(
        table="outages_new_entrants",
        table_lookup="Fuel type",
        table_value="Partial outage MTTR (hrs)",
    ),
    "lifetime": dict(
        table="lead_time_and_project_life",
        table_lookup="Technology",
        # v6.0 carried a "6" footnote marker on the header; schema normalisation
        # strips it so the canonical name has no footnote text.
        table_value="Technical life (years)",
    ),
    "total_lead_time": dict(
        table="lead_time_and_project_life",
        table_lookup="Technology",
        table_value="Total lead time (years)",
    ),
}
"""
New entrant generators summary table columns mapped to corresponding IASR table and
lookup information that can be used to retrieve values.

    `table`: IASR table name or a list of table names.
    `table_lookup`: Column in the table that acts as a key for merging into the summary
    `alternative_lookups`: A list of alternative key columns, e.g. "Project" as an
        alternative to  "Generator" in the additional projects table. If a lookup value
        is NA in the `table_lookup` column, it will be replaced by a lookup value from
        this list in the order specified.
    `table_value`: Column in the table that corresponds to the data to be merged in
    `alternative_values`: As for `alternative_lookups`, but for the data values in the
        table
    `new_col_name`: The name that will be used to rename the column in the summary table
    `table_col_prefix`: The string that is present at the start of each column name
        in the table as a result of row merging in isp-workbook-parser, to be used
        for opex mapping to rename columns in the table.
"""

_ECAA_STORAGE_STATIC_PROPERTY_TABLE_MAP = {
    # v7.4 folded the v6.0 batteries summary AND the v6.0 per-status
    # maximum_capacity tables into the single consolidated generators table.
    # Schema normalisation produces the same shape for v6.0 — batteries and
    # all gen statuses concat into this one table — so the storage code reads
    # the same source as the ECAA generator code, then filters by
    # technology_type containing "battery" downstream.
    "maximum_capacity_mw": dict(
        table="maximum_capacity_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Installed capacity (MW)",
    ),
    "energy_capacity_mwh": dict(
        table="maximum_capacity_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Storage Capacity (MWh)",
    ),
    "commissioning_date": dict(
        table="maximum_capacity_existing_committed_anticipated_additional_generators",
        table_lookup="Power Station",
        table_value="Indicative commissioning date",
    ),
    "fom_$/kw/annum": dict(
        table="fixed_opex_existing_committed_anticipated_additional_generators",
        # v7.4 canonical; v6.0's `Generator` header is renamed at cache load.
        table_lookup="Power Station",
        table_value="Fixed OPEX ($/kW/year)",
    ),
    "round_trip_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        # v7.4 canonical: short property names with unit suffix; v6.0's
        # `(utility)` qualifier is dropped by schema normalisation.
        table_value="Round trip efficiency_%",
    ),
    "charging_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        table_value="Charge efficiency_%",
    ),
    "discharging_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        table_value="Discharge efficiency_%",
    ),
}

_NEW_ENTRANT_STORAGE_STATIC_PROPERTY_TABLE_MAP = {
    "fom_$/kw/annum": dict(
        table="fixed_opex_new_entrants",
        table_lookup="Generator",
        table_col_prefix="Fixed OPEX ($/kW sent out/year)",
    ),
    "lifetime": dict(
        table="lead_time_and_project_life",
        table_lookup="Technology",
        table_value="Technical life (years)",
    ),
    "storage_duration_hours": dict(
        table="battery_properties",
        table_lookup="storage_name",
        # v7.4: column carries duration in hours under `Energy capacity_Hours`
        # (v6.0 misleadingly labelled the same hour values as MWh in the
        # `Units` column; schema normalisation renames to the v7.4 form).
        table_value="Energy capacity_Hours",
    ),
    "round_trip_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        # v7.4 canonical: short property names with unit suffix; v6.0's
        # `(utility)` qualifier is dropped by schema normalisation.
        table_value="Round trip efficiency_%",
    ),
    "charging_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        table_value="Charge efficiency_%",
    ),
    "discharging_efficiency_%": dict(
        table="battery_properties",
        table_lookup="storage_name",
        table_value="Discharge efficiency_%",
    ),
}

"""
 _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP is a dictionary that maps template functions to
 lists of dictionaries containing the CSV file name, region_id and policy_id for each
 parsed table.
     `csv`: A single CSV file name (excluding file extension)
     `region_id`: region corresponding to that parsed table, to be inputted
         into templated table
     `policy_id`: policy corresponding to that parsed table, to be inputted
         into templated table links with the manually_extracted_table
         `policy_generator_types`
 """
_TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP = {
    "template_renewable_share_targets": [
        {
            "csv": "vic_renewable_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vret",
        },
        {
            "csv": "qld_renewable_target_trajectory",
            "region_id": "QLD",
            "policy_id": "qret",
        },
    ],
    "template_powering_australia_plan": [
        {
            "csv": "powering_australia_plan_trajectory",
            "region_id": "NEM",
            "policy_id": "power_aus",
        },
    ],
    "template_technology_capacity_targets": [
        {
            "csv": "capacity_investment_scheme_renewable_trajectory",
            "region_id": "NEM",
            "policy_id": "cis_generator",
        },
        {
            "csv": "capacity_investment_scheme_storage_trajectory",
            "region_id": "NEM",
            "policy_id": "cis_storage",
        },
        {
            "csv": "nsw_roadmap_storage_trajectory",
            "region_id": "NSW",
            "policy_id": "nsw_eir_sto",
        },
        {
            "csv": "vic_storage_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vic_storage",
        },
        {
            "csv": "vic_offshore_wind_target_trajectory",
            "region_id": "VIC",
            "policy_id": "vic_offshore_wind",
        },
    ],
    "template_renewable_generation_targets": [
        {
            "csv": "nsw_roadmap_renewable_trajectory",
            "region_id": "NSW",
            "policy_id": "nsw_eir_gen",
        },
        {
            "csv": "tas_renewable_target_trajectory",
            "region_id": "TAS",
            "policy_id": "tret",
        },
    ],
}


# Subregion flow paths. AEMO restructured the NEM sub-regions in IASR 2025-26
# v7.4 (Aug 2025 release): Victoria split into WNV (Western North Victoria),
# MEL (Melbourne), and SEV (South East Victoria); NSA (Northern South Australia)
# added. The v6.0 module-level constants below are the v6.0 (10-flow-path)
# baseline; for v7.x runs, callers should use `subregion_flow_paths(version)`
# and related helpers, which return the v7.4 superset (14 flow paths).
_SUBREGION_FLOW_PATHS_V60 = [
    "CQ-NQ",
    "CQ-GG",
    "SQ-CQ",
    "NNSW-SQ",
    "CNSW-NNSW",
    "CNSW-SNW",
    "SNSW-CNSW",
    "VIC-SNSW",
    "TAS-VIC",
    "VIC-SESA",
    "SESA-CSA",
]

_SUBREGION_FLOW_PATHS_V74 = [
    "CQ-NQ",
    "CQ-GG",
    "SQ-CQ",
    "NNSW-SQ",
    "CNSW-NNSW",
    "CNSW-SNW",
    "SNSW-CNSW",
    "WNV-SNSW",       # was VIC-SNSW in v6.0
    "TAS-SEV",        # was TAS-VIC in v6.0
    "WNV-SESA",       # was VIC-SESA in v6.0
    "SESA-CSA",
    "CSA-NSA",        # new in v7.4 (NSA = Northern South Australia)
    "MEL-WNV",        # new in v7.4 (intra-Victoria, Melbourne to Western North VIC)
    "SEV-MEL",        # new in v7.4 (intra-Victoria, South East VIC to Melbourne)
]

# Per-flow-path identifier overrides for v7.x augmentation-cost tables.
# The shipped isp-workbook-parser config has inconsistent underscore vs dash
# usage; the team's repo-tracked 7.4 config normalises everything to dashes
# (see parser_configs/7.4/flow_path_costs_forecasts.yaml v7.4-adjusted notes).
# This dict is empty after normalisation but retained so future per-path
# overrides have a clear home.
_V74_AUGMENTATION_COST_PATH_OVERRIDES: dict[str, str] = {}

# Per-version ISP cost-scenario table suffixes. v6.0 used a two-suffix scheme
# (Progressive Change vs the combined Step Change + Green Energy Exports);
# v7.x uses three distinct scenarios.
_COST_SCENARIO_SUFFIXES_V60 = (
    "progressive_change",
    "step_change_and_green_energy_exports",
)
_COST_SCENARIO_SUFFIXES_V74 = (
    "slower_growth",
    "step_change",
    "accelerated_transition",
)


def cost_scenario_suffixes(iasr_workbook_version: str = "6.0") -> tuple[str, ...]:
    """Return the cost-scenario filename suffixes for a given workbook version."""
    return _COST_SCENARIO_SUFFIXES_V74 if iasr_workbook_version.startswith("7.") else _COST_SCENARIO_SUFFIXES_V60


def prepatory_activities_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """v6.0 had per-scenario preparatory_activities cost tables; v7.x dropped them."""
    if iasr_workbook_version.startswith("7."):
        return []
    return [
        "flow_path_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
        "flow_path_augmentation_costs_progressive_change_preparatory_activities",
    ]


def rez_connection_prepatory_activities_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """v6.0 had per-scenario rez preparatory_activities tables; v7.x dropped them."""
    if iasr_workbook_version.startswith("7."):
        return []
    return [
        "rez_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
        "rez_augmentation_costs_progressive_change_preparatory_activities",
    ]


def actionable_isp_projects_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """v6.0 had per-scenario actionable_isp_projects cost tables; v7.x dropped them."""
    if iasr_workbook_version.startswith("7."):
        return []
    return [
        "flow_path_augmentation_costs_step_change_and_green_energy_exports_actionable_isp_projects",
        "flow_path_augmentation_costs_progressive_change_actionable_isp_projects",
    ]


def rez_augmentation_cost_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """REZ augmentation cost tables per scenario per NEM region (always 5 regions)."""
    regions = ["QLD", "NSW", "VIC", "SA", "TAS"]
    return [
        f"rez_augmentation_costs_{scen}_{region}"
        for scen in cost_scenario_suffixes(iasr_workbook_version)
        for region in regions
    ]


def generator_property_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Per-property generator/storage data tables. Replaces the v6.0
    `_GENERATOR_PROPERTIES`-driven Cartesian-product list when version is v7.x.

    v7.x consolidates 4 ECAA generator types into one (`existing_committed_
    anticipated_additional_generators`), so per-property tables drop from
    ~5×N to ~2×N. Some properties still differentiate only by existing-vs-new.
    Pattern is per-property; we enumerate exactly.
    """
    if not iasr_workbook_version.startswith("7."):
        # v6.0 path: build from _GENERATOR_PROPERTIES Cartesian product
        from .lists import _ALL_GENERATOR_STORAGE_TYPES, _CONDENSED_GENERATOR_TYPES
        # Replicates the inline build in local_cache.py for v6.0
        v60_props = {
            "maximum_capacity": _ALL_GENERATOR_STORAGE_TYPES,
            "seasonal_ratings": _ALL_GENERATOR_STORAGE_TYPES,
            "maintenance": ["existing_generators", "new_entrants"],
            "fixed_opex": _CONDENSED_GENERATOR_TYPES,
            "variable_opex": _CONDENSED_GENERATOR_TYPES,
            "marginal_loss_factors": _ALL_GENERATOR_STORAGE_TYPES,
            "auxiliary_load": _CONDENSED_GENERATOR_TYPES,
            "heat_rates": _CONDENSED_GENERATOR_TYPES,
            "outages_2023-2024": ["existing_generators"],
            "long_duration_outages": ["existing_generators"],
            "outages": ["new_entrants"],
            "full_outages_forecast": ["existing_generators"],
            "partial_outages_forecast": ["existing_generators"],
            "gpg_min_stable_level": ["existing_generators", "new_entrants"],
        }
        return [
            f"{prop}_{gen_type}"
            for prop, gen_types in v60_props.items()
            for gen_type in gen_types
        ]
    # v7.x: per-property explicit lists
    v74_props = {
        "maximum_capacity": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "seasonal_ratings": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "maintenance": ["existing_generators", "new_entrants"],
        "fixed_opex": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "variable_opex": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "marginal_loss_factors": ["existing_generators", "new_entrants", "new_entrant_electrolysers"],
        "auxiliary_load": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "heat_rates": ["existing_committed_anticipated_additional_generators", "new_entrants"],
        "gpg_min_stable_level": ["existing_generators", "new_entrants"],
        "affine_heat_rates": ["existing_generators", "new_entrants"],
        # Outage tables: v7.x consolidated full+partial+2023-2024 into "other_outages"
        "long_duration_outages": ["existing_generators"],
        "other_outages": ["existing_generators"],
        "outages": ["new_entrants"],
    }
    return [
        f"{prop}_{gen_type}"
        for prop, gen_types in v74_props.items()
        for gen_type in gen_types
    ]


def fuel_price_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Fuel-price table names per workbook version.

    v6.0: scenario-suffixed coal/gas tables (coal_prices_step_change,
    gas_prices_progressive_change, etc.) + the consultant-scenario mapping.

    v7.x: AEMO restructured fuel prices into per-asset-type tables
    (gas_prices_existing_generators, gas_prices_new_entrants) and
    consolidated coal into one `coal_fuel_price` table (with scenarios as
    columns rather than separate tables). Also added biomass_fuel_price
    standalone.
    """
    if iasr_workbook_version.startswith("7."):
        return [
            "coal_fuel_price",
            "biomass_fuel_price",
            "gas_prices_existing_generators",
            "gas_prices_new_entrants",
            "gas_and_liquid_fuel_prices_consultant_scenario_mapping",
            "liquid_fuel_prices",
            "biomethane_prices",
            "hydrogen_prices",
            "industrial_fuel_costs",
            "residential_fuel_costs",
            "gpg_secondary_fuel_prices",
        ]
    return [
        "coal_and_biomass_price_consultant_scenario_mapping",
        "biomass_prices",
        "coal_prices_step_change",
        "coal_prices_progressive_change",
        "coal_prices_green_energy_exports",
        "gas_prices_step_change",
        "gas_prices_progressive_change",
        "gas_prices_green_energy_exports",
        "liquid_fuel_prices",
        "hydrogen_prices",
        "biomethane_prices",
    ]


def generator_storage_summary_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Generator + storage summary table names on the Summary Mapping sheet.

    v6.0: 6 separate tables (existing, committed, anticipated, batteries,
    additional, new_entrants). Each row in each table is a single asset of
    that status.

    v7.x: 4 tables. AEMO consolidated the first 4 (existing/committed/
    anticipated/additional) into `existing_committed_anticipated_additional_
    generator_summary` (NO separate batteries_summary — batteries are folded
    in via a status column). Added `consumer_energy_resources_summary` (new
    for v7.4 to surface CER trajectories) and `new_entrant_electrolysers_
    summary` (hydrogen sector expansion). Downstream templater code must
    split the consolidated table by status to recover the v6.0 4-way split
    where it relied on per-status tables.
    """
    if iasr_workbook_version.startswith("7."):
        return [
            "existing_committed_anticipated_additional_generator_summary",
            "consumer_energy_resources_summary",
            "new_entrants_summary",
            "new_entrant_electrolysers_summary",
        ]
    return [
        "existing_generators_summary",
        "committed_generators_summary",
        "anticipated_projects_summary",
        "batteries_summary",
        "additional_projects_summary",
        "new_entrants_summary",
    ]


def subregion_flow_paths(iasr_workbook_version: str = "6.0") -> list[str]:
    """Return the subregion flow-path identifiers for a given workbook version."""
    return _SUBREGION_FLOW_PATHS_V74 if iasr_workbook_version.startswith("7.") else _SUBREGION_FLOW_PATHS_V60


def flow_path_augmentation_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Augmentation-options table names per workbook version."""
    return [
        "flow_path_augmentation_options_" + fp
        for fp in subregion_flow_paths(iasr_workbook_version)
    ]


def flow_path_augmentation_cost_tables(iasr_workbook_version: str = "6.0") -> list[str]:
    """Augmentation-cost table names — one per (scenario, flow_path) pair.

    v6.0: 2 scenarios × 11 paths = 22 tables.
    v7.x: 3 scenarios × 14 paths = 42 tables.
    """
    paths = subregion_flow_paths(iasr_workbook_version)
    if iasr_workbook_version.startswith("7."):
        path_for_table = lambda p: _V74_AUGMENTATION_COST_PATH_OVERRIDES.get(p, p)
    else:
        path_for_table = lambda p: p
    return [
        f"flow_path_augmentation_costs_{scen}_{path_for_table(fp)}"
        for scen in cost_scenario_suffixes(iasr_workbook_version)
        for fp in paths
    ]


# v6.0-compatible module-level constants (used by all callers that pre-date
# the version-aware accessors). For v7.x runs, callers must switch to the
# `flow_path_augmentation_tables(version)` helper above.
_SUBREGION_FLOW_PATHS = _SUBREGION_FLOW_PATHS_V60

_FLOW_PATH_AGUMENTATION_TABLES = [
    "flow_path_augmentation_options_" + fp for fp in _SUBREGION_FLOW_PATHS
]

_REZ_CONNECTION_AGUMENTATION_TABLES = [
    "rez_augmentation_options_" + region for region in list(_NEM_REGION_IDS)
]

_FLOW_PATH_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE = [
    "flow_path_augmentation_costs_progressive_change_" + fp
    for fp in _SUBREGION_FLOW_PATHS
]

_FLOW_PATH_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_" + fp
    for fp in _SUBREGION_FLOW_PATHS
]

_FLOW_PATH_AUGMENTATION_COST_TABLES = (
    _FLOW_PATH_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE
    + _FLOW_PATH_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS
)

_REZ_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE = [
    "rez_augmentation_costs_progressive_change_" + region
    for region in list(_NEM_REGION_IDS)
]

_REZ_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS = [
    "rez_augmentation_costs_step_change_and_green_energy_exports_" + region
    for region in list(_NEM_REGION_IDS)
]

_REZ_AUGMENTATION_COST_TABLES = (
    _REZ_AUGMENTATION_COST_TABLES_PROGRESSIVE_CHANGE
    + _REZ_AUGMENTATION_COST_TABLES_STEP_CHANGE_AND_GREEN_ENERGY_EXPORTS
)


def flow_path_augmentation_cost_tables_by_scenario(
    iasr_workbook_version: str = "6.0",
) -> dict[str, list[str]]:
    """Return per-scenario flow-path augmentation cost table names.

    Output is a dict `{scenario_suffix: [table_name, ...]}` suitable for
    use as `_FLOW_PATH_CONFIG["table_names"]["cost"]`. v6.0 has 2 scenarios
    (progressive_change, step_change_and_green_energy_exports); v7.x has 3
    (slower_growth, step_change, accelerated_transition).
    """
    paths = subregion_flow_paths(iasr_workbook_version)
    if iasr_workbook_version.startswith("7."):
        path_for_table = lambda p: _V74_AUGMENTATION_COST_PATH_OVERRIDES.get(p, p)
    else:
        path_for_table = lambda p: p
    return {
        scen: [
            f"flow_path_augmentation_costs_{scen}_{path_for_table(fp)}" for fp in paths
        ]
        for scen in cost_scenario_suffixes(iasr_workbook_version)
    }


def rez_augmentation_cost_tables_by_scenario(
    iasr_workbook_version: str = "6.0",
) -> dict[str, list[str]]:
    """Return per-scenario REZ augmentation cost table names.

    Same shape as `flow_path_augmentation_cost_tables_by_scenario` but for
    REZ tables (keyed per scenario, 5 NEM regions).
    """
    regions = ["QLD", "NSW", "VIC", "SA", "TAS"]
    return {
        scen: [f"rez_augmentation_costs_{scen}_{region}" for region in regions]
        for scen in cost_scenario_suffixes(iasr_workbook_version)
    }

_FLOW_PATH_AGUMENTATION_NAME_ADJUSTMENTS = {
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Forward direction": "transfer_increase_forward_direction_MW",
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Reverse direction": "transfer_increase_reverse_direction_MW",
}

_PREPATORY_ACTIVITIES_TABLES = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
    "flow_path_augmentation_costs_progressive_change_preparatory_activities",
]

_REZ_CONNECTION_PREPATORY_ACTIVITIES_TABLES = [
    "rez_augmentation_costs_step_change_and_green_energy_exports_preparatory_activities",
    "rez_augmentation_costs_progressive_change_preparatory_activities",
]

_ACTIONABLE_ISP_PROJECTS_TABLES = [
    "flow_path_augmentation_costs_step_change_and_green_energy_exports_actionable_isp_projects",
    "flow_path_augmentation_costs_progressive_change_actionable_isp_projects",
]

_PREPATORY_ACTIVITIES_NAME_TO_OPTION_NAME = {
    "500kV QNI Connect (NSW works)": "NNSW–SQ Option 5",
    "500kV QNI Connect (QLD works)": "NNSW–SQ Option 5",
    "330kV QNI single circuit (NSW works)": "NNSW–SQ Option 1",
    "330kV QNI single circuit (QLD works)": "NNSW–SQ Option 1",
    "330kV QNI double circuit (NSW works)": "NNSW–SQ Option 2",
    "330kV QNI double circuit (QLD works)": "NNSW–SQ Option 2",
    "CQ-GG": "CQ-GG Option 1",
    "Sydney Southern Ring": "CNSW-SNW Option 2",
}

_REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME = {
    "Darling Downs REZ Expansion(Stage 1)": ["SWQLD1", "Option 1"],
    "South East SA REZ": ["S1-TBMO", "Option 1"],
    "South West Victoria REZ Option 1": ["SWV1", "Option 1"],
    "South West Victoria REZ Option 1A": ["SWV1", "Option 1A"],
    "South West Victoria REZ Option 1B": ["SWV1", "Option 1B"],
    "South West Victoria REZ Option 1C": ["SWV1", "Option 1C"],
    "South West Victoria REZ Option 2A": ["SWV1", "Option 2A"],
    "South West Victoria REZ Option 2B": ["SWV1", "Option 2B"],
    "South West Victoria REZ Option 3A": ["SWV1", "Option 3A"],
    "South West Victoria REZ Option 3B": ["SWV1", "Option 3B"],
}

_PREPATORY_ACTIVITIES_OPTION_NAME_TO_FLOW_PATH = {
    "NNSW–SQ Option 5": "NNSW-SQ",
    "NNSW–SQ Option 1": "NNSW-SQ",
    "NNSW–SQ Option 2": "NNSW-SQ",
    "CNSW-SNW Option 2": "CNSW-SNW",
    "CQ-GG Option 1": "CQ-GG",
}

_ACTIONABLE_ISP_PROJECTS_NAME_TO_OPTION_NAME = {
    "Humelink": "SNSW-CNSW Option 1 (HumeLink)",
    "VNI West": "VIC-SNSW Option 1 - VNI West (Kerang)",
    "Project Marinus Stage 1": "TAS-VIC Option 1 (Project Marinus Stage 1)",
    "Project Marinus Stage 2": "TAS-VIC Option 2 (Project Marinus Stage 2)",
}

_ACTIONABLE_ISP_PROJECTS_OPTION_NAME_TO_FLOW_PATH = {
    "SNSW-CNSW Option 1 (HumeLink)": "SNSW-CNSW",
    "VIC-SNSW Option 1 - VNI West (Kerang)": "VIC-SNSW",
    "TAS-VIC Option 1 (Project Marinus Stage 1)": "TAS-VIC",
    "TAS-VIC Option 2 (Project Marinus Stage 2)": "TAS-VIC",
}

# Transmission cost processing configurations.
#
# Historically these were module-level dicts hardcoding the v6.0 scenario
# suffixes and table name lists. To support multiple IASR workbook versions
# (currently v6.0 alongside v7.4) without threading `version` through every
# templater call signature, they are now factory functions: callers detect
# the active version from `iasr_tables` via `detect_iasr_version_from_tables`
# and build the appropriate config dict here.
_FLOW_PATH_CONFIG_COLUMN_RENAMES_IN = {
    "Flow path": "id",
    "Flow Path": "id",
    "Option Name": "option",
    "Option name": "option",  # v7.4 casing
    "Option": "option",
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Forward direction": "forward_capacity_increase",
    "Notional transfer level increase (MW) Note: Same increase applies to all transfer limit conditions (Peak demand, Summer typical and Winter reference)_Reverse direction": "reverse_capacity_increase",
}


def flow_path_config(iasr_workbook_version: str = "6.0") -> dict:
    """Build the flow-path transmission-cost processing config for a workbook version.

    Returns a dict shaped like the legacy module-level `_FLOW_PATH_CONFIG`
    but with version-correct table-name lists. Used by
    `templater.flow_paths.process_transmission_costs`.
    """
    return {
        "transmission_type": "flow_path",
        "in_coming_column_mappings": _FLOW_PATH_CONFIG_COLUMN_RENAMES_IN,
        "out_going_column_mappings": {
            "id": "flow_path",
            "nominal_capacity_increase": "additional_network_capacity_mw",
        },
        "table_names": {
            "augmentation": flow_path_augmentation_tables(iasr_workbook_version),
            "cost": flow_path_augmentation_cost_tables_by_scenario(
                iasr_workbook_version
            ),
            "prep_activities": prepatory_activities_tables(iasr_workbook_version),
            "actionable_projects": actionable_isp_projects_tables(
                iasr_workbook_version
            ),
        },
        "mappings": {
            "prep_activities_name_to_option": _PREPATORY_ACTIVITIES_NAME_TO_OPTION_NAME,
            "option_to_id": _PREPATORY_ACTIVITIES_OPTION_NAME_TO_FLOW_PATH,
            "actionable_name_to_option": _ACTIONABLE_ISP_PROJECTS_NAME_TO_OPTION_NAME,
            "actionable_option_to_id": _ACTIONABLE_ISP_PROJECTS_OPTION_NAME_TO_FLOW_PATH,
        },
    }


def rez_config(iasr_workbook_version: str = "6.0") -> dict:
    """Build the REZ transmission-cost processing config for a workbook version."""
    return {
        "transmission_type": "rez",
        "in_coming_column_mappings": {
            "REZ constraint ID": "id",
            "REZ / Constraint ID": "id",
            "REZ / constraint ID": "id",  # v7.4 casing
            "Option": "option",
            "REZ": "rez",
            "REZ Name": "rez",
            "REZ name": "rez",  # v7.4 casing
            "Additional network capacity (MW)": "nominal_capacity_increase",
        },
        "out_going_column_mappings": {
            "id": "rez_constraint_id",
            "nominal_capacity_increase": "additional_network_capacity_mw",
        },
        "table_names": {
            "augmentation": _REZ_CONNECTION_AGUMENTATION_TABLES,
            "cost": rez_augmentation_cost_tables_by_scenario(iasr_workbook_version),
            "prep_activities": rez_connection_prepatory_activities_tables(
                iasr_workbook_version
            ),
        },
        "prep_activities_mapping": _REZ_PREPATORY_ACTIVITIES_NAME_TO_REZ_AND_OPTION_NAME,
    }


def detect_iasr_version_from_tables(iasr_tables: dict) -> str:
    """Return "7.x" if `iasr_tables` looks like a v7.x cache, else "6.0".

    Detection uses the cost-table scenario suffix as the sentinel: v6.0
    uses `_step_change_and_green_energy_exports_*` while v7.x has
    `_step_change_*` (no combined-scenarios suffix). The consolidated
    generator summary table is NOT a reliable signal because the
    schema-normalisation layer produces it for v6.0 caches as well.

    Used by templater entry points to select the right scenario-suffix and
    table-name dispatch without having to pass `iasr_workbook_version`
    through every call signature.
    """
    v74_marker = "flow_path_augmentation_costs_step_change_CNSW-NNSW"
    if v74_marker in iasr_tables:
        return "7.4"
    return "6.0"


# Legacy module-level config dicts, kept for backward compatibility with any
# importers that grab them directly. v6.0-shape — call the factory functions
# above for version-aware configs.
_FLOW_PATH_CONFIG = flow_path_config("6.0")
_REZ_CONFIG = rez_config("6.0")


_VRE_RESOURCE_QUALITY_AND_TECH_CODES = {
    "Wind": ["WH", "WM"],
    "Wind - offshore (fixed)": "WFX",
    "Wind - offshore (floating)": "WFL",
    "Large scale Solar PV": "SAT",
    # v6.0 used "Solar Thermal (15hrs storage)"; v7.4 renamed to 16hrs.
    "Solar Thermal (15hrs storage)": "CST",
    "Solar Thermal (16hrs storage)": "CST",
}
