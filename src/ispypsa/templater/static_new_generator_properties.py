import logging
import re

import pandas as pd

from .helpers import (
    _assert_equipment_category_survives_lcf_dot,
    _assert_no_nan_load_bearing_column,
    _fuzzy_match_names,
    _manual_remove_footnotes_from_generator_names,
    _one_to_one_priority_based_fuzzy_matching,
    _rez_name_to_id_mapping,
    _snakecase_string,
    _standardise_storage_capitalisation,
    _where_any_substring_appears,
)
from .lists import _MINIMUM_REQUIRED_GENERATOR_COLUMNS, _NEW_GENERATOR_TYPES
from .mappings import (
    _NEW_ENTRANT_GENERATOR_NEW_COLUMN_MAPPING,
    _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP,
    _VRE_RESOURCE_QUALITY_AND_TECH_CODES,
)

_OBSOLETE_COLUMNS = [
    "Maximum capacity factor (%)",
    # v6.0 had a lowercase "Technology type" column with reduced tech detail
    # (the canonical column is "Technology Type" capital T — renamed from
    # v6.0's "New entrants" by schema normalisation). v7.4 doesn't have the
    # lowercase variant. Dropping here makes both versions produce identical
    # post-snake_case column sets.
    "Technology type",
]


def _template_new_generators_static_properties(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Processes the new entrant generators summary tables into an ISPyPSA
    template format

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA new entrant generators template
    """
    logging.info("Creating a new entrant generators template")

    new_generator_summaries = []
    for gen_type in _NEW_GENERATOR_TYPES:
        df = iasr_tables[_snakecase_string(gen_type) + "_summary"]
        # The downstream templater + translator key on `generator_name`
        # for property lookups (heat rate, FOM, VOM, etc.) and for the
        # build_costs.technology merge. v7.4 publishes the tech label under
        # `Technology Type`; v6.0's `New entrants` column is renamed to
        # `Technology Type` by schema normalisation. Copy (not rename) so
        # the original Technology Type column is preserved for downstream
        # code paths that use `technology_type` (e.g. battery/PHES filtering,
        # OPEX base-and-LCF lookup).
        df = df.copy()
        df["Generator Name"] = df["Technology Type"]
        new_generator_summaries.append(df)
    new_generator_summaries = pd.concat(new_generator_summaries, axis=0).reset_index(
        drop=True
    )
    cleaned_new_generator_summaries = _clean_generator_summary(new_generator_summaries)

    cleaned_new_generator_summaries = cleaned_new_generator_summaries.reset_index(
        drop=True
    )
    merged_cleaned_new_generator_and_storage_summaries = (
        _merge_and_set_new_generators_static_properties(
            cleaned_new_generator_summaries, iasr_tables
        )
    )

    new_storage_summaries = (
        merged_cleaned_new_generator_and_storage_summaries.loc[
            merged_cleaned_new_generator_and_storage_summaries[
                "technology_type"
            ].str.contains(r"battery|pumped hydro", case=False),
            :,
        ]
        .copy()
        .reset_index(drop=True)
    )

    new_generator_summaries = (
        merged_cleaned_new_generator_and_storage_summaries.loc[
            ~merged_cleaned_new_generator_and_storage_summaries[
                "technology_type"
            ].str.contains(r"battery|pumped hydro", case=False),
            :,
        ]
        .copy()
        .reset_index(drop=True)
    )

    _assert_no_nan_load_bearing_column(
        new_generator_summaries,
        "fom_$/kw/annum",
        name_col="generator_name",
        context="new-entrant generator",
    )

    required_cols_only = [
        col
        for col in _MINIMUM_REQUIRED_GENERATOR_COLUMNS
        if col in new_generator_summaries.columns
    ]

    return new_generator_summaries[required_cols_only]


def _clean_generator_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans generator summary tables

    1. Converts column names to snakecase
    2. Adds "_id" to the end of region/sub-region ID columns
    3. Removes redundant outage columns
    4. Enforces consistent formatting of "storage" str instances
    4. Adds the following columns with appropriate mappings:
            - `partial_outage_derating_factor_%`
            - `maximum_capacity_mw`
            - `lifetime`
            - `summer_peak_rating_%`
            - `technology_specific_lcf_%`
            - `minimum_stable_level_%`

    Args:
        df: Generator summary `pd.DataFrame`

    Returns:
        `pd.DataFrame`: Cleaned generator summary DataFrame
    """

    def _fix_forced_outage_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Removes until/post 2022 distinction in columns if it exists"""
        if (
            any(until_cols := [col for col in df.columns if "until" in col])
            and any(post_cols := [col for col in df.columns if "post" in col])
            and len(until_cols) == len(post_cols)
        ):
            df = df.rename(
                columns={col: col.replace("_until_2022", "") for col in until_cols}
            )
            df = df.drop(columns=post_cols)
        return df

    df = df.drop(columns=_OBSOLETE_COLUMNS, errors="ignore")
    df.columns = [_snakecase_string(col_name) for col_name in df.columns]
    df = df.rename(
        columns={col: (col + "_id") for col in df.columns if re.search(r"region$", col)}
    )
    # handle footnotes that have stuck around:
    df = _manual_remove_footnotes_from_generator_names(df)
    # enforces capitalisation structure for instances of str "storage" in generator_name col
    df["generator_name"] = _standardise_storage_capitalisation(df["generator_name"])
    df = _fix_forced_outage_columns(df)

    # Drop rows that contain new entrants for the Illawarra REZ (N12) - these
    # don't have any trace data from AEMO currently and "No VRE is projected for this REZ."
    # (AEMO 2024 | Appendix 3. Renewable Energy Zones, p.38)
    df = df.loc[~(df["rez_location"] == "Illawarra"), :]

    # v7.4 added "Distributed Resources Solar" / "Distributed Resources Batteries"
    # as new-entrant deployment rows (per-sub-region). Per methodology design call
    # 2026-05-21, Distributed Resources are excluded from new-entrant investment
    # scope — they're modelled exogenously via demand-side adjustments rather
    # than as candidate investments in the orchestrator-facing capacity expansion.
    df = df.loc[
        ~df["technology_type"].str.contains(
            "Distributed Resources", case=False, na=False
        ),
        :,
    ]

    # v7.4 publishes `Connection cost_Region` at sub-region granularity (e.g.
    # `NNSW`, `CNSW`); v6.0 used NEM region (e.g. `NSW`). The downstream
    # non-VRE connection-cost lookup table (`connection_costs_other`) is keyed
    # by NEM region. Aggregate sub-region → NEM region using each row's own
    # `region_id` column so the join key aligns. Phase 1 simplification: v7.4
    # sub-regional granularity collapses to NEM region (the connection-cost
    # table's natural key). Sub-region resolution is a Phase 2+ refinement.
    if (
        "connection_cost_region_id" in df.columns
        and "sub_region_id" in df.columns
        and "region_id" in df.columns
    ):
        sub_to_nem = (
            df[["sub_region_id", "region_id"]]
            .dropna()
            .drop_duplicates()
            .set_index("sub_region_id")["region_id"]
            .to_dict()
        )
        is_sub_region = df["connection_cost_region_id"].isin(sub_to_nem.keys())
        df.loc[is_sub_region, "connection_cost_region_id"] = df.loc[
            is_sub_region, "connection_cost_region_id"
        ].map(sub_to_nem)

    for (
        new_column,
        existing_column_mapping,
    ) in _NEW_ENTRANT_GENERATOR_NEW_COLUMN_MAPPING.items():
        df[new_column] = df[existing_column_mapping]

    return df


def _merge_and_set_new_generators_static_properties(
    df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Merges into and sets static (i.e. not time-varying) generator properties in the
    "New entrants summary" template, and renames columns if this is specified
    in the mapping.

    Uses `ispypsa.templater.mappings._NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    as the mapping.

    Args:
        df: Existing generator summary DataFrame
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: Existing generator template with static properties filled in
    """
    # merge in static properties using the static property mapping
    merged_static_cols = []
    for col, table_attrs in _NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP.items():
        # if col is an opex column, use separate function to handle merging in:
        if re.search("^[fv]om_", col):
            data = iasr_tables[table_attrs["table"]]
            df, col = _process_and_merge_opex(df, data, col, table_attrs, iasr_tables)
        else:
            if type(table_attrs["table"]) is list:
                data = [iasr_tables[table] for table in table_attrs["table"]]
                data = pd.concat(data, axis=0)
            else:
                data = iasr_tables[table_attrs["table"]]
            df, col = _merge_table_data(df, col, data, table_attrs)
        merged_static_cols.append(col)

    gpg_min_stable_level_new_entrants = iasr_tables["gpg_min_stable_level_new_entrants"]
    df = _process_and_merge_new_gpg_min_stable_lvl(
        df, gpg_min_stable_level_new_entrants, "minimum_stable_level_%"
    )
    df = _calculate_and_merge_tech_specific_lcfs(
        df, iasr_tables, "technology_specific_lcf_%"
    )
    df = _zero_renewable_heat_rates(df, "heat_rate_gj/mwh")
    df = _zero_solar_wind_battery_partial_outage_derating_factor(
        df, "partial_outage_derating_factor_%"
    )

    df = _add_identifier_columns(df, iasr_tables)

    # replace remaining string values in static property columns
    df = df.infer_objects()
    for col in [col for col in merged_static_cols if df[col].dtype == "object"]:
        df[col] = df[col].apply(lambda x: pd.NA if isinstance(x, str) else x)
    return df


def _merge_table_data(
    df: pd.DataFrame, col: str, table_data: pd.DataFrame, table_attrs: dict
) -> tuple[pd.DataFrame, str]:
    """Replace values in the provided column of the summary mapping with those
    in the table data using the provided attributes in
    `_NEW_GENERATOR_STATIC_PROPERTY_TABLE_MAP`
    """
    # handle alternative lookup and value columns
    for alt_attr in ("lookup", "value"):
        if f"alternative_{alt_attr}s" in table_attrs.keys():
            table_col = table_attrs[f"table_{alt_attr}"]
            for alt_col in table_attrs[f"alternative_{alt_attr}s"]:
                table_data[table_col] = table_data[table_col].where(
                    pd.notna, table_data[alt_col]
                )
    replacement_dict = (
        table_data.loc[:, [table_attrs["table_lookup"], table_attrs["table_value"]]]
        .set_index(table_attrs["table_lookup"])
        .squeeze()
        .to_dict()
    )
    # handles slight difference in capitalisation e.g. Bogong/Mackay vs Bogong/MacKay
    where_str = df[col].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col] = _fuzzy_match_names(
        df.loc[where_str, col],
        replacement_dict.keys(),
        f"merging in the new entrant generator static property {col}",
        not_match="existing",
        threshold=90,
    )
    df[col] = df[col].replace(replacement_dict)
    if "new_col_name" in table_attrs.keys():
        df = df.rename(columns={col: table_attrs["new_col_name"]})
        col = table_attrs["new_col_name"]
    return df, col


def _process_and_merge_opex(
    df: pd.DataFrame,
    table_data: pd.DataFrame,
    col_name: str,
    table_attrs: dict,
    iasr_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, str]:
    """Processes and merges in fixed or variable OPEX values for new entrant generators.

    Two shapes are supported (detected from the table columns):

    v6.0: per-region cost columns (e.g. `Fixed OPEX ($/kW sent out/year)_NSW Low`).
        Values are pre-multiplied per (region, cost-band). Templater maps
        `regional_build_cost_zone` → the matching column.

    v7.4 (per design call 2026-05-24, Reading 1): single `Base value` column.
        OPEX is computed as `base × O&M LCF / 100` where the LCF comes from
        `locational_cost_factors` keyed by the generator's sub-region. The
        cost-band variation v6.0 had (Low/Medium/High) was scenario-driven
        and is collapsed in v7.4 — no methodological loss for the team's
        Step Change scenario use.
    """
    if any("base value" in c.lower() for c in table_data.columns):
        return _compute_opex_from_base_and_lcf(df, iasr_tables, table_data, col_name)
    # update the mapping in this column to include generator name and the cost region initially given
    df[col_name] = df["generator_name"] + " " + df[col_name]
    table_data = table_data.rename(
        columns={
            col: col.replace(f"{table_attrs['table_col_prefix']}_", "")
            for col in table_data.columns
        }
    )
    opex_table = table_data.melt(
        id_vars=[table_attrs["table_lookup"]],
        var_name="cost_region",
        value_name="opex_value",
    )
    # add column with same generator + cost_region mapping as df[col_name]:
    opex_table["mapping"] = (
        opex_table[table_attrs["table_lookup"]] + " " + opex_table["cost_region"]
    )
    opex_replacement_dict = (
        opex_table[["mapping", "opex_value"]].set_index("mapping").squeeze().to_dict()
    )
    # use fuzzy matching in case of slight differences in generator names:
    where_str = df[col_name].apply(lambda x: isinstance(x, str))
    df.loc[where_str, col_name] = _fuzzy_match_names(
        df.loc[where_str, col_name],
        opex_replacement_dict.keys(),
        f"merging in the new entrant generator static property {col_name}",
        not_match="existing",
        threshold=90,
    )
    df[col_name] = df[col_name].replace(opex_replacement_dict)
    return df, col_name


def _compute_opex_from_base_and_lcf(
    df: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    base_table: pd.DataFrame,
    col_name: str,
) -> tuple[pd.DataFrame, str]:
    """v7.4 OPEX flow: `opex = base value × O&M LCF / 100`.

    The base table's identifier column is `Technology Type` (fixed_opex_new_entrants)
    or `Generator` (variable_opex_new_entrants) — auto-detected. The summary df's
    `technology_type` / `generator_name` column provides the matching key.
    Sub-region LCF comes from `locational_cost_factors` `O&M costs 3` (the only
    OPEX-relevant column in the LCF set).
    """
    base_value_col = next(c for c in base_table.columns if "base value" in c.lower())
    if "Technology Type" in base_table.columns:
        id_col_in_table = "Technology Type"
        id_col_in_summary = "technology_type"
    else:
        id_col_in_table = "Generator"
        id_col_in_summary = "generator_name"
    base_dict = base_table.set_index(id_col_in_table)[base_value_col].to_dict()

    lcf_table = iasr_tables["locational_cost_factors"]
    om_col = next(c for c in lcf_table.columns if "O&M" in c)
    zone_col = lcf_table.columns[0]
    lcf_dict = lcf_table.set_index(zone_col)[om_col].to_dict()

    def compute(row):
        base = base_dict.get(row[id_col_in_summary])
        # Prefer the generator's explicit cost zone if it exists in the LCF
        # table; otherwise fall back to its sub-region (v7.4 LCFs are keyed
        # by sub-region / REZ ID rather than by Low/Med/High band).
        zone = row.get("regional_build_cost_zone")
        lcf = lcf_dict.get(zone)
        if not isinstance(lcf, (int, float)):
            lcf = lcf_dict.get(row.get("sub_region_id"))
        if not isinstance(base, (int, float)) or not isinstance(lcf, (int, float)):
            return pd.NA
        return base * lcf / 100

    df[col_name] = df.apply(compute, axis=1)
    return df, col_name


def _calculate_and_merge_tech_specific_lcfs(
    df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame], tech_lcf_col: str
) -> pd.DataFrame:
    """Calculates the technology-specific locational cost factor as a percentage
    for each new entrant generator and merges into summary mapping table.
    """
    # loads in the three tables needed
    breakdown_ratios = iasr_tables["technology_cost_breakdown_ratios"]
    technology_specific_lcfs = iasr_tables["technology_specific_lcfs"]
    # loads all cols unless the str "O&M" is in col name
    locational_cost_factors = iasr_tables["locational_cost_factors"]
    locational_cost_factors = locational_cost_factors.set_index(
        locational_cost_factors.columns[0]
    )
    # Drop non-numeric companion columns v7.4 added (e.g. `REZ name`) and the
    # O&M column (handled separately for OPEX). The cost categories that
    # remain are what's used in the `.dot()` against breakdown_ratios.
    cols = [
        col
        for col in locational_cost_factors.columns
        if "O&M" not in col and col != "REZ name"
    ]
    locational_cost_factors = locational_cost_factors.loc[:, cols]

    # reshape technology_specific_lcfs and name columns manually. v7.4 adds a
    # `REZ name / Description` column alongside the cost-zone ID — treat both
    # as id_vars so they're not mistakenly melted into the Technology axis.
    id_var_cols = [
        c
        for c in ("Cost zone / REZ ID", "REZ name / Description")
        if c in technology_specific_lcfs.columns
    ]
    technology_specific_lcfs = technology_specific_lcfs.melt(
        id_vars=id_var_cols, value_name="LCF", var_name="Technology"
    ).dropna(axis=0, how="any")
    technology_specific_lcfs.rename(
        columns={"Cost zone / REZ ID": "Location"}, inplace=True
    )
    if "REZ name / Description" in technology_specific_lcfs.columns:
        technology_specific_lcfs = technology_specific_lcfs.drop(
            columns=["REZ name / Description"]
        )
    # ensures generator names in LCF tables match those in the summary table
    for df_to_match_gen_names in [technology_specific_lcfs, breakdown_ratios]:
        df_to_match_gen_names["Technology"] = _fuzzy_match_names(
            df_to_match_gen_names["Technology"],
            df["generator_name"].unique(),
            "calculating and merging in LCFs to static new entrant gen summary",
            not_match="existing",
            threshold=90,
        )
        df_to_match_gen_names.set_index("Technology", inplace=True)
    # v7.4/v7.8 renamed the LCF equipment category to "Equipment and installation
    # costs" (merging equipment + installation into one factor), while
    # breakdown_ratios still lists "Equipment costs" separately. The fuzzy matcher
    # below (threshold 80) cannot bridge that name gap — and "Installation costs"
    # (present in BOTH tables) grabs the only close match — so "Equipment costs"
    # (breakdown_ratios) falls out of the shared cost categories and is silently
    # dropped from the LCF .dot(). Equipment is the dominant capex category
    # (~60-80% of new-entrant build cost), so dropping it understates EVERY
    # non-PHES new-entrant's technology_specific_lcf to ~30-46% instead of ~100%,
    # which understates all new-entrant capital costs (and the fleet cost column)
    # to roughly a third. Map it explicitly so equipment gets its locational
    # factor. (Does not affect dispatch/marginal cost, which excludes capex.)
    locational_cost_factors = locational_cost_factors.rename(
        columns={"Equipment and installation costs": "Equipment costs"}
    )
    # use fuzzy matching to ensure that col names in tables to combine match up:
    fuzzy_column_renaming = _one_to_one_priority_based_fuzzy_matching(
        set(locational_cost_factors.columns.to_list()),
        set(breakdown_ratios.columns.to_list()),
        not_match="existing",
        threshold=80,
    )
    locational_cost_factors.rename(columns=fuzzy_column_renaming, inplace=True)
    # Restrict the .dot() to the intersection of cost categories. v7.4 added a
    # `Topographical cost` column to breakdown_ratios with no matching LCF row
    # in locational_cost_factors — including it would misalign the matrices.
    shared_cost_cats = [
        c for c in breakdown_ratios.columns if c in locational_cost_factors.columns
    ]
    _assert_equipment_category_survives_lcf_dot(shared_cost_cats)
    breakdown_ratios = breakdown_ratios.loc[:, shared_cost_cats].apply(
        pd.to_numeric, errors="coerce"
    )
    locational_cost_factors = locational_cost_factors.loc[:, shared_cost_cats].apply(
        pd.to_numeric, errors="coerce"
    )
    # loops over rows and use existing LCF for all pumped hydro gens, calculates for others
    # values are all converted to a percentage as needed.
    # Some techs in technology_specific_lcfs may not exist in breakdown_ratios
    # (e.g. v7.4's "Solar Thermal (16hrs storage)" whose 15hr v6.0 counterpart
    # has no v7.4 equivalent) — fall back to the existing LCF value for those.
    for tech, row in technology_specific_lcfs.iterrows():
        if re.search(r"^(Pump|BOTN)", tech) or tech not in breakdown_ratios.index:
            calculated_or_given_lcf = row["LCF"] * 100
        else:
            calculated_or_given_lcf = breakdown_ratios.loc[tech, :].dot(
                locational_cost_factors.loc[row["Location"], :]
            )
            calculated_or_given_lcf /= 100
        df.loc[
            ((df["generator_name"] == tech) & (df[tech_lcf_col] == row["Location"])),
            tech_lcf_col,
        ] = calculated_or_given_lcf
    # fills rows with no LCF (some PHES REZs) with pd.NA
    df[tech_lcf_col] = df[tech_lcf_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _process_and_merge_new_gpg_min_stable_lvl(
    df: pd.DataFrame, new_gpg_min_stable_lvls: pd.DataFrame, min_level_col: str
) -> pd.DataFrame:
    """Processes and merges in gas-fired generation minimum stable level data (%)

    Minimum stable level is given as a percentage of nameplate capacity, and set
    to zero for renewable generators (wind, solar, hydro), storage, OCGT, and
    hydrogen reciprocating engines.

    NOTE: v6 IASR workbook does not specify a minimum stable level for hydrogen
    reciprocating engines.
    """
    new_gpg_min_stable_lvls = new_gpg_min_stable_lvls.set_index("Technology")
    # manually maps percentages to the new min stable level column
    for tech, row in new_gpg_min_stable_lvls.iterrows():
        df.loc[df["technology_type"] == tech, min_level_col] = row[
            "Min Stable Level (% of nameplate)"
        ]
    # fills renewable generators, storage, hydrogen reciprocating engines and OCGT with 0.0
    df.loc[
        _where_any_substring_appears(
            df[min_level_col],
            ["solar", "wind", "pumped hydro", "battery", "ocgt", "hydrogen"],
        ),
        min_level_col,
    ] = 0.0
    # replace any remaining cells containing str (tech type) with pd.NA
    df[min_level_col] = df[min_level_col].apply(
        lambda x: pd.NA if isinstance(x, str) else x
    )
    return df


def _zero_renewable_heat_rates(df: pd.DataFrame, heat_rate_col: str) -> pd.DataFrame:
    """
    Fill any empty heat rate values with the technology type, and then set
    renewable energy (solar, solar thermal, wind, hydro) and battery storage
    heat rates to 0.0. Ensure "pumped hydro" used (not just "hydro") to avoid
    including hydrogen reciprocating engines.
    """
    df[heat_rate_col] = df[heat_rate_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(
            df[heat_rate_col], ["solar", "wind", "pumped hydro", "battery"]
        ),
        heat_rate_col,
    ] = 0.0
    return df


def _zero_solar_wind_battery_partial_outage_derating_factor(
    df: pd.DataFrame, po_derating_col: str
) -> pd.DataFrame:
    """
    Fill any empty partial outage derating factor values with the technology type, and
    then set values for solar, wind and batteries to 0
    """
    df[po_derating_col] = df[po_derating_col].where(pd.notna, df["technology_type"])
    df.loc[
        _where_any_substring_appears(df[po_derating_col], ["solar", "wind", "battery"]),
        po_derating_col,
    ] = 0.0
    return df


def _add_and_clean_rez_ids(
    df: pd.DataFrame, rez_id_col_name: str, renewable_energy_zones: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges REZ IDs into the new entrant generator table and cleans up REZ names.

    REZ IDs are unique letter/digit identifiers used in the IASR workbook. This function
    also handles the Non-REZ IDs for Victoria (V0) and New South Wales (N0). There are
    also some manual mappings to correct REZ names that have been updated/changed
    across tables (currently in the IASR workbook v6.0): 'North [East/West] Tasmania Coast'
    becomes 'North Tasmania Coast', 'Portland Coast' becomes 'Southern Ocean'.

    Args:
        df: new entrant generator DataFrame
        rez_id_col_name: str, name of the new column to be added.
        renewable_energy_zones: a pd.Dataframe of the IASR table `renewable_energy_zones`
            containing columns "ID" and "Name" used to map the REZ IDs.

    Returns:
        pd.DataFrame: new entrant generator DataFrame with REZ ID column added.
    """

    # Source-id-first: v7.4 summaries carry a clean source `rez_id` column that
    # already holds the 2026 REZ scheme (including the new zones DN1-3, N13, Q10)
    # plus V0/N0 for non-REZ generators. Use it directly. v6.0 had no REZ ID
    # column, so fall back to deriving the id from the `rez_location` name for any
    # row whose source id is blank. "Not Applicable" (v7.4) and NaN (v6.0) both
    # normalise to NA so downstream rez_id-based masks work. Deriving the id from
    # the name dropped the new 2026 zones, whose names don't match the names in
    # the renewable_energy_zones table.
    name_derived_rez_id = _rez_name_to_id_mapping(
        df["rez_location"].replace("Not Applicable", pd.NA),
        "rez_location",
        renewable_energy_zones,
    )
    if rez_id_col_name in df.columns:
        source_rez_id = df[rez_id_col_name].replace("Not Applicable", pd.NA)
        df[rez_id_col_name] = source_rez_id.fillna(name_derived_rez_id)
    else:
        df[rez_id_col_name] = name_derived_rez_id

    # Map the remaining rez/region name columns to ids as before. rez_id is
    # resolved above, so exclude it here to avoid round-tripping its ids back
    # through the name lookup.
    # update references to "North [East|West] Tasmania Coast" to "North Tasmania Coast"
    # update references to "Portland Coast" to "Southern Ocean"
    rez_or_region_cols = [
        col
        for col in df.columns
        if re.search(r"rez|region", col) and col != rez_id_col_name
    ]

    for col in rez_or_region_cols:
        df[col] = _rez_name_to_id_mapping(df[col], col, renewable_energy_zones)

    # No trace data has been provided by AEMO for the N12 REZ, as the 2024 ISP modelling
    # did not find any VRE built in this REZ. So drop generators in this REZ for now.
    df_with_only_rez_ids = df[df[rez_id_col_name] != "N12"].copy()

    return df_with_only_rez_ids


def _add_isp_resource_type_column(
    df: pd.DataFrame, isp_resource_type_col_name: str
) -> pd.DataFrame:
    """
    Adds a new column to the new entrant generator table to hold resource quality/technology
    code as defined in the IASR workbook and AEMO VRE traces.
    """

    df[isp_resource_type_col_name] = df["generator_name"].map(
        _VRE_RESOURCE_QUALITY_AND_TECH_CODES
    )
    # expand to account for medium/high quality wind resources:
    df_with_isp_resource_type = df.explode(isp_resource_type_col_name).reset_index(
        drop=True
    )

    return df_with_isp_resource_type


def _add_unique_generator_string_column(
    df: pd.DataFrame, generator_string_col_name: str = "generator"
) -> pd.DataFrame:
    """
    Adds a new column to the new entrant generator table to hold a unique string
    identifier for each generator.

    The unique string identifier is created by combining the technology type,
    technology descriptor, and either the rez_id or sub_region_id into a single
    string in the format: `{technology_type}_{isp_resource_type}_{rez_id|sub_region_id}`.
    The resulting string is cleaned up to remove special characters and converted
    to snakecase.
    """

    def _create_generator_string(row):
        # 1. Combine columns to create unique string as: `{technology_type}_{isp_resource_type}_{rez_id|sub_region_id}`
        # Using rez_id where not NaN, otherwise sub_region_id
        generator_string = row["technology_type"]
        if isinstance(row["isp_resource_type"], str):
            generator_string += "_" + row["isp_resource_type"]
        if isinstance(row["rez_id"], str):
            generator_string += "_" + row["rez_id"]
        else:
            generator_string += "_" + row["sub_region_id"]
        # 2. Clean up resulting strings to remove special characters and convert to snakecase
        generator_string = _snakecase_string(re.sub(r"[/\\]", " ", generator_string))

        # 3. Final fussy clean up of some REZ names that get split up by _snakecase_string:
        split_rez = re.search(
            r"_(?P<split_rez_string>(?P<rez_letter>[a-z]{1})_(?P<rez_number>\d{2}))$",
            generator_string,
        )
        if split_rez:
            split_rez_string = split_rez.group("split_rez_string")
            replacement_string = split_rez.group("rez_letter") + split_rez.group(
                "rez_number"
            )
            generator_string = re.sub(
                split_rez_string, replacement_string, generator_string
            )

        return generator_string

    df[generator_string_col_name] = df.apply(_create_generator_string, axis=1)

    return df


def _add_identifier_columns(df: pd.DataFrame, iasr_tables: dict[str, pd.DataFrame]):
    """
    Adds four new identifier columns to the new entrant generator table.

    The additional columns are created to hold REZ IDs, storage durations in hours,
    technology descriptors, and a 'generator' identifier that holds a combination
    of id columns and technology type for each unique generator. These identifiers
    are used primarily to map generators to corresponding VRE trace data, provide unique
    identifiers for new entrant generators, and to apply custom build limit constraints
    by technology type and/or resource quality.

    Args:
        df: a pd.Dataframe containing new entrant generator data
        iasr_tables: Dict of tables from the IASR workbook

    Returns:
        pd.DataFrame: New entrant generator table with additional columns 'rez_id',
            'storage_duration', 'isp_resource_type', and 'generator'.
    """
    df_with_rez_ids = _add_and_clean_rez_ids(
        df, "rez_id", iasr_tables["renewable_energy_zones"]
    )
    df_with_isp_resource_type = _add_isp_resource_type_column(
        df_with_rez_ids, "isp_resource_type"
    )

    df_with_unique_generator_str = _add_unique_generator_string_column(
        df_with_isp_resource_type, "generator"
    )

    return df_with_unique_generator_str.drop_duplicates(
        subset=["generator"], ignore_index=True
    )
