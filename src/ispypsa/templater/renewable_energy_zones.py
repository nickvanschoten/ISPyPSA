import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from .helpers import _snakecase_string


def _template_rez_build_limits(
    iasr_tables: dict[str, pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Create a template for renewable energy zones that contains data on resource and
    transmission limits and transmission expansion costs.

    v6.0 carried both resource limits (wind/solar capacity) and REZ-to-grid
    transmission limits + expansion costs in a single `initial_build_limits`
    table. v7.x split these across `initial_resource_limits` and
    `initial_transmission_limits`. The templater reads transmission columns
    from the latter when present (v7.4) and otherwise falls back to the same
    `initial_resource_limits` DataFrame (v6.0, where both sets of columns
    live in the same renamed table).

    Args:
        iasr_tables: dict of IASR tables from `isp-workbook-parser`. Reads
            `initial_resource_limits` for resource columns, and optionally
            `initial_transmission_limits` if AEMO publishes transmission
            data in a separate table (v7.4+).
        scenario: ISP scenario to generate template inputs based on.

    Returns:
        `pd.DataFrame`: `ISPyPSA` formatted REZ table resource and transmission limits
            table
    """
    logging.info("Creating a rez_build_limits template")
    rez_build_limits = _merge_resource_and_transmission_limits(iasr_tables)
    rez_build_limits.columns = [
        _snakecase_string(col) for col in rez_build_limits.columns
    ]
    rez_build_limits = rez_build_limits.rename(
        columns={
            "isp_sub_region": "isp_sub_region_id",
        }
    )
    cols_to_pass_to_float = [
        col
        for col in rez_build_limits.columns
        if col not in ["rez_id", "isp_sub_region_id"]
    ]
    for col in cols_to_pass_to_float:
        rez_build_limits[col] = pd.to_numeric(rez_build_limits[col], errors="coerce")
    cols_where_zero_goes_to_nan = [
        col for col in cols_to_pass_to_float if re.search(r"land_use_limit", col)
    ]
    cols_where_zero_goes_to_nan.append(
        "rez_resource_limit_violation_penalty_factor_$m/mw"
    )
    for col in cols_where_zero_goes_to_nan:
        rez_build_limits.loc[rez_build_limits[col] == 0.0, col] = np.nan

    rez_build_limits = _process_transmission_limit(rez_build_limits)

    rez_build_limits = _convert_cost_units(
        rez_build_limits, "rez_resource_limit_violation_penalty_factor_$m/mw"
    )

    land_use_limit_scenario_str = ""
    if scenario == "Green Energy Exports":
        land_use_limit_scenario_str = "_green_energy_exports_scenario"

    rez_build_limits = rez_build_limits.rename(
        columns={
            f"land_use_limits_in_mw{land_use_limit_scenario_str}_wind": "land_use_limits_mw_wind",
            f"land_use_limits_in_mw{land_use_limit_scenario_str}_solar": "land_use_limits_mw_solar",
            "rez_resource_limit_violation_penalty_factor_$m/mw": "rez_resource_limit_violation_penalty_factor_$/mw",
        }
    )
    cols_where_nan_goes_to_zero = [
        "wind_generation_total_limits_mw_high",
        "wind_generation_total_limits_mw_medium",
        "wind_generation_total_limits_mw_offshore_floating",
        "wind_generation_total_limits_mw_offshore_fixed",
        "solar_pv_plus_solar_thermal_limits_mw_solar",
        "land_use_limits_mw_wind",
        "land_use_limits_mw_solar",
    ]
    for col in cols_where_nan_goes_to_zero:
        rez_build_limits[col] = rez_build_limits[col].fillna(0.0)

    rez_build_limits["carrier"] = "AC"
    rez_build_limits = rez_build_limits.loc[
        :,
        [
            "rez_id",
            "isp_sub_region_id",
            "carrier",
            "wind_generation_total_limits_mw_high",
            "wind_generation_total_limits_mw_medium",
            "wind_generation_total_limits_mw_offshore_floating",
            "wind_generation_total_limits_mw_offshore_fixed",
            "solar_pv_plus_solar_thermal_limits_mw_solar",
            "rez_resource_limit_violation_penalty_factor_$/mw",
            # Remove while not being used.
            # "rez_transmission_network_limit_peak_demand",
            "rez_transmission_network_limit_summer_typical",
            # Remove while not being used.
            # "rez_transmission_network_limit_winter_reference",
            "land_use_limits_mw_wind",
            "land_use_limits_mw_solar",
        ],
    ]
    return rez_build_limits


def _merge_resource_and_transmission_limits(
    iasr_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Combine `initial_resource_limits` with the transmission columns AEMO now
    publishes separately in `initial_transmission_limits` (v7.4+), and bring in
    the per-REZ ISP sub-region from `renewable_energy_zones` if it isn't already
    in the resource table.

    For v6.0 caches both the transmission columns and the ISP sub-region live
    in `initial_resource_limits` (the renamed file from v6.0's
    `initial_build_limits`), so the cross-table merges are no-ops.
    """
    resource_df = iasr_tables["initial_resource_limits"].copy()
    if "initial_transmission_limits" in iasr_tables:
        transmission_df = iasr_tables["initial_transmission_limits"].copy()
        transmission_cols = [
            col for col in transmission_df.columns if col != "REZ name"
        ]
        resource_df = resource_df.merge(
            transmission_df[transmission_cols], on="REZ ID", how="left"
        )
    if "ISP sub-region" not in resource_df.columns:
        rez_df = iasr_tables["renewable_energy_zones"][["ID", "ISP sub-region"]]
        resource_df = resource_df.merge(
            rez_df, left_on="REZ ID", right_on="ID", how="left"
        ).drop(columns="ID")
    return resource_df


def _process_transmission_limit(data):
    """Replace 0.0 MW Transmission limits with nan if there is not a cost given for
    expansion.

    v7.4 made the Tranche 1 expansion-cost column suffix explicit; v6.0 caches
    get the same suffix added via the schema normalisation column rename.
    """
    cols = [
        "rez_transmission_network_limit_peak_demand",
        "rez_transmission_network_limit_summer_typical",
        "rez_transmission_network_limit_winter_reference",
    ]
    for col in cols:
        replacement_check = data[
            "indicative_transmission_expansion_cost_$m/mw_tranche_1"
        ].isna() & (data[col] == 0.0)
        data.loc[replacement_check, col] = np.nan
    return data


def _combine_transmission_expansion_cost_to_one_column(data):
    """The model can only utilise a single transmission expansion cost. If the tranche
    1 column is nan then this function adopts the tranche 2 cost if it is not
    nan. The process is repeated with tranche 3 if the cost is still nan.
    """
    tranche_one = "indicative_transmission_expansion_cost_$m/mw_tranche_1"
    tranche_two = "indicative_transmission_expansion_cost_$m/mw_tranche_2"
    tranche_three = "indicative_transmission_expansion_cost_$m/mw_tranche_3"

    first_replacement_check = data[tranche_one].isna() & ~data[tranche_two].isna()
    data.loc[first_replacement_check, tranche_one] = data.loc[
        first_replacement_check, tranche_two
    ]
    second_replacement_check = data[tranche_one].isna() & ~data[tranche_three].isna()
    data.loc[second_replacement_check, tranche_one] = data.loc[
        second_replacement_check, tranche_three
    ]
    return data


def _convert_cost_units(data, column):
    """Convert cost from millions of dollars per MW to $/MW"""
    data[column] = data[column] * 1e6
    return data
