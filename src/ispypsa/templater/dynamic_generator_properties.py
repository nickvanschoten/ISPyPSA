import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.templater.helpers import (
    _add_units_to_financial_year_columns,
    _assert_no_nan_load_bearing_column,
    _convert_financial_year_columns_to_float,
    _manual_remove_footnotes_from_generator_names,
    _rez_name_to_id_mapping,
    _standardise_storage_capitalisation,
)

from .helpers import _fuzzy_match_names, _snakecase_string
from .lists import _ECAA_GENERATOR_TYPES

# ISP scenario (config name) → v7.4 canonical scenario value used in the
# consolidated fuel-price tables. The mapping mirrors the cost-scenario
# dispatch in `flow_paths._determine_cost_scenario` for v7.x — see design
# call 2026-05-24. v7.4 retained "Step Change" but renamed the lower-
# ambition v6.0 "Progressive Change" → "Slower Growth" and the highest-
# ambition v6.0 "Green Energy Exports" → "Accelerated Transition".
_ISP_TO_V74_SCENARIO = {
    "Step Change": "Step Change",
    "Progressive Change": "Slower Growth",
    "Slower Growth": "Slower Growth",
    "Green Energy Exports": "Accelerated Transition",
    "Accelerated Transition": "Accelerated Transition",
}


def _add_new_entrant_wacc(
    new_entrant_table: pd.DataFrame,
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
) -> pd.DataFrame:
    """Add a per-technology `wacc` column to a new-entrant generator/storage table.

    AEMO publishes a technology- and scenario-specific weighted average cost of
    capital (the `wacc` IASR table, values in %) that its methodology uses to
    annuitise new-entrant build costs — thermal plant (CCGT/CCS/biomass) at
    ~10.5%, wind ~7.5%, solar ~7%, batteries ~8%, PHES ~8.5% under Step Change.
    The translator annuitises each new-entrant at this per-technology rate
    instead of a single scalar. When the `wacc` table isn't in the cache (v6.0
    or minimal test fixtures) the column is omitted and the translator falls
    back to the scalar config rate.
    """
    if "wacc" not in iasr_tables:
        return new_entrant_table
    wacc_by_technology = _wacc_by_technology(iasr_tables["wacc"], scenario)
    new_entrant_table = new_entrant_table.copy()
    new_entrant_table["wacc"] = _standardise_storage_capitalisation(
        new_entrant_table["technology_type"]
    ).map(wacc_by_technology)
    _assert_no_nan_load_bearing_column(
        new_entrant_table, "wacc", "technology_type", "new-entrant WACC"
    )
    return new_entrant_table


def _wacc_by_technology(wacc_table: pd.DataFrame, scenario: str) -> dict:
    """Map each technology name to its scenario WACC as a fraction (IASR lists %)."""
    scenario_col = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    technologies = _standardise_storage_capitalisation(
        pd.Series(wacc_table["Technology type"])
    )
    rates = pd.to_numeric(wacc_table[scenario_col], errors="coerce") / 100.0
    return dict(zip(technologies, rates))


def _regulated_transmission_wacc(wacc_table: pd.DataFrame, scenario: str) -> float:
    """AEMO's regulated electricity transmission WACC (fraction) for the scenario.

    ISP flow-path augmentation and REZ transmission expansion are regulated TNSP
    network investment (RIT-T assessed, recovered through regulated revenue), so
    AEMO evaluates them at the *regulated* electricity transmission WACC (Step
    Change 3.0%), not the unregulated rate (6.5%). This keeps transmission on the
    same IASR source as the per-technology generator WACCs instead of a flat
    non-IASR config value.
    """
    scenario_col = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    technology = wacc_table["Technology type"].str.strip()
    is_regulated_electricity_transmission = (
        technology.str.startswith("Electricity")
        & technology.str.contains("Transmission")
        & technology.str.contains("Regulated")
        & ~technology.str.contains("Unregulated")
    )
    rate = pd.to_numeric(
        wacc_table.loc[is_regulated_electricity_transmission, scenario_col],
        errors="coerce",
    )
    return float(rate.iloc[0]) / 100.0


def _template_generator_dynamic_properties(
    iasr_tables: dict[str, pd.DataFrame], scenario: str
) -> dict[str, pd.DataFrame | pd.Series]:
    """Creates ISPyPSA templates for dynamic generator properties (i.e. those that vary
    with calendar/financial year).

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `dict[pd.DataFrame]`: Templates for dynamic generator properties including coal
            prices, gas prices, full outage rates for existing generators, partial outage
            rates for existing generators and ECAA generator seasonal ratings.
    """
    logging.info("Creating a template for dynamic generator properties")

    # v7.4 canonical: single consolidated coal_fuel_price table with a
    # Scenario column. Map config's ISP scenario name to the v7.4 canonical
    # scenario value used in the table. v6.0 caches are normalised forward
    # to this same shape at cache load.
    v74_scenario = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    coal_prices = iasr_tables["coal_fuel_price"]
    coal_prices = coal_prices[coal_prices["Scenario"] == v74_scenario].drop(
        columns="Scenario"
    )
    coal_prices = _template_coal_prices(coal_prices)

    # v7.4 split gas prices into ECAA / new-entrant tables; concat both so
    # the downstream fuel-cost calculation can resolve any generator's
    # gas price. v6.0 normalisation produces both tables with identical
    # content (single combined v6.0 source).
    gas_prices = pd.concat(
        [
            iasr_tables["gas_prices_existing_generators"],
            iasr_tables["gas_prices_new_entrants"],
        ],
        ignore_index=True,
    )
    gas_prices = gas_prices[gas_prices["Gas price scenario"] == v74_scenario].drop(
        columns="Gas price scenario"
    )
    gas_prices = gas_prices.drop_duplicates(subset=gas_prices.columns[0]).reset_index(
        drop=True
    )
    gas_prices = _template_gas_prices(gas_prices)

    liquid_fuel_prices = iasr_tables["liquid_fuel_prices"]
    liquid_fuel_prices = _template_liquid_h2_biomethane_prices(
        liquid_fuel_prices, "liquid_fuel_price", scenario
    )

    hydrogen_prices = iasr_tables["hydrogen_prices"]
    hydrogen_prices = _template_liquid_h2_biomethane_prices(
        hydrogen_prices, "hydrogen_price", scenario
    )

    biomethane_prices = iasr_tables["biomethane_prices"]
    biomethane_prices = _template_liquid_h2_biomethane_prices(
        biomethane_prices, "biomethane_price", scenario
    )

    biomass_prices = _template_biomass_prices(iasr_tables, scenario)

    h2_gpg_emissions_reduction_factors = _template_h2_gpg_emissions_reduction_factors(
        iasr_tables, scenario
    )

    biom_gpg_emissions_reduction = iasr_tables["gpg_emissions_reduction_biomethane"]
    biom_gpg_emissions_reduction_factors = (
        _template_biom_gpg_emissions_reduction_factors(
            biom_gpg_emissions_reduction, scenario
        )
    )

    full_outage_forecasts = _template_existing_generators_full_outage_forecasts(
        iasr_tables["full_outages_forecast_existing_generators"]
    )

    partial_outage_forecasts = _template_existing_generators_partial_outage_forecasts(
        iasr_tables["partial_outages_forecast_existing_generators"]
    )

    # v7.4 canonical: single consolidated seasonal_ratings table for all ECAA
    # statuses. v6.0's four per-status tables are concat'd into this form
    # at cache load by schema normalisation.
    seasonal_ratings = _template_seasonal_ratings(
        [
            iasr_tables[
                "seasonal_ratings_existing_committed_anticipated_additional_generators"
            ]
        ]
    )

    build_costs = _template_new_entrant_build_costs(iasr_tables, scenario)
    wind_and_solar_connection_costs = (
        _template_new_entrant_wind_and_solar_connection_costs(iasr_tables, scenario)
    )

    connection_costs_other = iasr_tables["connection_costs_other"]
    non_vre_connection_costs = _template_new_entrant_non_vre_connection_costs(
        connection_costs_other
    )
    return {
        "coal_prices": coal_prices,
        "gas_prices": gas_prices,
        "liquid_fuel_prices": liquid_fuel_prices,
        "biomass_prices": biomass_prices,
        "hydrogen_prices": hydrogen_prices,
        "biomethane_prices": biomethane_prices,
        "gpg_emissions_reduction_h2": h2_gpg_emissions_reduction_factors,
        "gpg_emissions_reduction_biomethane": biom_gpg_emissions_reduction_factors,
        "full_outage_forecasts": full_outage_forecasts,
        "partial_outage_forecasts": partial_outage_forecasts,
        "seasonal_ratings": seasonal_ratings,
        "new_entrant_build_costs": build_costs,
        "new_entrant_wind_and_solar_connection_costs": wind_and_solar_connection_costs,
        "new_entrant_non_vre_connection_costs": non_vre_connection_costs,
    }


def _template_coal_prices(coal_prices: pd.DataFrame) -> pd.DataFrame:
    """Creates a coal price template

    Args:
        coal_prices: pd.DataFrame table from IASR workbook specifying coal prices
            forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for coal prices
    """
    coal_prices.columns = _add_units_to_financial_year_columns(
        coal_prices.columns, "$/GJ"
    )
    # `Scenario` column is dropped upstream by the per-scenario filter, but
    # tolerate either form.
    coal_prices = coal_prices.drop(columns="coal_price_scenario", errors="ignore")
    coal_prices = _convert_financial_year_columns_to_float(coal_prices)
    return coal_prices


def _template_gas_prices(gas_prices: pd.DataFrame) -> pd.DataFrame:
    """Creates a gas price template

    Args:
        gas_prices: pd.DataFrame table from IASR workbook specifying gas prices
            forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for gas prices
    """
    cols = _add_units_to_financial_year_columns(gas_prices.columns, "$/GJ")
    cols[0] = "generator"
    gas_prices.columns = cols
    gas_prices = gas_prices.drop(columns="gas_price_scenario", errors="ignore")
    gas_prices = _convert_financial_year_columns_to_float(gas_prices)
    return gas_prices


def _template_liquid_h2_biomethane_prices(
    price_table: pd.DataFrame, price_col_name: str, scenario: str
) -> pd.Series:
    """Creates a prices template for liquid fuel, hydrogen or biomethane.

    The function behaviour depends on the `scenario` specified in the model
    configuration and the fuel type defined in price_table.

    Args:
        price_table: pd.DataFrame table from IASR workbook specifying price forecasts
            for the given fuel type.
        price_col_name: name of the column containing the fuel type.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for specified prices (one of liquid fuel,
            hydrogen or biomethane).
    """
    price_table.columns = _add_units_to_financial_year_columns(
        price_table.columns, "$/GJ"
    )
    price_table = price_table.drop(columns=price_col_name).set_index(
        f"{price_col_name}_scenario"
    )
    price_table = _convert_financial_year_columns_to_float(price_table)
    price_table_scenario = price_table.loc[[scenario], :]
    price_table_scenario = price_table_scenario.reset_index(drop=True)
    return price_table_scenario


def _template_existing_generators_full_outage_forecasts(
    full_outages_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a full outage forecast template for existing generators

    Args:
        full_outages_forecast: pd.DataFrame table from IASR workbook specifying full
            outage forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for full outage forecasts
    """
    full_outages_forecast.columns = [
        _snakecase_string(col) for col in full_outages_forecast.columns
    ]
    full_outages_forecast = full_outages_forecast.set_index("fuel_type")
    full_outages_forecast = _apply_all_coal_averages(full_outages_forecast)
    full_outages_forecast = _convert_financial_year_columns_to_float(
        full_outages_forecast.drop(index="All Coal Average", errors="ignore")
    )
    full_outages_forecast = full_outages_forecast.reset_index()
    return full_outages_forecast


def _template_existing_generators_partial_outage_forecasts(
    partial_outages_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a partial outage forecast template for existing generators

    Args:
        partial_outages_forecast: pd.DataFrame table from IASR workbook specifying
            partial outage forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for partial outage forecasts
    """
    partial_outages_forecast.columns = [
        _snakecase_string(col) for col in partial_outages_forecast.columns
    ]
    partial_outages_forecast = partial_outages_forecast.set_index("fuel_type")
    partial_outages_forecast = _apply_all_coal_averages(partial_outages_forecast)
    partial_outages_forecast = _convert_financial_year_columns_to_float(
        partial_outages_forecast.drop(index="All Coal Average", errors="ignore")
    )
    partial_outages_forecast = partial_outages_forecast.reset_index()
    return partial_outages_forecast


def _template_seasonal_ratings(
    seasonal_ratings: list[pd.DataFrame],
) -> pd.DataFrame:
    """Creates a seasonal generator ratings template

    Args:
        seasonal_ratings: list of pd.DataFrame tables from IASR workbook specifying
            the seasonal ratings of the different generator types.

    Returns:
        `pd.DataFrame`: ISPyPSA template for seasonal generator ratings
    """

    seasonal_rating = pd.concat(seasonal_ratings, axis=0)
    seasonal_rating.columns = [
        _snakecase_string(col) for col in seasonal_rating.columns
    ]
    # v7.4 canonical name is `Power Station`; v6.0 had `Generator`. The
    # downstream filter (filter_template._filter_generator_dependent_tables)
    # keys on `generator`, so normalise to that here.
    if "power_station" in seasonal_rating.columns:
        seasonal_rating = seasonal_rating.rename(columns={"power_station": "generator"})
    seasonal_rating = _convert_seasonal_columns_to_float(seasonal_rating)
    return seasonal_rating


def _template_new_entrant_build_costs(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Creates a new entrants build cost template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant build costs
    """
    # v7.4 canonical: single `build_costs` table with `Technology`,
    # `GenCost Scenario`, `IASR Scenario`, `Source` columns and per-year
    # cost values. Filter by IASR Scenario. Pumped hydro is folded in.
    # v6.0 sources are normalised forward to this shape at cache load.
    v74_scenario = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    build_costs = iasr_tables["build_costs"]
    build_costs = build_costs[build_costs["IASR Scenario"] == v74_scenario]
    build_costs = build_costs.drop(
        columns=["GenCost Scenario", "IASR Scenario", "Source"], errors="ignore"
    )
    build_costs = _convert_financial_year_columns_to_float(build_costs)
    # convert data in $/kW to $/MW
    build_costs.columns = _add_units_to_financial_year_columns(
        build_costs.columns, "$/MW"
    )
    # enforce "storage" capitalisation to match up wiht new entrant generator names
    build_costs["technology"] = _standardise_storage_capitalisation(
        build_costs["technology"]
    )
    build_costs = build_costs.set_index("technology")
    build_costs *= 1000.0
    return build_costs.reset_index()


def _template_biomass_prices(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Creates a new entrant biomass prices template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant biomass pricess
    """
    # v7.4 canonical: single `biomass_fuel_price` table keyed by ISP scenario
    # directly (Scenario column). v6.0's two-table mapping
    # (biomass_prices + coal_and_biomass_price_consultant_scenario_mapping)
    # is collapsed into this form by schema normalisation at cache load.
    v74_scenario = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    biomass_prices = iasr_tables["biomass_fuel_price"]
    biomass_prices = biomass_prices[biomass_prices["Scenario"] == v74_scenario].drop(
        columns=["Biomass price", "Scenario"]
    )
    biomass_prices = _convert_financial_year_columns_to_float(biomass_prices)
    biomass_prices.columns = _add_units_to_financial_year_columns(
        biomass_prices.columns, "$/GJ"
    )
    return biomass_prices.reset_index(drop=True)


# Generator-to-region mapping for the two named H2 GPG generators ISPyPSA's
# Hyblend carrier flow expects. v6.0 modelled these explicitly; v7.4 publishes
# a per-region H2 blend trajectory that each named generator inherits from its
# sub-region (per (c3) Reading 1 design call 2026-05-24). Methodology tab:
# H2 blend fractions are applied uniformly to all gas peakers within each
# sub-region, per v7.4's regional framing.
_H2_GPG_GENERATOR_TO_REGION = {
    "Kogan Gas": "QLD",
    "SA Hydrogen Turbine": "SA",
}


def _template_h2_gpg_emissions_reduction_factors(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Per-generator H2 blend % time series for the H2 GPG generators ISPyPSA
    recognises (Kogan Gas, SA Hydrogen Turbine).

    Source is v7.4's `hydrogen_limit_for_gpg` (regions × scenarios × years).
    Each known H2 GPG generator inherits its region's time series. v6.0 caches
    get the same shape via schema normalisation
    (`consolidate_v60_h2_gpg_to_regional` combines the two v6.0 split tables
    into the regional layout, mapping Kogan→QLD, SA Hydrogen Turbine→SA).

    The downstream translator consumes this per-generator output to compute
    blended fuel prices for each Hyblend generator — see `translator/
    generators.py::_calculate_blended_fuel_prices` and the [[c3]] design call.

    Args:
        iasr_tables: Dict of tables from the IASR workbook.
        scenario: ISP scenario name from the model configuration.

    Returns:
        `pd.DataFrame`: one row per H2 GPG generator with year columns; empty
        DataFrame (no rows, same columns) if the scenario isn't present.
    """
    regional = iasr_tables["hydrogen_limit_for_gpg"]
    regional = regional[regional["Scenario"] == scenario].copy()
    regional = regional.drop(columns=["Scenario"]).set_index("Region")
    rows = []
    for generator, region in _H2_GPG_GENERATOR_TO_REGION.items():
        if region not in regional.index:
            continue
        row = regional.loc[region].to_dict()
        row["generator"] = generator
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["generator"])
    result = pd.DataFrame(rows).set_index("generator")
    result.columns = _add_units_to_financial_year_columns(result.columns, "%")
    result = _convert_financial_year_columns_to_float(result)
    return result.reset_index()


def _template_biom_gpg_emissions_reduction_factors(
    biom_gpg_emissions_reduction: pd.DataFrame, scenario: str
) -> pd.DataFrame:
    """Creates an emissions reduction factor template for GPG plant transitioning
    to biomethane.

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        biom_gpg_emissions_reduction: pd.DataFrame table from IASR workbook specifying
            gas fired generation emissions reduction factors from biomethane
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for biomethane GPG emissions reductions factors
    """
    # first column is unnamed: set name to "scenario"
    rename_unnamed_col_dict = {
        col: "scenario"
        for col in biom_gpg_emissions_reduction.columns
        if "Unnamed" in col
    }
    biom_gpg_emissions_reduction = biom_gpg_emissions_reduction.rename(
        columns=rename_unnamed_col_dict
    )
    biom_gpg_emissions_reduction.columns = _add_units_to_financial_year_columns(
        biom_gpg_emissions_reduction.columns, "%"
    )
    biom_gpg_emissions_reduction = biom_gpg_emissions_reduction.set_index("scenario")
    biom_gpg_emissions_reduction = _convert_financial_year_columns_to_float(
        biom_gpg_emissions_reduction
    )
    biom_gpg_emissions_reduction_scenario = biom_gpg_emissions_reduction.loc[
        [scenario], :
    ]
    return biom_gpg_emissions_reduction_scenario.reset_index(drop=True)


def _template_new_entrant_wind_and_solar_connection_costs(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Creates a new entrant wind and solar connection cost template

    Reads the version-naive consolidated `connection_cost_forecast_wind_and_solar`
    table (REZ names + Scenario + Connection capacity (MVA) + FY $ columns),
    filters to the active scenario, computes $/MW per FY, appends the system
    strength cost from `connection_costs_for_wind_and_solar`, and maps REZ
    names to IDs.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration


    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant wind and solar connection costs
    """
    forecasts = _filter_forecast_to_scenario(
        iasr_tables["connection_cost_forecast_wind_and_solar"], scenario
    )
    # Source-id-first: v7.x publishes a clean `REZ ID` column — key by it directly.
    # The FINAL 2026 ISP names V3 and V4 identically ("Western Victoria"), so
    # deriving the id from the name collapses both onto one id (V4) and loses V3's
    # connection costs. v6.0 has no `REZ ID` column, so fall back to the
    # (unique-in-v6.0) REZ name. Mirrors the source-id-first handling in
    # `static_new_generator_properties._add_rez_id_column`.
    key_col = "REZ ID" if "REZ ID" in forecasts.columns else "REZ names"
    forecasts = forecasts.set_index(key_col)
    forecasts = _convert_per_mva_columns_to_per_mw(forecasts)
    forecasts = _append_system_strength_cost(
        forecasts, iasr_tables["connection_costs_for_wind_and_solar"], key_col
    )
    forecasts = forecasts.replace("Note 1", np.nan).reset_index()
    if key_col == "REZ ID":
        # Downstream keys connection costs by REZ ID (matches
        # `new_entrant_generators.connection_cost_region_id`); the colliding name
        # column is replaced by the unambiguous id.
        forecasts["REZ names"] = forecasts.pop("REZ ID")
    else:
        forecasts["REZ names"] = _rez_name_to_id_mapping(
            forecasts["REZ names"],
            "REZ names",
            iasr_tables["renewable_energy_zones"],
        )
    return forecasts


def _filter_forecast_to_scenario(
    forecasts: pd.DataFrame, scenario: str
) -> pd.DataFrame:
    v74_scenario = _ISP_TO_V74_SCENARIO.get(scenario, scenario)
    filtered = forecasts[forecasts["Scenario"] == v74_scenario].drop(columns="Scenario")
    return filtered


def _convert_per_mva_columns_to_per_mw(forecasts: pd.DataFrame) -> pd.DataFrame:
    forecasts = _convert_financial_year_columns_to_float(forecasts)
    fy_cols = [c for c in forecasts.columns if re.match(r"[0-9]{4}-[0-9]{2}", c)]
    for col in fy_cols:
        forecasts[col] = forecasts[col] / forecasts["Connection capacity (MVA)"]
    forecasts.columns = _add_units_to_financial_year_columns(forecasts.columns, "$/MW")
    return forecasts


def _append_system_strength_cost(
    forecasts: pd.DataFrame, initial_connection_costs: pd.DataFrame, key_col: str
) -> pd.DataFrame:
    """Append a `System strength connection cost ($/MW)` column.

    v6.0 publishes a per-REZ system strength cost ($/kW) in
    `connection_costs_for_wind_and_solar`. v7.4 dropped this column from the
    workbook — AEMO's documentation indicates system strength costs are folded
    into the forecast totals in v7.4. When the column is absent, append zeros
    so the downstream contract is preserved without double-counting.

    `key_col` is the REZ key the caller indexed `forecasts` by (`REZ ID` for
    v7.x, `REZ names` for v6.0) — the system-strength series must be indexed by
    the same column so the `concat` aligns row-for-row.
    """
    initial = initial_connection_costs.set_index(key_col)
    series_name = "system_strength_connection_cost_$/mw"
    if "System Strength connection cost ($/kW)" in initial.columns:
        system_strength_cost = (
            initial["System Strength connection cost ($/kW)"] * 1000
        ).rename(series_name)
    else:
        system_strength_cost = pd.Series(
            0.0,
            index=forecasts.index,
            name=series_name,
        )
    return pd.concat([forecasts, system_strength_cost], axis=1)


def _template_new_entrant_non_vre_connection_costs(
    connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a new entrant non-VRE connection cost template

    Args:
        connection_costs: list of pd.DataFrame tables from IASR workbook specifying
            the seasonal ratings of the different generator types.

    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant non-VRE connection costs
    """
    connection_costs = _manual_remove_footnotes_from_generator_names(connection_costs)
    connection_costs = connection_costs.set_index("Region")
    # convert to $/MW and add units to columns
    col_rename_map = {}
    for col in connection_costs.columns:
        connection_costs[col] *= 1000
        col_rename_map[col] = _snakecase_string(col) + "_$/mw"
    connection_costs = connection_costs.rename(columns=col_rename_map)
    return connection_costs.reset_index()


def _convert_seasonal_columns_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Forcefully converts seasonal columns to float columns.

    Uses `pd.to_numeric(errors='coerce')` to tolerate stray non-numeric
    values that occasionally appear in the v7.4 workbook (e.g. a Region
    code spilled into a Summer Peak cell on a misaligned row). Non-numeric
    cells become NaN.
    """
    cols = [
        pd.to_numeric(df[col], errors="coerce")
        if re.match(r"summer", col) or re.match(r"winter", col)
        else df[col]
        for col in df.columns
    ]
    return pd.concat(cols, axis=1)


def _apply_all_coal_averages(outages_df: pd.DataFrame) -> pd.DataFrame:
    """Applies the All Coal Average to each coal fuel type.

    v6.0 outage forecasts published an aggregate "All Coal Average" row used
    to fill in years where individual coal-type values were missing. v7.4
    publishes the full time series for each coal type directly and omits
    the aggregate — in that case the fill-in is a no-op.
    """
    if "All Coal Average" not in outages_df.index:
        return outages_df
    where_coal_average = outages_df.loc["All Coal Average", :].notna()
    for coal_row in outages_df.index[outages_df.index.str.contains("Coal")]:
        outages_df.loc[coal_row, where_coal_average] = outages_df.loc[
            "All Coal Average", where_coal_average
        ]
    return outages_df
