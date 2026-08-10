import logging

import linopy
import numpy as np
import pandas as pd
import pypsa
import xarray as xr

_GJ_PER_PJ = 1.0e6


def _add_gas_supply_curve(
    network: pypsa.Network,
    gas_supply_curve: pd.DataFrame,
    generators: pd.DataFrame,
) -> None:
    """Adds a stepped gas fuel supply curve to the network's linopy model.

    Generator marginal costs already carry the IASR baseline gas price, so each
    tranche prices only a premium ('adder_$/gj') above that baseline. Per
    investment period, tranche purchase variables buy annual gas energy (GJ) up
    to each tranche's cap, and total annual gas burn (generation of Gas-carrier
    generators weighted by snapshot weightings and heat rates) must be covered
    by tranche purchases. The LP fills cheap tranches first, so gas consumed
    beyond each tranche boundary pays the next tranche's premium — a convex
    piecewise-linear fuel cost rising with total gas consumption.

    Must be called after `network.optimize.create_model()` and re-called after
    any model rebuild, like `_add_custom_constraints`.

    Args:
        network: The `pypsa.Network` object with its linopy model built.
        gas_supply_curve: `pd.DataFrame` with columns 'investment_period',
            'tranche', 'cap_pj' (NaN for uncapped) and 'adder_$/gj'.
        generators: `pd.DataFrame` of PyPSA friendly generator definitions,
            used for the 'carrier' and 'isp_heat_rate_gj/mwh' columns that are
            stripped from `network.generators` at build time.

    Returns: None
    """
    heat_rates = _get_gas_generator_heat_rates(generators, network)
    if heat_rates.empty:
        logging.warning(
            "Gas supply curve configured but the network has no gas generators, "
            "so no gas supply curve constraints were added."
        )
        return
    logging.info(
        f"Adding gas supply curve constraints over {len(heat_rates)} gas generators"
    )
    for period in network.investment_periods:
        tranches = gas_supply_curve[gas_supply_curve["investment_period"] == period]
        purchases = _add_tranche_purchase_variables(network.model, tranches, period)
        _constrain_gas_burn_to_purchases(network, heat_rates, purchases, period)
        _add_tranche_premiums_to_objective(network, tranches, purchases, period)


def _get_gas_generator_heat_rates(
    generators: pd.DataFrame, network: pypsa.Network
) -> pd.Series:
    """Heat rates (GJ/MWh) of the Gas-carrier generators present in the network."""
    gas = generators[generators["carrier"] == "Gas"]
    gas = gas[gas["name"].isin(network.generators.index)]
    heat_rates = pd.to_numeric(
        gas.set_index("name")["isp_heat_rate_gj/mwh"], errors="coerce"
    )
    return _fill_missing_heat_rates_from_technology_medians(heat_rates, gas)


def _fill_missing_heat_rates_from_technology_medians(
    heat_rates: pd.Series, gas: pd.DataFrame
) -> pd.Series:
    """Fills heat-rate gaps (e.g. some announced IASR units) from same-technology peers."""
    if not heat_rates.isna().any():
        return heat_rates
    technology = gas.set_index("name")["isp_technology_type"]
    filled = heat_rates.fillna(technology.map(heat_rates.groupby(technology).median()))
    if filled.isna().any():
        raise ValueError(
            f"Gas generators are missing heat rates and have no same-technology peer "
            f"to fill from, so the gas supply curve cannot meter their fuel use: "
            f"{sorted(filled[filled.isna()].index)}"
        )
    logging.warning(
        f"Gas generators missing heat rates in the IASR data; metering their gas "
        f"supply curve fuel use at their technology-type median heat rate: "
        f"{sorted(heat_rates[heat_rates.isna()].index)}"
    )
    return filled


def _add_tranche_purchase_variables(
    model: linopy.Model, tranches: pd.DataFrame, period: int
) -> linopy.Variable:
    """One purchase variable (GJ/year) per tranche, bounded by the tranche cap."""
    caps_gj = (tranches["cap_pj"] * _GJ_PER_PJ).fillna(np.inf)
    coords = pd.Index(tranches["tranche"], name="gas_tranche")
    return model.add_variables(
        lower=xr.DataArray(np.zeros(len(tranches)), coords=[coords]),
        upper=xr.DataArray(caps_gj.to_numpy(), coords=[coords]),
        name=f"gas_supply_purchases_gj_{period}",
    )


def _constrain_gas_burn_to_purchases(
    network: pypsa.Network,
    heat_rates: pd.Series,
    purchases: linopy.Variable,
    period: int,
) -> None:
    """Requires the period's annual gas burn (GJ) to be covered by tranche purchases."""
    p = network.model.variables.Generator_p.loc[:, heat_rates.index.to_list()]
    weights = network.snapshot_weightings["generators"].to_numpy()
    in_period = (network.snapshots.get_level_values(0) == period).astype(float)
    # Outer product of per-snapshot annualisation weights (zeroed outside the
    # period) and per-generator heat rates gives each Generator_p entry's GJ
    # contribution. Built with the variable's own coords so xarray aligns
    # rather than clashing with linopy's snapshot MultiIndex.
    gj_per_mw = xr.DataArray(
        np.outer(weights * in_period, heat_rates.to_numpy()),
        coords=p.coords,
        dims=p.dims,
    )
    annual_gas_gj = (p * gj_per_mw).sum()
    network.model.add_constraints(
        annual_gas_gj - purchases.sum() <= 0, name=f"gas_supply_curve_{period}"
    )


def _add_tranche_premiums_to_objective(
    network: pypsa.Network,
    tranches: pd.DataFrame,
    purchases: linopy.Variable,
    period: int,
) -> None:
    """Prices tranche purchases at their adders, weighted like other operational costs."""
    objective_weight = float(network.investment_period_weightings["objective"][period])
    adders = xr.DataArray(
        tranches["adder_$/gj"].to_numpy() * objective_weight,
        coords=[pd.Index(tranches["tranche"], name="gas_tranche")],
    )
    network.model.objective = network.model.objective + (adders * purchases).sum()
