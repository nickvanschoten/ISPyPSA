import logging

import linopy
import numpy as np
import pandas as pd
import pypsa
import xarray as xr

# Tranche purchase variables are denominated in TJ/year. GJ puts tranche caps at
# ~1e8-inf against a model whose other variables are ~1e0-1e5; HiGHS then warns of
# excessive column bounds and cuPDLP-C can terminate at iteration 0 with a spurious
# Optimal on an infeasible point (observed on the rep-week NEM LP). TJ lands the
# caps (~1e5), objective coefficients (~1e4) and burn-coupling coefficients
# (~1e-3-0.2) all inside the model's existing magnitude range.
_TJ_PER_PJ = 1.0e3
_GJ_PER_TJ = 1.0e3


def _add_fuel_supply_curve(
    network: pypsa.Network,
    fuel_supply_curve: pd.DataFrame,
    generators: pd.DataFrame,
    carrier: str,
) -> None:
    """Adds a stepped fuel supply curve for one carrier to the network's linopy model.

    Generator marginal costs already carry the IASR baseline fuel price, so each
    tranche prices only a premium ('adder_$/gj') above that baseline. Per
    investment period, tranche purchase variables buy annual fuel energy (TJ) up
    to each tranche's cap, and total annual fuel burn (generation of the
    carrier's generators weighted by snapshot weightings and heat rates) must be
    covered by tranche purchases. The LP fills cheap tranches first, so fuel
    consumed beyond each tranche boundary pays the next tranche's premium — a
    convex piecewise-linear fuel cost rising with total fuel consumption.

    Must be called after `network.optimize.create_model()` and re-called after
    any model rebuild, like `_add_custom_constraints`. Curves for different
    carriers coexist on one model: each gets its own purchase variables,
    coupling constraint and objective terms, named by the carrier.

    Args:
        network: The `pypsa.Network` object with its linopy model built.
        fuel_supply_curve: `pd.DataFrame` with columns 'investment_period',
            'tranche', 'cap_pj' (NaN for uncapped) and 'adder_$/gj'.
        generators: `pd.DataFrame` of PyPSA friendly generator definitions,
            used for the 'carrier' and 'isp_heat_rate_gj/mwh' columns that are
            stripped from `network.generators` at build time.
        carrier: the PyPSA carrier whose fuel the curve prices, e.g. 'Gas' or
            'Biomass'.

    Returns: None
    """
    heat_rates = _get_fuel_generator_heat_rates(generators, network, carrier)
    if heat_rates.empty:
        logging.warning(
            f"{carrier} supply curve configured but the network has no "
            f"{carrier.lower()} generators, so no {carrier.lower()} supply curve "
            f"constraints were added."
        )
        return
    logging.info(
        f"Adding {carrier.lower()} supply curve constraints over {len(heat_rates)} "
        f"{carrier.lower()} generators"
    )
    for period in network.investment_periods:
        tranches = fuel_supply_curve[fuel_supply_curve["investment_period"] == period]
        purchases = _add_tranche_purchase_variables(
            network.model, tranches, period, carrier
        )
        _constrain_fuel_burn_to_purchases(
            network, heat_rates, purchases, period, carrier
        )
        _add_tranche_premiums_to_objective(
            network, tranches, purchases, period, carrier
        )


def _get_fuel_generator_heat_rates(
    generators: pd.DataFrame, network: pypsa.Network, carrier: str
) -> pd.Series:
    """Heat rates (GJ/MWh) of the carrier's generators present in the network."""
    fuelled = generators[generators["carrier"] == carrier]
    fuelled = fuelled[fuelled["name"].isin(network.generators.index)]
    heat_rates = pd.to_numeric(
        fuelled.set_index("name")["isp_heat_rate_gj/mwh"], errors="coerce"
    )
    return _fill_missing_heat_rates_from_technology_medians(
        heat_rates, fuelled, carrier
    )


def _fill_missing_heat_rates_from_technology_medians(
    heat_rates: pd.Series, fuelled: pd.DataFrame, carrier: str
) -> pd.Series:
    """Fills heat-rate gaps (e.g. some announced IASR units) from same-technology peers."""
    if not heat_rates.isna().any():
        return heat_rates
    technology = fuelled.set_index("name")["isp_technology_type"]
    filled = heat_rates.fillna(technology.map(heat_rates.groupby(technology).median()))
    if filled.isna().any():
        raise ValueError(
            f"{carrier} generators are missing heat rates and have no same-technology "
            f"peer to fill from, so the {carrier.lower()} supply curve cannot meter "
            f"their fuel use: {sorted(filled[filled.isna()].index)}"
        )
    logging.warning(
        f"{carrier} generators missing heat rates in the IASR data; metering their "
        f"{carrier.lower()} supply curve fuel use at their technology-type median "
        f"heat rate: {sorted(heat_rates[heat_rates.isna()].index)}"
    )
    return filled


def _add_tranche_purchase_variables(
    model: linopy.Model, tranches: pd.DataFrame, period: int, carrier: str
) -> linopy.Variable:
    """One purchase variable (TJ/year) per tranche, bounded by the tranche cap."""
    caps_tj = (tranches["cap_pj"] * _TJ_PER_PJ).fillna(np.inf)
    coords = pd.Index(tranches["tranche"], name=f"{carrier.lower()}_tranche")
    return model.add_variables(
        lower=xr.DataArray(np.zeros(len(tranches)), coords=[coords]),
        upper=xr.DataArray(caps_tj.to_numpy(), coords=[coords]),
        name=f"{carrier.lower()}_supply_purchases_tj_{period}",
    )


def _constrain_fuel_burn_to_purchases(
    network: pypsa.Network,
    heat_rates: pd.Series,
    purchases: linopy.Variable,
    period: int,
    carrier: str,
) -> None:
    """Requires the period's annual fuel burn (TJ) to be covered by tranche purchases."""
    p = network.model.variables.Generator_p.loc[:, heat_rates.index.to_list()]
    weights = network.snapshot_weightings["generators"].to_numpy()
    in_period = (network.snapshots.get_level_values(0) == period).astype(float)
    # Outer product of per-snapshot annualisation weights (zeroed outside the
    # period) and per-generator heat rates gives each Generator_p entry's TJ
    # contribution. Built with the variable's own coords so xarray aligns
    # rather than clashing with linopy's snapshot MultiIndex.
    tj_per_mw = xr.DataArray(
        np.outer(weights * in_period, heat_rates.to_numpy() / _GJ_PER_TJ),
        coords=p.coords,
        dims=p.dims,
    )
    annual_fuel_tj = (p * tj_per_mw).sum()
    network.model.add_constraints(
        annual_fuel_tj - purchases.sum() <= 0,
        name=f"{carrier.lower()}_supply_curve_{period}",
    )


def _add_tranche_premiums_to_objective(
    network: pypsa.Network,
    tranches: pd.DataFrame,
    purchases: linopy.Variable,
    period: int,
    carrier: str,
) -> None:
    """Prices tranche purchases at their adders, weighted like other operational costs."""
    objective_weight = float(network.investment_period_weightings["objective"][period])
    adders = xr.DataArray(
        tranches["adder_$/gj"].to_numpy() * _GJ_PER_TJ * objective_weight,
        coords=[pd.Index(tranches["tranche"], name=f"{carrier.lower()}_tranche")],
    )
    network.model.objective = network.model.objective + (adders * purchases).sum()
