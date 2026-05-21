import re
from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.translator.helpers import convert_to_numeric_if_possible


# Monthly capacity-factor profile for conventional (non-pumped) hydro, applied
# as a static p_max_pu series for Water-carrier generators. ISPyPSA does not
# load hydro availability traces, so without this constraint the LP dispatches
# Water generators at ~85-100% CF (unbounded by anything except p_nom), inflating
# annual hydro generation by ~4x vs realistic levels.
#
# Values derived from AEMO Generation Information NEM monthly hydro generation
# (long-run averages, NSW + Tas hydro dominate the fleet). Annual mean ≈ 0.37,
# inside the realistic 30-45% CF band for Australian conventional hydro. This
# applies uniformly to all Water-carrier generators (Tumut, Murray, Eildon,
# Bendeela, etc.); per-generator CF differentiation requires AEMO Gen Info
# per-facility data which is out of scope for this fix.
_HYDRO_MONTHLY_CF = {
    1: 0.25, 2: 0.25,             # peak summer — low inflows
    3: 0.35, 4: 0.35, 5: 0.35,    # autumn
    6: 0.40, 7: 0.40, 8: 0.40,    # winter — peak inflows
    9: 0.45, 10: 0.45, 11: 0.45,  # spring — snowmelt
    12: 0.30,                     # early summer
}


def _build_seasonal_hydro_trace(snapshots: pd.MultiIndex) -> pd.DataFrame:
    """Build a wind/solar-shaped trace DataFrame for Water-carrier generators.

    PyPSA's network.snapshots MultiIndex levels are unnamed by default while
    ISPyPSA's trace DataFrames use named columns ("investment_periods",
    "snapshots") that get_set_index'd downstream. Construct by position to
    work either way."""
    investment_period_level = snapshots.get_level_values(0)
    timestep_level = snapshots.get_level_values(1)
    return pd.DataFrame(
        {
            "investment_periods": investment_period_level,
            "snapshots": timestep_level,
            "p_max_pu": [_HYDRO_MONTHLY_CF[m] for m in timestep_level.month],
        }
    )


def _get_trace_data(generator_name: str, path_to_traces: Path):
    """Fetches trace data for a generator from directories containing traces.

    Args:
        generator_name: String defining the generator's name
        path_to_traces: `pathlib.Path` for directory containing traces

    Returns:
        DataFrame with resource trace data.
    """
    generator_name_without_build_year = re.sub(r"_[0-9]{4}$", "", generator_name)
    filename = Path(f"{generator_name_without_build_year}.parquet")
    trace_filepath = path_to_traces / filename
    trace_data = pd.read_parquet(trace_filepath)
    return trace_data


def _get_marginal_cost_timeseries(
    generator_id: str, path_to_marginal_costs: Path
) -> pd.Series:
    """Fetches marginal cost timeseries data for a generator and returns a Series
    with marginal costs and (investment_period, snapshots) multi-index.

    Args:
        generator_id: String defining the generator's id (name with special characters
            replaced by "_").
        path_to_marginal_costs: `pathlib.Path` for directory containing marginal costs.

    Returns:
        Series with marginal cost timeseries data.
    """
    filename = Path(f"{generator_id}.parquet")
    trace_filepath = path_to_marginal_costs / filename
    marginal_costs = pd.read_parquet(trace_filepath)
    marginal_costs = marginal_costs.set_index(
        ["investment_periods", "snapshots"]
    ).squeeze()
    return marginal_costs


def _add_generator_to_network(
    generator_definition: dict,
    network: pypsa.Network,
    path_to_solar_traces: Path,
    path_to_wind_traces: Path,
    path_to_marginal_costs: Path,
) -> None:
    """Adds a generator to a pypsa.Network based on a dict containing PyPSA Generator
    attributes.

    If the carrier of a generator is Wind or Solar then a dynamic maximum availability
    for the generator is applied (via `p_max_pu`). Otherwise, the nominal capacity of the
    generator is used to apply a static maximum availability.

    Args:
        generator_definition: dict containing pypsa Generator parameters
        network: The `pypsa.Network` object
        path_to_solar_traces: `pathlib.Path` for directory containing solar traces
        path_to_wind_traces: `pathlib.Path` for directory containing wind traces

    Returns: None
    """
    generator_definition["class_name"] = "Generator"

    if generator_definition["carrier"] == "Wind":
        trace_data = _get_trace_data(generator_definition["name"], path_to_wind_traces)
    elif generator_definition["carrier"] == "Solar":
        trace_data = _get_trace_data(generator_definition["name"], path_to_solar_traces)
    elif generator_definition["carrier"] == "Water":
        trace_data = _build_seasonal_hydro_trace(network.snapshots)
    else:
        trace_data = None

    if trace_data is not None:
        trace_data = trace_data.set_index(["investment_periods", "snapshots"])
        generator_definition["p_max_pu"] = trace_data["p_max_pu"]

    if isinstance(generator_definition["marginal_cost"], str):
        marginal_cost_timeseries = _get_marginal_cost_timeseries(
            generator_definition["marginal_cost"], path_to_marginal_costs
        )
        generator_definition["marginal_cost"] = marginal_cost_timeseries

    pypsa_attributes_only = {
        key: value
        for key, value in generator_definition.items()
        if not key.startswith("isp_") or key == "isp_technology_type"
    }
    network.add(**pypsa_attributes_only)


def _add_generators_to_network(
    network: pypsa.Network,
    generators: pd.DataFrame,
    path_to_timeseries_data: Path,
) -> None:
    """Adds the generators in a pypsa-friendly `pd.DataFrame` to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data
    Returns: None
    """
    path_to_solar_traces = path_to_timeseries_data / Path("solar_traces")
    path_to_wind_traces = path_to_timeseries_data / Path("wind_traces")
    path_to_marginal_costs = path_to_timeseries_data / Path("marginal_cost_timeseries")

    # This is needed because numbers can be converted to strings if the data has been saved to a csv.
    generators = convert_to_numeric_if_possible(generators, cols=["marginal_cost"])

    generators.apply(
        lambda row: _add_generator_to_network(
            row.to_dict(),
            network,
            path_to_solar_traces,
            path_to_wind_traces,
            path_to_marginal_costs,
        ),
        axis=1,
    )


def _add_custom_constraint_generators_to_network(
    network: pypsa.Network, generators: pd.DataFrame
) -> None:
    """Adds the Generators defined in `custom_constraint_generators.csv` in the `path_pypsa_inputs` directory to the
    `pypsa.Network` object. These are generators that connect to a dummy bus, not part of the rest of the network,
    the generators are used to model custom constraint investment by referencing the p_nom of the generators in the
    custom constraints.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.

    Returns: None
    """
    generators["class_name"] = "Generator"
    generators.apply(lambda row: network.add(**row.to_dict()), axis=1)


def _update_generator_availability_timeseries(
    name: str,
    carrier: str,
    network: pypsa.Network,
    path_to_solar_traces: Path,
    path_to_wind_traces: Path,
) -> None:
    """Updates the timeseries availability of the generator in the `pypsa.Network`.

    The function is used to set up the model for operational modelling following
    capacity expansion optimisation. Once the model snapshots are updated then the
    generator time series also need to be updated to match.

    Args:
        name: str specifying the generators name
        carrier: the generator fuel type
        network: The `pypsa.Network` object
        path_to_solar_traces: `pathlib.Path` for directory containing solar traces
        path_to_wind_traces: `pathlib.Path` for directory containing wind traces

    Returns: None
    """

    if carrier == "Wind":
        trace_data = _get_trace_data(name, path_to_wind_traces)
    elif carrier == "Solar":
        trace_data = _get_trace_data(name, path_to_solar_traces)
    else:
        trace_data = None

    if trace_data is not None:
        trace_data = trace_data.set_index(["investment_periods", "snapshots"])
        network.generators_t.p_max_pu[name] = trace_data.loc[:, ["p_max_pu"]]


def _update_generators_availability_timeseries(
    network: pypsa.Network,
    generators: pd.DataFrame,
    path_to_timeseries_data: Path,
) -> None:
    """Updates the timeseries availability of the generators in the pypsa-friendly `
    pd.DataFrame` in the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data
    Returns: None
    """
    path_to_solar_traces = path_to_timeseries_data / Path("solar_traces")
    path_to_wind_traces = path_to_timeseries_data / Path("wind_traces")
    generators.apply(
        lambda row: _update_generator_availability_timeseries(
            row["name"],
            row["carrier"],
            network,
            path_to_solar_traces,
            path_to_wind_traces,
        ),
        axis=1,
    )
