"""
Multi-Period Macroeconomic Demand Scaler for the NEM Policy Sandbox.

Scales PyPSA network base temporal loads based on:
  - Compound macroeconomic growth (GDP × Elasticity + Population) per period
  - Regional heterogeneity (per-state GDP/Population overrides)
  - Electrification adders (EV charging, industrial electrification)
  - Profile shaping (EV evening peaks, rooftop solar midday subtraction)
"""

import logging
import numpy as np
import pandas as pd

from ispypsa.nextgen.core.temporal_clustering import (
    generate_ev_charging_profile,
    generate_rooftop_solar_profile,
    scale_profile_to_volume,
)

logger = logging.getLogger(__name__)

# NEM region bus name patterns
NEM_REGIONS = {
    "NSW": ["NSW", "NNSW", "CNSW", "SNSW"],
    "QLD": ["NQ", "CQ", "SQ", "QLD"],
    "VIC": ["VIC"],
    "SA":  ["SA"],
    "TAS": ["TAS"],
}

# Default electrification assumptions
# 100% EV penetration = 15% grid demand increase
EV_LOAD_FACTOR = 0.15
# 100% industrial electrification = 20% grid demand increase
IND_LOAD_FACTOR = 0.20


def _get_region_for_bus(bus_name: str) -> str | None:
    """Map a PyPSA bus name to its NEM region."""
    bus_upper = bus_name.upper()
    for region, patterns in NEM_REGIONS.items():
        for pat in patterns:
            if pat in bus_upper:
                return region
    return None


def _compute_organic_multiplier(
    pop_growth_pct: float,
    gdp_growth_pct: float,
    demand_elasticity: float,
    delta_t: int,
) -> float:
    """
    Compound organic demand multiplier.
    Formula: (1 + (GDP_Growth * Elasticity) + Population_Growth) ** delta_t
    Inputs are in percentage form (e.g., 2.0 for 2%).
    """
    pop = pop_growth_pct / 100.0
    gdp = gdp_growth_pct / 100.0
    return (1.0 + (gdp * demand_elasticity) + pop) ** delta_t


def apply_macroeconomic_scaling(network, scenario_params: dict):
    """
    Scale network.loads_t.p_set based on macroeconomic drivers, electrification
    inputs, and profile shaping. Multi-period aware.

    Logic follows Trap 4: Unit-Normalized Profiles + Rescale + Assertion Guard.

    IMPORTANT: Operates on numpy arrays to avoid corrupting the pandas
    MultiIndex internal state, which causes xarray AlignmentError in
    linopy during network.optimize().
    """
    if not hasattr(network, "loads_t") or network.loads_t.p_set is None or network.loads_t.p_set.empty:
        logger.warning("network.loads_t.p_set is empty. Skipping demand scaling.")
        return network

    p_set = network.loads_t.p_set
    original_index = p_set.index.copy()
    original_columns = p_set.columns.copy()
    # Work on a detached numpy copy to avoid corrupting the MultiIndex
    data = p_set.values.copy()

    initial_total_mwh = data.sum()
    logger.info(f"Starting total demand: {initial_total_mwh/1e6:.2f} TWh")

    # Resolve investment periods
    investment_periods = scenario_params.get("investment_periods", None)
    if investment_periods is None:
        investment_periods = [scenario_params.get("target_year", 2030)]

    is_multi_period = isinstance(original_index, pd.MultiIndex)

    # Default macro params
    default_pop = scenario_params.get("pop_growth", 0.0)
    default_gdp = scenario_params.get("gdp_growth", 0.0)
    default_elasticity = scenario_params.get("demand_elasticity", 0.0)
    regional_params = scenario_params.get("regional_params", None)

    # Electrification params (can be per-period dicts or flat floats)
    ev_pen_raw = scenario_params.get("ev_penetration", 0.0)
    ind_pen_raw = scenario_params.get("ind_electrification", 0.0)
    solar_pen_raw = scenario_params.get("rooftop_solar_penetration", 0.0)

    for period in investment_periods:
        delta_t = max(0, period - 2026)

        # Resolve per-period electrification values
        ev_pen = ev_pen_raw.get(str(period), 0.0) if isinstance(ev_pen_raw, dict) else ev_pen_raw
        ind_pen = ind_pen_raw.get(str(period), 0.0) if isinstance(ind_pen_raw, dict) else ind_pen_raw
        solar_pen = solar_pen_raw.get(str(period), 0.0) if isinstance(solar_pen_raw, dict) else solar_pen_raw

        # Get the row mask for this period
        if is_multi_period:
            period_mask = np.array(original_index.get_level_values(0) == period)
        else:
            period_mask = np.ones(len(data), dtype=bool)

        if not period_mask.any():
            continue

        n_hours = int(period_mask.sum())
        n_cols = data.shape[1]

        # 1. Apply Organic & Industrial growth (Volume scaling)
        for col_idx, col in enumerate(original_columns):
            actual_bus = network.loads.at[col, "bus"] if hasattr(network, "loads") and col in network.loads.index else col
            region = _get_region_for_bus(str(actual_bus))

            if regional_params and region and region in regional_params:
                rp = regional_params[region]
                pop = rp.get("pop_growth", default_pop)
                gdp = rp.get("gdp_growth", default_gdp)
                elast = rp.get("demand_elasticity", default_elasticity)
            else:
                pop, gdp, elast = default_pop, default_gdp, default_elasticity

            organic_mult = _compute_organic_multiplier(pop, gdp, elast, delta_t)
            ind_adder = (ind_pen / 100.0) * IND_LOAD_FACTOR
            total_mult = organic_mult * (1.0 + ind_adder)
            data[period_mask, col_idx] *= total_mult

        # 2. Add EV shaped profile
        if ev_pen > 0:
            post_organic_mwh = data[period_mask].sum()
            ev_annual_mwh = post_organic_mwh * (ev_pen / 100.0) * EV_LOAD_FACTOR

            ev_profile = generate_ev_charging_profile(n_hours)
            ev_scaled = scale_profile_to_volume(ev_profile, ev_annual_mwh)

            # Distribute proportionally across all load columns
            col_sums = data[period_mask].sum(axis=0)
            total_sum = col_sums.sum()
            if total_sum > 0:
                col_shares = col_sums / total_sum
                for col_idx in range(n_cols):
                    data[period_mask, col_idx] += ev_scaled * col_shares[col_idx]

        # 3. Subtract Rooftop Solar shaped profile
        if solar_pen > 0:
            post_organic_mwh = data[period_mask].sum()
            solar_annual_mwh = post_organic_mwh * (solar_pen / 100.0) * 0.10

            solar_profile = generate_rooftop_solar_profile(n_hours)
            solar_scaled = scale_profile_to_volume(solar_profile, solar_annual_mwh)

            col_sums = data[period_mask].sum(axis=0)
            total_sum = col_sums.sum()
            if total_sum > 0:
                col_shares = col_sums / total_sum
                for col_idx in range(n_cols):
                    adjusted = data[period_mask, col_idx] - solar_scaled * col_shares[col_idx]
                    data[period_mask, col_idx] = np.maximum(adjusted, 0.0)

        logger.info(
            f"Period {period}: scaled demand = {data[period_mask].sum() / 1e6:.2f} TWh"
        )

    # Reconstruct the DataFrame with the original, untouched index
    new_p_set = pd.DataFrame(data, index=original_index, columns=original_columns)
    network.loads_t.p_set = new_p_set

    final_total_mwh = network.loads_t.p_set.sum().sum()
    logger.info(f"Final scaled total demand: {final_total_mwh/1e6:.2f} TWh")

    return network
