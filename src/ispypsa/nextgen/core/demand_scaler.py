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

    Parameters
    ----------
    network : pypsa.Network
        The PyPSA network (may have multi-period snapshots).
    scenario_params : dict
        Keys:
        - investment_periods (list[int]): e.g. [2026, 2030, 2040, 2050]
        - pop_growth (float): default population growth %
        - gdp_growth (float): default GDP growth %
        - demand_elasticity (float)
        - regional_params (dict, optional): per-region overrides, e.g.
            {"NSW": {"pop_growth": 1.8, "gdp_growth": 2.5}, ...}
        - ev_penetration (dict or float): % penetration, per-period or flat
        - ind_electrification (dict or float): % penetration, per-period or flat
        - rooftop_solar_penetration (dict or float): % penetration, per-period or flat

    Returns
    -------
    network : pypsa.Network
        Modified in-place.
    """
    if not hasattr(network, "loads_t") or network.loads_t.p_set is None or network.loads_t.p_set.empty:
        logger.warning("network.loads_t.p_set is empty. Skipping demand scaling.")
        return network

    p_set = network.loads_t.p_set
    starting_twh = p_set.sum().sum() / 1e6
    logger.info(f"Starting total demand: {starting_twh:.2f} TWh")

    # Resolve investment periods
    investment_periods = scenario_params.get("investment_periods", None)
    if investment_periods is None:
        # Fallback: single-period behaviour using target_year
        target_year = scenario_params.get("target_year", 2030)
        investment_periods = [target_year]

    # Detect if snapshots are multi-indexed (period, timestep)
    is_multi_period = isinstance(p_set.index, pd.MultiIndex)

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

        # Get the slice of snapshots for this period
        if is_multi_period:
            try:
                period_mask = p_set.index.get_level_values(0) == period
            except Exception:
                logger.warning(f"Could not slice period {period} from multi-index. Skipping.")
                continue
        else:
            # Single-period: apply to all snapshots
            period_mask = np.ones(len(p_set), dtype=bool)

        period_slice = p_set.loc[period_mask]
        if period_slice.empty:
            continue

        n_hours = len(period_slice)

        for col in period_slice.columns:
            bus_name = col  # Load column typically corresponds to load name, not bus
            # Try to find the bus from network.loads if available
            actual_bus = bus_name
            if hasattr(network, "loads") and col in network.loads.index:
                actual_bus = network.loads.at[col, "bus"]

            region = _get_region_for_bus(str(actual_bus))

            # Resolve macro params (regional or default)
            if regional_params and region and region in regional_params:
                rp = regional_params[region]
                pop = rp.get("pop_growth", default_pop)
                gdp = rp.get("gdp_growth", default_gdp)
                elast = rp.get("demand_elasticity", default_elasticity)
            else:
                pop, gdp, elast = default_pop, default_gdp, default_elasticity

            # 1. Organic growth multiplier
            organic_mult = _compute_organic_multiplier(pop, gdp, elast, delta_t)

            # 2. Electrification multiplier (volume-based)
            ev_adder = (ev_pen / 100.0) * EV_LOAD_FACTOR
            ind_adder = (ind_pen / 100.0) * IND_LOAD_FACTOR
            electrification_mult = 1.0 + ev_adder + ind_adder

            # Apply volume scaling
            total_mult = organic_mult * electrification_mult
            p_set.loc[period_mask, col] = period_slice[col].values * total_mult

        # 3. Profile shaping — EV charging overlay (additive)
        if ev_pen > 0:
            base_period_mwh = p_set.loc[period_mask].sum().sum()
            ev_annual_mwh = base_period_mwh * (ev_pen / 100.0) * EV_LOAD_FACTOR
            ev_profile = generate_ev_charging_profile(n_hours)
            ev_scaled = scale_profile_to_volume(ev_profile, ev_annual_mwh)

            # Distribute proportionally across all load columns
            col_shares = p_set.loc[period_mask].sum() / p_set.loc[period_mask].sum().sum()
            for col in p_set.columns:
                p_set.loc[period_mask, col] = (
                    p_set.loc[period_mask, col].values + ev_scaled * col_shares[col]
                )

        # 4. Profile shaping — Rooftop solar subtraction
        if solar_pen > 0:
            base_period_mwh = p_set.loc[period_mask].sum().sum()
            solar_annual_mwh = base_period_mwh * (solar_pen / 100.0) * 0.10  # 100% = 10% demand reduction
            solar_profile = generate_rooftop_solar_profile(n_hours)
            solar_scaled = scale_profile_to_volume(solar_profile, solar_annual_mwh)

            col_shares = p_set.loc[period_mask].sum() / p_set.loc[period_mask].sum().sum()
            for col in p_set.columns:
                # Subtract solar (reduces net demand at midday)
                adjusted = p_set.loc[period_mask, col].values - solar_scaled * col_shares[col]
                # Floor at zero — can't have negative net demand
                p_set.loc[period_mask, col] = np.maximum(adjusted, 0.0)

        logger.info(
            f"Period {period}: delta_t={delta_t}, "
            f"period demand={p_set.loc[period_mask].sum().sum() / 1e6:.2f} TWh"
        )

    network.loads_t.p_set = p_set

    final_twh = network.loads_t.p_set.sum().sum() / 1e6
    logger.info(f"Final scaled total demand: {final_twh:.2f} TWh")

    return network
