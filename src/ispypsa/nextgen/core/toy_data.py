import pandas as pd
import numpy as np
import pypsa
from ispypsa.nextgen.core.gencost_ingestor import GenCostIngestor
try:
    from ispypsa.nextgen.config.models import TestbedConfig
except ImportError:
    TestbedConfig = None

def initialize_multi_horizon(n: pypsa.Network) -> None:
    """
    Sets up the multi-period investment framework for PyPSA.
    Includes time index (MultiIndex), step weightings, and the objective NPV discount rates.
    """
    investment_periods = [2030, 2040, 2050]
    n.investment_periods = investment_periods
    
    freq = "h"
    periods_per_year = 168 # 7 days
    
    snapshots = []
    for year in investment_periods:
        dates = pd.date_range(start=f"{year}-01-01 00:00:00", periods=periods_per_year, freq=freq)
        snapshots.extend([(year, d) for d in dates])
        
    n.set_snapshots(pd.MultiIndex.from_tuples(snapshots, names=["period", "timestep"]))
    
    discount_rate = 0.07 # Using 7% WACC from the GenCost Ingestor
    n.investment_period_weightings["years"] = [10, 10, 10]
    n.investment_period_weightings["objective"] = [
        1.0, 
        1.0 / (1 + discount_rate)**10, 
        1.0 / (1 + discount_rate)**20
    ]

def apply_synthetic_data(n: pypsa.Network, config=None) -> None:
    """
    Applies synthetic profiles and sets up multi-horizon GenCost generators.
    """
    # Build spatial penalty mapping if config is provided
    penalties = {}
    if config is not None and getattr(config, "nodes", None):
        for node in config.nodes:
            penalties[node.name + "_AC"] = getattr(node, "spatial_penalty_cost", 0.0)
    investment_periods = list(n.investment_periods)
    ingestor = GenCostIngestor(wacc=0.07)
    
    periods_per_year = 168
    hours = np.arange(periods_per_year)
    days = hours // 24
    
    # Demand shapes
    # 1. Residential: Diurnal peaks (morning and evening)
    res_daily_shape = 0.5 + 0.3 * np.sin(np.pi * (hours % 24 - 8) / 12) + 0.2 * np.sin(np.pi * (hours % 24 - 18) / 8)
    base_res_demand = np.maximum(0.2, res_daily_shape) * 600.0  # MW scale
    
    # 2. Industrial: Baseload, flatter profile
    base_ind_demand = np.ones(periods_per_year) * 400.0 # MW scale
    
    weather_front = np.ones(periods_per_year)
    weather_front[(days >= 0) & (days <= 2)] = 1.2
    weather_front[(days >= 3) & (days <= 4)] = 0.2
    weather_front[(days >= 5)] = 1.0
    
    solar_daily = np.where((hours % 24 > 7) & (hours % 24 < 19), 
                           np.sin(np.pi * (hours % 24 - 7) / 12), 0)
    solar_pu_base = np.clip(solar_daily * weather_front, 0, 1)
    
    np.random.seed(42)
    wind_base_arr = 0.5 + 0.2 * np.sin(np.pi * hours / 24) + np.random.normal(0, 0.1, periods_per_year)
    wind_pu_base = np.clip(wind_base_arr * weather_front, 0, 1)
    
    # Multi-horizon arrays
    res_demand_multi = []
    ind_demand_multi = []
    solar_pu_multi = []
    wind_pu_multi = []
    
    # Active Electrification Shock Modifiers (Step Change)
    res_demand_scalars = {2030: 1.0, 2040: 1.5, 2050: 2.2}
    ind_demand_scalars = {2030: 1.0, 2040: 1.2, 2050: 1.8}
    
    for year in investment_periods:
        res_demand_multi.extend(base_res_demand * res_demand_scalars[year])
        ind_demand_multi.extend(base_ind_demand * ind_demand_scalars[year])
        solar_pu_multi.extend(solar_pu_base)
        wind_pu_multi.extend(wind_pu_base)
        
    for bus in n.buses.index:
        penalty = penalties.get(bus, 0.0)
        
        if bus.endswith("_AC"):
            n.add("Load", f"{bus}_Residential_Demand", bus=bus, p_set=res_demand_multi)
            n.add("Load", f"{bus}_Industrial_Demand", bus=bus, p_set=ind_demand_multi)
            
            for year in investment_periods:
                # Vintage-specific generators bound to their exact year
                n.add(
                    "Generator", 
                    f"{bus}_Solar_{year}", 
                    bus=bus, 
                    carrier="solar",
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=ingestor.get_lifetime("Solar"),
                    capital_cost=ingestor.get_annualized_cost("Solar", year) + penalty,
                    marginal_cost=ingestor.get_marginal_cost("Solar"),
                    p_max_pu=solar_pu_multi
                )
                
                n.add(
                    "Generator", 
                    f"{bus}_Wind_{year}", 
                    bus=bus, 
                    carrier="wind",
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=ingestor.get_lifetime("Wind"),
                    capital_cost=ingestor.get_annualized_cost("Wind", year) + penalty,
                    marginal_cost=ingestor.get_marginal_cost("Wind"),
                    p_max_pu=wind_pu_multi
                )
                
                n.add(
                    "Generator",
                    f"{bus}_Gas_Peaker_{year}",
                    bus=bus,
                    carrier="gas",
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=ingestor.get_lifetime("Gas_Peaker"),
                    capital_cost=ingestor.get_annualized_cost("Gas_Peaker", year),
                    marginal_cost=ingestor.get_marginal_cost("Gas_Peaker"),
                )
