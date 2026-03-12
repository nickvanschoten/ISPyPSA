import pandas as pd
import numpy as np
import pypsa
from ispypsa.nextgen.core.gencost_ingestor import GenCostIngestor
try:
    from ispypsa.nextgen.config.models import TestbedConfig
except ImportError:
    TestbedConfig = None

# 1. Zonal Nodes (AEMO Sub-regions)
NEM_ZONES = [
    "NQ_AC", "CQ_AC", "SQ_AC",   # Queensland
    "NNSW_AC", "CNSW_AC", "SNSW_AC", # New South Wales & ACT
    "VIC_MELB_AC", "VIC_REG_AC", # Victoria
    "SA_AC",                     # South Australia
    "TAS_AC"                     # Tasmania
]

AEMO_COORDS = {
    "NQ_AC": {"lat": -20.0, "lon": 145.0}, "CQ_AC": {"lat": -23.5, "lon": 148.0},
    "SQ_AC": {"lat": -27.5, "lon": 150.5}, "NNSW_AC": {"lat": -30.0, "lon": 150.0},
    "CNSW_AC": {"lat": -33.0, "lon": 148.5}, "SNSW_AC": {"lat": -35.2, "lon": 147.5},
    "VIC_MELB_AC": {"lat": -37.8, "lon": 145.0}, "VIC_REG_AC": {"lat": -36.5, "lon": 143.0},
    "SA_AC": {"lat": -34.0, "lon": 138.5}, "TAS_AC": {"lat": -42.0, "lon": 146.5}
}

# 2. Major Interconnectors (VNI, QNI, Heywood, Basslink, etc.)
# Baseline Transfer Limits (MW)
INTERCONNECTORS = [
    {"bus0": "SQ_AC", "bus1": "NNSW_AC", "name": "QNI", "capacity": 1200.0},
    {"bus0": "SNSW_AC", "bus1": "VIC_REG_AC", "name": "VNI", "capacity": 1600.0},
    {"bus0": "VIC_REG_AC", "bus1": "SA_AC", "name": "Heywood", "capacity": 600.0},
    {"bus0": "VIC_REG_AC", "bus1": "SA_AC", "name": "Murraylink", "capacity": 220.0},
    {"bus0": "VIC_REG_AC", "bus1": "TAS_AC", "name": "Basslink", "capacity": 500.0},
]

# 3. Baseline Capacities with Scheduled Retirements (MW)
# Represented simplistically: Component, Vintage, Lifespan (Determining retirement)
BASELINE_GENERATORS = [
    # Coal dropping out (Built originally in 1980s/1990s, retiring 2030-2040)
    {"bus": "CQ_AC", "carrier": "black_coal", "capacity": 3000.0, "build_year": 1990, "lifetime": 45}, # Retires 2035 (drops before 2040 period)
    {"bus": "CNSW_AC", "carrier": "black_coal", "capacity": 6000.0, "build_year": 1985, "lifetime": 45}, # Retires 2030
    {"bus": "VIC_REG_AC", "carrier": "brown_coal", "capacity": 4500.0, "build_year": 1988, "lifetime": 45}, # Retires 2033 (drops before 2040 period)
    
    # Firming/Gas (Usually persists longer)
    {"bus": "SQ_AC", "carrier": "gas", "capacity": 1500.0, "build_year": 2005, "lifetime": 35}, # Retires 2040
    {"bus": "SA_AC", "carrier": "gas", "capacity": 1200.0, "build_year": 2010, "lifetime": 35}, # Retires 2045
    {"bus": "TAS_AC", "carrier": "hydro", "capacity": 2200.0, "build_year": 1970, "lifetime": 100}, # Legacy persist
    
    # Baseline Renewables (Installed prior to 2024 optimization)
    {"bus": "SA_AC", "carrier": "wind", "capacity": 2500.0, "build_year": 2015, "lifetime": 25}, # Retires 2040
    {"bus": "VIC_REG_AC", "carrier": "wind", "capacity": 3000.0, "build_year": 2018, "lifetime": 25}, # Retires 2043
    {"bus": "NQ_AC", "carrier": "solar", "capacity": 1500.0, "build_year": 2020, "lifetime": 25}, # Retires 2045
    {"bus": "NNSW_AC", "carrier": "solar", "capacity": 1200.0, "build_year": 2021, "lifetime": 25}, # Retires 2046
]

def initialize_multi_horizon(n: pypsa.Network) -> None:
    """
    Sets up the multi-period investment framework for PyPSA spanning to 2050.
    """
    investment_periods = [2030, 2040, 2050]
    n.investment_periods = investment_periods
    
    freq = "h"
    periods_per_year = 168 # 7 days synthetic wrap
    
    snapshots = []
    for year in investment_periods:
        dates = pd.date_range(start=f"{year}-01-01 00:00:00", periods=periods_per_year, freq=freq)
        snapshots.extend([(year, d) for d in dates])
        
    n.set_snapshots(pd.MultiIndex.from_tuples(snapshots, names=["period", "timestep"]))
    
    discount_rate = 0.07 # 7% WACC
    n.investment_period_weightings["years"] = [10, 10, 10]
    n.investment_period_weightings["objective"] = [
        1.0, 
        1.0 / (1 + discount_rate)**10, 
        1.0 / (1 + discount_rate)**20
    ]

def apply_nem_topology(n: pypsa.Network, config=None) -> None:
    """
    Replaces basic sandbox profiles by instantiating the true AEMO NEM geography, 
    baseline generators (with explicit scheduled retirements), and interconnectors.
    """
    def align_ts(data_array, snapshots): 
        s = pd.Series(list(data_array), index=snapshots)
        s.index.name = "snapshot"
        return s

    # 1. Instantiating True AEMO Zones
    for zone in NEM_ZONES:
        if zone not in n.buses.index:
             lat = AEMO_COORDS.get(zone, {}).get("lat", -25.0)
             lon = AEMO_COORDS.get(zone, {}).get("lon", 135.0)
             n.add("Bus", zone, v_nom=330.0, x=lon, y=lat, carrier="AC")
             
    # 2. Extract spatial penalties and sensitivities from Testbed config
    penalties = {}
    capex_mods = {}
    opex_mods = {}
    cf_mod = 1.0
    trans_mod = 1.0
    
    if config is not None:
        if getattr(config, "nodes", None):
            for node in config.nodes:
                penalties[node.name + "_AC"] = getattr(node, "spatial_penalty_cost", 0.0)
                
        if getattr(config, "sensitivities", None):
            sens = config.sensitivities
            capex_mods = sens.capex_modifiers
            opex_mods = sens.opex_modifiers
            cf_mod = sens.capacity_factor_modifier
            trans_mod = sens.transmission_cost_modifier

    investment_periods = list(n.investment_periods)
    ingestor = GenCostIngestor(wacc=0.07)
    
    # 3. Generate Basic Synthetic Hourly Yields (168Hrs)
    periods_per_year = 168
    hours = np.arange(periods_per_year)
    
    # Synthetic Shapes
    res_daily_shape = 0.5 + 0.3 * np.sin(np.pi * (hours % 24 - 8) / 12) + 0.2 * np.sin(np.pi * (hours % 24 - 18) / 8)
    base_res_demand = np.maximum(0.2, res_daily_shape) * 300.0 # Per Node
    base_ind_demand = np.ones(periods_per_year) * 200.0
    
    solar_pu_base = np.clip(np.where((hours % 24 > 7) & (hours % 24 < 19), np.sin(np.pi * (hours % 24 - 7) / 12), 0), 0, 1)
    np.random.seed(42)
    wind_pu_base = np.clip(0.5 + 0.2 * np.sin(np.pi * hours / 24) + np.random.normal(0, 0.1, periods_per_year), 0, 1)

    # Apply capacity factor modifier
    solar_pu_base = np.clip(solar_pu_base * cf_mod, 0, 1)
    wind_pu_base = np.clip(wind_pu_base * cf_mod, 0, 1)

    # Multi-horizon Arrays
    res_demand_multi = []
    ind_demand_multi = []
    solar_pu_multi = []
    wind_pu_multi = []
    
    res_demand_scalars = {2030: 1.0, 2040: 1.5, 2050: 2.2}
    ind_demand_scalars = {2030: 1.0, 2040: 1.2, 2050: 1.8}
    
    for year in investment_periods:
        res_demand_multi.extend(base_res_demand * res_demand_scalars[year])
        ind_demand_multi.extend(base_ind_demand * ind_demand_scalars[year])
        solar_pu_multi.extend(solar_pu_base)
        wind_pu_multi.extend(wind_pu_base)

    # 4. Attach Baseline Loads to all existing buses
    for bus in n.buses.index:
        if bus.endswith("_AC"):
            n.add("Load", f"{bus}_Residential_Demand", bus=bus, carrier="AC", p_set=align_ts(res_demand_multi, n.snapshots))
            n.add("Load", f"{bus}_Industrial_Demand", bus=bus, carrier="AC", p_set=align_ts(ind_demand_multi, n.snapshots))
            
            # Allow GenCost solver to freely expand Solar, Wind, and Gas at every node per vintage
            penalty = penalties.get(bus, 0.0)
            for year in investment_periods:
                n.add("Generator", f"{bus}_Solar_{year}", bus=bus, carrier="solar", p_nom_extendable=True,
                      build_year=year, lifetime=ingestor.get_lifetime("Solar"), 
                      capital_cost=(ingestor.get_annualized_cost("Solar", year) * capex_mods.get("solar", 1.0)) + penalty,
                      marginal_cost=ingestor.get_marginal_cost("Solar") * opex_mods.get("solar", 1.0), p_max_pu=align_ts(solar_pu_multi, n.snapshots))
                      
                n.add("Generator", f"{bus}_Wind_{year}", bus=bus, carrier="wind", p_nom_extendable=True,
                      build_year=year, lifetime=ingestor.get_lifetime("Wind"), 
                      capital_cost=(ingestor.get_annualized_cost("Wind", year) * capex_mods.get("wind", 1.0)) + penalty,
                      marginal_cost=ingestor.get_marginal_cost("Wind") * opex_mods.get("wind", 1.0), p_max_pu=align_ts(wind_pu_multi, n.snapshots))
                      
                n.add("Generator", f"{bus}_Gas_Peaker_{year}", bus=bus, carrier="gas", p_nom_extendable=True,
                      build_year=year, lifetime=ingestor.get_lifetime("Gas_Peaker"), 
                      capital_cost=ingestor.get_annualized_cost("Gas_Peaker", year) * capex_mods.get("gas", 1.0),
                      marginal_cost=ingestor.get_marginal_cost("Gas_Peaker") * opex_mods.get("gas", 1.0))

    # 5. Inject Explicit Interconnectors
    # Base capital cost assumed for interconnector expansions per MW
    base_interconnector_cost_per_mw = 150000.0 * trans_mod
    for inter in INTERCONNECTORS:
        if inter["bus0"] in n.buses.index and inter["bus1"] in n.buses.index:
            n.add("Link", f"{inter['name']}_Link", bus0=inter["bus0"], bus1=inter["bus1"], carrier="AC",
                  p_nom=inter["capacity"], p_nom_extendable=True, capital_cost=base_interconnector_cost_per_mw,
                  p_min_pu=-1, p_max_pu=1, build_year=2024, lifetime=100)
    
    # 6. Seed Baseline Capacities with True Target Retirements
    for cap in BASELINE_GENERATORS:
        if cap["bus"] in n.buses.index:
            p_max_pu = align_ts(solar_pu_multi, n.snapshots) if cap["carrier"] == "solar" else (align_ts(wind_pu_multi, n.snapshots) if cap["carrier"] == "wind" else 1.0)
            n.add("Generator", f"{cap['bus']}_{cap['carrier']}_Baseline_{cap['build_year']}",
                  bus=cap["bus"], carrier=cap["carrier"], p_nom=cap["capacity"],
                  build_year=cap["build_year"], lifetime=cap["lifetime"],
                  p_max_pu=p_max_pu,
                  # Marginal cost roughly parameterized. Fixed CapEx since it's sunk.
                  marginal_cost=40.0 if "coal" in cap["carrier"] else (120.0 if cap["carrier"]=="gas" else 0.0),
                  capital_cost=0.0) 

    # 7. Strictly Enforce MultiIndex Naming (Fixes PyPSA dim_0 AlignmentError)
    for c in n.iterate_components(["Load", "Generator", "Link"]):
        for attr, df in c.pnl.items():
            if df is not None and not df.empty:
                df.index.name = "snapshot"
