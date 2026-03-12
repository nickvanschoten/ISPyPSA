import pypsa
import pandas as pd
import json
import sys
import argparse
import logging
from pathlib import Path

# Assuming you have an export_manager and demand_scaler
# We will create mock / rudimentary versions if they don't exist yet, 
# or import them if you have them.
try:
    from ispypsa.nextgen.core.demand_scaler import apply_macroeconomic_scaling
except ImportError:
    logging.warning("Applying dummy demand scaler. ispypsa.nextgen.core.demand_scaler not found.")
    def apply_macroeconomic_scaling(n, p): return n

from ispypsa.nextgen.runners.phase4_5_runner import build_network
from ispypsa.nextgen.config.manager import DeepMergeConfigManager
from ispypsa.nextgen.config.models import TestbedConfig
from ispypsa.nextgen.io.high_frequency_export import MGAExportManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def apply_capex_shocks(network: pypsa.Network, payload: dict):
    """
    Apply Multipliers to capital_cost for wind, solar, and battery.
    """
    logger.info("Applying CAPEX shocks...")
    
    wind_mult = payload.get("wind_capex", 1.0)
    solar_mult = payload.get("solar_capex", 1.0)
    battery_mult = payload.get("battery_capex", 1.0)
    
    if not network.generators.empty:
        # Assuming typical carrier names
        wind_mask = network.generators.carrier.str.contains("wind", case=False, na=False)
        solar_mask = network.generators.carrier.str.contains("solar", case=False, na=False)
        
        if wind_mask.any():
            network.generators.loc[wind_mask, "capital_cost"] *= wind_mult
        if solar_mask.any():
            network.generators.loc[solar_mask, "capital_cost"] *= solar_mult
            
    if not network.stores.empty:
        battery_mask = network.stores.carrier.str.contains("battery", case=False, na=False)
        if battery_mask.any():
            network.stores.loc[battery_mask, "capital_cost"] *= battery_mult
            
    return network

def apply_fuel_and_carbon_pricing(network: pypsa.Network, payload: dict):
    """
    Update marginal_cost for thermal generators:
    (Fuel_Price / Efficiency) + (Carbon_Intensity * Carbon_Price) + VOM
    Note: VOM is kept simple or assumed 0 here for the MVP if not explicitly modeled.
    """
    logger.info("Applying Fuel and Carbon Pricing...")
    
    gas_price = payload.get("gas_price", 10.0)
    black_coal_price = payload.get("black_coal_price", 8.0)
    brown_coal_price = payload.get("brown_coal_price", 5.0)
    carbon_price = payload.get("carbon_price", 0.0)
    
    if network.generators.empty:
        return network
        
    for idx, row in network.generators.iterrows():
        carrier = str(row.get("carrier", "")).lower()
        efficiency = row.get("efficiency", 1.0)
        # Avoid division by zero
        if efficiency <= 0:
            efficiency = 1.0
            
        carbon_intensity = row.get("carrier_co2_emissions", 0.0) # Assume tCO2e / MWh thermal
        # if PyPSA standard carrier attribute is used: network.carriers.loc[carrier, "co2_emissions"]
        
        fuel_price = None
        if "gas" in carrier or "ocgt" in carrier or "ccgt" in carrier:
            fuel_price = gas_price
        elif "black coal" in carrier or "black_coal" in carrier:
            fuel_price = black_coal_price
        elif "brown coal" in carrier or "brown_coal" in carrier:
            fuel_price = brown_coal_price
            
        if fuel_price is not None:
            new_marginal_cost = (fuel_price / efficiency) + (carbon_intensity * carbon_price)
            network.generators.at[idx, "marginal_cost"] = new_marginal_cost
            
    return network

def export_results(network: pypsa.Network, scenario_name: str, export_dir: str = "results_export"):
    """
    Export capacities and dispatch to parquet files using MGAExportManager.
    """
    logger.info(f"Exporting results for scenario: {scenario_name}")
    out_dir = Path(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    exporter = MGAExportManager(str(out_dir))
    
    # Pass scenario name to customize export (or assume MGAExportManager exports to dynamic names based on it)
    # The MGA export manager does standard exports, we'll manually dump these two for safety
    try:
        exporter.export_all(network, scenario_name)
    except Exception as e:
        logger.warning(f"MGAExportManager failed or lack scenario_name support, falling back: {e}")
        # Base fallback
        # Export Capacities
        caps = []
        if not network.generators.empty:
            g = network.generators[['bus', 'carrier', 'p_nom_opt', 'p_nom']].copy()
            g['component_type'] = 'Generator'
            caps.append(g)
        if not network.stores.empty:
            s = network.stores[['bus', 'carrier', 'e_nom_opt', 'e_nom']].copy()
            s = s.rename(columns={'e_nom_opt': 'p_nom_opt', 'e_nom': 'p_nom'})
            s['component_type'] = 'Store'
            caps.append(s)
            
        if caps:
            df_caps = pd.concat(caps)
            if 'x' not in df_caps.columns: df_caps['x'] = 145.0
            if 'y' not in df_caps.columns: df_caps['y'] = -35.0
            df_caps.to_parquet(out_dir / f"capacities_{scenario_name}.parquet")
            
        # Export Dispatch
        if hasattr(network, 'generators_t') and not network.generators_t.p.empty:
            df_disp = network.generators_t.p.melt(ignore_index=False, var_name='component', value_name='active_power')
            df_disp.index.name = 'timestamp'
            df_disp = df_disp.reset_index()
            df_disp['component_type'] = 'Generator'
            df_disp.to_parquet(out_dir / f"dispatch_{scenario_name}.parquet")

def main():
    parser = argparse.ArgumentParser(description="PyPSA Orchestrator Bridge")
    parser.add_argument("payload_path", type=str, help="Path to the JSON scenario payload")
    args = parser.parse_args()

    payload_path = Path(args.payload_path)
    if not payload_path.exists():
        logger.error(f"Payload file not found: {payload_path}")
        sys.exit(1)

    with open(payload_path, "r") as f:
        payload = json.load(f)

    scenario_name = payload.get("scenario_name", "MVP_Run")
    run_mga = payload.get("mga_toggle", False)

    logger.info(f"--- Starting Orchestrator for Scenario: {scenario_name} ---")

    # 1. Load Real Base Network
    try:
        config_mgr = DeepMergeConfigManager("ispypsa_config.yaml")
        raw_config = config_mgr.active_config
        testbed_payload = raw_config.get("testbed", {})
        config = TestbedConfig(**testbed_payload)
        network = build_network(config, enable_coupling=True)
        logger.info("Successfully built base PyPSA real network.")
    except Exception as e:
        logger.error(f"Failed to build real network: {e}")
        sys.exit(1)

    # 2. Demand Scaling
    network = apply_macroeconomic_scaling(network, payload)
    
    # 3. Apply CAPEX Shocks
    network = apply_capex_shocks(network, payload)
    
    # 4. Apply Fuel & Carbon Pricing
    network = apply_fuel_and_carbon_pricing(network, payload)

    # 5. Execute Solver
    logger.info("Executing Gurobi solver...")
    try:
        network.optimize(solver_name='gurobi')
    except Exception as e:
        logger.error(f"Solver failed: {e}")
        sys.exit(1)
    
    if run_mga:
        logger.info("MGA Toggle is ON: Iterative spatial sweeps logic goes here...")
        # mga_runner(network)
    else:
        logger.info("MGA Toggle is OFF: Standard optimization complete.")

    # 6. Export Data
    export_results(network, scenario_name)
    
    logger.info("--- Orchestrator process completed successfully ---")

if __name__ == "__main__":
    main()
