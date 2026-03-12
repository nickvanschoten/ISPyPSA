import os
import gc
import pypsa
from pathlib import Path
import pandas as pd
import numpy as np
from ispypsa.nextgen.config.manager import DeepMergeConfigManager

from ispypsa.nextgen.config.models import TestbedConfig
from ispypsa.nextgen.core.network_builder import NextGenNetworkAssembler
from ispypsa.nextgen.core.abc_builders import ZonalHubBuilder
from ispypsa.nextgen.coupling.sector_components import ElectrolyserBuilder, HeatPumpBuilder, EVChargerBuilder
from ispypsa.nextgen.core.transport_links import UniversalTransportLinkBuilder
from ispypsa.nextgen.core.toy_data import apply_synthetic_data
from ispypsa.nextgen.io.high_frequency_export import MGAExportManager
from ispypsa.nextgen.coupling.iam_exchange import OutboundSignalGenerator, InboundDemandMapper, MockElasticityModel

def get_solver_kwargs(config: TestbedConfig) -> tuple[str, dict]:
    solver_name = config.solver_name
    if solver_name == "gurobi":
        options = config.solver_options.gurobi_options
    else:
        options = config.solver_options.highs_options
    if config.solver_options.threads:
        options["threads"] = config.solver_options.threads
    return solver_name, options

def resilient_optimize(network: pypsa.Network, config: TestbedConfig):
    solver_name, solver_options = get_solver_kwargs(config)
    
    if hasattr(network, "model"):
        # If it already has a model (e.g. from a prior step), we might need to recreate it if the network changed
        # but in Phase 7 we are modifying loads so we should recreate the model
        del network.model
        
    try:
        network.optimize.create_model()
        network.optimize.solve_model(solver_name=solver_name, **solver_options)
    except (ImportError, ModuleNotFoundError, AssertionError) as e:
        if solver_name == "gurobi":
            print(f"\n[WARNING] Gurobi error ({e}). Falling back to HiGHS...")
            solver_name = "highs"
            solver_options = config.solver_options.highs_options
            if config.solver_options.threads:
                solver_options["threads"] = config.solver_options.threads
            network.optimize.solve_model(solver_name=solver_name, **solver_options)
        else:
            raise
    except Exception as e:
        error_str = str(e)
        if solver_name == "gurobi" and ("10009" in error_str or "License" in error_str or "gurobipy" in error_str.lower()):
            print(f"\n[WARNING] Gurobi License error detected. Falling back to HiGHS...")
            solver_name = "highs"
            solver_options = config.solver_options.highs_options
            if config.solver_options.threads:
                solver_options["threads"] = config.solver_options.threads
            network.optimize.solve_model(solver_name=solver_name, **solver_options)
        else:
            raise

from ispypsa.nextgen.core.zonal_hub import MultiCarrierHubBuilder

def build_initial_network(config: TestbedConfig) -> pypsa.Network:
    n = pypsa.Network()
    from ispypsa.nextgen.core.nem_topology import initialize_multi_horizon, apply_nem_topology
    
    initialize_multi_horizon(n)
    apply_nem_topology(n, config)
    
    return n

def calculate_system_twh(network: pypsa.Network) -> float:
    # return total demand in TWh for convergence check reporting
    return network.loads_t.p_set.sum().sum() / 1e6

def run_soft_linking_loop(config_path: str):
    manager = DeepMergeConfigManager(Path(config_path))
    raw_config = manager.active_config
    
    testbed_payload = raw_config.get("testbed", {})
    testbed_payload["solver_name"] = raw_config.get("solver_name", "highs")
    testbed_payload["solver_options"] = raw_config.get("solver_options", {})
    
    config = TestbedConfig(**testbed_payload)
    
    n = build_initial_network(config)
    
    io_dir = Path("iam_io")
    io_dir.mkdir(exist_ok=True)
    
    outbound_gen = OutboundSignalGenerator(output_dir=io_dir)
    inbound_mapper = InboundDemandMapper(input_dir=io_dir)
    mock_iam = MockElasticityModel(ped=-0.2, target_sector="Industrial", io_dir=io_dir)
    export_manager = MGAExportManager(output_dir="results_export/phase7")
    
    solver_options = {"solver_name": "gurobi"}
    
    max_iterations = 10
    convergence_epsilon = 0.01 # 1%
    # Extract IAM damping parameter
    alpha_damping = getattr(config, "sensitivities", None).iam_alpha_damping if getattr(config, "sensitivities", None) else 0.5
    
    print("\n=== Commencing Phase 7 Soft-Linking Convergence Loop ===")
    
    prev_prices = None
    prev_demand = None
    
    for i in range(1, max_iterations + 1):
        print(f"\n--- Iteration {i} ---")
        
        # 1. PyPSA Solve
        print("  [PyPSA] Solving multi-horizon capacity expansion...")
        resilient_optimize(n, config)
        
        capex = n.statistics.capex()
        opex = n.statistics.opex()
        system_cost = float(np.nansum(capex.values) + np.nansum(opex.values))
        print(f"  [PyPSA] Optimal Objective (NPV): ${system_cost:,.2f}")
        
        # 2. Extract Prices
        df_prices = outbound_gen.generate_weighted_smp(n)
        current_prices = df_prices["weighted_price_aud"].values
        current_demand = calculate_system_twh(n)
        
        avg_price = np.mean(current_prices)
        print(f"  [PyPSA] Outbound Avg SMP: ${avg_price:.2f}/MWh | Total Demand: {current_demand:.2f} TWh")
        
        # Check Convergence
        if prev_prices is not None and prev_demand is not None:
            max_price_delta = np.max(np.abs(current_prices - prev_prices) / np.where(prev_prices > 0, prev_prices, 1))
            demand_delta = abs(current_demand - prev_demand) / prev_demand
            
            print(f"  [STATUS] Max Delta P: {max_price_delta*100:.2f}% | Overall Delta D: {demand_delta*100:.2f}%")
            
            if max_price_delta < convergence_epsilon and demand_delta < convergence_epsilon:
                print(f"  [SUCCESS] Market Equilibrium Reached at Iteration {i}!")
                export_manager.export_all(n, "Soft_Linked_Equilibrium")
                break
        
        if i == max_iterations:
             print(f"  [WARNING] Loop hit max iterations ({max_iterations}) without converging.")
             export_manager.export_all(n, "Soft_Linked_Timeout")
             break
             
        prev_prices = current_prices
        prev_demand = current_demand
        
        # 3. Macroeconomic Response
        print("  [IAM] Simulating External CGE Model Response...")
        mock_iam.run_macro_step(n)
        
        # 4. Inbound Mapping with Damping
        print(f"  [IAM] Downscaling PyPSA Load Matrix (alpha={alpha_damping})...")
        inbound_mapper.apply_sectoral_downscaling(n, alpha=alpha_damping)
        
        gc.collect()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 7 Macroeconomic Soft-Linking Loop")
    parser.add_argument("--config", type=str, default="ispypsa_config.yaml", help="Path to ispypsa_config.yaml")
    args = parser.parse_args()
    run_soft_linking_loop(args.config)
