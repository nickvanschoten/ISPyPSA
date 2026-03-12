import sys
import os
import gc
from pathlib import Path

# Fix python freeze on Windows caused by numexpr OpenMP deadlocks on import
os.environ["NUMEXPR_MAX_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

# Windows SMB Network Drive Access Violation bypass
import importlib.metadata
importlib.metadata.entry_points = lambda **kwargs: []

# Phase 1.7: Startup Sanity Check for Network Drives (SMB Latency fix)
def _check_local_drive():
    try:
        current_file = Path(__file__).resolve()
        drive = current_file.drive
        if drive:
            import ctypes
            drive_path = drive + "\\"
            # GetDriveTypeW returns 4 for DRIVE_REMOTE
            drive_type = ctypes.windll.kernel32.GetDriveTypeW(drive_path)
            if drive_type == 4:
                print(f"\n[WARNING] Project is running on a mapped network drive ({drive}).")
                print("[WARNING] This may cause 'Access Violation' errors and silent crashes due to SMB latency.")
                print("[WARNING] Please move the project to a local C:\\ directory for solver stability.\n")
        elif str(current_file).startswith(r"\\"):
            print("\n[WARNING] Project is running on a UNC network path.")
            print("[WARNING] This may cause 'Access Violation' errors and silent crashes. Please move to a local drive.\n")
    except Exception as e:
        # Fallback if ctypes or parsing fails
        pass

_check_local_drive()

from ispypsa.nextgen.config.manager import DeepMergeConfigManager
from ispypsa.nextgen.config.models import TestbedConfig
from ispypsa.nextgen.core.zonal_hub import MultiCarrierHubBuilder
from ispypsa.nextgen.core.transport_links import UniversalTransportLinkBuilder
from ispypsa.nextgen.core.network_builder import NextGenNetworkAssembler
from ispypsa.nextgen.core.toy_data import apply_synthetic_data
from ispypsa.nextgen.coupling.sector_components import ElectrolyserBuilder, HeatPumpBuilder, EVChargerBuilder
from ispypsa.nextgen.mga_ai.mga_constraints import PyPSAMGAConstraintGenerator
from ispypsa.nextgen.io.high_frequency_export import MGAExportManager
from ispypsa.nextgen.coupling.luto_bridge import OutboundCapacityEnvelopeManager

def build_network(config: TestbedConfig, enable_coupling: bool):
    """Builds a fresh network based on the config. Isolates state between passes."""
    print("  [DEBUG] Starting build_network...")
    node_names = [node.name for node in config.nodes]
    hub_builders = [MultiCarrierHubBuilder(carriers=["AC"])]
    # Phase 2: We no longer cast to dictionaries; pass polymorphic Pydantic models directly
    link_builders = [UniversalTransportLinkBuilder(connections=config.links)]
    
    print("  [DEBUG] Assembling Network Base...")
    assembler = NextGenNetworkAssembler(hub_builders, link_builders)
    network = assembler.assemble(node_names)
    
    if enable_coupling:
        print("  [DEBUG] Applying Sector Coupling...")
        # Phase 2: Class-based Sector Coupling
        couplers = [ElectrolyserBuilder(), HeatPumpBuilder(), EVChargerBuilder()]
        for node in config.nodes:
            for coupler in couplers:
                coupler.build_components(network, node.name)
            
    print("  [DEBUG] Applying Synthetic Data...")
    apply_synthetic_data(network, config)
    print("  [DEBUG] Network Build Complete.")
    return network


def get_solver_kwargs(config: TestbedConfig) -> tuple[str, dict]:
    """Extracts the appropriate kwargs based on the active solver."""
    s_conf = config.solver_options
    kwargs = {}
    if s_conf.threads is not None and s_conf.threads > 0:
        kwargs["threads"] = s_conf.threads
    
    if config.solver_name.lower() == "gurobi":
        kwargs.update(s_conf.gurobi_options)
    elif config.solver_name.lower() == "highs":
        kwargs.update(s_conf.highs_options)
        
    return config.solver_name.lower(), kwargs

def run_multi_pass(config_path: str):
    print("=== NextGen Phase 4.5: MGA & Visualization Export Runner ===")
    manager = DeepMergeConfigManager(Path(config_path))
    
    # The YAML file has a root structure and the testbed is nested under the "testbed" key.
    # It also has "solver_name" and "solver_options" at the root level.
    raw_config = manager.active_config
    
    # Construct a payload that matches the TestbedConfig shape
    testbed_payload = raw_config.get("testbed", {})
    testbed_payload["solver_name"] = raw_config.get("solver_name", "highs")
    testbed_payload["solver_options"] = raw_config.get("solver_options", {})
    
    config = TestbedConfig(**testbed_payload)
    out_dir = Path("results_export")
    export_manager = MGAExportManager(out_dir)
    
    solver_name, solver_options = get_solver_kwargs(config)
    
    def resilient_optimize(network, **kwargs):
        """Attempts to solve the network, falling back to HiGHS if Gurobi environment fails."""
        nonlocal solver_name, solver_options
        try:
            if not hasattr(network, "model"):
                network.optimize.create_model()
            network.optimize.solve_model(solver_name=solver_name, **kwargs)
        except (ImportError, ModuleNotFoundError, AssertionError) as e:
            if solver_name == "gurobi":
                print(f"\n[WARNING] Gurobi environment error ({e}). Falling back to HiGHS...")
                solver_name = "highs"
                _, solver_options = get_solver_kwargs(config) # In a real scenario we'd rebuild config. For now just reset.
                solver_options = config.solver_options.highs_options # strictly use highs options
                if config.solver_options.threads:
                    solver_options["threads"] = config.solver_options.threads
                network.optimize.solve_model(solver_name=solver_name, **kwargs)
            else:
                raise
        except Exception as e:
            # Check for specific gurobipy License errors if gurobipy is available
            error_str = str(e)
            if solver_name == "gurobi" and ("10009" in error_str or "License" in error_str or "gurobipy" in error_str.lower()):
                 print(f"\n[WARNING] Gurobi License/Initialization error detected. Falling back to HiGHS...")
                 solver_name = "highs"
                 solver_options = config.solver_options.highs_options
                 if config.solver_options.threads:
                    solver_options["threads"] = config.solver_options.threads
                 network.optimize.solve_model(solver_name=solver_name, **kwargs)
            else:
                # Let mathematical infeasibilities bubble up
                raise

    # PASS 1: AC-Only Baseline
    print(f"\n--- [PASS 1] Solving AC-Only Baseline (Target: {solver_name}) ---")
    n_base = build_network(config, enable_coupling=False)
    print(f"  [DEBUG] n_base built. Buses: {list(n_base.buses.index)}")
    print(f"  [DEBUG] n_base Links: {list(n_base.links.index)}")
    resilient_optimize(n_base, solver_options=solver_options)
    print(f"Base Objective: ${n_base.objective:,.2f}")
    export_manager.export_all(n_base, "AC_Baseline")
    
    # PASS 2: Least-Cost Coupled Scenario
    print(f"\n--- [PASS 2] Solving Least-Cost Coupled Baseline (Target: {solver_name}) ---")
    n_coupled = build_network(config, enable_coupling=True)
    print(f"  [DEBUG] n_coupled built. Buses: {list(n_coupled.buses.index)}")
    print(f"  [DEBUG] n_coupled Links: {list(n_coupled.links.index)}")
    # Important: We must use optimize.create_model() + solve_model() to retain the linopy model for MGA
    resilient_optimize(n_coupled, solver_options=solver_options)
    z_min = n_coupled.objective
    print(f"Coupled Objective (Z_min): €{z_min:,.2f}")
    export_manager.export_all(n_coupled, "Coupled_LeastCost")
    
    # PASS 3: MGA Alternative (Iterative Sweeps)
    print("\n--- [PASS 3] Solving MGA Alternatives ---")
    mga_generator = PyPSAMGAConstraintGenerator()
    
    # Apply dynamic slack constraint cost <= Z_min * (1 + epsilon)
    # Mutates n_coupled.model in-place
    mga_config = config.mga_options
    mga_generator.generate_slack_constraints(n_coupled, optimal_cost=z_min, slack_pct=mga_config.slack_epsilon)
    
    print(f"\n-> Sweeping MGA Target: {mga_config.target_action.upper()} {mga_config.target_component} (Carrier: {mga_config.target_carrier})")
    
    mga_generator.set_alternative_objective(
        network=n_coupled,
        target_component=mga_config.target_component,
        target_carrier=mga_config.target_carrier,
        target_action=mga_config.target_action
    )
    
    # Re-solve the preserved linopy model
    try:
        n_coupled.model.solve(solver_name=solver_name, **solver_options)
        import numpy as np
        capex = n_coupled.statistics.capex()
        opex = n_coupled.statistics.opex()
        alt_cost = float(np.nansum(capex.values) + np.nansum(opex.values))
        print(f"   MGA Actual System Cost: ${alt_cost:,.2f} ({(alt_cost/z_min)*100:.1f}% of Z_min)")
        export_manager.export_all(n_coupled, "Coupled_MGA_Alt")
    except Exception as solve_err:
         print(f"   [WARNING] MGA Iteration mathematically infeasible or failed: {solve_err}")
             
    finally:
        # Memory Management: Explicitly collect garbage after each heavy MGA iteration block
        gc.collect()

    print("\n  [EXPORT] Serializing Regional Capacity Envelopes for LUTO2...")
    luto_manager = OutboundCapacityEnvelopeManager()
    luto_manager.generate_regional_envelopes(n_coupled)
    
    print("\n=== All passes complete. Parquet files written to ./results_export ===")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="Path to config")
    args = parser.parse_args()
    run_multi_pass(args.config)
