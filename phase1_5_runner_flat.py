import sys
import os
from pathlib import Path

# Fix python freeze on Windows caused by numexpr OpenMP deadlocks on import
os.environ["NUMEXPR_MAX_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

# Pydantic v2 triggers Python 3.12 _path_stat Access Violation when scanning plugins on heavily populated Windows network drives.
import importlib.metadata
importlib.metadata.entry_points = lambda **kwargs: []

import pydantic
class _PluginBypass(pydantic.BaseModel): pass

print("DEBUG: Script started", flush=True)

# Removed sys.path injection to avoid Windows _path_stat access violation on network drives
print("DEBUG: Importing DeepMergeConfigManager...", flush=True)
from ispypsa.nextgen.config.manager import DeepMergeConfigManager
print("DEBUG: Importing models...", flush=True)
from ispypsa.nextgen.config.models import TestbedConfig
print("DEBUG: Importing builders...", flush=True)
from ispypsa.nextgen.core.zonal_hub import MultiCarrierHubBuilder
from ispypsa.nextgen.core.transport_links import UniversalTransportLinkBuilder
from ispypsa.nextgen.core.network_builder import NextGenNetworkAssembler
from ispypsa.nextgen.core.toy_data import apply_synthetic_data
print("DEBUG: Imports complete", flush=True)

from ispypsa.nextgen.config.manager import DeepMergeConfigManager
from ispypsa.nextgen.config.models import TestbedConfig
from ispypsa.nextgen.core.zonal_hub import MultiCarrierHubBuilder
from ispypsa.nextgen.core.transport_links import UniversalTransportLinkBuilder
from ispypsa.nextgen.core.network_builder import NextGenNetworkAssembler
from ispypsa.nextgen.core.toy_data import apply_synthetic_data

def add_hybrid_sector_components_if_enabled(network, config: TestbedConfig):
    if not config.enable_sector_coupling:
        return
        
    from ispypsa.nextgen.coupling.sector_components import add_hybrid_sector_components
    for node in config.nodes:
        add_hybrid_sector_components(network, node.name)

def run_pipeline(config_path: str):
    print("=== NextGen Energy System Model Phase 1.5 Runner ===")
    
    # 1. Configuration parsing
    print(f"Loading Configuration from: {config_path}")
    manager = DeepMergeConfigManager(Path(config_path))
    config = manager.get_validated_config(TestbedConfig)
    print(f"Scenario: {config.scenario_name}")
    print(f"Sector Coupling Enabled: {config.enable_sector_coupling}")
    
    # 2. Network Assembly
    node_names = [node.name for node in config.nodes]
    print(f"Building Network for Nodes: {node_names}")
    
    # Basic Hub config (only AC initially, coupling adds H2/Heat later if toggled)
    hub_builders = [MultiCarrierHubBuilder(carriers=["AC"])]
    
    # Reformat links to dictionaries
    links_data = [link.dict() for link in config.links]
    link_builders = [UniversalTransportLinkBuilder(connections=links_data)]
    
    assembler = NextGenNetworkAssembler(hub_builders, link_builders)
    network = assembler.assemble(node_names)
    
    # 3. Apply Sector Coupling
    add_hybrid_sector_components_if_enabled(network, config)
    
    # 4. Inject 168H Synthetic Profile Data Strict DatetimeIndex
    print("Applying 168-Hour Synthetic Load & Renewable Profiles...")
    apply_synthetic_data(network)
    
    # Print pre-optimization summary
    print(f"\nNetwork Snapshot: {len(network.snapshots)} hours, {len(network.buses)} Buses, {len(network.links)} Links, {len(network.generators)} Generators")
    
    # 5. Optimization & Execution with Gurobi
    print("\nStarting Gurobi Optimization Backend...")
    try:
        # Favor numeric stability with Method=2 (Barrier) and Crossover=0
        solver_options = {'Method': 2, 'Crossover': 0}
        
        status, condition = network.optimize(
            solver_name='gurobi',
            solver_options=solver_options
        )
        
        if status != "ok":
            print(f"WARNING: Solver returned status {status} - Condition {condition}")
        else:
            print(f"SUCCESS: System solved optimally. Objective Value: €{network.objective:,.2f}")
            
    except Exception as e:
        print(f"\n[FATAL ERROR] Optimization failed: {str(e)}")
        
        # IIS Computation hook
        if hasattr(network, "model"):
            print("Attempting to compute Irreducibly Inconsistent Subsystem (IIS) via Gurobi...")
            try:
                # To do this safely through linopy, we use its to_gurobipy() functionality.
                print("Writing IIS to 'infeasible.ilp'...")
                m = network.model.to_gurobipy()
                m.computeIIS()
                m.write("infeasible.ilp")
                print("IIS exported successfully. Review infeasible.ilp to identify breaking constraints.")
            except Exception as iis_e:
                print(f"Failed to compute IIS: {str(iis_e)}")
        sys.exit(1)
        
    print("\n--- Optimization Results ---")
    
    # 6. Results: Built Capacities
    print("\n[Built Capacities - Generator (MW)]")
    if not network.generators.empty:
        print(network.generators[['p_nom_opt', 'p_nom_extendable', 'carrier']].to_string())
    
    print("\n[Built Capacities - Links (MW)]")
    if not network.links.empty:
        print(network.links[['p_nom_opt', 'p_nom_extendable', 'carrier', 'efficiency']].to_string())
        
    print("\n[Built Capacities - Stores (MWh)]")
    if not network.stores.empty:
        print(network.stores[['e_nom_opt', 'e_nom_extendable']].to_string())

    # 7. Results: Market Shadow Prices (Dual Values)
    print("\n[Average Marginal Prices per Bus (EUR/MWh)]")
    if not network.buses_t.marginal_price.empty:
        # Show average over the 168h profile
        avg_prices = network.buses_t.marginal_price.mean().round(2)
        print(avg_prices.to_string())

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NextGen MVP Phase 1.5 Runner")
    parser.add_argument("--config", type=str, required=True, help="Path to YAML testbed config")
    args = parser.parse_args()
    
    run_pipeline(args.config)
