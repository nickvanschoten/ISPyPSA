import pypsa
from ispypsa.nextgen.core.nem_topology import initialize_multi_horizon, apply_nem_topology
from ispypsa.nextgen.config.models import TestbedConfig, NodeConfig

def test_nem_build():
    print("Initializing PyPSA Network...")
    n = pypsa.Network()
    
    # Needs a config payload to satisfy apply_nem_topology
    mock_config = TestbedConfig(
        scenario_name="NEM_Scale_Up_Test",
        nodes=[NodeConfig(name="NSW", type="Urban", spatial_penalty_cost=30000.0)],
        links=[]
    )
    
    print("Setting Multi-Horizon parameters...")
    initialize_multi_horizon(n)
    
    print("Applying NEM Topology...")
    apply_nem_topology(n, mock_config)
    
    print("\n--- NEM TOPOLOGY VERIFICATION ---")
    print(f"Buses: {len(n.buses)}")
    print(f"Links (Interconnectors): {len(n.links)}")
    print(f"Generators (Baseline & Vintages): {len(n.generators)}")
    
    print("\nSub-regions Configured:")
    print(n.buses.index.tolist()[:10])
    
    print("\nInterconnectors Activated:")
    print(n.links.index.tolist())
    
    print("\nSample Baseline Capacities:")
    baseline = n.generators[~n.generators.index.str.contains("Solar|Wind|Gas_Peaker", regex=True)]
    print(baseline[["bus", "carrier", "p_nom", "build_year", "lifetime"]])

if __name__ == "__main__":
    test_nem_build()
