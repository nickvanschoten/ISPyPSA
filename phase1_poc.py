import pypsa
from pathlib import Path
from pydantic import BaseModel

from ispypsa.nextgen.config.manager import DeepMergeConfigManager
from ispypsa.nextgen.core.abc_builders import ZonalHubBuilder

# 1. Define Pydantic Schema for the test config
class NextGenConfig(BaseModel):
    version: str
    scenario: str
    mga_slack_pct: float
    regions: list[str]

# 2. Implement a concrete Zonal Hub Builder
class BasicMultiCarrierHubBuilder(ZonalHubBuilder):
    def build_hub(self, network: pypsa.Network, region_name: str) -> None:
        """
        Creates an AC bus and a colocated H2 bus for the region.
        """
        print(f"Building NextGen Zonal Hub: {region_name}")
        network.add("Bus", f"{region_name}_AC", carrier="AC")
        network.add("Bus", f"{region_name}_H2", carrier="H2")
        
        # Example Link (Electrolyser)
        network.add(
            "Link", 
            f"{region_name}_Electrolyser",
            bus0=f"{region_name}_AC",
            bus1=f"{region_name}_H2",
            efficiency=0.65
        )

def main():
    print("--- NextGen Energy System Model MVP: Phase 1 PoC ---")
    
    # 3. Test Config Manager
    # Create dummy config files
    default_yaml = Path("default_config.yaml")
    override_yaml = Path("user_config.yaml")
    
    default_yaml.write_text("version: '1.0'\nscenario: 'ISP Default'\nmga_slack_pct: 0.0\nregions: ['NSW', 'VIC']")
    override_yaml.write_text("scenario: 'High Hydrogen'\nmga_slack_pct: 0.05")
    
    config_mgr = DeepMergeConfigManager(default_yaml)
    config_mgr.apply_override(override_yaml)
    
    # Validate with Pydantic
    config = config_mgr.get_validated_config(NextGenConfig)
    print(f"\n[Config] Active Scenario: {config.scenario}")
    print(f"[Config] MGA Slack Set to: {config.mga_slack_pct * 100}%")
    
    # 4. Test Core Builders
    print("\n[Network] Initializing Network Components...")
    n = pypsa.Network()
    
    builder = BasicMultiCarrierHubBuilder()
    for region in config.regions:
        builder.build_hub(n, region)
        
    print("\n[Network] Summary:")
    print(f"Buses: {n.buses.index.tolist()}")
    print(f"Links: {n.links.index.tolist()}")
    
    # Cleanup
    default_yaml.unlink()
    override_yaml.unlink()

if __name__ == "__main__":
    main()
