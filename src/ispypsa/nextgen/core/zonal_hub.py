import pypsa
from ispypsa.nextgen.core.abc_builders import ZonalHubBuilder

class MultiCarrierHubBuilder(ZonalHubBuilder):
    """
    Concrete implementation of a Zonal Hub Builder.
    Creates buses for multiple energy carriers within a specified region.
    """
    
    def __init__(self, carriers: list[str] = None):
        """
        Initialize the hub builder.
        
        Args:
            carriers: A list of carrier names (e.g. ['AC', 'H2', 'Heat']). 
                      Defaults to ['AC'] if not provided.
        """
        self.carriers = carriers or ["AC"]
        
    def build_hub(self, network: pypsa.Network, region_name: str) -> None:
        """
        Constructs multi-carrier buses for a given zonal hub.
        """
        # Ensure the carriers exist in the network
        for carrier in self.carriers:
            if carrier not in network.carriers.index:
                network.add("Carrier", carrier)
                
            # Create a bus for this specific carrier in the region
            bus_name = f"{region_name}_{carrier}"
            if bus_name not in network.buses.index:
                network.add(
                    "Bus", 
                    bus_name, 
                    carrier=carrier,
                    location=region_name
                )
