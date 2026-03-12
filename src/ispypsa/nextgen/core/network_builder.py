import pypsa
from typing import List

from ispypsa.nextgen.core.abc_builders import ZonalHubBuilder, SectorLinkBuilder

class NextGenNetworkAssembler:
    """
    Orchestrates the creation and assembly of PyPSA networks using 
    ZonalHubBuilders and SectorLinkBuilders.
    """
    
    def __init__(self, 
                 hub_builders: List[ZonalHubBuilder], 
                 link_builders: List[SectorLinkBuilder]):
        self.hub_builders = hub_builders
        self.link_builders = link_builders
        
    def assemble(self, regions: List[str]) -> pypsa.Network:
        """
        Assembles a base PyPSA Network.
        
        Args:
            regions: A list of region names to build hubs for.
            
        Returns:
            An instantiated and assembled pypsa.Network.
        """
        n = pypsa.Network()
        
        # 1. Apply multi-horizon structure (Base periods, MultiIndex, Discount Rates)
        from ispypsa.nextgen.core.toy_data import initialize_multi_horizon
        initialize_multi_horizon(n)
        
        # 1. Build all hubs across requested regions
        for region in regions:
            for hub_builder in self.hub_builders:
                hub_builder.build_hub(n, region)
                
        # 2. Add inter-regional links
        for link_builder in self.link_builders:
            link_builder.build_links(n)
            
        return n
