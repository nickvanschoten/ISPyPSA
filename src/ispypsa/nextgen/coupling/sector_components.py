from abc import ABC, abstractmethod
import pypsa
from ispypsa.nextgen.core.gencost_ingestor import GenCostIngestor

class SectorCouplerBuilder(ABC):
    """Abstract Base Class for generating multi-vector conversion technologies."""
    
    @abstractmethod
    def build_components(self, network: pypsa.Network, region: str) -> None:
        """Instantiate the physical links, stores, and parasitic loads for the technology."""
        pass
        
    def _ensure_buses(self, network: pypsa.Network, region: str, required_carriers: list[str]) -> None:
        """Helper to ensure necessary carriers and buses exist."""
        for carrier in required_carriers:
            if carrier not in network.carriers.index:
                network.add("Carrier", carrier)
            
            bus_name = f"{region}_{carrier}"
            if bus_name not in network.buses.index:
                network.add("Bus", bus_name, carrier=carrier, location=region)

class ElectrolyserBuilder(SectorCouplerBuilder):
    def build_components(self, network: pypsa.Network, region: str) -> None:
        self._ensure_buses(network, region, ["AC", "H2"])
        ingestor = GenCostIngestor(wacc=0.07)
        
        for year in network.investment_periods:
            link_name = f"{region}_Electrolyser_{year}"
            if link_name not in network.links.index:
                 network.add(
                    "Link",
                    link_name,
                    bus0=f"{region}_AC",
                    bus1=f"{region}_H2",
                    efficiency=0.68,
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=ingestor.get_lifetime("Electrolyser"),
                    capital_cost=ingestor.get_annualized_cost("Electrolyser", year),
                    marginal_cost=ingestor.get_marginal_cost("Electrolyser")
                )

class HeatPumpBuilder(SectorCouplerBuilder):
    def build_components(self, network: pypsa.Network, region: str) -> None:
        self._ensure_buses(network, region, ["AC", "Heat"])
        for year in network.investment_periods:
            link_name = f"{region}_HeatPump_{year}"
            if link_name not in network.links.index:
                 network.add(
                    "Link",
                    link_name,
                    bus0=f"{region}_AC",
                    bus1=f"{region}_Heat",
                    efficiency=3.0, # COP
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=20,
                    capital_cost=0.0
                )

class EVChargerBuilder(SectorCouplerBuilder):
    def build_components(self, network: pypsa.Network, region: str) -> None:
        self._ensure_buses(network, region, ["AC", "EV"])
        
        for year in network.investment_periods:
            link_name = f"{region}_EV_Charger_{year}"
            if link_name not in network.links.index:
                network.add(
                    "Link",
                    link_name,
                    bus0=f"{region}_AC",
                    bus1=f"{region}_EV",
                    efficiency=0.9,
                    p_nom_extendable=True,
                    build_year=year,
                    lifetime=20,
                    capital_cost=0.0
                )
            
            store_name = f"{region}_EV_Battery_{year}"
            if store_name not in network.stores.index:
                network.add(
                    "Store",
                    store_name,
                    bus=f"{region}_EV",
                    e_cyclic=True,
                    e_nom_extendable=True,
                    build_year=year,
                    lifetime=15,
                    capital_cost=0.0
                )
