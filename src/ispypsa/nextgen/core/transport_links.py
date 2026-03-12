import pypsa
from ispypsa.nextgen.core.abc_builders import SectorLinkBuilder
from ispypsa.nextgen.config.models import TransportLinkConfig, HydrogenPipelineConfig, HVACLineConfig
from ispypsa.nextgen.core.gencost_ingestor import GenCostIngestor

class UniversalTransportLinkBuilder(SectorLinkBuilder):
    """
    Constructs inter-hub transport links for various carriers (e.g., electricity transmission, H2 pipelines).
    Now consumes strictly typed Pydantic models.
    """
    
    def __init__(self, connections: list[TransportLinkConfig]):
        """
        Initialize the transport link builder with predefined inter-hub connections.
        
        Args:
            connections: list of TransportLinkConfig objects outlining the links.
        """
        self.connections = connections

    def build_links(self, network: pypsa.Network) -> None:
        """
        Constructs standard transport/transmission links between Zonal Hubs based on mapped Pydantic configurations.
        """
        ingestor = GenCostIngestor(wacc=0.07)
        periods = network.investment_periods if hasattr(network, "investment_periods") and len(network.investment_periods) > 0 else [2030]
        
        for i, config in enumerate(self.connections):
            carrier = config.carrier
            base_bus0 = config.bus0
            base_bus1 = config.bus1
            
            # Format buses to follow the ZonalHub naming convention
            bus0 = base_bus0 if base_bus0.endswith(f"_{carrier}") else f"{base_bus0}_{carrier}"
            bus1 = base_bus1 if base_bus1.endswith(f"_{carrier}") else f"{base_bus1}_{carrier}"
            
            # Ensure carriers exist
            if carrier not in network.carriers.index:
                network.add("Carrier", carrier)
                
            for year in periods:
                link_name = f"{config.name or f'Link_{bus0}_to_{bus1}_{i}'}_{year}"
                
                if link_name not in network.links.index:
                    is_hvac = isinstance(config, HVACLineConfig)
                    
                    # Estimate cost from length if missing
                    cap_cost = config.capital_cost
                    if cap_cost is None or cap_cost == 0:
                        base_cost = ingestor.get_annualized_cost("HVAC_Line", year)
                        # Assume base cost is per km, and config has length
                        scale_factor = getattr(config, 'length', 1.0) if is_hvac else 1.0
                        cap_cost = base_cost * scale_factor

                    kwargs = {
                        "bus0": bus0,
                        "bus1": bus1,
                        "carrier": carrier,
                        "p_nom_extendable": config.p_nom_extendable,
                        "efficiency": config.efficiency,
                        "capital_cost": cap_cost,
                        "build_year": year,
                        "lifetime": ingestor.get_lifetime("HVAC_Line")
                    }
                    
                    # Dynamic unwrapping based on formal config type
                    if isinstance(config, HydrogenPipelineConfig): # Phase 2 - Physics enforced ABC coupling
                       bus2 = config.electrical_bus
                       if not bus2.endswith("_AC"):
                           bus2 = f"{bus2}_AC"
                       kwargs["bus2"] = bus2
                       kwargs["efficiency2"] = config.efficiency2
                       
                    elif isinstance(config, HVACLineConfig):
                       kwargs["length"] = config.length

                    # Phase 2 bugfix: PyPSA Xarray grouping fails if a referenced bus isn't strictly in network.buses
                    # We dynamically ensure all referenced buses exist (bus0, bus1, and sometimes bus2)
                    for b_key in ["bus0", "bus1", "bus2"]:
                        if b_key in kwargs:
                            b_val = kwargs[b_key]
                            if b_val not in network.buses.index:
                                b_carrier = b_val.split("_")[-1] # Infer carrier from suffix
                                if b_carrier not in network.carriers.index:
                                    network.add("Carrier", b_carrier)
                                network.add("Bus", b_val, carrier=b_carrier)

                    network.add("Link", link_name, **kwargs)

        # PyPSA xarray optimization engine crashes with `KeyError: bus` if ANY 
        # `bus2` is `pandas.NA` or `numpy.nan`. We must normalize missing multiports to `""`.
        if not network.links.empty:
            if "bus2" in network.links.columns:
                network.links["bus2"] = network.links["bus2"].fillna("")
            if "bus3" in network.links.columns:
                network.links["bus3"] = network.links["bus3"].fillna("")
                
            # PyPSA 0.23 / Linopy also crashes with `ValueError: ndim=0` if coefficients
            # are multiplied against NaNs before filtering. Ensure numeric completion.
            for eff_col in ["efficiency", "efficiency2", "efficiency3", "efficiency4"]:
                if eff_col in network.links.columns:
                    network.links[eff_col] = network.links[eff_col].fillna(1.0)
