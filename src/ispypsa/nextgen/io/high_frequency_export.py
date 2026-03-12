import pandas as pd
from pathlib import Path
import pypsa

class MGAExportManager:
    """
    High-frequency data extraction layer for PyPSA optimization results.
    Serializes network state, economics, and dispatch profiles to columnar Parquet
    for downstream ingestion by IAMs (LUTO2, AusTIMES).
    """
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _extract_p_nom_opt(self, df_comp: pd.DataFrame, is_store: bool = False) -> pd.Series:
        nom_opt_col = "e_nom_opt" if is_store else "p_nom_opt"
        nom_col = "e_nom" if is_store else "p_nom"
        
        if nom_opt_col in df_comp.columns:
            return df_comp[nom_opt_col].fillna(df_comp.get(nom_col, 0.0))
        return df_comp.get(nom_col, 0.0)

    def export_spatial_capacities(self, network: pypsa.Network, scenario_id: str):
        """
        Extracts optimized capacity builds and merges with spatial bus metadata.
        Target: Land-use allocation models (LUTO2).
        """
        capacities = []
        
        # Generators
        if not network.generators.empty:
            df = network.generators[["bus", "carrier"]].copy()
            df["p_nom_opt"] = self._extract_p_nom_opt(network.generators)
            df["component_type"] = "Generator"
            capacities.append(df)
            
        # Storage Units
        if not network.storage_units.empty:
            df = network.storage_units[["bus", "carrier"]].copy()
            df["p_nom_opt"] = self._extract_p_nom_opt(network.storage_units)
            df["component_type"] = "StorageUnit"
            capacities.append(df)
            
        # Stores
        if not network.stores.empty:
            df = network.stores[["bus", "carrier"]].copy()
            df["p_nom_opt"] = self._extract_p_nom_opt(network.stores, is_store=True)
            df["component_type"] = "Store"
            capacities.append(df)
            
        # Links
        if not network.links.empty:
            # Uses bus0 as primary location footprint
            df = network.links[["bus0", "bus1", "carrier"]].copy()
            df = df.rename(columns={"bus0": "bus"}) 
            df["p_nom_opt"] = self._extract_p_nom_opt(network.links)
            df["component_type"] = "Link"
            capacities.append(df)
            
        if capacities:
            df_cap = pd.concat(capacities)
            df_cap = df_cap.reset_index().rename(columns={"index": "component_id"})
            
            # Map spatial attributes from buses if available (e.g., x, y location)
            if "x" in network.buses.columns and "y" in network.buses.columns:
                bus_locs = network.buses[["x", "y"]]
                df_cap = df_cap.merge(bus_locs, left_on="bus", right_index=True, how="left")
                
            df_cap["scenario_id"] = scenario_id
            df_cap.to_parquet(self.output_dir / f"spatial_capacities_{scenario_id}.parquet", engine="pyarrow")
            
    def export_system_economics(self, network: pypsa.Network, scenario_id: str):
        """
        Extracts and separates capital costs, fixed/marginal O&M, and nodal marginal prices.
        Target: Macroeconomic pathways (AusTIMES).
        """
        economics = []
        
        # Generator Economics
        if not network.generators.empty:
            df = network.generators[["bus", "carrier", "capital_cost", "marginal_cost"]].copy()
            df["p_nom_opt"] = self._extract_p_nom_opt(network.generators)
            df["total_annualized_capex"] = df["capital_cost"] * df["p_nom_opt"]
            df["component_type"] = "Generator"
            economics.append(df)
            
        # Link Economics
        if not network.links.empty:
            df = network.links[["bus0", "carrier", "capital_cost", "marginal_cost"]].copy()
            df = df.rename(columns={"bus0": "bus"})
            df["p_nom_opt"] = self._extract_p_nom_opt(network.links)
            df["total_annualized_capex"] = df["capital_cost"] * df["p_nom_opt"]
            # Exclude missing capital/marginal cost link mappings to avoid NaN pollution in economic aggregates
            df["component_type"] = "Link"
            economics.append(df)
            
        if economics:
            df_econ = pd.concat(economics)
            df_econ = df_econ.reset_index().rename(columns={"index": "component_id"})
            df_econ["scenario_id"] = scenario_id
            df_econ.to_parquet(self.output_dir / f"system_economics_{scenario_id}.parquet", engine="pyarrow")
            
        # Nodal Marginal Prices (SMPs)
        if hasattr(network, "buses_t") and not network.buses_t.marginal_price.empty:
            df_smp = network.buses_t.marginal_price.copy()
            df_smp = df_smp.reset_index()
            
            if "timestep" in df_smp.columns:
                df_smp = df_smp.rename(columns={"timestep": "timestamp"})
            elif "snapshot" in df_smp.columns:
                df_smp = df_smp.rename(columns={"snapshot": "timestamp"})
            elif "index" in df_smp.columns:
                df_smp = df_smp.rename(columns={"index": "timestamp"})
                
            id_vars = ["period", "timestamp"] if "period" in df_smp.columns else ["timestamp"]
            df_smp_long = df_smp.melt(id_vars=id_vars, var_name="bus", value_name="marginal_price")
            df_smp_long["scenario_id"] = scenario_id
            df_smp_long.to_parquet(self.output_dir / f"nodal_prices_{scenario_id}.parquet", engine="pyarrow")
            
    def _melt_timeseries(self, df: pd.DataFrame, var_name: str, value_name: str) -> pd.DataFrame:
        df = df.reset_index()
        if "timestep" in df.columns:
            df = df.rename(columns={"timestep": "timestamp"})
        elif "snapshot" in df.columns:
            df = df.rename(columns={"snapshot": "timestamp"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "timestamp"})
            
        id_vars = ["period", "timestamp"] if "period" in df.columns else ["timestamp"]
        return df.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)

    def export_dispatch_profiles(self, network: pypsa.Network, scenario_id: str):
        """
        Extracts high-frequency time-series dispatch data.
        Target: Short-term balancing, dispatch visualization.
        """
        dispatch_frames = []
        
        if hasattr(network, "generators_t") and not network.generators_t.p.empty:
            df_gen_long = self._melt_timeseries(network.generators_t.p.copy(), "component_id", "active_power")
            df_gen_long["component_type"] = "Generator"
            dispatch_frames.append(df_gen_long)
            
        if hasattr(network, "links_t") and not network.links_t.p0.empty:
            df_link_long = self._melt_timeseries(network.links_t.p0.copy(), "component_id", "active_power")
            df_link_long["component_type"] = "Link"
            dispatch_frames.append(df_link_long)
            
        if hasattr(network, "loads_t") and not network.loads_t.p_set.empty:
             df_load_long = self._melt_timeseries(network.loads_t.p_set.copy(), "component_id", "active_power")
             df_load_long["component_type"] = "Load"
             dispatch_frames.append(df_load_long)
             
        if dispatch_frames:
            df_dispatch = pd.concat(dispatch_frames)
            df_dispatch["scenario_id"] = scenario_id
            
            # Sub-mapping to map component_id to corresponding bus. Very useful downstream.
            comp_bus_map = {}
            if not network.generators.empty:
                comp_bus_map.update(network.generators["bus"].to_dict())
            if not network.links.empty:
                comp_bus_map.update(network.links["bus0"].to_dict())
            if not network.loads.empty:
                comp_bus_map.update(network.loads["bus"].to_dict())
                
            df_dispatch["bus"] = df_dispatch["component_id"].map(comp_bus_map)
            df_dispatch.to_parquet(self.output_dir / f"dispatch_profiles_{scenario_id}.parquet", engine="pyarrow")

    def export_all(self, network: pypsa.Network, scenario_id: str):
        """Runs all structured exports for a given scenario."""
        print(f"  [EXPORT] Serializing optimal state for: {scenario_id}")
        self.export_spatial_capacities(network, scenario_id)
        self.export_system_economics(network, scenario_id)
        self.export_dispatch_profiles(network, scenario_id)
