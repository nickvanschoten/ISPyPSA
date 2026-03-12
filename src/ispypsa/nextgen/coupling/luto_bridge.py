import pandas as pd
import pypsa
from pathlib import Path

class OutboundCapacityEnvelopeManager:
    """Extracts PyPSA optimal capacities and exports them as regional macro-constraints for LUTO2."""
    
    def __init__(self, output_dir: str | Path = "luto_io"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_regional_envelopes(self, network: pypsa.Network) -> pd.DataFrame:
        """
        Extracts p_nom_opt, aggregating by [region, build_year, carrier].
        """
        if network.generators.empty or "p_nom_opt" not in network.generators.columns:
            raise ValueError("No optimal capacities found. Network must be solved first.")
            
        # We only care about spatial components (Generators/Stores). Links are topological.
        df_gen = network.generators[["bus", "build_year", "carrier", "p_nom_opt"]].copy()
        
        # Clean the bus name to get the raw region name (e.g., 'NSW_AC' -> 'NSW')
        df_gen["region"] = df_gen["bus"].astype(str).str.replace("_AC", "")
        
        # Aggregate the capacities
        grouped = df_gen.groupby(["region", "build_year", "carrier"], as_index=False)["p_nom_opt"].sum()
        grouped.rename(columns={"p_nom_opt": "capacity_mw"}, inplace=True)
        
        # Filter out negligible builds (e.g., solver precision artifacts)
        grouped = grouped[grouped["capacity_mw"] > 0.01]
        
        # Export
        out_path = self.output_dir / "regional_capacity_envelopes.csv"
        grouped.to_csv(out_path, index=False)
        print(f"  [LUTO2] Exported Regional Capacity Envelopes to {out_path}")
        
        return grouped
