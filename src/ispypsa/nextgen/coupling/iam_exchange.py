import pandas as pd
import numpy as np
import pypsa
from pathlib import Path

class OutboundSignalGenerator:
    """Extracts PyPSA prices and converts them into macro-friendly Load-Weighted System Marginal Prices (SMP)."""
    
    def __init__(self, output_dir: str | Path = "iam_io"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_weighted_smp(self, network: pypsa.Network) -> pd.DataFrame:
        """
        Calculates the Load-Weighted SMP for each investment period.
        Formula: Sum(Price_t * Load_t) / Sum(Load_t) for each period.
        """
        if not hasattr(network, "buses_t") or network.buses_t.marginal_price.empty:
            raise ValueError("No marginal prices found. Make sure the network is solved.")
            
        prices = network.buses_t.marginal_price
        loads = network.loads_t.p_set
        
        load_bus_map = network.loads.bus.to_dict()
        
        period_smps = []
        
        for period in network.investment_periods:
            if period not in prices.index.get_level_values("period"):
                continue
                
            p_period = prices.xs(period, level="period")
            l_period = loads.xs(period, level="period")
            
            total_cost_period = 0.0
            total_load_period = 0.0
            
            for load_col in l_period.columns:
                bus = load_bus_map[load_col]
                if bus in p_period.columns:
                    # Sum product of hourly price * hourly load
                    cost_t = p_period[bus] * l_period[load_col]
                    total_cost_period += cost_t.sum()
                    total_load_period += l_period[load_col].sum()
                    
            weighted_smp = total_cost_period / total_load_period if total_load_period > 0 else 0.0
            period_smps.append({"period": period, "weighted_price_aud": weighted_smp})
            
        df_out = pd.DataFrame(period_smps)
        df_out.to_csv(self.output_dir / "outbound_smp.csv", index=False)
        return df_out

class InboundDemandMapper:
    """Receives target TWh from external macro models and heuristically maps it down to hourly nodal profiles."""
    
    def __init__(self, input_dir: str | Path = "iam_io"):
        self.input_dir = Path(input_dir)
        
    def apply_sectoral_downscaling(self, network: pypsa.Network, alpha: float = 0.5) -> None:
        """
        Reads `inbound_demand.csv` (cols: period, sector, target_twh).
        Applies targets with under-relaxation (damping) to prevent cobweb oscillation.
        D_i = alpha * D_target + (1 - alpha) * D_{i-1}
        """
        inbound_file = self.input_dir / "inbound_demand.csv"
        if not inbound_file.exists():
            print("  [IAM] No inbound demand targets found. Skipping downscaling.")
            return
            
        df_in = pd.read_csv(inbound_file)
        loads = network.loads_t.p_set
        
        for _, row in df_in.iterrows():
            period = int(row["period"])
            sector = str(row["sector"]).capitalize()
            target_twh = float(row["target_twh"])
            target_mwh = target_twh * 1e6
            
            if period not in loads.index.get_level_values("period"):
                continue
                
            sector_loads = [c for c in network.loads.index if f"_{sector}_Demand" in c]
            if not sector_loads:
                print(f"  [IAM] Warning: No loads matched sector '{sector}'.")
                continue
                
            l_period = loads.xs(period, level="period")[sector_loads]
            current_mwh = l_period.sum().sum()
            
            if current_mwh > 0:
                # Damping Logic:
                damped_target_mwh = (alpha * target_mwh) + ((1.0 - alpha) * current_mwh)
                scalar = damped_target_mwh / current_mwh
                
                idx = pd.IndexSlice
                network.loads_t.p_set.loc[idx[period, :], sector_loads] *= scalar
                
                print(f"  [IAM] Scaled {sector} in {period} by {scalar:.3f}x (Raw Target: {target_twh:.2f} TWh, Damped: {damped_target_mwh/1e6:.2f} TWh)")


class MockElasticityModel:
    """
    Rudimentary Computable General Equilibrium (CGE) Simulator predicting macro load responses.
    
    This class acts as a placeholder for a true IAM/CGE model, simulating price 
    elasticity of demand (PED) against PyPSA's solved market prices.
    
    Attributes:
        ped (float): Price Elasticity of Demand coefficient.
        target_sector (str): The demand sector to apply elasticity to.
        io_dir (Path): The IO directory for reading SMP prices and writing demand targets.
        previous_prices (dict): Cache of prior prices to calculate percentage changes.
    """
    def __init__(self, ped: float = -0.2, target_sector: str = "Industrial", io_dir: str | Path = "iam_io"):
        self.ped = ped
        self.target_sector = target_sector
        self.io_dir = Path(io_dir)
        self.previous_prices = {}
        
    def run_macro_step(self, network: pypsa.Network) -> pd.DataFrame:
        """
        Executes a simulated macro-economic response step.
        
        Reads the outbound SMP prices, applies the PED equation against the baseline, 
        and serializes new TWh targets for the InboundDemandMapper.
        
        Args:
            network (pypsa.Network): The solved PyPSA network for the current iteration.
            
        Returns:
            pd.DataFrame: A dataframe containing the new `[period, sector, target_twh]` macro limits.
        """
        df_prices = pd.read_csv(self.io_dir / "outbound_smp.csv")
        
        loads = network.loads_t.p_set
        sector_loads = [c for c in network.loads.index if f"_{self.target_sector}_Demand" in c]
        
        responses = []
        for _, row in df_prices.iterrows():
            period = int(row["period"])
            current_price = float(row["weighted_price_aud"])
            
            if period in loads.index.get_level_values("period") and sector_loads:
                l_period = loads.xs(period, level="period")[sector_loads]
                current_twh = l_period.sum().sum() / 1e6
                
                last_price = self.previous_prices.get(period)
                if last_price is None:
                    # Simulated historical baseline price (e.g. $80/MWh) avoiding a 0% delta on first pass.
                    last_price = 80.0
                    
                if last_price > 0:
                    pct_change_price = (current_price - last_price) / last_price
                else:
                    pct_change_price = 0.0
                    
                target_twh = current_twh * (1.0 + self.ped * pct_change_price)
                responses.append({"period": period, "sector": self.target_sector, "target_twh": target_twh})
                
                self.previous_prices[period] = current_price
                
        df_resp = pd.DataFrame(responses)
        if not df_resp.empty:
            df_resp.to_csv(self.io_dir / "inbound_demand.csv", index=False)
        return df_resp
