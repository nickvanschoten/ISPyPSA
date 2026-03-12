import pandas as pd
from pathlib import Path

from ispypsa.nextgen.coupling.soft_link_abc import SoftLinkInterface

class ParquetBasedIAMInterface(SoftLinkInterface):
    """
    Implements a high-performance Parquet-based exchange interface for IAM soft-linking.
    """
    def __init__(self, working_dir: str):
        self.working_dir = Path(working_dir)
        self.working_dir.mkdir(parents=True, exist_ok=True)
        
    def export_energy_budget_request(self) -> pd.DataFrame:
        """
        Exports the current demands and trajectories to the external IAM.
        In a real scenario, this would aggregate PyPSA results.
        Strictly preserves pandas.DatetimeIndex and numeric types.
        """
        df = pd.DataFrame({
            "Electricity_Demand_TWh": [300.0, 450.0, 600.0],
            "Hydrogen_Demand_PJ": [50.0, 200.0, 500.0]
        })
        # Strict alignment requires a proper DatetimeIndex
        df.index = pd.date_range(start="2030-01-01", periods=3, freq="YS", name="Year")
        
        out_path = self.working_dir / "pypsa_export.parquet"
        df.to_parquet(out_path, engine="pyarrow", compression="snappy")
        return df

    def import_macro_constraints(self, dataframe: pd.DataFrame = None) -> None:
        """
        Imports annual or macro-economic constraints from the IAM.
        """
        in_path = self.working_dir / "iam_import.parquet"
        if dataframe is None and in_path.exists():
            dataframe = pd.read_parquet(in_path, engine="pyarrow")
            
        if dataframe is not None:
            # Here we would map the IAM carbon/energy budgets into PyPSA global constraints
            pass
