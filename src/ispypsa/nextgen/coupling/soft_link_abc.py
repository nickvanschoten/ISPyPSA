from abc import ABC, abstractmethod
import pandas as pd


class SoftLinkInterface(ABC):
    """
    Abstract Base Class for Soft-Linking to an external IAM (e.g., MESSAGE, AusTIMES).
    """

    @abstractmethod
    def export_energy_budget_request(self) -> pd.DataFrame:
        """
        Exports the current demands and trajectories to the external IAM.
        """
        pass

    @abstractmethod
    def import_macro_constraints(self, dataframe: pd.DataFrame) -> None:
        """
        Imports annual or macro-economic constraints from the IAM to restrict
        the subsequent PyPSA high-frequency dispatch optimization.
        """
        pass
