from abc import ABC, abstractmethod
import pypsa
import pandas as pd


class SurrogateModel(ABC):
    """
    Abstract Base Class for ML Surrogate Models.
    Used to rapidly approximate PyPSA dispatch results during external IAM iterations.
    """

    @abstractmethod
    def predict_system_cost(self, inputs: pd.DataFrame) -> float:
        """
        Predict the total system cost without running the full PyPSA optimization.
        """
        pass

    @abstractmethod
    def predict_dispatch(self, inputs: pd.DataFrame) -> pd.DataFrame:
        """
        Predict the dispatch generation mix.
        """
        pass


class MGAConstraintGenerator(ABC):
    """
    Abstract Base Class for Modeling to Generate Alternatives.
    """

    @abstractmethod
    def generate_slack_constraints(self, network: pypsa.Network, optimal_cost: float, slack_pct: float) -> None:
        """
        Add constraints to the PyPSA network objective function ensuring new solutions
        do not exceed optimal_cost * (1 + slack_pct).
        """
        pass

    @abstractmethod
    def set_alternative_objective(self, network: pypsa.Network, variables: list[str], direction: str = 'min') -> None:
        """
        Change the objective function to minimize/maximize specific alternative variables
        within the defined slack space.
        """
        pass
