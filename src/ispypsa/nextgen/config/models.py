from pydantic import BaseModel, Field, model_validator
from typing import Any, Literal
import warnings

class BaseComponentConfig(BaseModel):
    """Base config for all network components enforcing naming standards."""
    name: str | None = None

class NodeConfig(BaseComponentConfig):
    type: str  # e.g., "Urban", "Renewable", "Port"
    spatial_penalty_cost: float = 0.0  # Optional $/MW agricultural opportunity cost penalizing expansions

class TransportLinkConfig(BaseComponentConfig):
    """Base configuration for inter-hub transport links."""
    bus0: str
    bus1: str
    carrier: str
    p_nom_extendable: bool = True
    efficiency: float = 1.0
    capital_cost: float = 0.0

class HVACLineConfig(TransportLinkConfig):
    """Configuration for standard high-voltage AC transmission lines."""
    carrier: Literal["AC"] = "AC"
    length: float = 0.0

class HydrogenPipelineConfig(TransportLinkConfig):
    """
    Configuration for Hydrogen pipelines, structurally enforcing physics limits 
    like parasitic electrical loads for compressor stations.
    """
    carrier: Literal["H2"] = "H2"
    electrical_bus: str
    efficiency2: float = -0.02 # Default MWe parasitic draw per MWH2 delivered
    
    @model_validator(mode="after")
    def validate_physics(self) -> 'HydrogenPipelineConfig':
        if self.efficiency2 > 0:
            raise ValueError("HydrogenPipelineConfig efficiency2 must be negative to represent parasitic compression load.")
        if self.electrical_bus == self.bus0 or self.electrical_bus == self.bus1:
             warnings.warn(f"electrical_bus '{self.electrical_bus}' shares a name with a primary H2 bus. Ensure this is an AC bus.")
        return self

class SolverConfig(BaseModel):
    threads: int | None = 0
    time_limit: int | None = None
    gurobi_options: dict[str, Any] = {}
    highs_options: dict[str, Any] = {}

class MGAConfig(BaseModel):
    """Defines the parameters for a Modeling to Generate Alternatives (MGA) sweep."""
    cost_source: str = "default"
    slack_epsilon: float = 0.05
    target_component: Literal["Generator", "Line", "Link", "Store", "StorageUnit"] = "Generator"
    target_carrier: str | None = None
    target_action: Literal["minimize", "maximize"] = "minimize"

class SensitivityConfig(BaseModel):
    """Macroeconomic and spatial parameters for UI-driven sensitivity analysis."""
    capex_modifiers: dict[str, float] = Field(default_factory=dict)
    opex_modifiers: dict[str, float] = Field(default_factory=dict)
    transmission_cost_modifier: float = 1.0
    capacity_factor_modifier: float = 1.0
    iam_alpha_damping: float = 0.5

class TestbedConfig(BaseModel):
    scenario_name: str
    enable_sector_coupling: bool = False
    solver_name: str = "highs"
    solver_options: SolverConfig = Field(default_factory=SolverConfig)
    mga_options: MGAConfig = Field(default_factory=MGAConfig)
    sensitivities: SensitivityConfig = Field(default_factory=SensitivityConfig)
    nodes: list[NodeConfig]
    links: list[HVACLineConfig | HydrogenPipelineConfig | TransportLinkConfig]
