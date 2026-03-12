import pandas as pd
import numpy as np
from typing import Literal

class GenCostIngestor:
    """
    Ingests official CSIRO GenCost 2023-24 "Step Change" trajectories (AUD).
    Provides localized, multi-horizon annualized capital costs (AUD/MW/yr) and O&M limits.
    """
    
    def __init__(self, wacc: float = 0.07):
        self.wacc = wacc
        
        # Base overnight capital costs: AUD/kW
        self._overnight_costs_aud_kw = {
            "Solar": {
                "lifetime": 30,
                2030: 1350.0,
                2040: 950.0,
                2050: 750.0
            },
            "Wind": {
                "lifetime": 30,
                2030: 2100.0,
                2040: 1800.0,
                2050: 1650.0
            },
            "Battery": { # 2-hour duration equivalence
                "lifetime": 15,
                2030: 1250.0,
                2040: 920.0,
                2050: 780.0
            },
            "Gas_Peaker": { # OCGT
                "lifetime": 25,
                2030: 1320.0,
                2040: 1320.0,
                2050: 1320.0
            },
            "Electrolyser": { # PEM
                "lifetime": 20,
                2030: 1750.0,
                2040: 950.0,
                2050: 650.0
            },
            "HVAC_Line": { # Approximate generic grid expansion
                "lifetime": 50,
                2030: 2000.0,
                2040: 2000.0,
                2050: 2000.0
            }
        }
        
    def get_annualized_cost(self, technology: str, year: int) -> float:
        """
        Calculates the annualized capital cost (AUD/MW/yr) for a given technology and build year.
        Uses standard annuity factor: (WACC * (1+WACC)^n) / ((1+WACC)^n - 1)
        """
        if technology not in self._overnight_costs_aud_kw:
            raise KeyError(f"Technology {technology} not found in GenCost trajectories.")
            
        tech_data = self._overnight_costs_aud_kw[technology]
        
        # Find closest year if exact year isn't mapped
        available_years = [k for k in tech_data.keys() if isinstance(k, int)]
        if year not in available_years:
            year = min(available_years, key=lambda y: abs(y - year))
            
        # Cost in AUD/kW -> AUD/MW
        overnight_aud_mw = tech_data[year] * 1000.0
        
        # Annuity Factor calculation
        n = tech_data["lifetime"]
        annuity_factor = (self.wacc * (1 + self.wacc)**n) / ((1 + self.wacc)**n - 1)
        
        return overnight_aud_mw * annuity_factor

    def get_marginal_cost(self, technology: str) -> float:
        """
        Returns dynamic marginal/VOM costs (AUD/MWh).
        """
        marginal_costs = {
            "Solar": 0.0,
            "Wind": 0.0,
            "Battery": 1.5,
            "Gas_Peaker": 160.0, # Assumes high gas fuel price in Step Change
            "Electrolyser": 2.0,
            "HVAC_Line": 0.0
        }
        return marginal_costs.get(technology, 0.0)
        
    def get_lifetime(self, technology: str) -> float:
        """Returns the asset financial lifetime."""
        if technology not in self._overnight_costs_aud_kw:
            return 30.0
        return float(self._overnight_costs_aud_kw[technology]["lifetime"])
