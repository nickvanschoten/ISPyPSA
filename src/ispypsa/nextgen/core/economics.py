"""
Financial Mathematics for PyPSA-AUS.
Implements AEMO-mandated annuity formulas for capital cost calculation.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def calculate_annuity(overnight_cost: float, wacc: float, lifetime: int, fixed_opex: float) -> float:
    """
    Calculates the annualized capital cost ($/unit-year) using the AEMO formula.
    
    Formula:
    P = [ (WACC / (1 - (1 + WACC)**(-lifetime))) * Overnight_Cost ] + Fixed_OPEX
    """
    if wacc == 0:
        return (overnight_cost / lifetime) + fixed_opex
        
    annuity_factor = wacc / (1 - (1 + wacc)**(-lifetime))
    return (annuity_factor * overnight_cost) + fixed_opex

def apply_gencost_to_network(network, iasr_data: dict, scenario_wacc: float = None):
    """
    Maps IASR assumption data into the PyPSA network generators and stores.
    """
    logger.info("Applying GenCost annuity calculations to network components...")
    
    # Process Generators
    for idx, row in network.generators.iterrows():
        carrier = str(row["carrier"]).lower()
        match_key = next((k for k in iasr_data if k in carrier), None)
        
        if match_key:
            data = iasr_data[match_key]
            wacc = scenario_wacc if scenario_wacc is not None else data.get("wacc", 0.07)
            
            ann_cost = calculate_annuity(
                overnight_cost=data["overnight_cost"],
                wacc=wacc,
                lifetime=data["lifetime"],
                fixed_opex=data["fixed_opex"]
            )
            network.generators.at[idx, "capital_cost"] = ann_cost * 1000.0
            
    # Process Stores
    for idx, row in network.stores.iterrows():
        carrier = str(row["carrier"]).lower()
        match_key = next((k for k in iasr_data if k in carrier), None)
        
        if match_key:
            data = iasr_data[match_key]
            wacc = scenario_wacc if scenario_wacc is not None else data.get("wacc", 0.07)
            
            ann_cost = calculate_annuity(
                overnight_cost=data["overnight_cost"],
                wacc=wacc,
                lifetime=data["lifetime"],
                fixed_opex=data["fixed_opex"]
            )
            network.stores.at[idx, "capital_cost"] = ann_cost * 1000.0
            
    return network
