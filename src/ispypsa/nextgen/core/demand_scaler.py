import logging

logger = logging.getLogger(__name__)

def apply_macroeconomic_scaling(network, scenario_params):
    """
    Scales the PyPSA network base temporal loads based on macroeconomic drivers
    and electrification inputs.

    Parameters
    ----------
    network : pypsa.Network
        The PyPSA network containing `loads_t.p_set`.
    scenario_params : dict
        A dictionary containing the macroeconomic and electrification parameters:
        - target_year (int)
        - pop_growth (float): % e.g., 2.0 for 2%
        - gdp_growth (float): % e.g., 2.5 for 2.5%
        - demand_elasticity (float): e.g., -0.1
        - ev_penetration (float): % e.g., 50.0 for 50%
        - ind_electrification (float): % e.g., 30.0 for 30%

    Returns
    -------
    network : pypsa.Network
        The modified PyPSA network.
    """
    
    # Strictly check that network.loads_t.p_set is not empty
    if not hasattr(network, 'loads_t') or network.loads_t.p_set is None or network.loads_t.p_set.empty:
        logger.warning("network.loads_t.p_set is empty or does not exist. Skipping demand scaling.")
        return network

    starting_total_demand_mwh = network.loads_t.p_set.sum().sum()
    starting_total_demand_twh = starting_total_demand_mwh / 1e6
    logger.info(f"Starting total demand: {starting_total_demand_twh:.2f} TWh")

    # 1. Mathematical Compounding Logic
    target_year = scenario_params.get("target_year", 2030)
    delta_t = max(0, target_year - 2026)

    # Convert percentages to decimals
    pop_growth = scenario_params.get("pop_growth", 0.0) / 100.0
    gdp_growth = scenario_params.get("gdp_growth", 0.0) / 100.0
    demand_elasticity = scenario_params.get("demand_elasticity", 0.0)

    # Calculate Organic Growth Multiplier: (1 + (GDP_Growth * Elasticity) + Population_Growth) ** delta_t
    organic_multiplier = (1.0 + (gdp_growth * demand_elasticity) + pop_growth) ** delta_t

    # 2. Electrification Adders
    ev_penetration = scenario_params.get("ev_penetration", 0.0) / 100.0
    ind_electrification = scenario_params.get("ind_electrification", 0.0) / 100.0

    # Assumptions: 100% EV = 15% increase, 100% Ind = 20% increase
    ev_adder = ev_penetration * 0.15
    ind_adder = ind_electrification * 0.20
    electrification_multiplier = 1.0 + ev_adder + ind_adder

    # Apply both multipliers to the base load
    total_multiplier = organic_multiplier * electrification_multiplier
    network.loads_t.p_set = network.loads_t.p_set * total_multiplier

    final_total_demand_mwh = network.loads_t.p_set.sum().sum()
    final_total_demand_twh = final_total_demand_mwh / 1e6
    logger.info(f"Final scaled total demand: {final_total_demand_twh:.2f} TWh")

    return network
