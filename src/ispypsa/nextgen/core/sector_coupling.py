"""
Endogenous Sector Coupling for PyPSA-AUS.
Implements conversion chains for EVs and Hydrogen without fixed load-trace adders.
"""
import logging
import pypsa
import pandas as pd

logger = logging.getLogger(__name__)

def add_hydrogen_chain(network, bus_id: str, annual_target_mth2: float, electrolyser_mw: float = 1000):
    """
    Adds a Hydrogen conversion chain: Power Bus -> Electrolyser -> H2 Bus.
    Enforces annual production target via GlobalConstraint.
    """
    logger.info(f"Adding Hydrogen conversion chain at {bus_id}...")
    
    h2_bus = f"{bus_id}_H2"
    if h2_bus not in network.buses.index:
        network.add("Bus", h2_bus, carrier="H2")
        
    network.add(
        "Link",
        f"{bus_id}_electrolyser",
        bus0=bus_id,
        bus1=h2_bus,
        carrier="electrolyser",
        p_nom_extendable=True,
        p_nom=0,
        p_nom_max=electrolyser_mw,
        efficiency=0.75  # IASR standard
    )
    
    # Global Constraint for annual H2 target
    # 1 MtH2 = 33.33 TWh_LHV
    target_mwh = annual_target_mth2 * 33.33 * 1e6
    
    network.add(
        "GlobalConstraint",
        f"h2_target_{bus_id}",
        carrier_attribute="H2",
        sense=">=",
        constant=target_mwh,
        type="primary_energy"
    )
    
    return network

def add_ev_chain(network, bus_id: str, fleet_size: int, ev_battery_kwh: float = 60, charge_rate_kw: float = 7):
    """
    Adds an EV conversion chain: Power Bus -> Charger -> EV Bus -> EV Battery (Store).
    The solver optimizes charging timing to meet driving demand.
    """
    logger.info(f"Adding endogenous EV chain at {bus_id}...")
    
    ev_bus = f"{bus_id}_EV"
    if ev_bus not in network.buses.index:
        network.add("Bus", ev_bus, carrier="EV")
        
    # Charger Link
    total_charge_mw = (fleet_size * charge_rate_kw) / 1000.0
    network.add(
        "Link",
        f"{bus_id}_ev_charger",
        bus0=bus_id,
        bus1=ev_bus,
        carrier="ev_charger",
        p_nom=total_charge_mw,
        p_nom_extendable=False,
        efficiency=0.9  # Charger efficiency
    )
    
    # EV Battery (Store)
    total_e_nom = (fleet_size * ev_battery_kwh) / 1000.0
    network.add(
        "Store",
        f"{bus_id}_ev_battery",
        bus=ev_bus,
        carrier="ev_battery",
        e_nom=total_e_nom,
        e_nom_extendable=False,
        e_cyclic=True,
        standing_loss=0.0001
    )
    
    return network
