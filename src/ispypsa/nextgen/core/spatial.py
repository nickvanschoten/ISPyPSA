"""
Spatial Constraints & REZ Hosting Limits for PyPSA-AUS.
Binds AEMO hosting capacity limits to generator p_nom_max.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def apply_rez_limits(network, rez_limits: pd.DataFrame):
    """
    Applies hosting capacity limits (MW) to Renewable Energy Zones.
    """
    logger.info("Applying spatial REZ hosting limits to VRE generators...")
    
    for _, row in rez_limits.iterrows():
        rez_id = row['REZ_ID']
        tech = row['Technology'].lower()
        limit = row['Limit_MW']
        
        mask = (network.generators.carrier.str.contains(tech, case=False)) & \
               (network.generators.index.str.contains(rez_id, case=False))
        
        matching_gens = network.generators.loc[mask]
        
        if not matching_gens.empty:
            logger.debug(f"  Mapping {rez_id} {tech} limit ({limit} MW) to {len(matching_gens)} gens.")
            network.generators.loc[mask, "p_nom_max"] = limit
            
    return network
