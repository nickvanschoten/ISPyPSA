"""
Brownfield Retirement Logic for NEM Thermal Generators.

Supports two modes:
  - AEMO Schedule: Enforce official closure years via build_year + lifetime
  - Economic: Let Gurobi retire via dispatch economics (no forced closures)
"""

import logging
import pypsa

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AEMO ISP 2024 Indicative Closure Schedule
# Maps generator name patterns (case-insensitive) to their closure year.
# Source: AEMO 2024 Integrated System Plan, Table A-4
# ---------------------------------------------------------------------------
AEMO_RETIREMENT_SCHEDULE = {
    "liddell":              2023,  # Already closed
    "callide_b":            2028,
    "yallourn":             2028,
    "eraring":              2025,  # Already closed
    "vales_point":          2033,
    "bayswater":            2033,
    "gladstone":            2035,
    "mt_piper":             2040,
    "tarong":               2037,
    "tarong_north":         2037,
    "stanwell":             2046,
    "callide_c":            2051,
    "kogan_creek":          2042,
    "millmerran":           2051,
    "loy_yang_a":           2035,
    "loy_yang_b":           2028,
    "hazelwood":            2017,  # Already closed
}

# Default commissioning years (approximate) for build_year assignment
COMMISSIONING_YEARS = {
    "liddell":              1971,
    "callide_b":            1988,
    "yallourn":             1982,
    "eraring":              1982,
    "vales_point":          1978,
    "bayswater":            1985,
    "gladstone":            1976,
    "mt_piper":             1993,
    "tarong":               1984,
    "tarong_north":         2002,
    "stanwell":             1993,
    "callide_c":            2001,
    "kogan_creek":          2007,
    "millmerran":           2002,
    "loy_yang_a":           1984,
    "loy_yang_b":           1993,
    "hazelwood":            1964,
}


def _match_schedule_key(gen_name: str) -> str | None:
    """Case-insensitive fuzzy match of a generator name to a schedule key."""
    gen_lower = gen_name.lower().replace(" ", "_").replace("-", "_")
    for key in AEMO_RETIREMENT_SCHEDULE:
        if key in gen_lower:
            return key
    return None


def apply_retirement_logic(
    network: pypsa.Network,
    mode: str,
    investment_periods: list[int] | None = None,
):
    """
    Apply brownfield retirement logic to thermal generators.

    IMPORTANT: In PyPSA multi-period mode, we must NOT toggle p_nom_extendable
    to False. Doing so changes the set returned by n.get_extendable_i(), which
    creates mismatched xarray dimensions during linopy model construction,
    causing an AlignmentError. Instead, we cap capacity using p_nom_max.

    Parameters
    ----------
    network : pypsa.Network
        The PyPSA network with generators already added.
    mode : str
        Either "aemo_schedule" or "economic".
    investment_periods : list[int] or None
        The investment periods being modelled (e.g. [2030, 2040, 2050]).

    Returns
    -------
    network : pypsa.Network
        Modified in-place.
    """
    if network.generators.empty:
        logger.warning("No generators in network. Skipping retirement logic.")
        return network

    thermal_carriers = {"coal", "black_coal", "brown_coal", "gas", "ocgt", "ccgt"}

    def is_thermal(c):
        if not isinstance(c, str):
            return False
        return any(tc in c.lower() for tc in thermal_carriers)

    thermal_mask = network.generators["carrier"].apply(is_thermal)
    thermal_gens = network.generators.loc[thermal_mask]
    logger.info(f"Found {len(thermal_gens)} thermal generators for retirement logic.")

    if mode == "aemo_schedule":
        if investment_periods is None:
            investment_periods = [2030, 2040, 2050]

        for idx in thermal_gens.index:
            gen_name = str(idx)
            schedule_key = _match_schedule_key(gen_name)

            if schedule_key:
                closure_year = AEMO_RETIREMENT_SCHEDULE[schedule_key]
                commission_year = COMMISSIONING_YEARS.get(schedule_key, 1980)

                network.generators.at[idx, "build_year"] = commission_year
                network.generators.at[idx, "lifetime"] = closure_year - commission_year

                logger.info(
                    f"  AEMO Schedule: {gen_name} → build_year={commission_year}, "
                    f"lifetime={closure_year - commission_year}, retires {closure_year}"
                )

            # Cap capacity at existing p_nom (prevent new builds)
            # Keep p_nom_extendable=True to avoid linopy dimension mismatch
            existing = network.generators.at[idx, "p_nom"]
            network.generators.at[idx, "p_nom_max"] = existing

    elif mode == "economic":
        for idx in thermal_gens.index:
            network.generators.at[idx, "build_year"] = 1970
            network.generators.at[idx, "lifetime"] = 100

            # Cap at existing capacity
            existing = network.generators.at[idx, "p_nom"]
            network.generators.at[idx, "p_nom_max"] = existing

            logger.info(f"  Economic mode: {idx} available all periods (dispatch-based retirement)")

    else:
        logger.warning(f"Unknown retirement mode '{mode}'. Skipping.")

    return network
