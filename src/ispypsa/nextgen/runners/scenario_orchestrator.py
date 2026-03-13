"""
Multi-Period Scenario Orchestrator for the NEM Policy Sandbox.

Pipeline:
  1. Load base network
  2. Configure multi-period investment horizons
  3. Apply temporal clustering (representative weeks + extremes)
  4. Apply macroeconomic demand scaling (regional + profile shaping)
  5. Apply brownfield retirement logic
  6. Apply WACC annuity recalculation
  7. Apply CAPEX shocks
  8. Apply carbon mechanism (price trajectory or cumulative budget)
  9. Apply fuel pricing
  10. Optimize with Gurobi
  11. Export results
"""

import pypsa
import pandas as pd
import numpy as np
import json
import sys
import argparse
import logging
from pathlib import Path

try:
    from pypsa.costs import annuity as pypsa_annuity
except ImportError:
    def pypsa_annuity(rate, lifetime):
        if rate == 0:
            return 1 / lifetime
        return rate / (1.0 - (1.0 + rate) ** (-lifetime))

from ispypsa.nextgen.core.demand_scaler import apply_macroeconomic_scaling
from ispypsa.nextgen.core.temporal_clustering import cluster_to_representative_weeks
from ispypsa.nextgen.core.retirement_logic import apply_retirement_logic
from ispypsa.nextgen.runners.phase4_5_runner import build_network
from ispypsa.nextgen.config.manager import DeepMergeConfigManager
from ispypsa.nextgen.config.models import TestbedConfig
from ispypsa.nextgen.io.high_frequency_export import MGAExportManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default investment periods
DEFAULT_PERIODS = [2026, 2030, 2040, 2050]

# Asset lifetimes for WACC annuity calculation (years)
ASSET_LIFETIMES = {
    "solar": 25,
    "wind": 25,
    "battery": 15,
    "gas": 30,
    "ocgt": 30,
    "ccgt": 30,
    "coal": 40,
    "black_coal": 40,
    "brown_coal": 40,
    "hydro": 50,
}


def _get_carrier_lifetime(carrier: str) -> int:
    """Look up asset lifetime from carrier name."""
    carrier_lower = str(carrier).lower()
    for key, lifetime in ASSET_LIFETIMES.items():
        if key in carrier_lower:
            return lifetime
    return 25  # Default fallback


# ---------------------------------------------------------------------------
# WACC & Annuity (Module 3)
# ---------------------------------------------------------------------------

def apply_wacc_annuity(network: pypsa.Network, wacc: float):
    """
    Set the network discount rate and annuitize capital costs for all
    extendable generators and stores.

    Uses pypsa.costs.annuity() to convert overnight CAPEX to annual CAPEX.
    PyPSA's native investment_period_weightings handles temporal discounting
    — no manual discount factors applied here to avoid double-discounting.
    """
    logger.info(f"Applying WACC annuity: rate={wacc:.4f}")
    network.discount_rate = wacc

    # Annuitize generator capital costs
    if not network.generators.empty:
        for idx, row in network.generators.iterrows():
            if row.get("p_nom_extendable", False):
                lifetime = _get_carrier_lifetime(row.get("carrier", ""))
                ann_factor = pypsa_annuity(wacc, lifetime)
                network.generators.at[idx, "capital_cost"] *= ann_factor

    # Annuitize store capital costs
    if not network.stores.empty:
        for idx, row in network.stores.iterrows():
            if row.get("e_nom_extendable", False):
                lifetime = _get_carrier_lifetime(row.get("carrier", "battery"))
                ann_factor = pypsa_annuity(wacc, lifetime)
                network.stores.at[idx, "capital_cost"] *= ann_factor

    # Annuitize storage unit capital costs
    if not network.storage_units.empty:
        for idx, row in network.storage_units.iterrows():
            if row.get("p_nom_extendable", False):
                lifetime = _get_carrier_lifetime(row.get("carrier", "battery"))
                ann_factor = pypsa_annuity(wacc, lifetime)
                network.storage_units.at[idx, "capital_cost"] *= ann_factor

    return network


# ---------------------------------------------------------------------------
# CAPEX Shocks
# ---------------------------------------------------------------------------

def apply_capex_shocks(network: pypsa.Network, payload: dict):
    """Apply user-defined multipliers to capital_cost for wind, solar, and battery."""
    logger.info("Applying CAPEX shocks...")

    wind_mult = payload.get("wind_capex", 1.0)
    solar_mult = payload.get("solar_capex", 1.0)
    battery_mult = payload.get("battery_capex", 1.0)

    if not network.generators.empty:
        wind_mask = network.generators.carrier.str.contains("wind", case=False, na=False)
        solar_mask = network.generators.carrier.str.contains("solar", case=False, na=False)
        if wind_mask.any():
            network.generators.loc[wind_mask, "capital_cost"] *= wind_mult
        if solar_mask.any():
            network.generators.loc[solar_mask, "capital_cost"] *= solar_mult

    if not network.stores.empty:
        battery_mask = network.stores.carrier.str.contains("battery", case=False, na=False)
        if battery_mask.any():
            network.stores.loc[battery_mask, "capital_cost"] *= battery_mult

    return network


# ---------------------------------------------------------------------------
# Emissions Intensities (Carbon Physics Patch)
# ---------------------------------------------------------------------------

def apply_emissions_intensities(network: pypsa.Network):
    """
    Set default CO2 emission intensities (tCO2/MWh_elec) for thermal carriers.
    Mapping: brown_coal:1.2, black_coal:0.9, ocgt:0.6, ccgt:0.4, gas:0.55
    """
    intensities = {
        "brown_coal": 1.2,
        "black_coal": 0.9,
        "ocgt": 0.6,
        "ccgt": 0.4,
        "gas": 0.55,
    }
    logger.info("Applying emissions intensities patch...")

    # Ensure co2_emissions exists in carriers
    if "co2_emissions" not in network.carriers.columns:
        network.carriers["co2_emissions"] = 0.0

    # Ensure carrier_co2_emissions exists in generators
    if "carrier_co2_emissions" not in network.generators.columns:
        network.generators["carrier_co2_emissions"] = 0.0

    for carrier, intensity in intensities.items():
        # Update carrier-level attribute
        if carrier in network.carriers.index:
            network.carriers.at[carrier, "co2_emissions"] = intensity
        
        # Update generator-level attribute for all matching carriers
        mask = network.generators.carrier == carrier
        if mask.any():
            network.generators.loc[mask, "carrier_co2_emissions"] = intensity

    return network


# ---------------------------------------------------------------------------
# Dual Carbon Mechanism (Module 4)
# ---------------------------------------------------------------------------

def apply_carbon_mechanism(network: pypsa.Network, payload: dict):
    """
    Apply the user's chosen carbon mechanism:
      - "price_trajectory": per-period $/tCO2 injected into thermal marginal costs
      - "cumulative_budget": PyPSA GlobalConstraint CO2Limit across all periods
    """
    carbon_mode = payload.get("carbon_mode", "price_trajectory")
    logger.info(f"Applying carbon mechanism: {carbon_mode}")

    if carbon_mode == "price_trajectory":
        # Per-period carbon prices
        carbon_prices = payload.get("carbon_prices", {})
        flat_price = payload.get("carbon_price", 0.0)

        if network.generators.empty:
            return network

        for idx, row in network.generators.iterrows():
            carrier = str(row.get("carrier", "")).lower()
            if not any(tc in carrier for tc in ["gas", "ocgt", "ccgt", "coal", "brown_coal", "black_coal"]):
                continue

            efficiency = row.get("efficiency", 1.0)
            if efficiency <= 0:
                efficiency = 1.0
            carbon_intensity = row.get("carrier_co2_emissions", 0.0)

            # Determine the period this generator belongs to (from build_year suffix or name)
            gen_name = str(idx)
            period_price = flat_price
            for period_str, price in carbon_prices.items():
                if str(period_str) in gen_name:
                    period_price = price
                    break

            # Fuel pricing
            fuel_price = None
            if "gas" in carrier or "ocgt" in carrier or "ccgt" in carrier:
                fuel_price = payload.get("gas_price", 10.0)
            elif "black_coal" in carrier or "black coal" in carrier:
                fuel_price = payload.get("black_coal_price", 8.0)
            elif "brown_coal" in carrier or "brown coal" in carrier:
                fuel_price = payload.get("brown_coal_price", 5.0)

            if fuel_price is not None:
                new_mc = (fuel_price / efficiency) + (carbon_intensity * period_price)
                network.generators.at[idx, "marginal_cost"] = new_mc

    elif carbon_mode == "cumulative_budget":
        budget_mt = payload.get("carbon_budget_mt", 1000.0)
        budget_t = budget_mt * 1e6  # Convert MtCO2 to tCO2

        # Emissions intensities are now set by apply_emissions_intensities()

        # Add the global CO2 constraint
        network.add(
            "GlobalConstraint",
            "co2_budget",
            type="primary_energy",
            carrier_attribute="co2_emissions",
            sense="<=",
            constant=budget_t,
        )
        logger.info(f"Added cumulative CO2 budget constraint: {budget_mt:.0f} MtCO2")

        # Still apply fuel pricing (without carbon adder — budget handles it)
        _apply_fuel_pricing_no_carbon(network, payload)

    return network


def _apply_fuel_pricing_no_carbon(network: pypsa.Network, payload: dict):
    """Apply fuel costs to thermal generators without carbon adder (budget mode)."""
    if network.generators.empty:
        return

    for idx, row in network.generators.iterrows():
        carrier = str(row.get("carrier", "")).lower()
        efficiency = row.get("efficiency", 1.0)
        if efficiency <= 0:
            efficiency = 1.0

        fuel_price = None
        if "gas" in carrier or "ocgt" in carrier or "ccgt" in carrier:
            fuel_price = payload.get("gas_price", 10.0)
        elif "black_coal" in carrier or "black coal" in carrier:
            fuel_price = payload.get("black_coal_price", 8.0)
        elif "brown_coal" in carrier or "brown coal" in carrier:
            fuel_price = payload.get("brown_coal_price", 5.0)

        if fuel_price is not None:
            network.generators.at[idx, "marginal_cost"] = fuel_price / efficiency


# ---------------------------------------------------------------------------
# Results Export
# ---------------------------------------------------------------------------

def export_results(network: pypsa.Network, scenario_name: str, export_dir: str = "results_export"):
    """Export capacities and dispatch to Parquet files."""
    logger.info(f"Exporting results for scenario: {scenario_name}")
    out_dir = Path(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    exporter = MGAExportManager(str(out_dir))

    try:
        exporter.export_all(network, scenario_name)
    except Exception as e:
        logger.warning(f"MGAExportManager failed, falling back: {e}")
        # Fallback export
        caps = []
        if not network.generators.empty:
            g = network.generators[["bus", "carrier"]].copy()
            g["p_nom_opt"] = network.generators.get("p_nom_opt", network.generators["p_nom"])
            g["component_type"] = "Generator"
            caps.append(g)
        if not network.stores.empty:
            s = network.stores[["bus", "carrier"]].copy()
            s["p_nom_opt"] = network.stores.get("e_nom_opt", network.stores.get("e_nom", 0))
            s["component_type"] = "Store"
            caps.append(s)

        if caps:
            df_caps = pd.concat(caps)
            df_caps.to_parquet(out_dir / f"spatial_capacities_{scenario_name}.parquet")

        if hasattr(network, "generators_t") and not network.generators_t.p.empty:
            df_disp = network.generators_t.p.melt(
                ignore_index=False, var_name="component", value_name="active_power"
            )
            df_disp.index.name = "timestamp"
            df_disp = df_disp.reset_index()
            df_disp["component_type"] = "Generator"
            df_disp.to_parquet(out_dir / f"dispatch_profiles_{scenario_name}.parquet")


# ---------------------------------------------------------------------------
# Structured Solver Error Output
# ---------------------------------------------------------------------------

def _write_solver_error(error_msg: str, error_file: str = "solver_error.json"):
    """Write a structured error file for the Streamlit UI to read."""
    error_payload = {
        "status": "error",
        "message": error_msg,
    }
    with open(error_file, "w") as f:
        json.dump(error_payload, f, indent=2)
    logger.error(f"Solver error written to {error_file}: {error_msg}")


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Multi-Period NEM Scenario Orchestrator")
    parser.add_argument("payload_path", type=str, help="Path to the JSON scenario payload")
    args = parser.parse_args()

    payload_path = Path(args.payload_path)
    if not payload_path.exists():
        logger.error(f"Payload file not found: {payload_path}")
        sys.exit(1)

    with open(payload_path, "r") as f:
        payload = json.load(f)

    scenario_name = payload.get("scenario_name", "Multi_Period_Run")
    run_mga = payload.get("mga_toggle", False)
    investment_periods = payload.get("investment_periods", DEFAULT_PERIODS)
    wacc = payload.get("wacc", 0.07)
    retirement_mode = payload.get("retirement_mode", "aemo_schedule")

    logger.info(f"--- Starting Multi-Period Orchestrator: {scenario_name} ---")
    logger.info(f"Investment periods: {investment_periods}")
    logger.info(f"WACC: {wacc:.2%}, Retirement: {retirement_mode}")

    # 1. Build Base Network
    try:
        config_mgr = DeepMergeConfigManager("ispypsa_config.yaml")
        raw_config = config_mgr.active_config
        testbed_payload = raw_config.get("testbed", {})
        config = TestbedConfig(**testbed_payload)
        network = build_network(config, enable_coupling=True)
        logger.info("Base network built successfully.")
    except Exception as e:
        _write_solver_error(f"Network build failed: {e}")
        sys.exit(1)

    # 2. Apply Temporal Clustering (representative weeks + extremes)
    # Applied to both single-period and multi-period networks if resolution is high.
    if hasattr(network, "loads_t") and not network.loads_t.p_set.empty:
        n_weeks = payload.get("representative_weeks", 3)
        snapshots_per_period = len(network.snapshots)
        if isinstance(network.snapshots, pd.MultiIndex):
            periods = network.snapshots.get_level_values(0).unique()
            snapshots_per_period = len(network.snapshots) // len(periods)
        
        # Only cluster if snapshots per period > 1 week * 24h * 2 (e.g. 336h)
        if snapshots_per_period > (n_weeks * 7 * 24):
            try:
                vre_t = None
                if hasattr(network, "generators_t") and hasattr(network.generators_t, "p_max_pu"):
                    if not network.generators_t.p_max_pu.empty:
                        vre_t = network.generators_t.p_max_pu

                selected_hours, snapshot_weightings = cluster_to_representative_weeks(
                    network.loads_t.p_set, n_weeks=n_weeks, vre_t=vre_t
                )

                original_index = network.snapshots
                selected_idx = original_index[selected_hours]
                network.set_snapshots(selected_idx)

                # Reset weightings to 1.0 first
                network.snapshot_weightings.loc[:, :] = 1.0

                for i, hour_idx in enumerate(selected_hours):
                    weight = snapshot_weightings.get(hour_idx, 1.0)
                    snap = selected_idx[i]
                    network.snapshot_weightings.at[snap, "objective"] = weight
                    network.snapshot_weightings.at[snap, "stores"] = weight
                    network.snapshot_weightings.at[snap, "generators"] = weight

                logger.info(
                    f"Applied temporal clustering: {len(selected_idx)} snapshots selected "
                    f"from {len(original_index)}."
                )
            except Exception as e:
                logger.warning(f"Temporal clustering failed, using full resolution: {e}")
        else:
            logger.info(f"Snapshots per period ({snapshots_per_period}) is already low; skipping clustering.")

    # 3. Demand Scaling (multi-period, regional, profile shaping)
    network = apply_macroeconomic_scaling(network, payload)

    # 4. Brownfield Retirement Logic
    network = apply_retirement_logic(network, retirement_mode, investment_periods)

    # 5. WACC Annuity
    network = apply_wacc_annuity(network, wacc)

    # 6. CAPEX Shocks
    network = apply_capex_shocks(network, payload)

    # 7. Emissions Intensities (Carbon Physics Patch)
    network = apply_emissions_intensities(network)

    # 8. Carbon Mechanism (price trajectory or cumulative budget)
    network = apply_carbon_mechanism(network, payload)

    # 9. Re-align snapshot index naming (Fixes PyPSA dim_0 AlignmentError)
    # Our pipeline steps (demand_scaler, retirement_logic, etc.) may modify
    # time-series DataFrames in ways that reset the MultiIndex name from
    # "snapshot" to something else. PyPSA's linopy model construction
    # requires the coordinate to be named "snapshot" consistently.
    if isinstance(network.snapshots, pd.MultiIndex):
        # Force a fresh re-assignment of the exact MultiIndex to 
        # let PyPSA regenerate its internal xarray coordinate metadata
        network.set_snapshots(network.snapshots)

    for c in network.iterate_components(["Load", "Generator", "Link", "Store", "StorageUnit"]):
        for attr, df in c.pnl.items():
            if df is not None and not df.empty:
                df.index.name = "snapshot"
    logger.info("Re-aligned snapshot index naming for all time-series components.")

    # 9. Optimize
    logger.info("Executing Gurobi solver...")

    try:
        status = network.optimize(solver_name="gurobi")

        # Check for infeasibility
        if hasattr(status, "__iter__") and len(status) >= 2:
            condition = str(status[1]).lower()
        else:
            condition = str(status).lower()

        if "infeasible" in condition:
            msg = (
                "Optimization INFEASIBLE \u2014 the scenario constraints cannot be satisfied. "
                "This usually means the carbon budget is too tight, or retirement schedules "
                "remove too much capacity before new builds can come online."
            )
            _write_solver_error(msg)
            sys.exit(1)

        logger.info(f"Optimization successful: {status}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        error_str = str(e).split('\n')[0]
        _write_solver_error(f"Solver crashed: {error_str}")
        sys.exit(1)

    # 10. MGA (placeholder)
    if run_mga:
        logger.info("MGA Toggle ON: Spatial exploration logic placeholder.")

    # 11. Export
    logger.info("Exporting high-frequency Parquet results...")
    try:
        export_results(network, scenario_name)
    except Exception as e:
        import traceback
        traceback.print_exc()
        _write_solver_error(f"Export failed: {e}")
        sys.exit(1)

    logger.info(f"--- Scenario {scenario_name} Complete ---")


if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        _write_solver_error(f"Fatal Orchestrator Error: {e}")
        sys.exit(1)
