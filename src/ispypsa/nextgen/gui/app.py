import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import json
import subprocess
import sys

# --- SESSION STATE INITIALIZATION ---
if "is_running" not in st.session_state:
    st.session_state.is_running = False
if "form_preset" not in st.session_state:
    st.session_state.form_preset = None  # Will be set below after PRESETS definition

st.set_page_config(
    page_title="NEM Transition Pathway Command Center",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- NGFS PRESETS ---
PRESETS = {
    "Current Policies": {
        "target_year": 2030,
        "pop_growth": 1.5,
        "gdp_growth": 2.0,
        "demand_elasticity": -0.1,
        "wind_capex": 1.0,
        "solar_capex": 1.0,
        "battery_capex": 1.0,
        "gas_price": 10.0,
        "black_coal_price": 8.0,
        "brown_coal_price": 5.0,
        "carbon_price": 0.0,
        "wacc": 7.0,
        "retirement_mode": "aemo_schedule",
        "carbon_mode": "price_trajectory",
        "mga_toggle": False,
    },
    "Delayed Transition": {
        "target_year": 2040,
        "pop_growth": 1.2,
        "gdp_growth": 1.8,
        "demand_elasticity": -0.2,
        "wind_capex": 0.8,
        "solar_capex": 0.8,
        "battery_capex": 0.7,
        "gas_price": 12.0,
        "black_coal_price": 10.0,
        "brown_coal_price": 7.0,
        "carbon_price": 20.0,
        "wacc": 6.0,
        "retirement_mode": "aemo_schedule",
        "carbon_mode": "price_trajectory",
        "mga_toggle": False,
    },
    "Net Zero 2050": {
        "target_year": 2050,
        "pop_growth": 1.0,
        "gdp_growth": 2.5,
        "demand_elasticity": -0.4,
        "wind_capex": 0.5,
        "solar_capex": 0.4,
        "battery_capex": 0.4,
        "gas_price": 15.0,
        "black_coal_price": 15.0,
        "brown_coal_price": 10.0,
        "carbon_price": 100.0,
        "wacc": 5.0,
        "retirement_mode": "aemo_schedule",
        "carbon_mode": "cumulative_budget",
        "mga_toggle": True,
    },
}

if st.session_state.form_preset is None:
    st.session_state.form_preset = PRESETS["Current Policies"].copy()


def set_preset(preset_name):
    st.session_state.form_preset = PRESETS[preset_name].copy()


# --- SIDEBAR: SCENARIO BUILDER ---
st.sidebar.title("NEM Transition Pathway Planner")
st.sidebar.markdown("### NGFS Policy Presets")

col1, col2, col3 = st.sidebar.columns(3)
with col1:
    if st.button("Current Policies"):
        set_preset("Current Policies")
with col2:
    if st.button("Delayed Transition"):
        set_preset("Delayed Transition")
with col3:
    if st.button("Net Zero 2050"):
        set_preset("Net Zero 2050")

s = st.session_state.form_preset

with st.sidebar.form("scenario_form"):
    st.markdown("### Scenario Configuration")
    scenario_name = st.text_input("Scenario Name", value="Multi_Period_Run")

    # --- Investment Horizon ---
    st.markdown("#### Investment Horizon")
    periods_2030 = st.checkbox("Include 2030", value=True)
    periods_2040 = st.checkbox("Include 2040", value=True)
    periods_2050 = st.checkbox("Include 2050", value=True)

    # --- Macro Drivers ---
    st.markdown("#### Macroeconomic Drivers")
    pop_growth = st.number_input("Population Growth (%)", value=float(s["pop_growth"]), step=0.1)
    gdp_growth = st.number_input("Real GDP Growth (%)", value=float(s["gdp_growth"]), step=0.1)
    demand_elasticity = st.number_input("Demand Elasticity", value=float(s["demand_elasticity"]), step=0.1)

    # --- WACC (Module 3) ---
    st.markdown("#### Capital Cost Parameters")
    wacc = st.slider("Weighted Average Cost of Capital (%)", min_value=3.0, max_value=12.0,
                      value=float(s.get("wacc", 7.0)), step=0.5)

    # --- Cost Sensitivities ---
    st.markdown("#### Technology Cost Multipliers")
    wind_capex = st.number_input("Wind CAPEX Multiplier", value=float(s["wind_capex"]), step=0.1)
    solar_capex = st.number_input("Solar CAPEX Multiplier", value=float(s["solar_capex"]), step=0.1)
    battery_capex = st.number_input("Battery CAPEX Multiplier", value=float(s["battery_capex"]), step=0.1)

    # --- Fuel Prices ---
    st.markdown("#### Fuel Prices")
    gas_price = st.number_input("Gas Price ($/GJ)", value=float(s["gas_price"]), step=1.0)
    black_coal_price = st.number_input("Black Coal Price ($/GJ)", value=float(s["black_coal_price"]), step=1.0)
    brown_coal_price = st.number_input("Brown Coal Price ($/GJ)", value=float(s["brown_coal_price"]), step=1.0)

    # --- Brownfield Retirement (Module 2) ---
    st.markdown("#### Coal & Gas Retirement")
    retirement_mode = st.radio(
        "Retirement Logic",
        options=["aemo_schedule", "economic"],
        format_func=lambda x: "Enforce AEMO ISP Schedule" if x == "aemo_schedule" else "Economic Retirement (Market-Driven)",
        index=0 if s.get("retirement_mode", "aemo_schedule") == "aemo_schedule" else 1,
    )

    # --- Carbon Mechanism (Module 4) ---
    st.markdown("#### Carbon Policy")
    carbon_mode = st.radio(
        "Emission Constraint Method",
        options=["price_trajectory", "cumulative_budget"],
        format_func=lambda x: "Carbon Price Trajectory ($/tCO₂)" if x == "price_trajectory" else "Cumulative Carbon Budget (MtCO₂)",
        index=0 if s.get("carbon_mode", "price_trajectory") == "price_trajectory" else 1,
    )

    if carbon_mode == "price_trajectory":
        carbon_price_2030 = st.number_input("Carbon Price 2030 ($/tCO₂)", value=float(s.get("carbon_price", 0.0)), step=5.0)
        carbon_price_2040 = st.number_input("Carbon Price 2040 ($/tCO₂)", value=float(s.get("carbon_price", 0.0)) * 1.5, step=5.0)
        carbon_price_2050 = st.number_input("Carbon Price 2050 ($/tCO₂)", value=float(s.get("carbon_price", 0.0)) * 2.5, step=5.0)
    else:
        carbon_budget_mt = st.number_input("Total NEM CO₂ Budget 2026–2050 (MtCO₂)", value=800.0, step=50.0)

    # --- Analysis Options ---
    st.markdown("#### Analysis Options")
    mga_toggle = st.checkbox("Run Spatial Exploration (Increases solution time)", value=s["mga_toggle"])

    submitted = st.form_submit_button(
        "Run NEM Optimization",
        type="primary",
        disabled=st.session_state.is_running,
    )

    if submitted:
        st.session_state.is_running = True
        st.rerun()

# --- Regional Heterogeneity Expander (Module 1) ---
regional_params = None
with st.sidebar.expander("Enable Regional Heterogeneity"):
    enable_regional = st.checkbox("Use per-state growth assumptions", value=False)
    if enable_regional:
        st.caption("Override macroeconomic drivers per NEM region.")
        regional_params = {}
        for region in ["NSW", "QLD", "VIC", "SA", "TAS"]:
            st.markdown(f"**{region}**")
            c1, c2 = st.columns(2)
            with c1:
                rpop = st.number_input(f"{region} Pop Growth (%)", value=pop_growth, step=0.1, key=f"rpop_{region}")
            with c2:
                rgdp = st.number_input(f"{region} GDP Growth (%)", value=gdp_growth, step=0.1, key=f"rgdp_{region}")
            regional_params[region] = {"pop_growth": rpop, "gdp_growth": rgdp}

# --- Electrification Profiles Expander (Module 1) ---
ev_penetration = 0.0
rooftop_solar_penetration = 0.0
with st.sidebar.expander("Electrification & Profile Shaping"):
    st.caption("Set penetration trajectories for demand profile decomposition.")
    ev_penetration = st.slider("EV Penetration (%)", 0.0, 100.0, 0.0, step=5.0)
    rooftop_solar_penetration = st.slider("Rooftop Solar Penetration (%)", 0.0, 100.0, 0.0, step=5.0)

# --- SOLVER EXECUTION ---
if st.session_state.is_running:
    # Build investment periods list
    investment_periods = [2026]
    if periods_2030:
        investment_periods.append(2030)
    if periods_2040:
        investment_periods.append(2040)
    if periods_2050:
        investment_periods.append(2050)

    payload = {
        "scenario_name": scenario_name,
        "investment_periods": investment_periods,
        "pop_growth": pop_growth,
        "gdp_growth": gdp_growth,
        "demand_elasticity": demand_elasticity,
        "wacc": wacc / 100.0,  # Convert % to decimal
        "wind_capex": wind_capex,
        "solar_capex": solar_capex,
        "battery_capex": battery_capex,
        "gas_price": gas_price,
        "black_coal_price": black_coal_price,
        "brown_coal_price": brown_coal_price,
        "retirement_mode": retirement_mode,
        "carbon_mode": carbon_mode,
        "mga_toggle": mga_toggle,
        "ev_penetration": ev_penetration,
        "rooftop_solar_penetration": rooftop_solar_penetration,
    }

    # Add carbon-mode-specific params
    if carbon_mode == "price_trajectory":
        payload["carbon_prices"] = {
            "2030": carbon_price_2030,
            "2040": carbon_price_2040,
            "2050": carbon_price_2050,
        }
        payload["carbon_price"] = carbon_price_2030  # Fallback flat price
    else:
        payload["carbon_budget_mt"] = carbon_budget_mt
        payload["carbon_price"] = 0.0

    # Add regional params if enabled
    if regional_params:
        payload["regional_params"] = regional_params

    payload_path = Path("scenario_payload.json")
    with open(payload_path, "w") as f:
        json.dump(payload, f, indent=4)

    # Remove any stale error file
    error_file = Path("solver_error.json")
    if error_file.exists():
        error_file.unlink()

    # --- Async Subprocess with Progressive Status ---
    status_container = st.status("Initializing multi-period optimization...", expanded=True)

    with status_container:
        st.write("📦 Assembling scenario payload...")
        st.write(f"📊 Investment periods: {investment_periods}")
        st.write(f"💰 WACC: {wacc:.1f}%  |  Retirement: {retirement_mode}")

        runner_script = "src/ispypsa/nextgen/runners/scenario_orchestrator.py"

        st.write("⚙️ Building network and launching Gurobi...")

        result = subprocess.run(
            [sys.executable, runner_script, str(payload_path)],
            capture_output=True,
            text=True,
        )

    st.session_state.is_running = False

    if result.returncode != 0:
        # Check for structured error file
        if error_file.exists():
            with open(error_file) as ef:
                err_data = json.load(ef)
            error_msg = err_data.get("message", "Unknown error")

            if "infeasible" in error_msg.lower():
                st.error(
                    "⚠️ **Scenario Infeasible** — The model could not find a feasible solution. "
                    "This typically means the carbon budget is too restrictive, demand exceeds "
                    "available generation capacity, or retirement assumptions leave insufficient supply."
                )
            else:
                st.error(f"Optimization failed: {error_msg}")
        else:
            st.error("Optimization failed to find a solution or crashed.")

        if result.stderr:
            with st.expander("Solver Log (stderr)"):
                st.code(result.stderr, language="log")
        if result.stdout:
            with st.expander("Solver Log (stdout)"):
                st.code(result.stdout, language="log")
        st.stop()
    else:
        status_container.update(label="Optimization complete!", state="complete")
        st.success(f"✅ Multi-period optimization completed for '{scenario_name}'.")
        st.rerun()


# --- DATA LOADING & DISCOVERY ---
@st.cache_data
def discover_scenarios(export_dir: str = "results_export"):
    """Scan results_export for scenario Parquet files."""
    dir_path = Path(export_dir)
    if not dir_path.exists():
        return []
    scenarios = set()
    for pattern in ["spatial_capacities_*.parquet", "capacities_*.parquet"]:
        for file in dir_path.rglob(pattern):
            scen_id = file.stem.replace("spatial_capacities_", "").replace("capacities_", "")
            scenarios.add(scen_id)
    for pattern in ["dispatch_profiles_*.parquet", "dispatch_*.parquet"]:
        for file in dir_path.rglob(pattern):
            scen_id = file.stem.replace("dispatch_profiles_", "").replace("dispatch_", "")
            scenarios.add(scen_id)
    return sorted(list(scenarios))


@st.cache_data
def load_data(scenario_id: str, export_dir: str = "results_export"):
    """Load capacities and dispatch Parquet files for a scenario."""
    dir_path = Path(export_dir)
    data = {"capacities": None, "dispatch": None, "economics": None}
    try:
        for pattern in [f"spatial_capacities_{scenario_id}.parquet", f"capacities_{scenario_id}.parquet"]:
            files = list(dir_path.rglob(pattern))
            if files:
                data["capacities"] = pd.read_parquet(files[0])
                break
        for pattern in [f"dispatch_profiles_{scenario_id}.parquet", f"dispatch_{scenario_id}.parquet"]:
            files = list(dir_path.rglob(pattern))
            if files:
                data["dispatch"] = pd.read_parquet(files[0])
                break
        econ_files = list(dir_path.rglob(f"system_economics_{scenario_id}.parquet"))
        if econ_files:
            data["economics"] = pd.read_parquet(econ_files[0])
    except Exception as e:
        st.error(f"Error loading files for {scenario_id}: {e}")
    return data


# --- MAIN DASHBOARD ---
st.title("NEM Transition Pathway Command Center")

available_scenarios = discover_scenarios()

st.markdown("### Scenario Comparison")

if not available_scenarios:
    st.warning("No scenario results found in `results_export/`. Run an optimization to generate data.")

selected_scenarios = st.multiselect(
    "Select scenarios to evaluate:",
    options=available_scenarios,
    default=available_scenarios[:2] if len(available_scenarios) >= 2 else available_scenarios,
)

# --- KPI RIBBON ---
st.markdown("### Strategic Performance Indicators")

if not selected_scenarios:
    st.info("Select at least one scenario to display performance metrics.")
else:
    for scen in selected_scenarios:
        data = load_data(scen)

        st.markdown(f"**Scenario: {scen}**")

        col1, col2, col3, col4, col5, col6 = st.columns(6)

        caps = data.get("capacities")
        disp = data.get("dispatch")
        econ = data.get("economics")

        sys_lcoe = "N/A"
        tot_capacity = "N/A"
        tot_capex = "N/A"
        emissions = "N/A"
        peak_load = "N/A"
        ren_pen = "N/A"

        if caps is not None and not caps.empty:
            if "p_nom_opt" in caps.columns:
                total_mw = caps["p_nom_opt"].sum()
                tot_capacity = f"{total_mw / 1000:,.1f} GW"

        if econ is not None and not econ.empty:
            if "total_annualized_capex" in econ.columns:
                total_capex_val = econ["total_annualized_capex"].sum()
                tot_capex = f"${total_capex_val / 1e9:,.2f}B"

        if disp is not None and not disp.empty:
            total_gen = disp[disp["component_type"] == "Generator"]["active_power"].sum()
            if "carrier" in disp.columns and total_gen > 0:
                renewables = disp[disp["carrier"].str.lower().isin(["wind", "solar", "hydro"])]
                ren_gen = renewables["active_power"].sum()
                ren_pen = f"{(ren_gen / total_gen) * 100:.1f}%"

        with col1:
            st.metric("System LCOE ($/MWh)", sys_lcoe)
        with col2:
            st.metric("Total Capacity", tot_capacity)
        with col3:
            st.metric("Total Annual CAPEX", tot_capex)
        with col4:
            st.metric("Emissions", emissions)
        with col5:
            st.metric("Peak Residual Load", peak_load)
        with col6:
            st.metric("Renewable Share", ren_pen)

        st.divider()