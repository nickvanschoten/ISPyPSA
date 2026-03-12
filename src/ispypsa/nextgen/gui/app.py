import streamlit as st
import pandas as pd
from pathlib import Path
import json
import subprocess
import sys

# --- SESSION STATE & FLAGS ---
if "is_running" not in st.session_state:
    st.session_state.is_running = False
import pandas as pd
from pathlib import Path
import json
import subprocess
import sys

st.set_page_config(page_title="NEM Scenario Command Center", layout="wide", initial_sidebar_state="expanded")

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
        "mga_toggle": False
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
        "mga_toggle": False
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
        "mga_toggle": True
    }
}

# --- SESSION STATE INITIALIZATION ---
if "form_preset" not in st.session_state:
    st.session_state.form_preset = PRESETS["Current Policies"].copy()

def set_preset(preset_name):
    st.session_state.form_preset = PRESETS[preset_name].copy()

# --- SIDEBAR: SCENARIO BUILDER ---
st.sidebar.title("Infrastructure Planning Scenarios")
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
    st.markdown("### Configuration")
    scenario_name = st.text_input("Custom Scenario Name", value="Custom_Run")
    
    target_year = st.selectbox(
        "Target Year", 
        options=[2030, 2040, 2050], 
        index=[2030, 2040, 2050].index(s["target_year"])
    )
    
    st.markdown("#### Macro Drivers")
    pop_growth = st.number_input("Population Growth (%)", value=float(s["pop_growth"]), step=0.1)
    gdp_growth = st.number_input("Real GDP Growth (%)", value=float(s["gdp_growth"]), step=0.1)
    demand_elasticity = st.number_input("Demand Elasticity", value=float(s["demand_elasticity"]), step=0.1)
    
    st.markdown("#### Cost Sensitivities")
    wind_capex = st.number_input("Wind/Solar/Battery CAPEX Multiplier", value=float(s["wind_capex"]), step=0.1)
    solar_capex = st.number_input("Solar CAPEX Multiplier", value=float(s["solar_capex"]), step=0.1)
    battery_capex = st.number_input("Battery CAPEX Multiplier", value=float(s["battery_capex"]), step=0.1)
    
    gas_price = st.number_input("Gas Price ($/GJ)", value=float(s["gas_price"]), step=1.0)
    black_coal_price = st.number_input("Black Coal Price ($/GJ)", value=float(s["black_coal_price"]), step=1.0)
    brown_coal_price = st.number_input("Brown Coal Price ($/GJ)", value=float(s["brown_coal_price"]), step=1.0)
    carbon_price = st.number_input("Carbon Price ($/tCO2e)", value=float(s["carbon_price"]), step=5.0)
    
    st.markdown("#### Analysis Options")
    mga_toggle = st.checkbox("Run Spatial Exploration (Increases solution time)", value=s["mga_toggle"])
    
    submitted = st.form_submit_button(
        "Run Regional Optimization", 
        type="primary", 
        disabled=st.session_state.is_running
    )

    if submitted:
        st.session_state.is_running = True
        st.rerun()

if st.session_state.is_running:
        # Step 1: Just UI shell, so we print a mock success message
        # Step 3: Connect UI to PyPSA Backend MVP
        payload = {
            "scenario_name": scenario_name,
            "target_year": target_year,
            "pop_growth": pop_growth,
            "gdp_growth": gdp_growth,
            "demand_elasticity": demand_elasticity,
            "wind_capex": wind_capex,
            "solar_capex": solar_capex,
            "battery_capex": battery_capex,
            "gas_price": gas_price,
            "black_coal_price": black_coal_price,
            "brown_coal_price": brown_coal_price,
            "carbon_price": carbon_price,
            "mga_toggle": mga_toggle
        }
        
        payload_path = Path("scenario_payload.json")
        with open(payload_path, "w") as f:
            json.dump(payload, f, indent=4)
            
        st.info(f"Payload saved to {payload_path}")
        if not payload_path.exists():
            st.error(f"Could not find generated payload at {payload_path}")
            st.stop()
            
        with st.spinner("Initializing Gurobi solver... this may take a few minutes."):
            runner_script = "src/ispypsa/nextgen/runners/scenario_orchestrator.py"
            result = subprocess.run(
                [sys.executable, runner_script, str(payload_path)],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                st.session_state.is_running = False
                st.error("Optimization failed to find a feasible solution or crashed.")
                if result.stderr:
                    st.code(result.stderr, language="log")
                elif result.stdout:
                    st.code(result.stdout, language="log")
                st.stop()
            else:
                st.session_state.is_running = False
                st.success(f"Execution completed for '{scenario_name}'.")
                st.rerun()

# --- DATA LOADING & DISCOVERY ---
@st.cache_data
def discover_scenarios(export_dir: str = "results_export"):
    """
    Scans the results_export directory for capacities_*.parquet and dispatch_*.parquet
    Extracts the scenario names dynamically.
    """
    dir_path = Path(export_dir)
    if not dir_path.exists():
        return []
        
    scenarios = set()
    for file in dir_path.rglob("capacities_*.parquet"):
        scen_id = file.stem.replace("capacities_", "")
        scenarios.add(scen_id)
    for file in dir_path.rglob("dispatch_*.parquet"):
        scen_id = file.stem.replace("dispatch_", "")
        scenarios.add(scen_id)
        
    return sorted(list(scenarios))

@st.cache_data
def load_data(scenario_id: str, export_dir: str = "results_export"):
    """
    Robust loading for capacities and dispatch files for a given scenario.
    """
    dir_path = Path(export_dir)
    data = {"capacities": None, "dispatch": None}
    try:
        cap_files = list(dir_path.rglob(f"capacities_{scenario_id}.parquet"))
        if cap_files:
            data["capacities"] = pd.read_parquet(cap_files[0])
            
        disp_files = list(dir_path.rglob(f"dispatch_{scenario_id}.parquet"))
        if disp_files:
            data["dispatch"] = pd.read_parquet(disp_files[0])
    except Exception as e:
        st.error(f"Error loading files for {scenario_id}: {e}")
        
    return data

# --- MAIN DASHBOARD AREA ---
st.title("NEM Scenario Command Center")

available_scenarios = discover_scenarios()

st.markdown("### Compare Compiled Scenarios")

if not available_scenarios:
    st.warning("No scenario parquet files found in `results_export/`. Please generate some data first or ensure the correct path.")

selected_scenarios = st.multiselect(
    "Select scenarios to evaluate:",
    options=available_scenarios,
    default=available_scenarios[:2] if len(available_scenarios) >= 2 else available_scenarios
)

# --- KPI RIBBON ---
st.markdown("### Strategic Performance Indicators")

if not selected_scenarios:
    st.info("Select at least one scenario from the multiselect above to display KPIs.")
else:
    for scen in selected_scenarios:
        data = load_data(scen)
        
        st.markdown(f"**Scenario: {scen}**")
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        # Calculate real metrics if data exists
        caps = data.get("capacities") if not isinstance(data.get("capacities"), type(None)) else pd.DataFrame()
        disp = data.get("dispatch") if not isinstance(data.get("dispatch"), type(None)) else pd.DataFrame()
        
        sys_lcoe = "N/A"
        tot_capex = "N/A"
        emissions = "N/A"
        peak_res_load = "N/A"
        curtailment = "N/A"
        ren_pen = "N/A"

        if not caps.empty:
            # We don't have annualized exact capital_cost in capacities output, just p_nom_opt
            # But we can sum up installed capacity to show scale
            total_mw = caps['p_nom_opt'].sum()
            tot_capex = f"{total_mw / 1000:,.1f} GW"
            
        if not disp.empty:
            # Calculate Renewable Penetration
            if "carrier" in disp.columns:
                renewables = disp[disp['carrier'].str.lower().isin(['wind', 'solar', 'hydro'])]
                total_gen = disp[disp['component_type'] == 'Generator']['active_power'].sum()
                if total_gen > 0:
                    ren_pen = f"{(renewables['active_power'].sum() / total_gen) * 100:.1f}%"
            else:
                total_gen = disp[disp['component_type'] == 'Generator']['active_power'].sum()
                if total_gen > 0:
                    ren_pen = f"{total_gen/1e6:,.1f} TWh" # Fallback show total gen

        with col1:
            st.metric("System LCOE ($/MWh)", sys_lcoe)
        with col2:
            st.metric("Total Capacity Built", tot_capex)
        with col3:
            st.metric("Emissions Intensity (MtCO2e/MWh)", emissions)
        with col4:
            st.metric("Peak Residual Load (MW)", peak_res_load)
        with col5:
            st.metric("Curtailment Rate (%)", curtailment)
        with col6:
            st.metric("Renewable Penetration", ren_pen)
            
        st.divider()