import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import json
import subprocess
import sys
import plotly.graph_objects as go
import plotly.express as px

# --- COLOR PALETTE ---
CARRIER_COLORS = {
    "solar": "#FFD700",
    "rooftop_solar": "#FFFACD",
    "wind": "#87CEEB",
    "battery": "#FF69B4",
    "gas": "#D3D3D3",
    "ocgt": "#A9A9A9",
    "ccgt": "#808080",
    "black_coal": "#000000",
    "brown_coal": "#8B4513",
    "hydro": "#1E90FF",
    "load": "#FF4500",
}

def get_color(carrier):
    c = str(carrier).lower()
    for k, v in CARRIER_COLORS.items():
        if k in c:
            return v
    return "#333333"

# --- VISUALIZATION PILLARS ---

def plot_macro_transition(caps_df, periods):
    """Pillar 1: Macro Transition (capacity mix stacked bar by period)."""
    if caps_df is None or caps_df.empty or "carrier" not in caps_df.columns:
        return None
    
    data = []
    # ...
    for p in periods:
        # Asset is active if: build_year <= period < build_year + lifetime
        # If build_year or lifetime is missing, assume it's an existing asset active in all periods
        mask = pd.Series(True, index=caps_df.index)
        if "build_year" in caps_df.columns:
            mask &= (caps_df["build_year"] <= p)
        if "lifetime" in caps_df.columns:
            mask &= (caps_df["build_year"] + caps_df["lifetime"] > p)
            
        active = caps_df[mask].groupby("carrier")["p_nom_opt"].sum().reset_index()
        active["period"] = str(p)
        data.append(active)
        
    if not data:
        return None
        
    df = pd.concat(data)
    fig = px.bar(
        df, x="period", y="p_nom_opt", color="carrier",
        title="Pillar 1: Macro Transition (Capacity Mix)",
        labels={"p_nom_opt": "Installed Capacity (MW)", "period": "Investment Period"},
        color_discrete_map={c: get_color(c) for c in df["carrier"].unique()},
        barmode="stack",
    )
    fig.update_layout(legend_title_text="Carrier")
    return fig

def plot_duck_curve(disp_df):
    """Pillar 2: Duck Curve evolution (intra-day average load shape per period)."""
    if disp_df is None or disp_df.empty:
        return None
        
    # Filter for loads
    df = disp_df[disp_df["component_type"] == "Load"].copy()
    if df.empty:
        return None
        
    # Extract hour from timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    
    # Group by period and hour, average active_power
    df_grouped = df.groupby(["period", "hour"])["active_power"].mean().reset_index()
    df_grouped["period"] = df_grouped["period"].astype(str)
    
    fig = px.line(
        df_grouped, x="hour", y="active_power", color="period",
        title="Pillar 2: Duck Curve Evolution (Avg Daily Load)",
        labels={"active_power": "Average Demand (MW)", "hour": "Hour of Day"},
        line_shape="spline",
    )
    fig.update_layout(xaxis=dict(tickmode="linear", tick0=0, dtick=2))
    return fig

def plot_operational_reality(disp_df, target_period=None):
    """Pillar 3: Operational Reality (dispatch area chart for a representative week)."""
    if disp_df is None or disp_df.empty or "carrier" not in disp_df.columns:
        return None

    # ...

        target_period = disp_df["period"].unique()[-1]
        
    df = disp_df[(disp_df["period"] == target_period) & (disp_df["component_type"] == "Generator")].copy()
    if df.empty:
        return None
        
    # Group by timestamp and carrier
    df_grouped = df.groupby(["timestamp", "carrier"])["active_power"].sum().reset_index()
    
    # Take only the first 168 hours (one week) for clarity
    unique_ts = sorted(df_grouped["timestamp"].unique())
    if len(unique_ts) > 168:
        df_grouped = df_grouped[df_grouped["timestamp"].isin(unique_ts[:168])]
        
    fig = px.area(
        df_grouped, x="timestamp", y="active_power", color="carrier",
        title=f"Pillar 3: Operational Reality ({target_period} Representative Week)",
        labels={"active_power": "Generation (MW)", "timestamp": "Time"},
        color_discrete_map={c: get_color(c) for c in df_grouped["carrier"].unique()},
    )
    return fig

def plot_policy_outcomes(econ_df, disp_df):
    """Pillar 4: Policy Outcomes (emissions + system cost trajectory)."""
    if disp_df is None or disp_df.empty:
        return None
        
    intensities = {
        "brown_coal": 1.2,
        "black_coal": 0.9,
        "ocgt": 0.6,
        "ccgt": 0.4,
        "gas": 0.55,
    }
    
    # Calculate Emissions
    df_gen = disp_df[disp_df["component_type"] == "Generator"].copy()
    if "carrier" in df_gen.columns:
        df_gen["emissions"] = df_gen.apply(
            lambda x: x["active_power"] * intensities.get(str(x["carrier"]).lower(), 0.0), axis=1
        )
        emissions = df_gen.groupby("period")["emissions"].sum() / 1e6 # MtCO2
    else:
        emissions = pd.Series(0.0, index=disp_df["period"].unique())
    
    # Calculate Costs
    # System cost = Total Annualized CAPEX + Total Marginal Cost
    costs = {}
    periods = sorted(disp_df["period"].unique())
    
    for p in periods:
        p_cost = 0
        if econ_df is not None and not econ_df.empty:
            # Active CAPEX in this period
            mask = pd.Series(True, index=econ_df.index)
            if "build_year" in econ_df.columns:
                mask &= (econ_df["build_year"] <= p)
            if "lifetime" in econ_df.columns:
                mask &= (econ_df["build_year"] + econ_df["lifetime"] > p)
            p_cost += econ_df.loc[mask, "total_annualized_capex"].sum()
            
        # Marginal cost for this period
        # We need to join with marginal_cost from econ_df
        if econ_df is not None and not econ_df.empty:
            mc_map = econ_df.set_index("component_id")["marginal_cost"].to_dict()
            df_gen_p = df_gen[df_gen["period"] == p].copy()
            df_gen_p["mc"] = df_gen_p["component_id"].map(mc_map).fillna(0.0)
            p_cost += (df_gen_p["active_power"] * df_gen_p["mc"]).sum()
            
        costs[p] = p_cost / 1e9 # B$
        
    df_policy = pd.DataFrame({
        "period": [str(p) for p in periods],
        "Emissions (MtCO2)": [emissions.get(p, 0.0) for p in periods],
        "System Cost (B$)": [costs.get(p, 0.0) for p in periods]
    })
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_policy["period"], y=df_policy["Emissions (MtCO2)"],
        name="Emissions (MtCO2)", marker_color="indianred"
    ))
    fig.add_trace(go.Scatter(
        x=df_policy["period"], y=df_policy["System Cost (B$)"],
        name="System Cost (B$)", yaxis="y2", line=dict(color="royalblue", width=4)
    ))
    
    fig.update_layout(
        title="Pillar 4: Policy Outcomes",
        yaxis=dict(title="Emissions (MtCO2)"),
        yaxis2=dict(title="System Cost (B$)", overlaying="y", side="right"),
        legend=dict(x=1.1, y=1.1)
    )
    return fig

# --- SESSION STATE INITIALIZATION ---
# ... (rest of session state logic unchanged)
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
    representative_weeks = st.slider(
        "Representative Weeks (per period)",
        min_value=1, max_value=10, value=3, step=1,
        help="Reducing weeks speeds up optimization but may miss extreme events."
    )
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
ind_electrification = 0.0
h2_annual_target = 0.0
ev_fleet_size = 0
with st.sidebar.expander("Electrification & Sector Coupling"):
    st.caption("Set penetration trajectories and endogenous conversion chains.")
    ev_penetration = st.slider("EV Volume Penetration (%)", 0.0, 100.0, 0.0, step=5.0)
    ev_fleet_size = st.number_input("EV Fleet Size (units)", value=0, step=100000)
    ind_electrification = st.slider("Industrial Electrification (%)", 0.0, 100.0, 0.0, step=5.0)
    h2_annual_target = st.number_input("Annual Hydrogen Target (MtH2)", value=0.0, step=0.1)
    rooftop_solar_penetration = st.slider("Rooftop Solar Penetration (%)", 0.0, 100.0, 0.0, step=5.0)

# --- SOLVER EXECUTION ---
if st.session_state.is_running:
    # Build investment periods list
    # Use locals().get() to safely check if the form variables are defined
    # in the current execution context.
    investment_periods = [2026]
    if locals().get("periods_2030", True):
        investment_periods.append(2030)
    if locals().get("periods_2040", True):
        investment_periods.append(2040)
    if locals().get("periods_2050", True):
        investment_periods.append(2050)

    payload = {
        "scenario_name": locals().get("scenario_name", "Multi_Period_Run"),
        "investment_periods": investment_periods,
        "representative_weeks": locals().get("representative_weeks", 3),
        "pop_growth": locals().get("pop_growth", 1.5),
        "gdp_growth": locals().get("gdp_growth", 2.0),
        "demand_elasticity": locals().get("demand_elasticity", -0.1),
        "wacc": locals().get("wacc", 7.0) / 100.0,
        "wind_capex": locals().get("wind_capex", 1.0),
        "solar_capex": locals().get("solar_capex", 1.0),
        "battery_capex": locals().get("battery_capex", 1.0),
        "gas_price": locals().get("gas_price", 10.0),
        "black_coal_price": locals().get("black_coal_price", 8.0),
        "brown_coal_price": locals().get("brown_coal_price", 5.0),
        "retirement_mode": locals().get("retirement_mode", "aemo_schedule"),
        "carbon_mode": locals().get("carbon_mode", "price_trajectory"),
        "mga_toggle": locals().get("mga_toggle", False),
        "ev_penetration": locals().get("ev_penetration", 0.0),
        "ind_electrification": locals().get("ind_electrification", 0.0),
        "rooftop_solar_penetration": locals().get("rooftop_solar_penetration", 0.0),
        "h2_annual_target_mth2": locals().get("h2_annual_target", 0.0),
        "ev_fleet_size": locals().get("ev_fleet_size", 0),
    }

    # Add carbon-mode-specific params
    c_mode = payload["carbon_mode"]
    if c_mode == "price_trajectory":
        payload["carbon_prices"] = {
            "2030": locals().get("carbon_price_2030", 0.0),
            "2040": locals().get("carbon_price_2040", 0.0),
            "2050": locals().get("carbon_price_2050", 0.0),
        }
        payload["carbon_price"] = payload["carbon_prices"]["2030"]
    else:
        payload["carbon_budget_mt"] = locals().get("carbon_budget_mt", 800.0)
        payload["carbon_price"] = 0.0

    if locals().get("regional_params"):
        payload["regional_params"] = regional_params

    from ispypsa.nextgen.runners.async_worker import run_optimization_task, run_local_async
    
    # Attempt to submit to Celery first, fallback to local thread if Redis is missing
    try:
        task = run_optimization_task.delay(payload)
        st.session_state.task_id = task.id
        st.session_state.is_celery = True
    except Exception as e:
        st.warning("⚠️ Redis not found. Running in local background thread instead.")
        task_id = run_local_async(payload)
        st.session_state.task_id = task_id
        st.session_state.is_celery = False
        
    st.session_state.is_running = False
    st.rerun()

# --- Async Status Fragment ---
if hasattr(st.session_state, "task_id"):
    @st.fragment(run_every="5s")
    def poll_task_status():
        from ispypsa.nextgen.runners.async_worker import app as celery_app, get_local_status
        
        if st.session_state.get("is_celery", False):
            res = celery_app.AsyncResult(st.session_state.task_id)
            state, info = res.state, res.info
        else:
            status = get_local_status(st.session_state.task_id)
            state, info = status["state"], status["info"]
        
        if state == 'PROGRESS':
            st.info(f"⏳ Optimization in progress: {info.get('message', 'Working...')}")
        elif state == 'SUCCESS':
            st.success("✅ Optimization Complete!")
            if st.button("Refresh Results"):
                del st.session_state.task_id
                st.rerun()
        elif state == 'FAILURE':
            st.error(f"❌ Optimization failed: {info}")
            if st.button("Clear Error"):
                del st.session_state.task_id
                st.rerun()
                
    poll_task_status()


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

if not available_scenarios:
    st.warning("No scenario results found in `results_export/`. Run an optimization to generate data.")
    st.stop()

selected_scenarios = st.multiselect(
    "Select scenarios to evaluate:",
    options=available_scenarios,
    default=available_scenarios[:1],
)

if not selected_scenarios:
    st.info("Select at least one scenario to display performance metrics.")
else:
    for scen in selected_scenarios:
        data = load_data(scen)
        caps = data.get("capacities")
        disp = data.get("dispatch")
        econ = data.get("economics")

        if disp is None or disp.empty:
            st.error(f"No dispatch data found for scenario: {scen}")
            continue

        periods = sorted(disp["period"].unique())

        st.header(f"🚀 Scenario Analysis: {scen}")

        # --- KPI RIBBON ---
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        tot_capacity = "N/A"
        tot_capex = "N/A"
        emissions = "N/A"
        ren_pen = "N/A"

        if caps is not None and not caps.empty:
            last_p = periods[-1]
            mask = pd.Series(True, index=caps.index)
            if "build_year" in caps.columns: mask &= (caps["build_year"] <= last_p)
            if "lifetime" in caps.columns: mask &= (caps["build_year"] + caps["lifetime"] > last_p)
            total_mw = caps.loc[mask, "p_nom_opt"].sum()
            tot_capacity = f"{total_mw / 1000:,.1f} GW"

        if econ is not None and not econ.empty:
            total_capex_val = econ["total_annualized_capex"].sum()
            tot_capex = f"${total_capex_val / 1e9:,.2f}B"

        if disp is not None and not disp.empty:
            gen_mask = disp["component_type"] == "Generator"
            total_gen = disp[gen_mask]["active_power"].sum()
            if total_gen > 0:
                # Add safety check for 'carrier' column
                if "carrier" in disp.columns:
                    renewables = disp[gen_mask & disp["carrier"].str.lower().isin(["wind", "solar", "hydro"])]
                    ren_gen = renewables["active_power"].sum()
                    ren_pen = f"{(ren_gen / total_gen) * 100:.1f}%"
                    
                    # Emissions for last period
                    intensities = {"brown_coal": 1.2, "black_coal": 0.9, "ocgt": 0.6, "ccgt": 0.4, "gas": 0.55}
                    df_last = disp[gen_mask & (disp["period"] == last_p)].copy()
                    df_last["e"] = df_last.apply(lambda x: x["active_power"] * intensities.get(str(x["carrier"]).lower(), 0.0), axis=1)
                    emissions = f"{df_last['e'].sum() / 1e6:.1f} Mt"
                else:
                    ren_pen = "N/A (Re-run Scenario)"
                    emissions = "N/A (Re-run Scenario)"

        with col1: st.metric("Final Period Emissions", emissions)
        with col2: st.metric("Final System Capacity", tot_capacity)
        with col3: st.metric("Total Annualized CAPEX", tot_capex)
        with col4: st.metric("Avg Renewable Share", ren_pen)
        with col5: st.metric("Investment Periods", len(periods))
        with col6: st.metric("Target Year", periods[-1])

        # --- 4-PILLAR DASHBOARD GRID ---
        c1, c2 = st.columns(2)
        with c1:
            fig1 = plot_macro_transition(caps, periods)
            if fig1: st.plotly_chart(fig1, use_container_width=True, key=f"macro_{scen}")

            fig3 = plot_operational_reality(disp)
            if fig3: st.plotly_chart(fig3, use_container_width=True, key=f"ops_{scen}")

        with c2:
            fig2 = plot_duck_curve(disp)
            if fig2: st.plotly_chart(fig2, use_container_width=True, key=f"duck_{scen}")

            fig4 = plot_policy_outcomes(econ, disp)
            if fig4: st.plotly_chart(fig4, use_container_width=True, key=f"policy_{scen}")

        st.divider()