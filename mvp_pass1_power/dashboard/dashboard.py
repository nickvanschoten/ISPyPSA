"""ISPyPSA Pass 1 Power — Archetype Catalogue Dashboard.

Streamlit single-file dashboard for exploring the six production archetypes.

Dependencies (not in pyproject.toml — install separately for dashboard use):
    pip install streamlit plotly

Run:
    streamlit run mvp_pass1_power/dashboard/dashboard.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from mvp_pass1_power.postprocess.extract_dispatch_timeseries import (
    extract_dispatch_timeseries,
    find_run_dir,
    list_available_runs,
)

# ---------- constants ----------

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_DATA_DIR = str(_SCRIPT_DIR.parent / "outputs")
_DEFAULT_RUNS_DIR = str(_SCRIPT_DIR.parent / "bench" / "runs_myopic")

# Bottom-to-top order for the dispatch stack chart. Baseload thermal at the
# bottom, mid-merit dispatchable above, VRE topping off, with storage discharge
# slotted between dispatchable and VRE. Carriers not in this list are appended.
_DISPATCH_STACK_ORDER = [
    "Black Coal", "Brown Coal", "Nuclear",                 # baseload thermal
    "Biomass", "Water",                                     # low-CO2 dispatchable
    "Gas", "Hyblend", "Hydrogen", "Liquid Fuel",            # mid-merit dispatchable
    "Wind", "Solar",                                        # variable renewables
]

# Decile thresholds for peak / off-peak slicing within a representative week.
_PEAK_DECILE = 0.9
_OFFPEAK_DECILE = 0.1

# Operational-View chart tooltips — surface the residual-peak-demand-week caveat
# next to the charts where the methodological risk of misreading is highest.
# The CF profile chart deliberately has no tooltip — qualitative technology
# shapes hold across weeks, so the interpretation risk is low.
_TOOLTIP_REP_WEEK_INTRO = (
    "- Data is from ISPyPSA's **residual peak demand week** — typically a winter "
    "evening week (June/July) when NEM demand is highest. Configured in IASR as "
    "the representative period for capacity planning and stress-testing.\n"
    "- This represents the **hardest week for the grid**, not a typical week. "
    "The system operates differently across the year — lower demand in spring/"
    "autumn, different VRE patterns, more curtailment during off-peak periods "
    "of low-demand weeks.\n"
)
_TOOLTIP_DISPATCH = (
    _TOOLTIP_REP_WEEK_INTRO
    + "- **Firming and storage utilisation shown here is at its annual maximum**; "
    "typical weeks show less reliance on firming."
)
_TOOLTIP_CURTAILMENT = (
    _TOOLTIP_REP_WEEK_INTRO
    + "- **Curtailment appears low because peak demand absorbs VRE generation**. "
    "The bulk of annual curtailment occurs during off-peak hours of low-demand "
    "weeks not shown here."
)
_TOOLTIP_PEAK_OFFPEAK = (
    _TOOLTIP_REP_WEEK_INTRO
    + "- **The 'off-peak' hours within this peak week are not the system's true "
    "off-peak hours.** This comparison is within-peak-week, not across-the-year."
)

ARCHETYPE_COLORS = {
    "cost_optimal":         "#1f77b4",
    "rapid_coal_phaseout":  "#2ca02c",
    "gas_fleet_maintained": "#ff7f0e",
    "storage_led":          "#9467bd",
    "fossil_incumbent":     "#d62728",
    "nuclear_baseload":     "#8c564b",
}

PRODUCTION_ARCHETYPES = list(ARCHETYPE_COLORS.keys())

ARCHETYPE_DESCRIPTIONS = {
    "cost_optimal":         "Unconstrained least-cost expansion under AEMO 2024 IASR Step Change.",
    "rapid_coal_phaseout":  "Coal retired by 2030; gas remains available; LP decides on cost.",
    "gas_fleet_maintained": "Coal retired by 2030; gas held ≥ 12,500 MW at 2030 and 2035.",
    "storage_led":          "Coal by 2035; no gas (incl. CCS); storage ≥ 1.25× AEMO trajectory per year.",
    "fossil_incumbent":     "Extended coal life; constrained renewable build pathway (MGA upper bound).",
    "nuclear_baseload":     "Coalition 2024 phased nuclear: ≥ 2,000 MW @ 2045, ≥ 4,000 MW @ 2050.",
}

TECHNOLOGY_COLORS = {
    "Black Coal":  "#333333",
    "Brown Coal":  "#8B4513",
    "Gas":         "#FF8C00",
    "Liquid Fuel": "#DAA520",
    "Biomass":     "#228B22",
    "Hydrogen":    "#00BFFF",
    "Hyblend":     "#87CEEB",
    "Biomethane":  "#90EE90",
    "Nuclear":     "#9400D3",
    "Wind":        "#32CD32",
    "Solar":       "#FFD700",
    "Water":       "#1E90FF",
    "Storage":     "#708090",
}

# AEMO Overview calibration reference values (TWh)
_AEMO_OVERVIEW_2030_TWH = 202.0
_AEMO_OVERVIEW_2050_TWH = 313.0


# ---------- data loading ----------


def _load_csv(path: Path, label: str) -> pd.DataFrame | None:
    """Load a CSV, show a warning and return None if missing."""
    if not path.exists():
        st.warning(f"File not found: {path}")
        return None
    return pd.read_csv(path)


def _load_method_years(data_dir: Path) -> pd.DataFrame | None:
    return _load_csv(data_dir / "simple_msm" / "method_years.csv", "method_years.csv")


def _load_granular(data_dir: Path, name: str) -> pd.DataFrame | None:
    return _load_csv(data_dir / "granular" / name, name)


# ---------- GWP recomputation ----------


def _recompute_carbon_intensity(
    method_years: pd.DataFrame, gwp: dict[str, float]
) -> pd.Series:
    """Recompute tCO2e/MWh from physical masses using the chosen GWP basis.

    Falls back to the stored total_CO2e column if physical-mass columns are absent.
    """
    diag_path_cols = ["ch4_physical_kg_per_mwh", "n2o_physical_kg_per_mwh", "co2_kg_per_mwh"]
    if all(c in method_years.columns for c in diag_path_cols):
        co2e = (
            method_years["co2_kg_per_mwh"] / 1000.0
            + method_years["ch4_physical_kg_per_mwh"] / 1000.0 * gwp["CH4"]
            + method_years["n2o_physical_kg_per_mwh"] / 1000.0 * gwp["N2O"]
        )
        return co2e
    # Fallback: parse total_CO2e from the JSON-serialised energy_emissions_by_pollutant column.
    import json

    def _extract_total(val):
        try:
            return json.loads(val).get("total_CO2e", float("nan"))
        except Exception:
            return float("nan")

    return method_years["energy_emissions_by_pollutant"].apply(_extract_total)


def _archetype_from_method_id(method_id: str) -> str:
    """Derive archetype_id from method_id by stripping prefix.

    Normalises aliases so names match the granular CSVs:
      cost_optimal_baseline → cost_optimal
    """
    prefix = "electricity__grid_supply__"
    if isinstance(method_id, str) and method_id.startswith(prefix):
        raw = method_id[len(prefix):]
        return {"cost_optimal_baseline": "cost_optimal"}.get(raw, raw)
    return method_id


# ---------- chart builders ----------


def _line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_map: dict,
    title: str,
    yaxis_title: str,
) -> go.Figure:
    """Generic multi-series line chart."""
    fig = go.Figure()
    for group, grp_df in df.groupby(color_col):
        grp_df = grp_df.sort_values(x_col)
        fig.add_trace(go.Scatter(
            x=grp_df[x_col],
            y=grp_df[y_col],
            name=group,
            mode="lines+markers",
            line={"color": color_map.get(group, "#aaaaaa")},
        ))
    fig.update_layout(
        title=title,
        xaxis_title="Year",
        yaxis_title=yaxis_title,
        legend_title=color_col,
        height=380,
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
    )
    return fig


def _stacked_area(
    df: pd.DataFrame,
    year_col: str,
    value_col: str,
    tech_col: str,
    title: str,
    yaxis_title: str,
) -> go.Figure:
    """Stacked area chart grouped by technology."""
    fig = go.Figure()
    for tech in sorted(df[tech_col].unique()):
        tdf = df[df[tech_col] == tech].sort_values(year_col)
        fig.add_trace(go.Scatter(
            x=tdf[year_col],
            y=tdf[value_col],
            name=tech,
            stackgroup="one",
            mode="lines",
            line={"color": TECHNOLOGY_COLORS.get(tech, "#aaaaaa"), "width": 0},
            fillcolor=TECHNOLOGY_COLORS.get(tech, "#aaaaaa"),
        ))
    fig.update_layout(
        title=title,
        xaxis_title="Year",
        yaxis_title=yaxis_title,
        height=380,
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
    )
    return fig


_FUEL_LABEL = {
    "coal": "Coal",
    "natural_gas": "Natural Gas",
    "biomass": "Biomass",
    "diesel": "Diesel",
    "hydrogen": "Hydrogen",
    "biomethane": "Biomethane",
}

_FUEL_DISPLAY_ORDER = ["coal", "natural_gas", "biomass", "diesel", "hydrogen", "biomethane"]


def _energy_intensity_chart(ei: pd.DataFrame, color_map: dict) -> go.Figure:
    """Small-multiples subplot: one panel per active fuel, per-archetype lines."""
    active_fuels = [f for f in _FUEL_DISPLAY_ORDER
                    if f in ei["fuel"].values and (ei[ei["fuel"] == f]["gj_per_mwh_delivered"] > 0.001).any()]
    n = len(active_fuels)
    if n == 0:
        return go.Figure()

    cols = min(n, 3)
    rows = (n + cols - 1) // cols
    fig = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=[_FUEL_LABEL.get(f, f) for f in active_fuels],
        shared_xaxes=False,
    )
    for idx, fuel in enumerate(active_fuels):
        r, c = divmod(idx, cols)
        fuel_df = ei[ei["fuel"] == fuel]
        for arch, adf in fuel_df.groupby("archetype"):
            if arch not in color_map:
                continue
            adf = adf.sort_values("year")
            fig.add_trace(
                go.Scatter(
                    x=adf["year"], y=adf["gj_per_mwh_delivered"],
                    name=arch, legendgroup=arch,
                    showlegend=(idx == 0),
                    mode="lines+markers",
                    line={"color": color_map[arch]},
                ),
                row=r + 1, col=c + 1,
            )
        fig.update_yaxes(title_text="GJ/MWh", row=r + 1, col=c + 1)
        fig.update_xaxes(title_text="Year", row=r + 1, col=c + 1)

    fig.update_layout(
        title="Energy intensity by fuel (GJ per MWh delivered to load)",
        height=340 * rows,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    return fig


def _build_intensity_export(
    diag: pd.DataFrame,
    ren: pd.DataFrame | None,
    my: pd.DataFrame | None,
    ei: pd.DataFrame | None,
) -> pd.DataFrame:
    """Single flat CSV with all intensity metrics for orchestrator consumption."""
    base = diag[["archetype_id", "year",
                 "output_cost_per_unit_excl_fuel_AUD_per_MWh", "carbon_intensity"]].copy()
    base.rename(columns={
        "output_cost_per_unit_excl_fuel_AUD_per_MWh": "output_cost_per_unit_aud_per_mwh",
        "carbon_intensity": "carbon_intensity_tco2e_per_mwh",
    }, inplace=True)

    if ren is not None:
        base = base.merge(
            ren[["archetype_id", "year", "renewable_share_pct"]],
            on=["archetype_id", "year"], how="left",
        )

    if my is not None:
        base = base.merge(
            my[["archetype_id", "year", "max_share"]],
            on=["archetype_id", "year"], how="left",
        )

    if ei is not None:
        ei_pivot = ei.pivot_table(
            index=["archetype", "year"], columns="fuel",
            values="gj_per_mwh_delivered", fill_value=0.0,
        ).reset_index()
        ei_pivot.rename(columns={"archetype": "archetype_id"}, inplace=True)
        fuel_cols = {c: f"{c}_gj_per_mwh" for c in ei_pivot.columns if c not in ("archetype_id", "year")}
        ei_pivot.rename(columns=fuel_cols, inplace=True)
        base = base.merge(ei_pivot, on=["archetype_id", "year"], how="left")

    return base.sort_values(["archetype_id", "year"]).reset_index(drop=True)


# ---------- section renderers ----------


def _render_intensity_curves(data_dir: Path, gwp: dict) -> None:
    st.subheader("Section 1 — Intensity Curves")
    st.caption(
        "All production archetypes. Carbon intensity recomputed from physical-mass CH4/N2O "
        "using the GWP basis selected in the sidebar. Renewable share = Wind + Solar + Biomass "
        "(Water/hydro excluded — see Methodology tab)."
    )

    my = _load_method_years(data_dir)
    diag = _load_csv(data_dir / "simple_msm" / "diagnostics.csv", "diagnostics.csv")
    ren = _load_csv(data_dir / "granular" / "renewable_share.csv", "renewable_share.csv")
    ei = _load_csv(data_dir / "granular" / "energy_intensity_by_fuel.csv",
                   "energy_intensity_by_fuel.csv")

    if my is not None:
        my["archetype_id"] = my["method_id"].apply(_archetype_from_method_id)

    if diag is not None:
        diag["archetype_id"] = diag["method_id"].apply(_archetype_from_method_id)
        diag["carbon_intensity"] = _recompute_carbon_intensity(diag, gwp)

    # Row 1: cost + carbon
    if diag is not None:
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(
                _line_chart(diag, "year", "output_cost_per_unit_excl_fuel_AUD_per_MWh",
                            "archetype_id", ARCHETYPE_COLORS,
                            "Cost intensity (excl. fuel)", "AUD/MWh"),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                _line_chart(diag, "year", "carbon_intensity",
                            "archetype_id", ARCHETYPE_COLORS,
                            "Carbon intensity", "tCO2e/MWh"),
                use_container_width=True,
            )

    # Row 2: renewable share + max share
    col3, col4 = st.columns(2)
    with col3:
        if ren is not None:
            st.plotly_chart(
                _line_chart(ren, "year", "renewable_share_pct",
                            "archetype_id", ARCHETYPE_COLORS,
                            "Renewable share (Wind + Solar + Biomass)", "%"),
                use_container_width=True,
            )
    with col4:
        if my is not None:
            my_ms = my.copy()
            my_ms["max_share_pct"] = my_ms["max_share"].fillna(1.0) * 100.0
            st.plotly_chart(
                _line_chart(my_ms, "year", "max_share_pct",
                            "archetype_id", ARCHETYPE_COLORS,
                            "Max share (deployment ceiling)", "%"),
                use_container_width=True,
            )
            st.caption(
                "All archetypes assigned max_share = 1.0 (no deployment ceiling declared). "
                "These are author-supplied bounds in ARCHETYPE_CATALOGUE, not LP-derived. "
                "See Configuration Notes tab for methodology notes on what max_share means and when it "
                "should be differentiated across archetypes."
            )

    # Row 3: energy intensity by fuel (small multiples)
    if ei is not None:
        st.plotly_chart(_energy_intensity_chart(ei, ARCHETYPE_COLORS), use_container_width=True)

    # Download: unified flat CSV with all metrics
    if diag is not None:
        export_df = _build_intensity_export(diag, ren, my, ei)
        st.download_button(
            "Download intensity curves CSV",
            export_df.to_csv(index=False).encode(),
            "intensity_curves.csv", "text/csv",
        )


def _render_granular(data_dir: Path, selected_archetypes: list[str]) -> None:
    st.subheader("Section 2 — Granular Results")

    cap = _load_granular(data_dir, "capacity_gw.csv")
    gen = _load_granular(data_dir, "generation_twh.csv")
    sto = _load_granular(data_dir, "storage_capacity.csv")
    dg = _load_granular(data_dir, "demand_generation.csv")
    cf = _load_granular(data_dir, "capacity_factors.csv")

    for arch in selected_archetypes:
        st.markdown(f"**{arch}** — {ARCHETYPE_DESCRIPTIONS.get(arch, '')}")

        if gen is not None:
            gen_arch = gen[gen["archetype_id"] == arch]
            if not gen_arch.empty:
                st.plotly_chart(
                    _stacked_area(gen_arch, "year", "generation_twh", "technology",
                                  f"Generation by technology — {arch}", "TWh"),
                    use_container_width=True,
                )

        if cap is not None:
            cap_arch = cap[cap["archetype_id"] == arch]
            if not cap_arch.empty:
                st.plotly_chart(
                    _stacked_area(cap_arch, "year", "capacity_gw", "technology",
                                  f"Installed capacity — {arch}", "GW"),
                    use_container_width=True,
                )

        if sto is not None:
            sto_arch = sto[sto["archetype_id"] == arch].sort_values("year")
            if not sto_arch.empty:
                fig_sto = go.Figure()
                fig_sto.add_trace(go.Bar(x=sto_arch["year"], y=sto_arch["storage_power_gw"],
                                         name="Power (GW)", marker_color="#708090"))
                fig_sto.add_trace(go.Scatter(
                    x=sto_arch["year"], y=sto_arch["storage_energy_gwh"],
                    name="Energy (GWh)", yaxis="y2",
                    line={"color": "#2ca02c", "width": 2}, mode="lines+markers",
                ))
                fig_sto.update_layout(
                    title=f"Storage capacity — {arch}",
                    xaxis_title="Year",
                    yaxis_title="GW",
                    yaxis2={"title": "GWh", "overlaying": "y", "side": "right"},
                    height=320,
                    margin={"l": 40, "r": 60, "t": 40, "b": 40},
                )
                st.plotly_chart(fig_sto, use_container_width=True)

        if dg is not None:
            dg_arch = dg[dg["archetype_id"] == arch].sort_values("year")
            if not dg_arch.empty:
                fig_dg = go.Figure()
                fig_dg.add_trace(go.Bar(x=dg_arch["year"], y=dg_arch["demand_twh"],
                                        name="Demand (TWh)", marker_color="#1f77b4"))
                fig_dg.add_trace(go.Bar(x=dg_arch["year"], y=dg_arch["total_generation_twh"],
                                        name="Generation (TWh)", marker_color="#aec7e8"))
                gap_above_threshold = dg_arch["supply_gap_pct"].abs() > 1.0
                if gap_above_threshold.any():
                    flagged = dg_arch[gap_above_threshold]
                    fig_dg.add_trace(go.Scatter(
                        x=flagged["year"], y=flagged["demand_twh"],
                        mode="markers", name="Gap >1%",
                        marker={"color": "red", "size": 12, "symbol": "x"},
                    ))
                fig_dg.update_layout(
                    title=f"Demand vs generation — {arch}",
                    barmode="group", xaxis_title="Year", yaxis_title="TWh",
                    height=320,
                    margin={"l": 40, "r": 20, "t": 40, "b": 40},
                )
                st.plotly_chart(fig_dg, use_container_width=True)

        if cf is not None:
            cf_arch = cf[cf["archetype_id"] == arch]
            if not cf_arch.empty:
                pivot = cf_arch.pivot_table(
                    index="technology", columns="year", values="capacity_factor"
                )
                st.dataframe(pivot.style.format("{:.2%}"), use_container_width=True)

        st.divider()


def _render_calibration(data_dir: Path) -> None:
    st.subheader("Section 4 — Calibration")
    st.caption("Comparison of cost_optimal archetype total generation against AEMO Overview 2024.")

    gen = _load_granular(data_dir, "generation_twh.csv")
    if gen is None:
        return

    co_gen = gen[gen["archetype_id"] == "cost_optimal"]
    if co_gen.empty:
        st.info("No cost_optimal granular generation data available.")
        return

    annual_gen = co_gen.groupby("year")["generation_twh"].sum().reset_index()

    for _, row in annual_gen[annual_gen["year"].isin([2030, 2050])].iterrows():
        year = int(row["year"])
        modelled = row["generation_twh"]
        aemo = _AEMO_OVERVIEW_2030_TWH if year == 2030 else _AEMO_OVERVIEW_2050_TWH
        diff_pct = (modelled - aemo) / aemo * 100.0
        col_a, col_b, col_c = st.columns(3)
        col_a.metric(f"Modelled {year} (TWh)", f"{modelled:.1f}")
        col_b.metric(f"AEMO Overview {year} (TWh)", f"{aemo:.1f}")
        if abs(diff_pct) > 5.0:
            col_c.metric("Discrepancy", f"{diff_pct:+.1f}%",
                         delta_color="inverse" if diff_pct > 0 else "normal")
            if year == 2030:
                st.error(
                    f"2030 generation overshoot: {diff_pct:+.1f}% vs AEMO Overview. "
                    "Known data-side discrepancy — established across three solvers "
                    "(HiGHS simplex, PDLP, Gurobi). Likely due to IASR "
                    "trace-representative-week scaling rather than solver choice."
                )
        else:
            col_c.metric("Discrepancy", f"{diff_pct:+.1f}%")


@st.cache_data(show_spinner=False)
def _cached_dispatch_timeseries(run_dir_str: str) -> dict | None:
    """Wrap the heavy NetCDF read so repeat tab interactions are instant."""
    return extract_dispatch_timeseries(Path(run_dir_str))


def _ordered_dispatch_carriers(columns: list[str]) -> list[str]:
    """Return generator carriers ordered for the dispatch stack."""
    ordered = [c for c in _DISPATCH_STACK_ORDER if c in columns]
    extras = [c for c in columns if c not in _DISPATCH_STACK_ORDER]
    return ordered + sorted(extras)


def _dispatch_stack_chart(ts: dict, archetype: str, year: int) -> go.Figure:
    """Stacked area: generator dispatch + storage discharge above zero, storage charge below."""
    fig = go.Figure()
    _add_generator_stack(fig, ts["dispatch_by_carrier"])
    _add_storage_traces(fig, ts["storage_dispatch"], ts["storage_charge"])
    _add_demand_line(fig, ts["demand"])
    fig.update_layout(
        title=f"Dispatch — {archetype}, {year} representative week",
        xaxis_title="Snapshot (30-min resolution)",
        yaxis_title="MW",
        height=460,
        hovermode="x unified",
        margin={"l": 40, "r": 20, "t": 50, "b": 40},
        legend={"orientation": "h", "y": -0.18},
    )
    return fig


def _add_generator_stack(fig: go.Figure, dispatch_by_carrier: pd.DataFrame) -> None:
    """Add one stacked-area trace per generator carrier in display order."""
    for carrier in _ordered_dispatch_carriers(list(dispatch_by_carrier.columns)):
        color = TECHNOLOGY_COLORS.get(carrier, "#aaaaaa")
        fig.add_trace(go.Scatter(
            x=dispatch_by_carrier.index, y=dispatch_by_carrier[carrier],
            name=carrier, stackgroup="generation", mode="lines",
            line={"color": color, "width": 0}, fillcolor=color,
        ))


def _add_storage_traces(
    fig: go.Figure, storage_dispatch: pd.DataFrame, storage_charge: pd.DataFrame
) -> None:
    """Storage discharge stacked above generators; charge as negative band below zero."""
    if not storage_dispatch.empty:
        total_discharge = storage_dispatch.sum(axis=1)
        fig.add_trace(go.Scatter(
            x=total_discharge.index, y=total_discharge,
            name="Storage discharge", stackgroup="generation", mode="lines",
            line={"color": "#708090", "width": 0}, fillcolor="#708090",
        ))
    if not storage_charge.empty:
        total_charge = -storage_charge.sum(axis=1)
        fig.add_trace(go.Scatter(
            x=total_charge.index, y=total_charge,
            name="Storage charge", stackgroup="charge", mode="lines",
            line={"color": "#a9a9a9", "width": 0}, fillcolor="#a9a9a9",
        ))


def _add_demand_line(fig: go.Figure, demand: pd.Series) -> None:
    """Demand line on top of the stack for visual sanity check."""
    fig.add_trace(go.Scatter(
        x=demand.index, y=demand, name="Demand", mode="lines",
        line={"color": "black", "width": 2, "dash": "dot"},
    ))


def _curtailment_chart(ts: dict, archetype: str, year: int) -> go.Figure:
    """Stacked area of curtailed VRE potential per snapshot."""
    fig = go.Figure()
    curtailment = ts["curtailment"]
    for carrier in curtailment.columns:
        color = TECHNOLOGY_COLORS.get(carrier, "#aaaaaa")
        fig.add_trace(go.Scatter(
            x=curtailment.index, y=curtailment[carrier],
            name=f"{carrier} curtailed", stackgroup="curt", mode="lines",
            line={"color": color, "width": 0}, fillcolor=color, opacity=0.6,
        ))
    weighted_mwh = _annualised_curtailment_mwh(ts)
    total_label = " + ".join(f"{k} {v:,.0f}" for k, v in weighted_mwh.items())
    fig.update_layout(
        title=f"VRE curtailment — {archetype}, {year} (annualised: {total_label} MWh)",
        xaxis_title="Snapshot", yaxis_title="Curtailed MW",
        height=300, margin={"l": 40, "r": 20, "t": 50, "b": 40},
        hovermode="x unified",
    )
    return fig


def _annualised_curtailment_mwh(ts: dict) -> dict[str, float]:
    """Weight per-snapshot curtailment by the snapshot duration to annual MWh."""
    weights = ts["period_hours"]
    out: dict[str, float] = {}
    for carrier in ts["curtailment"].columns:
        out[carrier] = float((ts["curtailment"][carrier] * weights).sum())
    return out


def _cf_profile_chart(ts: dict, archetype: str, year: int) -> go.Figure:
    """Small-multiples: capacity factor per carrier over the rep week."""
    cf_by_carrier = _capacity_factor_by_carrier(ts)
    carriers = [c for c in _DISPATCH_STACK_ORDER if c in cf_by_carrier.columns]
    storage_cf = _storage_cf(ts)
    if not storage_cf.empty:
        carriers = carriers + ["Storage (battery+PHES)"]
        cf_by_carrier["Storage (battery+PHES)"] = storage_cf
    if not carriers:
        return go.Figure()
    cols = min(len(carriers), 3)
    rows = (len(carriers) + cols - 1) // cols
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=carriers, shared_xaxes=False)
    for idx, carrier in enumerate(carriers):
        r, c = divmod(idx, cols)
        color = TECHNOLOGY_COLORS.get(carrier.split(" ")[0], "#1f77b4")
        fig.add_trace(go.Scatter(
            x=cf_by_carrier.index, y=cf_by_carrier[carrier].clip(lower=0, upper=1),
            mode="lines", name=carrier, line={"color": color},
            showlegend=False,
        ), row=r + 1, col=c + 1)
        fig.update_yaxes(range=[0, 1], row=r + 1, col=c + 1)
    fig.update_layout(
        title=f"Capacity factor profiles — {archetype}, {year} (rep week)",
        height=240 * rows, margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    return fig


def _capacity_factor_by_carrier(ts: dict) -> pd.DataFrame:
    """Per-snapshot CF per generator carrier — dispatch / installed capacity."""
    network_capacity = _carrier_installed_capacity(ts)
    cf = pd.DataFrame(index=ts["snapshot_index"])
    for carrier in ts["dispatch_by_carrier"].columns:
        cap = network_capacity.get(carrier, 0)
        if cap <= 0:
            continue
        cf[carrier] = ts["dispatch_by_carrier"][carrier] / cap
    return cf


def _carrier_installed_capacity(ts: dict) -> pd.Series:
    """Installed MW per carrier — peak dispatch is a lower bound on installed capacity.

    The solved network exposes per-generator p_nom_opt, but the dashboard already
    aggregates by carrier. We use the per-snapshot maximum dispatch as a proxy
    for installed capacity (an underestimate equal to peak realised dispatch);
    this is sufficient for the qualitative CF-shape comparison this chart serves.
    """
    return ts["dispatch_by_carrier"].max(axis=0).replace(0, float("nan"))


def _storage_cf(ts: dict) -> pd.Series:
    """Combined storage CF (discharge / installed power) across all StorageUnits."""
    discharge = ts["storage_dispatch"].sum(axis=1)
    peak = discharge.max()
    return discharge / peak if peak > 0 else pd.Series(dtype=float)


def _peak_offpeak_donuts(ts: dict, archetype: str, year: int) -> tuple[go.Figure, go.Figure]:
    """Two pie charts: dispatch composition during peak vs off-peak demand hours."""
    peak_hours = _hours_above_decile(ts["demand"], _PEAK_DECILE)
    offpeak_hours = _hours_below_decile(ts["demand"], _OFFPEAK_DECILE)
    peak_mix = _composition_at_hours(ts, peak_hours)
    offpeak_mix = _composition_at_hours(ts, offpeak_hours)
    return (
        _make_pie(peak_mix, f"Peak demand (top decile) — {archetype} {year}"),
        _make_pie(offpeak_mix, f"Off-peak (bottom decile) — {archetype} {year}"),
    )


def _hours_above_decile(series: pd.Series, decile: float) -> pd.Index:
    """Snapshots where the series value is at or above the given quantile."""
    threshold = series.quantile(decile)
    return series.index[series >= threshold]


def _hours_below_decile(series: pd.Series, decile: float) -> pd.Index:
    """Snapshots where the series value is at or below the given quantile."""
    threshold = series.quantile(decile)
    return series.index[series <= threshold]


def _composition_at_hours(ts: dict, hours: pd.Index) -> pd.Series:
    """Sum dispatch by carrier across the chosen snapshots (incl. storage discharge)."""
    gens_at = ts["dispatch_by_carrier"].loc[hours].sum(axis=0)
    storage_at = ts["storage_dispatch"].loc[hours].sum(axis=0) if not ts["storage_dispatch"].empty else pd.Series(dtype=float)
    storage_total = float(storage_at.sum()) if not storage_at.empty else 0.0
    combined = gens_at.copy()
    if storage_total > 0:
        combined["Storage discharge"] = storage_total
    return combined[combined > 0]


def _make_pie(values: pd.Series, title: str) -> go.Figure:
    """Plotly donut chart with technology colours."""
    colors = [TECHNOLOGY_COLORS.get(c.split(" ")[0], "#aaaaaa") for c in values.index]
    fig = go.Figure(go.Pie(
        labels=list(values.index), values=values.values,
        hole=0.45, marker={"colors": colors},
        textinfo="label+percent", sort=False,
    ))
    fig.update_layout(title=title, height=400, margin={"l": 20, "r": 20, "t": 50, "b": 20})
    return fig


def _render_operational(runs_dir: Path, run_id_prefix: str) -> None:
    """Operational View tab — dispatch stack, curtailment, CF profiles, peak/off-peak."""
    st.subheader("Section 3 — Operational View")
    st.caption(
        "Hourly dispatch within the representative week (residual-peak-demand period). "
        "30-minute resolution. One rep week per (archetype, milestone year) — the LP's "
        "myopic decomposition uses a single named_representative_week per investment period."
    )
    available = list_available_runs(runs_dir)
    if not _operational_data_available(available, run_id_prefix, runs_dir):
        return
    selected_archetype, selected_year = _operational_selectors(available[run_id_prefix])
    run_dir = find_run_dir(runs_dir, run_id_prefix, selected_archetype, selected_year)
    if run_dir is None:
        st.warning(f"No solved network for {selected_archetype} {selected_year} under {run_id_prefix}.")
        return
    with st.spinner(f"Loading {selected_archetype} {selected_year} dispatch…"):
        ts = _cached_dispatch_timeseries(str(run_dir))
    if ts is None:
        st.warning(f"Solved NetCDF missing for {selected_archetype} {selected_year}.")
        return
    _render_operational_charts(ts, selected_archetype, selected_year)


def _operational_data_available(available: dict, run_id_prefix: str, runs_dir: Path) -> bool:
    """Show a helpful message and return False if no solved runs are present."""
    if not available:
        st.warning(f"No solved networks found under {runs_dir}.")
        return False
    if run_id_prefix not in available:
        st.warning(
            f"Selected run prefix `{run_id_prefix}` not found. Available: {sorted(available.keys())}"
        )
        return False
    return True


def _operational_selectors(archetypes_with_years: dict) -> tuple[str, int]:
    """Two top-of-tab selectors: archetype × milestone year."""
    col_a, col_b = st.columns(2)
    archetypes = sorted(archetypes_with_years.keys())
    with col_a:
        selected_archetype = st.selectbox("Archetype", archetypes, key="op_arch")
    with col_b:
        years = archetypes_with_years[selected_archetype]
        selected_year = st.selectbox("Milestone year", years, index=len(years) - 1, key="op_year")
    return selected_archetype, selected_year


def _render_operational_charts(ts: dict, archetype: str, year: int) -> None:
    """Render the four operational charts in order."""
    st.markdown(
        "#### Dispatch stack (with storage charge as negative band)",
        help=_TOOLTIP_DISPATCH,
    )
    st.plotly_chart(_dispatch_stack_chart(ts, archetype, year), use_container_width=True)

    st.markdown("#### VRE curtailment", help=_TOOLTIP_CURTAILMENT)
    st.caption(
        "Curtailed VRE = `p_max_pu × p_nom_opt − actual_dispatch`. Non-zero curtailment "
        "indicates economic spillage (the LP would dispatch the renewable but transmission "
        "or load constraints prevent it). Annualised MWh weights each 30-min snapshot by "
        "its snapshot_weighting."
    )
    st.plotly_chart(_curtailment_chart(ts, archetype, year), use_container_width=True)

    st.markdown("#### Capacity factor profiles by technology")
    st.caption(
        "Per-snapshot dispatch divided by peak realised dispatch (a lower-bound proxy for "
        "installed capacity). Shows how Wind, Solar, Water (hydro), and dispatchable "
        "technologies have qualitatively different temporal profiles."
    )
    st.plotly_chart(_cf_profile_chart(ts, archetype, year), use_container_width=True)

    st.markdown(
        "#### Dispatch composition: peak vs off-peak demand",
        help=_TOOLTIP_PEAK_OFFPEAK,
    )
    st.caption(
        "Peak = top decile of hourly demand within the rep week. Off-peak = bottom decile. "
        "Shows which technologies the LP relies on for firm capacity vs which dominate "
        "when load is low."
    )
    peak_fig, offpeak_fig = _peak_offpeak_donuts(ts, archetype, year)
    col_p, col_o = st.columns(2)
    with col_p:
        st.plotly_chart(peak_fig, use_container_width=True)
    with col_o:
        st.plotly_chart(offpeak_fig, use_container_width=True)

    st.caption(
        "_Richer operational views (additional representative weeks for low-demand and "
        "shoulder periods) would require IASR configuration changes — flagged as a v2 "
        "methodology question._"
    )


def _render_methodology() -> None:
    """Methodology tab — assumptions, parameters, and sources."""
    st.subheader("Section 5 — Methodology")
    st.caption("Quick reference for assumption sources and modelling choices.")
    _methodology_scenario()
    _methodology_capacity_factors()
    _methodology_costs()
    _methodology_financial()
    _methodology_emissions()
    _methodology_team_choices()
    _methodology_limitations()


def _methodology_scenario() -> None:
    with st.expander("Scenario and structural choices", expanded=True):
        st.markdown("""
**Scenario:** AEMO 2024 IASR Step Change (`scenario: "Step Change"` in
`mvp_pass1_power/configs/baseline.yaml`). Sourced from the
[IASR 2024 v6.0 workbook](https://aemo.com.au/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp).

**Spatial granularity:** 16 NEM sub-regions per IASR (`regional_granularity: sub_regions`).
REZ-level dispatch is aggregated to sub-region for renderable output; the LP
itself solves at the discrete-node level with REZ-to-sub-region links.

**Milestone years:** 2025, 2030, 2035, 2040, 2045, 2050. Each milestone year is
solved as a single-period LP; new-entrant capacity is carried forward to the
next period (myopic chaining with the IASR baseline retirement schedule).

**Temporal representation:** one representative week per milestone year using
the IASR `residual-peak-demand` selection, at 30-minute resolution
(`resolution_min: 30`). Snapshot weightings scale each 30-min snapshot to its
share of the year.

**Solver:** HiGHS 1.12 default primal simplex. Gurobi 11 available as a fallback
under the team's CSIRO licence (capped at v11 per
[`gurobi_licence_csiro`](../../memory/gurobi_licence_csiro.md)).
""")


def _methodology_capacity_factors() -> None:
    with st.expander("Capacity factor assumptions and sources"):
        st.markdown("""
| Technology | CF source | Notes |
|---|---|---|
| Wind | AEMO IASR per-zone trace data (2018 reference year) | `p_max_pu` per zone per resource type from `isp_trace_parser` |
| Solar | AEMO IASR per-zone trace data | as above |
| Conventional hydro (Water) | Synthesised monthly profile from AEMO Generation Information NEM long-run averages | Uniform across facilities, annual mean ~37% — see `_HYDRO_MONTHLY_CF` in `src/ispypsa/pypsa_build/generators.py` |
| Pumped storage | Per-facility hardcoded in `mvp_pass1_power/archetypes/_pumped_storage_fix.py` | See PHES table below |
| Nuclear | IEA *Projected Costs of Generating Electricity 2020* baseload range (85–95%) | Not enforced as p_max_pu; LP-determined via build economics |
| Coal, gas, biomass | LP-determined | Bounded only by IASR seasonal MW ratings + maintenance derate |

**Pumped storage parameters** (sources: AEMO Gen Info; facility operator project briefs;
AEMO 2024 IASR v6.0 anticipated/committed summaries):

| Facility | Sub-region | Power (MW) | Duration (h) | Round-trip η | Status |
|---|---|---:|---:|---:|---|
| Wivenhoe | SQ | 570 | 9 | 76% | Existing |
| Shoalhaven | CNSW | 247 | 6 | 76% | Existing |
| Borumba | SQ | 1998 | 24 | 78% | Anticipated (2031-09) |
| Snowy 2.0 | SNSW | 2200 | 159 | 76% | Committed (2028-12) |
""")


def _methodology_costs() -> None:
    with st.expander("Cost parameters and sources"):
        st.markdown("""
**Capital cost (annuitised) and fixed O&M:**
[CSIRO GenCost 2024-25 Final](https://www.csiro.au/en/news/all/news/2025/july/2024-25-gencost-final-report)
(July 2025). Sourced via IASR `new_entrant_build_costs.csv`, `fom_$/kw/annum`,
`technology_specific_lcf_%` for sub-regional adjustment.

**Variable O&M:** IASR `vom_$/mwh_sent_out` per technology.

**Fuel costs:** IASR scenario-specific tables (`coal_prices_step_change.csv`,
`gas_prices_step_change.csv`, `biomass_prices.csv`, etc.). Coal seam gas /
biomethane blending handled per `gpg_emissions_reduction_h2`.

**Nuclear (special case — not in IASR):**

| Parameter | Value | Source |
|---|---:|---|
| Capital cost | 31,100,000 AUD/MW | CSIRO GenCost 2024-25 Final ES Table B.2 (SMR; UAMPS CFPP reference) |
| Lifetime | 60 years | GenCost assumption |
| Min stable level | 53 % | GenCost 2024-25 baseload CF range floor |
| VOM | 10 AUD/MWh | IEA *Projected Costs of Generating Electricity 2020* |
| Heat rate | 0 GJ/MWh | Nuclear routed as a non-fuel carrier (`non_fuel_carriers` in `translator/generators.py:713`) |
| Emission factor | 0 kg CO2e/GJ (Scope 1) | NGER 2024 — nuclear fission, no combustion |

**Cost decoupling (architectural choice):** ISPyPSA's LP minimises *bundled* cost
(`fuel_price × heat_rate + VOM` in `marginal_cost`). The post-processor at
`postprocess/extract_method_years.py` subtracts dispatch-weighted fuel cost so
the contract emits a fuel-decoupled `output_cost_per_unit` — the orchestrator
prices fuel commodities independently. Bundled cost is preserved as a
diagnostic column.
""")


def _methodology_financial() -> None:
    with st.expander("Discount rate and financial assumptions"):
        st.markdown("""
**WACC:** 7.0 % nominal (`wacc: 0.07` in `configs/baseline.yaml`). The IASR
default is 5.95 % pre-tax real, but the MVP currently uses a single nominal
figure — refining this to a real/nominal distinction is a v2 task.

**Discount rate (general):** 5.0 % (`discount_rate: 0.05`).

**Annuitisation lifetime:** 30 years
(`network.annuitisation_lifetime: 30`). Build costs are annuitised over this
horizon irrespective of technology lifetime. Asset-specific lifetimes are
applied separately for capacity-available calculations.

**Technology lifetimes (input):** IASR per-technology, with these defaults
applied for fork-injected items:

| Technology | Lifetime (yr) |
|---|---:|
| Nuclear | 60 |
| Pumped storage (existing) | per IASR closure_year (e.g. Wivenhoe 2084, Shoalhaven 2069) |
| Pumped storage (committed/anticipated) | per IASR (Snowy 2.0 2129, Borumba 2130) |
""")


def _methodology_emissions() -> None:
    with st.expander("Emissions methodology"):
        st.markdown("""
**Source:** National Greenhouse Accounts Factors 2024 (DCCEEW, July 2024) —
[NGA Factors 2024](https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors-2024).
Underlying legal basis:
[NGER (Measurement) Determination 2008](https://www.legislation.gov.au/Series/F2008L02309)
Schedule 1.

| Carrier | NGA Table | CO2 | CH4 (CO2e) | N2O (CO2e) | Total kg CO2e/GJ |
|---|---|---:|---:|---:|---:|
| Black Coal | Table 4 | 90.0 | 0.04 | 0.20 | **90.24** |
| Brown Coal | Table 4 | 93.5 | 0.02 | 0.30 | **93.82** |
| Gas (pipeline) | Table 5 | 51.4 | 0.10 | 0.03 | **51.53** |
| Liquid Fuel (diesel) | Table 8 | 69.9 | 0.10 | 0.20 | **70.20** |
| Biomass | Table 4 | 0 | 0.80 | 1.00 | **1.80** |
| Biomethane | Table 5 | 0 | 0.10 | 0.03 | **0.13** |
| Nuclear / Hydrogen / Wind / Solar / Water | — | 0 | 0 | 0 | **0** |

**GWP basis (default):** AR5 NGER (CH4 = 28, N2O = 265) — matches NGA Factors 2024 publication basis.

**GWP basis (toggle):** AR6 IPCC WGI Table 7.SM.7 (CH4 = 27, N2O = 273, 100-year incl.
climate-carbon feedback). Switch via the sidebar; carbon intensity is
recomputed from preserved physical-mass CH4/N2O columns in `diagnostics.csv`.

**Per-pollutant decomposition:** `energy_emissions_by_pollutant` in
`method_years.csv` exposes CO2, CH4_CO2e, N2O_CO2e separately so downstream
consumers can compose under their preferred GWP basis. Physical-mass
columns (`diagnostic_ch4_physical_kg_per_mwh`, `diagnostic_n2o_physical_kg_per_mwh`)
enable basis-switching without re-running the pipeline.

**N2O cross-framework note:** the current NGER Measurement Determination 2008
Compilation No. 18 (31/08/2024) carries a *single* N2O factor per fuel — pre-2015
editions had separate electricity-generation columns (~7× higher for bituminous
coal). If a reference framework still uses the pre-2015 factors, reported N2O
intensities will be ~7× higher than these — known divergence, not a model error.
""")


def _methodology_team_choices() -> None:
    with st.expander("Team architectural choices (vs externally-sourced data)"):
        st.markdown("""
The following are *decisions* made by the modelling team, distinct from
authoritative data inputs (IASR, NGER, GenCost):

**Hydro fix (2026-05-21).** Conventional hydro now constrained to a monthly
seasonal CF profile (uniform across facilities, ~37 % annual mean). Pumped
storage re-modelled as PyPSA `StorageUnit` components with per-facility power /
energy / round-trip efficiency parameters; pumping energy is correctly deducted
from net generation. See
[`HYDRO_NUCLEAR_FIX_REPORT.md`](../HYDRO_NUCLEAR_FIX_REPORT.md) for the
implementation and data provenance per facility.

**Renewable classification.** `_RENEWABLE_CARRIERS = {Wind, Solar, Biomass}` in
`postprocess/extract_granular_outputs.py:34`. Water (hydro) is excluded as a
*methodology choice*, not because of unrealistic dispatch — after the hydro
fix, Water dispatches at ~37 % CF and inclusion would be defensible. The
metric is kept strictly VRE+biomass for comparability with prior runs.

**Cost-decoupling invariant.** Pass 1 outputs separate commodity fuel costs
from non-fuel opex so the Pass 2 orchestrator can price fuels under its own
endogenous prices. Bundled cost is preserved as a diagnostic.

**Over-generation invariant for archetype catalogue.** Six structurally distinct
archetypes are exposed to Pass 2 even when an archetype's LP solution converges
with another — e.g. `nuclear_baseload` builds zero nuclear in the corrected
run, but stays in the catalogue. The orchestrator selects which archetype to
use; Pass 1 over-generates structural options rather than pre-emptively pruning.

**Phase 2 catalogue redesign — AEMO-anchored deployment mandates.** Three of
the six archetypes now carry an explicit deployment mandate as PyPSA
`custom_constraints` rows, anchored against AEMO's published 2024 ISP Step
Change projections (Coalition 2024 policy reference for nuclear, which AEMO
does not model). `gas_fleet_maintained` floors NEM gas at ≥ 12,500 MW at
2030 and 2035 (binding +616 MW and +648 MW above AEMO). `storage_led` floors
StorageUnit power at 1.25× AEMO Step Change trajectory each milestone year.
`nuclear_baseload` floors total nuclear at ≥ 2,000 MW @ 2045 and ≥ 4,000 MW
@ 2050. Existing-asset capacity contribution is subtracted from the RHS so
the LP only sees decision variables on the LHS; mandate years outside the
configured `investment_periods` are skipped with a warning.

**Phase 2 Option B maintenance overlay.** Every archetype runs through a
pre-pass that adds an ageing premium to `fom_$/kw/annum` for ECAA thermal
generators inside the EOL window. Coal: linear ramp to +50 AUD/kW/yr over
the final 10 years (Bayswater refurbishment disclosure ≈ 76 AUD/kW/yr used
as upper bound). Gas: ramp to +20 AUD/kW/yr over the final 5 years. New
entrants are unaffected. Documented as a fleet-level simplification; Pass 3
should use per-plant refurbishment schedules.

**Phase 2 EOL renewable repowering.** Every archetype also runs through a
repowering pre-pass that extends ECAA wind / solar `closure_year` by 20
years and adds an annualised repowering capex premium to `fom_$/kw/annum`
(1,000 AUD/kW for wind, 800 AUD/kW for solar, ~50% of greenfield CapEx per
IRENA Renewable Power Generation Costs 2023 and GenCost 2024-25 §3.5).
Pass-1 simplification — no capacity-factor uplift (would require trace
modification, out of MVP scope) and not a true LP investment decision.
Addresses the 2045 wind capacity dip observed in prior production runs.

**Myopic-nuclear documented limitation.** Under myopic period
decomposition, the LP can treat nuclear capacity as commissionable in the
mandate year (instant deployment). Real nuclear construction lead time is
~10 years. The `nuclear_baseload` cost trajectory therefore represents the
structural cost of "nuclear-included" rather than a realistic deployment
forecast. Surfaced explicitly so the cost figures aren't misread.

**Myopic decomposition.** Each milestone year is solved as a single-period LP
with the previous period's new-entrant builds carried forward. Cross-period
optimisation is not performed; this trades global optimality for tractability
and is documented as an MVP constraint.
""")


def _methodology_limitations() -> None:
    with st.expander("Known limitations and caveats"):
        st.markdown("""
- **2030 demand over-shoot (+15.2 % vs AEMO Overview 202 TWh).** Data-side
  discrepancy persistent across HiGHS simplex, PDLP 1e-3, and Gurobi.
  Robust to the hydro/PHES fix (prior was +14.2 %). Suspected to be IASR
  representative-week scaling; not a solver artefact.
- **Uniform seasonal hydro CF.** All Water-carrier generators share the same
  monthly profile. Per-facility CFs from AEMO Generation Information would be
  more accurate but are out of scope for the MVP.
- **Hardcoded pumped storage parameters.** Energy capacity, round-trip
  efficiency, and commissioning dates are coded into
  `_pumped_storage_fix.py` because `isp-workbook-parser` does not extract
  PHES properties. A proper fix is upstream in that parser.
- **Shoalhaven dispatches at 3.1 % CF** in cost_optimal 2050. An LP economic
  finding (small 247 MW × 6h PHES dominated by same-region batteries), not a
  bug — surfaced here so it isn't misread.
- **max_share = 1.0 for all archetypes.** No deployment-rate ceiling
  differentiation across archetypes. v2 modelling decision.
- **PDLP at 1e-3 returns `model_status: Unknown`.** Solution values are
  populated correctly; the status field is misleading. HiGHS primal simplex is
  the default for production runs to avoid this reporting quirk.
""")


def _render_config_notes(selected_gwp_label: str, gwp: dict) -> None:
    with st.expander("Section 6 — Configuration Notes", expanded=True):
        st.markdown(f"""
**Production run:** `20260521_155855` — full NEM, 6 archetypes × 6 milestone years.
3.99 h wall-clock (up from 2.48 h pre-fix; the realistic hydro/PHES constraints
make the LP work harder). All 36 periods Optimal under HiGHS primal simplex — no
Gurobi fallback required. Previous run preserved at `outputs/*_prior_20260518/`.

**IASR scenario:** Step Change 2024 v6.0

**Solver:** HiGHS default primal simplex (myopic period decomposition for multi-period runs)

**GWP basis (current):** {selected_gwp_label} — CH4={gwp['CH4']}, N2O={gwp['N2O']}

**Archetype constraints (Phase 2 redesigned catalogue, AEMO-anchored mandates):**

| Archetype | Coal closure | New gas | Deployment mandate |
|---|---|---|---|
| cost_optimal | IASR schedule | Available | None (baseline) |
| rapid_coal_phaseout | ≤ 2030 | Available | None |
| gas_fleet_maintained | ≤ 2030 | Available | Gas ≥ 12,500 MW @ 2030 & 2035 |
| storage_led | ≤ 2035 | All gas dropped (incl. CCS) | Storage ≥ 1.25× AEMO trajectory per year |
| fossil_incumbent | +10 yrs extension | Available | Solar dropped, 75% wind dropped |
| nuclear_baseload | IASR schedule | Available | Nuclear ≥ 2,000 MW @ 2045, ≥ 4,000 MW @ 2050 |

Deployment mandates anchor against AEMO's published 2024 ISP Step Change
projections (Coalition 2024 policy reference for nuclear, which AEMO does not
model). Narrative sections below still reference the prior catalogue's run
results — they will be refreshed once Phase 6 production runs complete.

**Renewable classification (for renewable_share):**
Wind, Solar, Biomass. Water (hydro) and Hydrogen are excluded as a *methodology
choice* — Water now dispatches at realistic 30–45% CF after the hydro fix
(unchanged inclusion is defensible; we have kept it excluded so the metric remains
strictly VRE+biomass and is comparable to prior runs). Nuclear is also excluded
(contested AU context).

**Max share methodology:**
All archetypes currently declare max_share = 1.0 (no deployment ceiling). These are
author-supplied bounds in `ARCHETYPE_CATALOGUE` in `emit_simple_msm.py`, not derived
from the ISPyPSA LP. In the simple-msm framework, max_share bounds the fraction of
total electricity supply this method can contribute. Setting 1.0 for all archetypes
means the orchestrator may choose any single archetype as 100% of the electricity
supply; differentiated bounds (e.g. rapid_coal_phaseout capped at 0.8 through 2030 to
reflect deployment ramp constraints) are a v2 modelling decision not implemented here.

**Implied abatement cost sense-check (all years, rapid_coal_phaseout vs cost_optimal).**
Uses `output_cost_per_unit` (fuel-decoupled, post-Bug-1 fix). Values from the
corrected production run (`20260521_155855`):

| Year | Δ cost (AUD/MWh) | Δ CO₂e (tCO₂e/MWh) | Implied MAC (AUD/tCO₂e) |
|---|---:|---:|---:|
| 2030 | +53.87 | −0.408 | **132** |
| 2035 | +21.77 | −0.203 | **108** |
| 2040 | +13.69 | −0.125 | **110** |
| 2045 | +7.92 | −0.081 | **98** |
| 2050 | +3.77 | −0.033 | **115** |

The range **98–132 AUD/tCO₂e** sits at the upper end of the NEM decarbonisation
literature band (~80–150 AUD/tCO₂e). The hydro fix lifted the 2030 figure
materially (~+31% vs the pre-fix value) because the system now has to build
firming capacity to cover the load that the prior phantom-hydro dispatch was
silently meeting. Outer-year deltas are within ~10% of the pre-fix values.
gas_fleet_maintained and storage_led are identical to rapid_coal_phaseout at 2030+ (gas
bridge provides no additional flexibility under IASR Step Change; the LP
never chooses new unabated gas even when it's available — confirmed
structural under realistic hydro, not an artefact of the prior modelling).

fossil_incumbent is not an abatement pathway. At 2030 it is cheaper (+$6.15/MWh
delivered) but dirtier (+0.089 tCO₂e/MWh) than cost_optimal; by 2045–2050 it is
strictly dominated (higher cost AND more emissions). At 2050: fossil_incumbent
pays +177 AUD/MWh for +0.173 tCO₂e/MWh — an implicit carbon penalty of
~**1,028 AUD/tCO₂e** on the excess emissions relative to cost_optimal. The
gap widened from ~790 AUD/tCO₂e pre-fix because the constrained-renewable
pathway now pays for PHES round-trip losses where the prior runs got hydro
firming "for free". This widening is methodologically correct.

**Nuclear (`nuclear_baseload`):** Now a first-class carrier in ISPyPSA (the
prior KeyError workaround has been removed). Cost assumptions: CSIRO GenCost
2024-25 Final (July 2025) — 31.1M AUD/MW capital cost, 53% min stable level,
60-year lifetime. Heat rate held at 0 GJ/MWh (nuclear fuel cost not in IASR
fuel-cost tables; routed as a non-fuel carrier), with VOM 10 AUD/MWh from IEA
*Projected Costs of Generating Electricity 2020*. **The LP built zero nuclear
in all six milestone years 2025–2050.** Nuclear_included's `output_cost_per_unit`
trajectory is identical to cost_optimal in every year — the LP found the same
optimum with the nuclear option ignored. The archetype is in the catalogue as a
structural option the LP rejects on economic grounds, satisfying the
over-generation invariant for downstream Pass 2 consumers.

**Energy intensity sense-check (corrected run):**
System-level GJ/MWh delivered at 2025 (cost_optimal): coal 5.92 (→ ~10 GJ/MWh
per generator at ~59% coal share, consistent with sub-critical black coal),
natural_gas 0.62 (→ ~8 GJ/MWh per OCGT-dominant new-gas mix). Values rose vs
the prior run (4.37 / 0.45) because the corrected dispatch mix has more fossil
output per MWh delivered — the prior runs' phantom hydro silently supplied
~40 TWh/yr that fossil now has to cover. rapid_coal_phaseout 2030 fuel mix:
natural_gas 1.96, hydrogen 0.66 GJ/MWh (gas grew under realistic hydro; H2
dropped as gas is cheaper to dispatch). fossil_incumbent 2050 biomass: 5.16
GJ/MWh — anomalously high (was 3.74); the constrained-build pathway leans
even more on biomass for firming now that pumped storage costs energy to
cycle. Heat rates per-generator are unchanged from the prior run; only the
dispatch mix shifted.

**Methodology choices vs remaining limitations:**
- *Methodology choices* (conscious, kept for v2): Water excluded from
  renewable_share; uniform max_share=1.0; AR5 NGER as default GWP basis;
  myopic period decomposition without cross-period new-entrant chaining.
- *Remaining limitations* (accepted for MVP): uniform monthly hydro CF profile
  applied to all Water-carrier generators (no per-facility variation; AEMO
  Generation Information per-facility data is out of scope); pumped storage
  parameters hardcoded in `mvp_pass1_power/archetypes/_pumped_storage_fix.py`
  (the IASR workbook does not carry energy capacity or round-trip efficiency
  for PHES, and `isp-workbook-parser` would need an upstream extension);
  Shoalhaven dispatches at 3.1% CF in cost_optimal 2050 — an LP economic
  finding (small 247 MW × 6h PHES dominated by same-region batteries), not
  a model bug.

**Data caveats:**
- **2030 NEM consumption ~15% above AEMO Overview 202 TWh baseline** (cost_optimal
  232.7 TWh vs AEMO 202 TWh). Persists in the corrected run (was +14.2% prior,
  now +15.2%). Confirmed across HiGHS simplex, PDLP, and Gurobi — data-side
  issue (IASR representative-week scaling), not a solver or hydro artefact.
- Myopic decomposition does not chain new-entrant capacity commitments across
  periods; each period solves independently from the IASR baseline.
- PDLP at 1e-3 tolerance returns `model_status: Unknown` even when all three
  convergence metrics are below threshold. Solution values are populated
  correctly.
- Physical-mass CH4/N2O columns enable AR6 GWP switching; AR5 NGER values
  stored in nger_factor_table.csv are the authoritative NGER-compliant basis.
- **N2O factors — current NGER framework:** The NGER Measurement Determination
  2008 Compilation No. 18 (31/08/2024) carries a **single** N2O factor per fuel
  with no electricity-generation sub-table. Bituminous coal N2O = 0.2
  kg CO2-e/GJ; natural gas N2O = 0.03 kg CO2-e/GJ. Early NGER editions
  (pre-2015) had separate electricity-generation columns (~1.4 kg CO2-e/GJ
  for bituminous coal); that distinction was removed in a later amendment.
  If a reference framework still uses the pre-2015 electricity-generation
  N2O factors, reported N2O intensities will be ~7× higher than these
  values. That is a known cross-framework divergence, not a model error here.
""")


# ---------- main app ----------


def main() -> None:
    st.set_page_config(
        page_title="ISPyPSA Pass 1 Power — Archetype Catalogue",
        layout="wide",
    )
    st.title("ISPyPSA Pass 1 Power — Archetype Catalogue")
    st.markdown(
        "Six structural pathway archetypes for Australian electricity-sector modelling, "
        "derived from AEMO's 2024 IASR Step Change scenario via ISPyPSA + PyPSA + HiGHS. "
        "Cost and emission intensities are per MWh delivered to end-use loads."
    )

    settings = _sidebar_settings()

    tab_intensity, tab_granular, tab_operational, tab_calibration, tab_methodology, tab_config = st.tabs([
        "Intensity Curves",
        "Granular Results",
        "Operational View",
        "Calibration",
        "Methodology",
        "Configuration Notes",
    ])

    with tab_intensity:
        _render_intensity_curves(settings["data_dir"], settings["gwp"])

    with tab_granular:
        if settings["selected_archetypes"]:
            _render_granular(settings["data_dir"], settings["selected_archetypes"])
        else:
            st.info("Select at least one archetype in the sidebar to see granular results.")

    with tab_operational:
        _render_operational(settings["runs_dir"], settings["run_id_prefix"])

    with tab_calibration:
        _render_calibration(settings["data_dir"])

    with tab_methodology:
        _render_methodology()

    with tab_config:
        _render_config_notes(settings["gwp_label"], settings["gwp"])


def _sidebar_settings() -> dict:
    """Collect all sidebar inputs into a single settings dict."""
    with st.sidebar:
        st.header("Settings")
        gwp_label = st.radio(
            "GWP basis",
            ["AR5 NGER (CH4=28, N2O=265)", "AR6 IPCC (CH4=27, N2O=273)"],
            index=0,
        )
        gwp = {"CH4": 28, "N2O": 265} if "AR5" in gwp_label else {"CH4": 27, "N2O": 273}

        st.divider()
        st.caption("Post-processed CSVs (granular + simple-msm)")
        data_dir = Path(st.text_input("Output data directory", _DEFAULT_DATA_DIR))

        st.caption("Solved networks (Operational View tab)")
        runs_dir = Path(st.text_input("Runs directory", _DEFAULT_RUNS_DIR))
        run_id_prefix = _select_run_id_prefix(runs_dir)

        st.divider()
        selected_archetypes = st.multiselect(
            "Archetypes for granular section",
            PRODUCTION_ARCHETYPES,
            default=PRODUCTION_ARCHETYPES[:2],
        )
    return {
        "gwp_label": gwp_label,
        "gwp": gwp,
        "data_dir": data_dir,
        "runs_dir": runs_dir,
        "run_id_prefix": run_id_prefix,
        "selected_archetypes": selected_archetypes,
    }


def _select_run_id_prefix(runs_dir: Path) -> str:
    """Pick the most recent production-run timestamp under runs_dir, with a manual override."""
    available = list_available_runs(runs_dir)
    prefixes = sorted(available.keys())
    if not prefixes:
        st.warning("No solved runs found; Operational View tab will be empty.")
        return ""
    return st.selectbox("Production run", prefixes, index=len(prefixes) - 1)


if __name__ == "__main__":
    main()
