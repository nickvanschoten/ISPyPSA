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

# ---------- constants ----------

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_DATA_DIR = str(_SCRIPT_DIR.parent / "outputs")

ARCHETYPE_COLORS = {
    "cost_optimal":     "#1f77b4",
    "fast_fossil_exit": "#2ca02c",
    "gas_bridge":       "#ff7f0e",
    "storage_led":      "#9467bd",
    "fossil_incumbent": "#d62728",
    "nuclear_included": "#8c564b",
}

PRODUCTION_ARCHETYPES = list(ARCHETYPE_COLORS.keys())

ARCHETYPE_DESCRIPTIONS = {
    "cost_optimal":     "Unconstrained least-cost expansion under AEMO 2024 IASR Step Change.",
    "fast_fossil_exit": "Coal retired by 2030; no new unabated gas; firming via CCS/H2/biomass.",
    "gas_bridge":       "Coal retired by 2030; new gas available as transition bridge.",
    "storage_led":      "Coal by 2035; all gas new-entrants excluded; storage+H2+biomass firm.",
    "fossil_incumbent": "Extended coal life; constrained renewable build pathway.",
    "nuclear_included": "IASR coal schedule; Advanced Nuclear available in all sub-regions.",
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
        "(Water/hydro excluded — see Section 4 data caveats)."
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
                "See Section 4 for methodology notes on what max_share means and when it "
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
    st.subheader("Section 3 — Calibration")
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


def _render_config_notes(selected_gwp_label: str, gwp: dict) -> None:
    with st.expander("Section 4 — Configuration Notes"):
        st.markdown(f"""
**IASR scenario:** Step Change 2024 v6.0

**Solver:** HiGHS default primal simplex (myopic period decomposition for multi-period runs)

**GWP basis (current):** {selected_gwp_label} — CH4={gwp['CH4']}, N2O={gwp['N2O']}

**Archetype constraints:**

| Archetype | Coal closure | New gas | Other |
|---|---|---|---|
| cost_optimal | IASR schedule | Available | Baseline |
| fast_fossil_exit | ≤ 2030 | No unabated (OCGT/CCGT dropped) | CCS/H2/biomass kept |
| gas_bridge | ≤ 2030 | Available as bridge | — |
| storage_led | ≤ 2035 | All gas dropped (incl. CCS) | H2/biomass kept |
| fossil_incumbent | +10 yrs extension | Available | Solar dropped, 75% wind dropped |
| nuclear_included | IASR schedule | Available | Advanced Nuclear added per sub-region |

**Renewable classification (for renewable_share):**
Wind, Solar, Biomass. Water (hydro) and Hydrogen are excluded from renewable share.
Nuclear is also excluded (contested AU context).

**Max share methodology:**
All archetypes currently declare max_share = 1.0 (no deployment ceiling). These are
author-supplied bounds in `ARCHETYPE_CATALOGUE` in `emit_simple_msm.py`, not derived
from the ISPyPSA LP. In the simple-msm framework, max_share bounds the fraction of
total electricity supply this method can contribute. Setting 1.0 for all archetypes
means the orchestrator may choose any single archetype as 100% of the electricity
supply; differentiated bounds (e.g. fast_fossil_exit capped at 0.8 through 2030 to
reflect deployment ramp constraints) are a v2 modelling decision not implemented here.

**Implied abatement cost sense-check (all years, fast_fossil_exit vs cost_optimal).**
Uses `output_cost_per_unit` (fuel-decoupled). Corrected after Bug 1 fix (2026-05-19).

| Year | Δ cost (AUD/MWh) | Δ CO₂e (tCO₂e/MWh) | Implied MAC (AUD/tCO₂e) |
|---|---:|---:|---:|
| 2030 | +36.97 | −0.367 | **101** |
| 2035 | +19.08 | −0.179 | **107** |
| 2040 | +12.13 | −0.116 | **105** |
| 2045 | +7.11 | −0.071 | **100** |
| 2050 | +3.81 | −0.030 | **127** |

The range **100–127 AUD/tCO₂e** is broadly consistent with NEM decarbonisation
literature (~80–150 AUD/tCO₂e). gas_bridge and storage_led are identical to
fast_fossil_exit at 2030+ (gas bridge provides no additional flexibility under IASR
Step Change; the LP never chooses new unabated gas even when it's available).

fossil_incumbent is not an abatement pathway. At 2030 it is cheaper (+$1.63/MWh) but
dirtier (+0.037 tCO₂e/MWh) than cost_optimal; by 2045–2050 it is strictly dominated
(higher cost AND more emissions). At 2050: fossil_incumbent pays +130 AUD/MWh for
+0.165 tCO₂e/MWh — an implicit carbon penalty of ~790 AUD/tCO₂e on the excess
emissions relative to cost_optimal.

**Energy intensity sense-check:**
System-level GJ/MWh delivered at 2025: coal 4.37 (→ ~10.2 GJ/MWh per generator at 43%
coal share ✓), natural_gas 0.45 (→ ~8.3 GJ/MWh at 5.4% gas share, OCGT-dominant ✓).
fast_fossil_exit 2030 hydrogen: 1.08 GJ/MWh — high because coal exit forces significant
H2 dispatch from hydrogen turbines with high heat rates (~15 GJ/MWh). fossil_incumbent
2050 biomass: 3.74 GJ/MWh — anomalously high, indicating LP over-relies on biomass for
firming when the aging thermal fleet cannot meet ramp constraints cheaply.

**Data caveats:**
- **Hydro (Water carrier) modeling limitation:** ISPyPSA does not load hydro availability
  traces. All Water generators run with p_max_pu=1.0 and zero marginal cost; the LP
  dispatches them at 85–100% capacity factor (~60 TWh/year vs ~15–17 TWh realistic
  NEM-wide). Water generation and capacity figures are LP-optimal given unconstrained
  hydro, not physically realistic. Water is excluded from renewable_share_pct for this
  reason. Pumped hydro (Wivenhoe, Shoalhaven, Borumba, Snowy 2.0) is modelled as
  Generators, not StorageUnits — no pumping energy is deducted.
- 2030 NEM consumption ~14% above AEMO Overview 202 TWh baseline. Consistent across
  HiGHS simplex and Gurobi — confirmed data-side issue (IASR representative-week
  scaling), not a solver artefact.
- Myopic decomposition does not chain new-entrant capacity commitments across periods;
  each period solves independently from the IASR baseline.
- PDLP at 1e-3 tolerance returns `model_status: Unknown` even when all three convergence
  metrics are below threshold. Solution values are populated correctly.
- Physical-mass CH4/N2O columns enable AR6 GWP switching; AR5 NGER values stored
  in nger_factor_table.csv are the authoritative NGER-compliant basis.
- **N2O factors — current NGER framework:** The NGER Measurement Determination 2008
  Compilation No. 18 (31/08/2024) carries a **single** N2O factor per fuel with no
  electricity-generation sub-table. Bituminous coal N2O = 0.2 kg CO2-e/GJ; natural
  gas N2O = 0.03 kg CO2-e/GJ. Early NGER editions (pre-2015) had separate
  electricity-generation columns (~1.4 kg CO2-e/GJ for bituminous coal); that
  distinction was removed in a later amendment. If a reference framework still uses
  the pre-2015 electricity-generation N2O factors, reported N2O intensities will be
  ~7× higher than these values. That is a known cross-framework divergence, not a
  model error here.
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

    # Sidebar
    with st.sidebar:
        st.header("Settings")
        gwp_choice = st.radio(
            "GWP basis",
            ["AR5 NGER (CH4=28, N2O=265)", "AR6 IPCC (CH4=27, N2O=273)"],
            index=0,
        )
        gwp = {"CH4": 28, "N2O": 265} if "AR5" in gwp_choice else {"CH4": 27, "N2O": 273}

        data_dir = Path(st.text_input("Data directory", _DEFAULT_DATA_DIR))

        selected_archetypes = st.multiselect(
            "Archetypes for granular section",
            PRODUCTION_ARCHETYPES,
            default=PRODUCTION_ARCHETYPES[:2],
        )

    _render_intensity_curves(data_dir, gwp)
    st.divider()

    if selected_archetypes:
        _render_granular(data_dir, selected_archetypes)
    else:
        st.info("Select at least one archetype in the sidebar to see granular results.")

    _render_calibration(data_dir)
    st.divider()

    _render_config_notes(gwp_choice, gwp)


if __name__ == "__main__":
    main()
