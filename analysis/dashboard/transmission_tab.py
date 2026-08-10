"""Stage 0 interactive product: transmission spatiotemporal view + cost-of-
electricity KPI over the already-solved corrected ccx frontier (7 carbon
prices x 5 years). Read-only — reads pre-extracted parquet/CSV only, no PyPSA
network access and no live computation.

Plugs into `frontier_dashboard.py` as a new tab; inherits its registered
"frontier" Plotly template (CSIRO chrome) with no per-figure restyling, and
reuses its price-sequential highlight figure for the cost-KPI trajectory
rather than duplicating it. See the Stage 0 handoff for the design this
implements: `analysis/outputs/frontier_ccx/TIER3_REPWEEK_AUDIT_AND_SYNTHESIS.md`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.colors as pcolors
import plotly.graph_objects as go
import streamlit as st

from analysis.dashboard.fidelity import render_fidelity_badge

# Approximate lat/lon for each NEM sub-region's reference node (declared as
# data — sub_regions.csv gives reference-node place names, not coordinates).
_SUBREGION_COORDS = {
    "NQ":   (-19.26, 146.80),  # Ross
    "CQ":   (-22.50, 149.80),  # Broadsound
    "GG":   (-23.85, 151.10),  # Calliope River
    "SQ":   (-27.30, 152.90),  # South Pine
    "NNSW": (-30.50, 151.65),  # Armidale
    "CNSW": (-32.55, 148.90),  # Wellington
    "SNSW": (-35.30, 149.10),  # Canberra
    "SNW":  (-33.80, 150.90),  # Sydney West
    "WNV":  (-37.60, 143.90),  # Moorabool
    "SEV":  (-38.25, 146.40),  # Hazelwood
    "MEL":  (-37.70, 145.00),  # Thomastown
    "NSA":  (-32.50, 137.80),  # Davenport
    "CSA":  (-34.80, 138.50),  # Torrens Island
    "SESA": (-37.80, 140.80),  # South East
    "TAS":  (-41.10, 146.80),  # George Town
}

_UTILISATION_COLORSCALE = "Reds"


# ---------- data loading ----------


def _cells_manifest_path(corridors_dir: Path) -> Path:
    return corridors_dir / "cells.csv"


def _load_cells(corridors_dir: Path) -> pd.DataFrame:
    return pd.read_csv(_cells_manifest_path(corridors_dir))


def _load_daily_flows(corridors_dir: Path, cell_id: str) -> pd.DataFrame:
    df = pd.read_parquet(corridors_dir / cell_id / "flows_daily.parquet")
    df["date"] = pd.to_datetime(df["date"])
    return df


def _load_corridors(corridors_dir: Path, cell_id: str) -> pd.DataFrame:
    return pd.read_parquet(corridors_dir / cell_id / "corridors.parquet")


def _load_30min_window(corridors_dir: Path, cell_id: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    filters = [("snapshot", ">=", start), ("snapshot", "<", end)]
    return pd.read_parquet(corridors_dir / cell_id / "flows_30min.parquet", filters=filters)


# ---------- range aggregation helpers ----------


def _aggregate_over_range(daily: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    window = daily[(daily["date"] >= start) & (daily["date"] <= end)]
    return window.groupby("corridor").agg(
        flow_mean_mw=("flow_mean_mw", "mean"),
        util_max=("util_max", "max"),
        congested_hours=("congested_hours", "sum"),
    )


def _congestion_table(corridors: pd.DataFrame, windowed: pd.DataFrame) -> pd.DataFrame:
    merged = corridors.set_index("corridor").join(windowed)
    rating = merged["rating_fwd_mw"].where(merged["flow_mean_mw"] >= 0, merged["rating_rev_mw"])
    merged["headroom_mw"] = (rating * (1 - merged["util_max"])).clip(lower=0)
    return merged.reset_index()[
        ["corridor", "node_from", "node_to", "flow_mean_mw", "util_max", "congested_hours", "headroom_mw"]
    ].sort_values("util_max", ascending=False)


# ---------- map ----------


def _corridor_arc(row, colour: str) -> go.Scattergeo:
    lat_from, lon_from = _SUBREGION_COORDS[row.node_from]
    lat_to, lon_to = _SUBREGION_COORDS[row.node_to]
    forward = row.flow_mean_mw >= 0
    return go.Scattergeo(
        lat=[lat_from, lat_to], lon=[lon_from, lon_to],
        mode="lines", line=dict(width=2 + 6 * row.util_max, color=colour),
        hovertemplate=(
            f"{row.corridor}<br>direction: {'forward' if forward else 'reverse'}"
            f"<br>mean flow: {row.flow_mean_mw:.0f} MW<br>util (max in window): {row.util_max:.0%}"
            f"<br>congested hours: {row.congested_hours:.0f}<extra></extra>"
        ),
        showlegend=False,
    )


def _colorbar_anchor_trace(util_values: pd.Series) -> go.Scattergeo:
    """An invisible marker trace so the sequential utilisation ramp gets a
    colorbar — Plotly has no native line-colorbar for multi-trace Scattergeo."""
    return go.Scattergeo(
        lat=[None], lon=[None], mode="markers",
        marker=dict(
            size=0.01, color=[0], colorscale=_UTILISATION_COLORSCALE,
            cmin=0, cmax=util_values.max(),
            colorbar=dict(title="util_max"),
        ),
        showlegend=False, hoverinfo="skip",
    )


def _build_transmission_map(congestion: pd.DataFrame) -> go.Figure:
    colours = pcolors.sample_colorscale(_UTILISATION_COLORSCALE, congestion["util_max"].clip(0, 1).tolist())
    fig = go.Figure()
    fig.add_trace(_colorbar_anchor_trace(congestion["util_max"]))
    for row, colour in zip(congestion.itertuples(), colours):
        fig.add_trace(_corridor_arc(row, colour))
    lats = [c[0] for c in _SUBREGION_COORDS.values()]
    lons = [c[1] for c in _SUBREGION_COORDS.values()]
    fig.add_trace(go.Scattergeo(
        lat=lats, lon=lons, mode="markers+text",
        text=list(_SUBREGION_COORDS.keys()), textposition="top center",
        marker=dict(size=8, color="#333333"), showlegend=False, hoverinfo="text",
    ))
    fig.update_geos(scope="world", lataxis_range=[-45, -18], lonaxis_range=[135, 155],
                     showland=True, landcolor="#f0f0f0", showcountries=False)
    fig.update_layout(height=600, title="Corridor utilisation — colour + width = util_max (selected window)")
    return fig


# ---------- 30-min drill-down chart ----------


def _drilldown_chart(window: pd.DataFrame, corridor: str) -> go.Figure:
    series = window[window["corridor"] == corridor].sort_values("snapshot")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=series["snapshot"], y=series["flow_mw"], name="Flow (MW)"))
    fig.add_trace(go.Scatter(x=series["snapshot"], y=series["utilisation"] * 100, name="Utilisation (%)", yaxis="y2"))
    fig.update_layout(
        height=350, title=f"{corridor} — 30-minute detail",
        yaxis=dict(title="Signed flow (MW)"),
        yaxis2=dict(title="Utilisation (%)", overlaying="y", side="right"),
    )
    return fig


# ---------- tab renderer ----------


def render_transmission_tab(
    corridors_dir: Path,
    frontier: pd.DataFrame,
    prices: list[float],
    years: list[int],
    price_colours: dict[float, str],
    price_label,
    price_highlight_figure,
) -> None:
    """Renders the Transmission tab: cell selector, congestion map, time
    scrub + 30-min drill-down, and the cost-of-electricity KPI for the
    selected cell (reusing the frontier tab's price-highlight figure builder
    for the trajectory, rather than duplicating it)."""
    if not _cells_manifest_path(corridors_dir).exists():
        st.warning(
            f"No corridor-flow extraction found at {corridors_dir}. Run "
            "`python -m analysis.postprocess.extract_corridor_flows` first."
        )
        return
    cells = _load_cells(corridors_dir)

    c1, c2 = st.columns([1, 1])
    with c1:
        price = st.select_slider("Carbon price", options=prices, value=prices[len(prices) // 2],
                                  format_func=price_label, key="tx_price")
    with c2:
        year = st.select_slider("Milestone year", options=years, value=years[-1], key="tx_year")
    cell_id = f"c{int(price)}_{year}"
    cell_row = cells[cells["cell_id"] == cell_id]
    if cell_row.empty:
        st.warning(f"No corridor extraction for {cell_id}.")
        return
    fidelity = cell_row.iloc[0]["fidelity"]
    render_fidelity_badge(fidelity)

    daily = _load_daily_flows(corridors_dir, cell_id)
    corridors = _load_corridors(corridors_dir, cell_id)
    min_date, max_date = daily["date"].min(), daily["date"].max()
    start, end = st.slider(
        "Date range (daily aggregate; sustained high util_max flags drought-window congestion)",
        min_value=min_date.to_pydatetime(), max_value=max_date.to_pydatetime(),
        value=(min_date.to_pydatetime(), max_date.to_pydatetime()),
    )
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    windowed = _aggregate_over_range(daily, start, end)
    congestion = _congestion_table(corridors, windowed)

    map_col, kpi_col = st.columns([2, 1])
    with map_col:
        st.plotly_chart(_build_transmission_map(congestion), width="stretch")
    with kpi_col:
        cost = frontier.loc[
            (frontier["carbon_price_aud_per_tco2e"] == price) & (frontier["year"] == year),
            "cost_per_mwh_excl_fuel_carbon",
        ]
        st.metric("Cost of electricity (AUD/MWh, excl. fuel & carbon)", f"{cost.iloc[0]:.1f}")
        st.plotly_chart(
            price_highlight_figure(
                frontier, price, "cost_per_mwh_excl_fuel_carbon",
                "Fleet cost, excl. fuel & carbon ($/MWh)", prices, price_colours, ".1f",
            ),
            width="stretch",
        )

    st.subheader("Congestion summary (selected window)")
    st.dataframe(congestion, width="stretch", hide_index=True)

    st.subheader("30-minute drill-down")
    d1, d2 = st.columns(2)
    with d1:
        drill_date = st.date_input("Day to inspect", value=start.date(), min_value=start.date(), max_value=end.date())
    with d2:
        corridor_choice = st.selectbox("Corridor", sorted(corridors["corridor"]))
    window_30min = _load_30min_window(
        corridors_dir, cell_id,
        pd.Timestamp(drill_date), pd.Timestamp(drill_date) + pd.Timedelta(days=1),
    )
    st.plotly_chart(_drilldown_chart(window_30min, corridor_choice), width="stretch")
