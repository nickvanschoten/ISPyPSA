"""Frontier dashboard v2 — grid-electricity carbon-price-sweep menu.

A VIEW of the delivered v2 menu, not a re-entry of its numbers. Every plotted
value is read at runtime from the contract artifacts so the dashboard cannot
drift from the menu:

  - analysis/outputs/frontier/frontier_points.tidy.csv         (contract)
  - analysis/outputs/frontier/compositions_NONCONTRACT.tidy.csv (gated)
  - analysis/outputs/frontier/METHODOLOGY.md                    (docs source)

Design: a continuous cost-emissions frontier traced by an internal carbon price,
NOT six discrete policy archetypes (that was v1). Mixed audience — a clean glance
surface (OpenElectricity register) with full documentation one layer down. The
load-bearing rule: no caveat that governs how a number is read lives below the
fold. The three surface caveats (cost-axis meaning, the c000/2050 break, and the
carbon-price-as-shaping-parameter framing) are always visible.
"""

from __future__ import annotations

import base64
import re
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so `from analysis...` imports
# resolve when streamlit launches this script via plain python (mirrors the
# same guard in dashboard.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
import plotly.colors as pcolors
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from plotly.subplots import make_subplots

from analysis.dashboard.transmission_tab import render_transmission_tab

# --------------------------------------------------------------------------- #
# Paths + source-of-truth artifacts
# --------------------------------------------------------------------------- #
_FRONTIER_DIR = Path(__file__).resolve().parents[1] / "outputs" / "frontier_ccx"
_FRONTIER_CSV = _FRONTIER_DIR / "frontier_points.tidy.csv"
_COMPOSITIONS_CSV = _FRONTIER_DIR / "compositions_NONCONTRACT.tidy.csv"
_METHODOLOGY_MD = _FRONTIER_DIR / "METHODOLOGY.md"
_CORRIDORS_DIR = _FRONTIER_DIR / "corridors"

# Official CSIRO logo asset (supplied by the human). Searched in the repo root
# and the dashboard dir; falls back to a marked text placeholder if absent.
_LOGO_CANDIDATES = (
    Path(__file__).resolve().parents[2] / "CSIRO_Logo.png",
    Path(__file__).resolve().parent / "CSIRO_Logo.png",
)

# The axis label IS the caveat: the single most likely misread is taking this for
# a retail/wholesale electricity price. Never shorten it to "$/MWh".
_COST_AXIS = "Fleet cost, excl. fuel &amp; carbon ($/MWh)"
_CO2_AXIS = "CO₂ intensity (t/MWh)"

# Carrier draw order for the gated composition view (baseload thermal -> VRE).
_CARRIER_ORDER = [
    "Black Coal",
    "Brown Coal",
    "Gas",
    "Hyblend",
    "Liquid Fuel",
    "Hydrogen",
    "Biomass",
    "Water",
    "Wind",
    "Solar",
    "Battery",
    "Storage",
]

# =========================================================================== #
# THREE COLOUR SYSTEMS, THREE JOBS — they do NOT cross (see commission).
# THEMING ONLY: colours/typography/spacing. Never touches a data value, an axis
# title, an annotation, or a tick position — those live in the figure builders
# and are inherited unchanged.
#
#   1. CSIRO chrome      → header, tabs, backgrounds, links, active state.
#                          NEVER applied to chart data series.
#   2. OpenElectricity   → composition-tab carrier bands (semantic energy
#      carrier-categorical    colours: green wind, yellow solar, ...). NEVER the
#                          page chrome.
#   3. Price-sequential  → frontier/time tabs (Viridis-by-price). Kept as-is —
#                          price is sequential, a sequential colourmap is correct.
# =========================================================================== #

# --- (1) CSIRO brand token layer -------------------------------------------- #
# REFERENCE-MATCHED, PENDING OFFICIAL SPEC. These hex/typeface values are
# sampled to match the visible CSIRO reference (navy header, teal-cyan accent,
# light body); they are NOT certified CSIRO brand tokens. Swap this single block
# when the official brand spec + assets are supplied. The logo is a marked
# placeholder — the CSIRO mark is NOT reproduced/traced from any screenshot.
_CSIRO = {
    "navy": "#0a2a4a",  # header band            (reference-matched)
    "navy_deep": "#06203a",  # header gradient foot   (reference-matched)
    "cyan": "#00a9ce",  # accent / links / active(reference-matched)
    "cyan_soft": "#7fd4e6",  # title tint on navy     (reference-matched)
    "body_bg": "#f4f6f8",  # page background        (reference-matched)
    "panel": "#ffffff",  # card / chart canvas
    "ink": "#1f2733",  # body text
    "muted": "#5b6675",  # secondary text / meta
    "border": "#e3e6ec",  # card borders / gridlines
    "font": "Inter, 'Segoe UI', system-ui, -apple-system, sans-serif",
    "logo_placeholder": "[ CSIRO logo — official asset to be supplied ]",
}

# --- (2) OpenElectricity carrier-categorical (composition tab ONLY) --------- #
# Semantic energy-carrier colours a domain reader already knows (a green band is
# wind without consulting the legend). Approximate OpenElectricity conventions.
_CARRIER_COLOURS = {
    "Solar": "#f5c518",  # solar yellow
    "Wind": "#4ca646",  # wind green
    "Water": "#4d8fcc",  # hydro blue
    "Biomass": "#33a29b",  # bioenergy teal
    "Gas": "#e8833a",  # gas orange
    "Hyblend": "#c77d52",  # gas/H2 blend (muted orange-brown)
    "Hydrogen": "#8e6fc3",  # hydrogen violet
    "Black Coal": "#2b2b2b",  # coal near-black
    "Brown Coal": "#6b4f2a",  # brown coal
    "Liquid Fuel": "#c0504d",  # distillate red
    "Storage": "#8e5ba6",  # battery discharge — purple (OE convention)
}
_CARRIER_FALLBACK = "#9aa0a6"  # grey for any unmapped carrier
_STORAGE_CHARGE_COLOUR = "rgba(142,91,166,0.45)"  # battery charge band (below axis)

# Non-generation series carried in the dispatch parquet alongside the carrier
# bands: a demand line, the negative charge band, and the per-snapshot emissions
# rate. They are NEVER stacked into the generation area — the figure builders
# pull them out by name.
_SPECIAL_SERIES = {"Demand", "Storage charge", "CO2 t/h"}

# --- Chart-canvas neutrals (the Plotly template; NOT chrome, NOT data) ------ #
# Charts sit on clean white canvases framed by CSS; ink text, thin grey grid.
# Navy/cyan chrome deliberately does NOT appear here.
_INK = _CSIRO["ink"]
_PAPER = "#ffffff"
_PANEL = "#ffffff"
_GRID = _CSIRO["border"]
_FONT = _CSIRO["font"]

# Neutral fallback colorway for any categorical trace WITHOUT an explicit colour.
# (Carriers now get explicit semantic colours; price traces get Viridis-by-price.
# This only catches anything unforeseen — kept neutral so nothing accidentally
# borrows chrome navy/cyan.)
_PALETTE = [
    "#5b6675",
    "#8a8d93",
    "#3a6ea5",
    "#6aa84f",
    "#c0504d",
    "#8e7cc3",
    "#d4a017",
    "#5a8f7b",
    "#a64d79",
    "#33a29b",
]


def _register_template() -> None:
    """Register the 'frontier' Plotly template and make it the default, so every
    figure inherits the identity with no per-figure restyling that could drift."""
    tmpl = go.layout.Template()
    tmpl.layout = go.Layout(
        font=dict(family=_FONT, size=13, color=_INK),
        paper_bgcolor=_PAPER,
        plot_bgcolor=_PANEL,
        colorway=_PALETTE,
        margin=dict(l=70, r=60, t=64, b=96),
        title=dict(
            font=dict(family=_FONT, size=18, color=_INK), x=0.01, xanchor="left"
        ),
        hoverlabel=dict(
            bgcolor=_INK,
            bordercolor=_INK,
            font=dict(family=_FONT, color="#ffffff", size=12),
        ),
        # Horizontal legend BELOW the plot. In the contained-width layout a
        # right-side vertical legend clipped its labels against the framed-canvas
        # edge; a bottom horizontal legend has the full width to lay out and
        # cannot clip. Applied template-wide (single source) so every legend
        # inherits it.
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="left",
            x=0,
            font=dict(family=_FONT, size=12),
            bgcolor="rgba(255,255,255,0)",
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor=_GRID,
            zeroline=False,
            linecolor=_GRID,
            ticks="outside",
            tickcolor=_GRID,
            title=dict(font=dict(family=_FONT, size=13, color=_INK)),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=_GRID,
            zeroline=False,
            linecolor=_GRID,
            ticks="outside",
            tickcolor=_GRID,
            title=dict(font=dict(family=_FONT, size=13, color=_INK)),
        ),
    )
    pio.templates["frontier"] = tmpl
    pio.templates.default = "frontier"


_register_template()


# --------------------------------------------------------------------------- #
# Data loading (cached; CSVs are the source of truth)
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def _load_frontier() -> pd.DataFrame:
    df = pd.read_csv(_FRONTIER_CSV)
    df["carbon_price_aud_per_tco2e"] = df["carbon_price_aud_per_tco2e"].astype(float)
    df["year"] = df["year"].astype(int)
    return df


@st.cache_data(show_spinner=False)
def _load_compositions() -> pd.DataFrame:
    df = pd.read_csv(_COMPOSITIONS_CSV)
    df["carbon_price_aud_per_tco2e"] = df["sweep_id"].apply(_price_from_sweep_id)
    return df


@st.cache_data(show_spinner=False)
def _load_methodology_sections() -> dict[str, str]:
    """Split METHODOLOGY.md into {section_title: body} on its `## ` headers.

    The doc tabs render these verbatim so they cannot contradict the methodology
    file — drift-prevention identical to reading the data CSVs directly.
    """
    text = _METHODOLOGY_MD.read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    title, buf = None, []
    for line in text.splitlines():
        header = re.match(r"^##\s+(.*)", line)
        if header:
            if title is not None:
                sections[title] = "\n".join(buf).strip()
            title, buf = header.group(1).strip(), []
        elif title is not None:
            buf.append(line)
    if title is not None:
        sections[title] = "\n".join(buf).strip()
    return sections


def _price_from_sweep_id(sweep_id: str) -> float:
    """'c150' -> 150.0. The sweep_id trailing number is the carbon price."""
    return float(re.sub(r"[^0-9]", "", sweep_id))


def _section(sections: dict[str, str], key_substr: str) -> str:
    """Fetch a methodology section by case-insensitive substring of its title."""
    for title, body in sections.items():
        if key_substr.lower() in title.lower():
            return f"### {title}\n\n{body}"
    return ""


# --------------------------------------------------------------------------- #
# Colour: carbon price as a continuous sweep (sequential ramp, not categorical)
# --------------------------------------------------------------------------- #
def _price_colours(prices: list[float]) -> dict[float, str]:
    positions = [i / (len(prices) - 1) for i in range(len(prices))]
    ramp = pcolors.sample_colorscale("Viridis", positions)
    return dict(zip(prices, ramp))


def _price_label(price: float) -> str:
    return f"${price:.0f}/tCO₂e"


# --------------------------------------------------------------------------- #
# View 1 — cost-emissions frontier (headline)
# --------------------------------------------------------------------------- #
def _frontier_figure(
    df: pd.DataFrame, year: int, prices: list[float], show_other_years: bool
) -> go.Figure:
    fig = go.Figure()

    if show_other_years:
        for other in sorted(df["year"].unique()):
            if other == year:
                continue
            d = df[df["year"] == other].sort_values("carbon_price_aud_per_tco2e")
            fig.add_trace(
                go.Scatter(
                    x=d["cost_per_mwh_excl_fuel_carbon"],
                    y=d["co2_t_per_mwh"],
                    mode="lines",
                    line=dict(color="rgba(180,180,180,0.5)", width=1),
                    name=f"{other}",
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    d = df[df["year"] == year].sort_values("carbon_price_aud_per_tco2e")
    # connecting line (the frontier curve, in price order)
    fig.add_trace(
        go.Scatter(
            x=d["cost_per_mwh_excl_fuel_carbon"],
            y=d["co2_t_per_mwh"],
            mode="lines",
            line=dict(color="rgba(90,90,90,0.6)", width=1.5),
            hoverinfo="skip",
            showlegend=False,
        )
    )
    # the price-coloured points
    fig.add_trace(
        go.Scatter(
            x=d["cost_per_mwh_excl_fuel_carbon"],
            y=d["co2_t_per_mwh"],
            mode="markers+text",
            text=[_price_label(p) for p in d["carbon_price_aud_per_tco2e"]],
            textposition="middle right",
            textfont=dict(size=11),
            marker=dict(
                size=15,
                color=d["carbon_price_aud_per_tco2e"],
                colorscale="Viridis",
                cmin=min(prices),
                cmax=max(prices),
                line=dict(color="white", width=1),
                colorbar=dict(title="Carbon price<br>($/tCO₂e)"),
            ),
            customdata=d[["carbon_price_aud_per_tco2e", "tolerance_robust"]],
            hovertemplate=(
                "Carbon price: $%{customdata[0]:.0f}/tCO₂e<br>"
                f"{_COST_AXIS}: $%{{x:.1f}}<br>"
                "CO₂ intensity: %{y:.4f} t/MWh<br>"
                "tolerance_robust: %{customdata[1]}<extra></extra>"
            ),
            showlegend=False,
        )
    )

    fig.update_layout(
        title=f"Cost–emissions frontier — {year}",
        xaxis_title=_COST_AXIS,
        yaxis_title=_CO2_AXIS,
        height=560,
    )
    return fig


# --------------------------------------------------------------------------- #
# View 2 — time evolution (the cross-year story)
# --------------------------------------------------------------------------- #
def _time_evolution_figure(
    df: pd.DataFrame,
    metric: str,
    metric_label: str,
    prices: list[float],
    price_colours: dict[float, str],
    yfmt: str,
) -> go.Figure:
    fig = go.Figure()
    for price in prices:
        d = df[df["carbon_price_aud_per_tco2e"] == price].sort_values("year")
        fig.add_trace(
            go.Scatter(
                x=d["year"],
                y=d[metric],
                mode="lines+markers",
                name=_price_label(price),
                line=dict(color=price_colours[price], width=2),
                marker=dict(size=7),
                connectgaps=False,  # never bridge a missing point
                customdata=d[["carbon_price_aud_per_tco2e", "tolerance_robust"]],
                hovertemplate=(
                    "Carbon price: $%{customdata[0]:.0f}/tCO₂e<br>"
                    "Year: %{x}<br>"
                    f"{metric_label}: %{{y:{yfmt}}}<br>"
                    "tolerance_robust: %{customdata[1]}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title=f"{metric_label} over time",
        xaxis_title="Year",
        yaxis_title=metric_label,
        height=520,
        legend=dict(title="Carbon price"),
    )
    fig.update_xaxes(tickvals=sorted(df["year"].unique()))
    return fig


# --------------------------------------------------------------------------- #
# View 3 — carbon-price selector (move along the price axis)
# --------------------------------------------------------------------------- #
def _price_highlight_figure(
    df: pd.DataFrame,
    selected: float,
    metric: str,
    metric_label: str,
    prices: list[float],
    price_colours: dict[float, str],
    yfmt: str,
) -> go.Figure:
    fig = go.Figure()
    for price in prices:
        d = df[df["carbon_price_aud_per_tco2e"] == price].sort_values("year")
        chosen = price == selected
        fig.add_trace(
            go.Scatter(
                x=d["year"],
                y=d[metric],
                mode="lines+markers",
                name=_price_label(price),
                connectgaps=False,
                line=dict(
                    color=price_colours[price] if chosen else "rgba(200,200,200,0.7)",
                    width=4 if chosen else 1,
                ),
                marker=dict(size=9 if chosen else 4),
                opacity=1.0 if chosen else 0.6,
                hovertemplate=(
                    "$%{text}/tCO₂e<br>Year %{x}<br>"
                    f"{metric_label}: %{{y:{yfmt}}}<extra></extra>"
                ),
                text=[f"{price:.0f}"] * len(d),
            )
        )
    fig.update_layout(
        title=f"{metric_label} — {_price_label(selected)} highlighted",
        xaxis_title="Year",
        yaxis_title=metric_label,
        height=420,
        showlegend=False,
    )
    fig.update_xaxes(tickvals=sorted(df["year"].unique()))
    return fig


# --------------------------------------------------------------------------- #
# View 4 — technology mix (gated, demoted, soft-flagged)
# --------------------------------------------------------------------------- #
def _composition_figure(comp: pd.DataFrame, price: float) -> go.Figure:
    d = comp[comp["carbon_price_aud_per_tco2e"] == price]
    carriers = [c for c in _CARRIER_ORDER if c in set(d["carrier"])]
    carriers += [c for c in sorted(d["carrier"].unique()) if c not in carriers]
    fig = go.Figure()
    for carrier in carriers:
        dc = d[d["carrier"] == carrier].sort_values("year")
        if dc["capacity_gw"].abs().sum() < 1e-6:
            continue
        colour = _CARRIER_COLOURS.get(carrier, _CARRIER_FALLBACK)
        fig.add_trace(
            go.Scatter(
                x=dc["year"],
                y=dc["capacity_gw"],
                mode="lines",
                stackgroup="cap",
                name=carrier,
                line=dict(width=0.5, color=colour),
                fillcolor=colour,
                hovertemplate=(
                    f"{carrier}<br>Year %{{x}}<br>%{{y:.1f}} GW "
                    "(indicative — soft at tolerance)<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title=f"Capacity mix by carrier — {_price_label(price)} "
        "(INDICATIVE, non-contract)",
        xaxis_title="Year",
        yaxis_title="Capacity (GW)",
        height=520,
    )
    fig.update_xaxes(tickvals=sorted(d["year"].unique()))
    return fig


# --------------------------------------------------------------------------- #
# View 5 — fuel input intensity (restored from v1). These per-MWh-delivered fuel
# inputs (gj_per_mwh_*) ARE a contract output: input_commodities / input_
# coefficients are what the Pass-2 orchestrator prices. One small-multiple panel
# per commodity present; price-sequential lines over time (same encoding as the
# frontier/time tabs — NOT carrier-categorical, this is the price sweep).
# --------------------------------------------------------------------------- #
_FUEL_COMMODITIES = [
    ("coal", "Coal"),
    ("natural_gas", "Natural gas"),
    ("biomass", "Biomass"),
    ("diesel", "Diesel"),
    ("hydrogen", "Hydrogen"),
]


def _fuel_intensity_figure(
    df: pd.DataFrame,
    prices: list[float],
    price_colours: dict[float, str],
    years: list[int],
) -> go.Figure:
    present = [
        (c, lbl)
        for c, lbl in _FUEL_COMMODITIES
        if f"gj_per_mwh_{c}" in df.columns and df[f"gj_per_mwh_{c}"].abs().sum() > 1e-9
    ]
    ncol = 2
    nrow = (len(present) + ncol - 1) // ncol
    fig = make_subplots(
        rows=nrow,
        cols=ncol,
        subplot_titles=[lbl for _, lbl in present],
        vertical_spacing=0.13,
        horizontal_spacing=0.09,
    )
    for i, (commodity, _lbl) in enumerate(present):
        row, col = i // ncol + 1, i % ncol + 1
        for price in prices:
            d = df[df["carbon_price_aud_per_tco2e"] == price].sort_values("year")
            fig.add_trace(
                go.Scatter(
                    x=d["year"],
                    y=d[f"gj_per_mwh_{commodity}"],
                    mode="lines+markers",
                    line=dict(color=price_colours[price], width=2),
                    marker=dict(size=5),
                    name=_price_label(price),
                    legendgroup=_price_label(price),
                    showlegend=(i == 0),
                    connectgaps=False,
                    hovertemplate=(
                        "$%{text}/tCO₂e<br>Year %{x}<br>"
                        f"{commodity}: %{{y:.3f}} GJ/MWh<extra></extra>"
                    ),
                    text=[f"{price:.0f}"] * len(d),
                ),
                row=row,
                col=col,
            )
        fig.update_yaxes(title_text="GJ/MWh", row=row, col=col)
        fig.update_xaxes(tickvals=years, row=row, col=col)
    fig.update_layout(
        title="Fuel input intensity by commodity (GJ per MWh delivered)",
        height=270 * nrow + 90,
    )
    return fig


# --------------------------------------------------------------------------- #
# View 6 — cost decomposition (restored from v1's cost-intensity-separation
# emphasis). At the year-t LP basis the identity is exact: bundled =
# year-t-incremental (kept) + fuel + carbon (BOTH stripped, because Pass-2 prices
# them). Makes visible WHY the headline fleet cost is low — most of the bundled
# LP cost is the fuel + carbon that the contract column deliberately removes.
# Non-chrome, non-carrier categorical palette (it's a cost breakdown, not a
# price sweep or a carrier mix).
# --------------------------------------------------------------------------- #
def _cost_decomposition_figure(df: pd.DataFrame, selected: float) -> go.Figure:
    d = df[df["carbon_price_aud_per_tco2e"] == selected].sort_values("year")
    parts = [
        (
            "diagnostic_cost_per_mwh_year_t_incremental",
            "Kept: year-t cost (excl. fuel & carbon)",
            "#3a6ea5",
        ),
        (
            "diagnostic_fuel_cost_per_mwh",
            "Stripped: fuel (Pass-2 prices it)",
            "#e8833a",
        ),
        (
            "diagnostic_carbon_cost_per_mwh",
            "Stripped: carbon (Pass-2 prices it)",
            "#8a8d93",
        ),
    ]
    fig = go.Figure()
    for col, label, colour in parts:
        fig.add_trace(
            go.Bar(
                x=d["year"],
                y=d[col],
                name=label,
                marker_color=colour,
                hovertemplate=f"{label}<br>Year %{{x}}<br>$%{{y:.1f}}/MWh<extra></extra>",
            )
        )
    fig.update_layout(
        barmode="stack",
        title=f"Year-t cost decomposition — {_price_label(selected)}: "
        "kept + stripped = bundled LP cost",
        xaxis_title="Year",
        yaxis_title="$/MWh (year-t basis)",
        height=420,
    )
    fig.update_xaxes(tickvals=sorted(d["year"].unique()))
    return fig


# --------------------------------------------------------------------------- #
# View 0 — GRANULAR GENERATION (the headline OpenElectricity-register view).
# 30-minute dispatch-by-carrier per (carbon-price, year) cell, pre-extracted to
# dispatch/dispatch_30min_<sweep>.parquet (the .nc load is ~50 s/cell — too slow
# to do live). This is the SOLVED PRIMAL dispatch — the same primal that
# underlies the frontier coordinates, so it inherits their validity (incl. the
# 2050 primal-sane note). Carrier-semantic colours (system 2, NOT chrome); demand
# overlaid as a line. A window control trades the seasonal envelope (year, daily
# mean) for diurnal detail (a 30-minute week).
# --------------------------------------------------------------------------- #
_DISPATCH_DIR = _FRONTIER_DIR / "dispatch"
_SNAPSHOTS_PER_WEEK = 336  # 7 days × 48 half-hours


@st.cache_data(show_spinner=False)
def _load_dispatch(sweep_id: str) -> pd.DataFrame | None:
    """One trajectory's pre-extracted 30-min dispatch, or None if not built yet."""
    path = _DISPATCH_DIR / f"dispatch_30min_{sweep_id}.parquet"
    if not path.exists():
        return None
    d = pd.read_parquet(path)
    d["snapshot"] = pd.to_datetime(d["snapshot"])
    return d


def _windowed(
    disp: pd.DataFrame, year: int, mode: str, week_idx: int
) -> tuple[pd.DataFrame, str]:
    """The chosen (cell, window) as a wide [snapshot × series] frame in NATIVE
    units (MW for power, tCO₂e/h for the CO2 series — so the blanket GW scaling is
    applied per figure, never to the emissions series). 'Year' = daily mean (~365
    points, the seasonal envelope); 'week' = a 336-snapshot 30-min slice."""
    d = disp[disp["year"] == year]
    wide = d.pivot_table(
        index="snapshot", columns="carrier", values="mw", aggfunc="sum"
    ).sort_index()
    if mode == "Sample week (30-min)":
        lo = week_idx * _SNAPSHOTS_PER_WEEK
        return wide.iloc[lo : lo + _SNAPSHOTS_PER_WEEK], "Half-hour"
    return wide.resample("D").mean(), "Day"


def _generation_figure(
    wide: pd.DataFrame, price: float, year: int, xtitle: str
) -> go.Figure:
    """Stacked generation by carrier (GW). Battery discharge is a positive band
    ('Storage'); charge is a negative band below the axis ('Storage charge',
    OpenElectricity-style) — the load-shifting that makes high-VRE dispatch feasible
    is then visible. Demand is a dotted line."""
    gen_cols = [c for c in wide.columns if c not in _SPECIAL_SERIES]
    order = [c for c in _CARRIER_ORDER if c in gen_cols]
    order += [c for c in gen_cols if c not in order]
    fig = go.Figure()
    for carrier in order:
        gw = wide[carrier] / 1000.0
        if gw.abs().sum() < 1e-9:
            continue
        colour = _CARRIER_COLOURS.get(carrier, _CARRIER_FALLBACK)
        fig.add_trace(
            go.Scatter(
                x=gw.index,
                y=gw,
                mode="lines",
                stackgroup="gen",
                name=carrier,
                line=dict(width=0.5, color=colour),
                fillcolor=colour,
                hovertemplate=f"{carrier}<br>%{{x}}<br>%{{y:.1f}} GW<extra></extra>",
            )
        )
    if "Storage charge" in wide.columns and wide["Storage charge"].abs().sum() > 1e-6:
        gw = wide["Storage charge"] / 1000.0  # already negative → stacks below axis
        fig.add_trace(
            go.Scatter(
                x=gw.index,
                y=gw,
                mode="lines",
                stackgroup="charge",
                name="Storage charge",
                line=dict(width=0.5, color=_STORAGE_CHARGE_COLOUR),
                fillcolor=_STORAGE_CHARGE_COLOUR,
                hovertemplate="Storage charge<br>%{x}<br>%{y:.1f} GW<extra></extra>",
            )
        )
    if "Demand" in wide.columns:
        gw = wide["Demand"] / 1000.0
        fig.add_trace(
            go.Scatter(
                x=gw.index,
                y=gw,
                mode="lines",
                name="Demand",
                line=dict(color=_INK, width=1.6, dash="dot"),
                hovertemplate="Demand<br>%{x}<br>%{y:.1f} GW<extra></extra>",
            )
        )
    fig.update_layout(
        title=f"Generation by carrier — {_price_label(price)}, {year}",
        xaxis_title=xtitle,
        yaxis_title="Generation (GW)",
        height=480,
    )
    return fig


def _emissions_panel_figure(wide: pd.DataFrame, xtitle: str) -> go.Figure:
    """Linked emissions panels sharing the generation stack's time axis: the
    Scope-1 emissions rate (tCO₂e/h) and the emission intensity (tCO₂e/MWh sent
    out = rate ÷ total supply)."""
    co2 = wide.get("CO2 t/h")
    gen_cols = [c for c in wide.columns if c not in _SPECIAL_SERIES]
    total_mw = wide[gen_cols].clip(lower=0).sum(axis=1)
    intensity = (co2 / total_mw.replace(0.0, pd.NA)).fillna(0.0)
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.09,
        subplot_titles=("Emissions rate (tCO₂e/h)", "Emission intensity (tCO₂e/MWh)"),
    )
    fig.add_trace(
        go.Scatter(
            x=co2.index,
            y=co2,
            mode="lines",
            fill="tozeroy",
            line=dict(color="#6b6f76", width=0.8),
            fillcolor="rgba(120,120,128,0.35)",
            hovertemplate="%{x}<br>%{y:,.0f} tCO₂e/h<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=intensity.index,
            y=intensity,
            mode="lines",
            line=dict(color="#b00020", width=1.4),
            hovertemplate="%{x}<br>%{y:.3f} tCO₂e/MWh<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.update_yaxes(title_text="tCO₂e/h", row=1, col=1)
    fig.update_yaxes(title_text="tCO₂e/MWh", row=2, col=1)
    fig.update_xaxes(title_text=xtitle, row=2, col=1)
    fig.update_layout(
        height=360, showlegend=False, title="Emissions (time-aligned with the stack)"
    )
    return fig


def _annual_energy_figure(disp: pd.DataFrame, year: int, price: float) -> go.Figure:
    """Annual PRIMARY energy by carrier (TWh) — the light always-on overview.
    Excludes storage (discharge is re-released stored energy, not primary
    generation) and the demand/CO2 series. Energy = Σ MW × 0.5 h."""
    exclude = _SPECIAL_SERIES | {"Storage"}
    d = disp[(disp["year"] == year) & (~disp["carrier"].isin(exclude))]
    twh = (d.groupby("carrier")["mw"].sum() * 0.5 / 1e6).sort_values()
    twh = twh[twh.abs() > 1e-6]
    colours = [_CARRIER_COLOURS.get(c, _CARRIER_FALLBACK) for c in twh.index]
    fig = go.Figure(
        go.Bar(
            x=twh.values,
            y=list(twh.index),
            orientation="h",
            marker_color=colours,
            hovertemplate="%{y}<br>%{x:.1f} TWh<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Annual energy by carrier — {_price_label(price)}, {year}",
        xaxis_title="Energy (TWh/yr)",
        height=480,
        showlegend=False,
    )
    return fig


def _generation_tab(prices: list[float], years: list[int]) -> None:
    """OpenElectricity-register view: cell + window controls, the stacked
    generation (with storage charge/discharge), the annual-energy overview, and
    the linked emissions panels — all from the solved-network primal dispatch."""
    st.caption(
        "Half-hourly dispatch from the **solved network primal** — the same primal "
        "the frontier coordinates are read from. Stacked bands are generation; "
        "**battery discharge is the purple band, charge the negative band below the "
        "axis** (the load-shifting that makes high-VRE dispatch feasible); the dotted "
        "line is demand. The emissions panels below share the time axis."
    )
    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        price = st.select_slider(
            "Carbon price",
            options=prices,
            value=prices[len(prices) // 2],
            format_func=_price_label,
            key="gen_price",
        )
    with c2:
        year = st.select_slider(
            "Milestone year", options=years, value=years[-1], key="gen_year"
        )
    with c3:
        mode = st.radio(
            "Window",
            ["Year (daily average)", "Sample week (30-min)"],
            key="gen_mode",
            horizontal=True,
        )
    week_idx = 0
    if mode == "Sample week (30-min)":
        week_idx = (
            st.slider(
                "Week of reference year (1 = early July, FY start)",
                1,
                52,
                2,
                key="gen_week",
            )
            - 1
        )

    disp = _load_dispatch(f"c{int(price)}")
    if disp is None:
        st.warning(
            f"Dispatch store for c{int(price)} not found. Build it with "
            "`build_dispatch_store.py` (one parquet per trajectory)."
        )
        return
    wide, xtitle = _windowed(disp, year, mode, week_idx)
    top_l, top_r = st.columns([2, 1])
    with top_l:
        st.plotly_chart(_generation_figure(wide, price, year, xtitle), width="stretch")
    with top_r:
        st.plotly_chart(_annual_energy_figure(disp, year, price), width="stretch")
    st.plotly_chart(_emissions_panel_figure(wide, xtitle), width="stretch")


# --------------------------------------------------------------------------- #
# Skeleton-level theming — the structural changes that shed the default-Streamlit
# look: contained body (gutters + max-width), CSIRO navy header band, branded
# tab bar, framed chart canvases, and a typographic hierarchy. CSS only reaches
# chrome; chart DATA styling lives in the Plotly template + carrier map. All
# CSIRO chrome here; no navy/cyan touches a chart series.
# CRITICAL: the soft-quantity / gap banners are made MORE prominent (caveats,
# not decoration) — never muted, never moved.
# --------------------------------------------------------------------------- #
def _inject_css() -> None:
    c = _CSIRO
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        html, body, [class*="css"], .stMarkdown, .stApp {{
            font-family: {c["font"]};
            color: {c["ink"]};
        }}
        .stApp {{ background: {c["body_bg"]}; }}

        /* (skeleton) Contained body — gutters + max-width kill the full-bleed
           default-Streamlit tell. */
        /* (skeleton) Full-width dense layout — no centred-column cap. The screen
           real estate carries information (OpenElectricity register), not margin. */
        .block-container {{
            max-width: 100%;
            padding-top: 1.0rem; padding-bottom: 3rem;
            padding-left: 2.0rem; padding-right: 2.0rem;
        }}

        /* (skeleton) CSIRO navy header band */
        .csiro-header {{
            background: linear-gradient(180deg, {c["navy"]} 0%, {c["navy_deep"]} 100%);
            border-radius: 10px; padding: 1.4rem 1.6rem; margin-bottom: 1.4rem;
            color: #ffffff;
        }}
        .csiro-logo {{
            display: inline-block; font-size: 0.72rem; letter-spacing: 0.04em;
            color: {c["cyan_soft"]}; border: 1px dashed {c["cyan"]};
            border-radius: 5px; padding: 0.28rem 0.6rem; margin-bottom: 0.9rem;
        }}
        .csiro-logo-img {{
            height: 54px; width: 54px; display: block; margin-bottom: 0.8rem;
        }}
        .csiro-title {{
            font-size: 2.0rem; font-weight: 300; letter-spacing: -0.5px;
            color: {c["cyan_soft"]}; margin: 0 0 0.5rem 0; line-height: 1.15;
        }}
        .csiro-sub {{
            font-size: 1.0rem; font-weight: 400; color: #eaf1f5;
            margin: 0 0 0.5rem 0; max-width: 70ch;
        }}
        .csiro-sub strong {{ color: #ffffff; }}
        .csiro-meta {{
            font-size: 0.82rem; color: {c["cyan_soft"]}; opacity: 0.85; margin: 0;
        }}

        /* (skeleton) Tab bar — CSIRO treatment, not the default thin underline */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.15rem; border-bottom: 2px solid {c["border"]};
        }}
        .stTabs [data-baseweb="tab"] {{
            font-weight: 500; padding: 0.5rem 1.05rem; color: {c["muted"]};
        }}
        .stTabs [aria-selected="true"] {{
            color: {c["cyan"]}; border-bottom: 3px solid {c["cyan"]};
            background: rgba(0,169,206,0.06); border-radius: 6px 6px 0 0;
        }}

        /* (skeleton) Framed chart canvases — charts sit on carded white, not
           flush-on-grey. */
        [data-testid="stPlotlyChart"] {{
            background: {c["panel"]}; border: 1px solid {c["border"]};
            border-radius: 10px; padding: 0.5rem 0.6rem;
            box-shadow: 0 1px 3px rgba(10,42,74,0.05);
        }}

        /* (skeleton) Typographic hierarchy */
        h2, h3 {{ color: {c["navy"]}; font-weight: 600; letter-spacing: -0.2px; }}
        [data-testid="stCaptionContainer"] {{ color: {c["muted"]}; }}

        /* Links / primary accents to CSIRO cyan */
        a, a:visited {{ color: {c["cyan"]}; }}

        /* Caveat banners: KEEP LOUD — heavier left border + weight so the
           non-contract / gap warnings read as caveats, never as chrome. */
        [data-testid="stAlert"] {{
            border-left: 6px solid currentColor; font-weight: 500;
            border-radius: 6px;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def _logo_html() -> str:
    """The supplied CSIRO logo as an inline data-URI <img>, or the marked text
    placeholder if the asset isn't present. Embedded inline so it sits inside
    the navy header HTML block (Streamlit markdown can't reference local files)."""
    for path in _LOGO_CANDIDATES:
        if path.exists():
            b64 = base64.b64encode(path.read_bytes()).decode()
            return (
                f'<img class="csiro-logo-img" '
                f'src="data:image/png;base64,{b64}" alt="CSIRO"/>'
            )
    return f'<div class="csiro-logo">{_CSIRO["logo_placeholder"]}</div>'


def _render_header() -> None:
    """CSIRO navy header band: logo + light-weight title + the carbon-price-as-
    shaping subtitle (surface caveat #3, kept above the tabs) + a small
    Pass-1/source meta line. Rendered as one HTML block so the navy chrome can
    carry the title — no chart colours involved."""
    c = _CSIRO
    st.markdown(
        f"""
        <div class="csiro-header">
          {_logo_html()}
          <div class="csiro-title">Grid-electricity cost–emissions frontier</div>
          <div class="csiro-sub">
            <strong>Seven trajectories traced by an internal carbon price — a
            build-shaping parameter, <em>not</em> forecasts or proposed
            pathways.</strong> Each line is a sustained-carbon-price scenario;
            the price sweeps the frontier. Cost is the fleet's annualised fixed
            cost <strong>excluding fuel and carbon</strong> (priced downstream) —
            it is <strong>not</strong> a retail or wholesale electricity price.
          </div>
          <div class="csiro-meta">
            Pass-1 menu for a multi-sector optimiser · read live from
            frontier_points.tidy.csv / compositions_NONCONTRACT.tidy.csv ·
            CSIRO branding reference-matched, pending official spec
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# --------------------------------------------------------------------------- #
# Page
# --------------------------------------------------------------------------- #
def main() -> None:
    st.set_page_config(
        page_title="Grid-electricity cost–emissions frontier", layout="wide"
    )
    _inject_css()

    if not _FRONTIER_CSV.exists():
        st.error(f"Menu artifact not found: {_FRONTIER_CSV}")
        st.stop()

    frontier = _load_frontier()
    sections = _load_methodology_sections()
    prices = sorted(frontier["carbon_price_aud_per_tco2e"].unique())
    years = sorted(frontier["year"].unique())
    price_colours = _price_colours(prices)

    # ---- CSIRO navy header band: title + carbon-price-as-shaping caveat
    #      (surface caveat #3, kept above the tabs) + Pass-1/source meta ----
    _render_header()

    # ---- Surface-level VALIDITY provenance (read-governing, above the tabs).
    #      Post-production-run state: the carry-forward defect is fixed + validated,
    #      all 35 cells solved Optimal on FINAL v7.8 data. The 2050 cells carry the
    #      benign dual-non-convergence artifact (primal-sound), and the renewable
    #      share is a conservative floor. Full caveat set in the doc tabs.
    st.success(
        "✓ **Production menu — all 35 cells valid (full 7×5 on FINAL 2026 ISP, "
        "workbook v7.8).** The carry-forward defect is fixed and validated; every "
        "chain solved Optimal across 2030–2050 with endogenous economic retirement. "
        "**2050 cells are near-optimal-by-continuity** — the largest LPs end with a "
        "benign dual-non-convergence artifact (objective field corrupted, but the "
        "primal is sound and every cost is recomputed from it), flagged by "
        "`tolerance_robust=False` rather than treated as invalid. The renewable share "
        "is a **conservative floor** (the retirement approximations all push the same "
        "way). Full caveat set in the Assumptions / Sources / Scope tabs."
    )

    # Four subject-grouped tabs (consolidated from nine single-chart tabs): each
    # carries multiple linked, proportioned panels so the full width reads as
    # information density, not one stretched chart.
    tab_gen, tab_frontier, tab_transmission, tab_comp, tab_method = st.tabs(
        [
            "Generation",
            "Cost–emissions frontier",
            "Transmission",
            "Composition & fuels",
            "Methodology & provenance",
        ]
    )

    # ---- Tab 1: granular generation (headline, OpenElectricity register) ----
    with tab_gen:
        _generation_tab(prices, years)

    # ---- Tab 2: frontier — scatter + time-evolution side-by-side (proportioned,
    #      so neither reads as a full-width smear) + a carbon-price deep-dive ----
    with tab_frontier:
        fc1, fc2 = st.columns([1, 1])
        with fc1:
            year = st.select_slider(
                "Milestone year (scatter)",
                options=years,
                value=years[-1],
                key="frontier_year",
            )
            show_other = st.checkbox("Show other years (faint)", value=True)
            st.plotly_chart(
                _frontier_figure(frontier, year, prices, show_other), width="stretch"
            )
            st.caption(
                "Higher carbon price → higher fleet cost, lower CO₂; faint "
                "grey curves are the other years."
            )
        with fc2:
            te_metrics = {
                "CO₂ intensity": ("co2_t_per_mwh", _CO2_AXIS, ".4f"),
                "Fleet cost": ("cost_per_mwh_excl_fuel_carbon", _COST_AXIS, ".1f"),
                "Renewable share %": (
                    "renewable_share_pct_bulk_grid",
                    "Renewable share % (incl. hydro, bulk grid)",
                    ".1f",
                ),
                "Annual generation (TWh)": (
                    "annual_generation_twh",
                    "Annual generation (TWh)",
                    ".1f",
                ),
            }
            metric_choice = st.radio(
                "Metric (time evolution)",
                list(te_metrics),
                horizontal=True,
                key="te_metric",
            )
            col, axis, yfmt = te_metrics[metric_choice]
            st.plotly_chart(
                _time_evolution_figure(
                    frontier, col, axis, prices, price_colours, yfmt
                ),
                width="stretch",
            )
            st.caption(
                "All seven trajectories run 2030–2050; 2050 carries the "
                "dual-non-convergence note (primal-sound), not absent."
            )

        st.markdown("---")
        st.markdown("#### Carbon-price deep-dive")
        selected = st.select_slider(
            "Highlight a carbon price",
            options=prices,
            value=prices[len(prices) // 2],
            format_func=_price_label,
            key="sel_price",
        )
        sc1, sc2 = st.columns(2)
        with sc1:
            st.plotly_chart(
                _price_highlight_figure(
                    frontier,
                    selected,
                    "co2_t_per_mwh",
                    _CO2_AXIS,
                    prices,
                    price_colours,
                    ".4f",
                ),
                width="stretch",
            )
        with sc2:
            st.plotly_chart(
                _price_highlight_figure(
                    frontier,
                    selected,
                    "cost_per_mwh_excl_fuel_carbon",
                    _COST_AXIS,
                    prices,
                    price_colours,
                    ".1f",
                ),
                width="stretch",
            )
        dc1, dc2 = st.columns([3, 2])
        with dc1:
            st.plotly_chart(
                _cost_decomposition_figure(frontier, selected), width="stretch"
            )
            st.caption(
                "Year-t cost decomposition: the contract keeps only the blue "
                "segment; fuel + carbon are stripped (Pass-2 prices them)."
            )
        with dc2:
            _selected_trajectory_table(frontier, selected)

    # ---- Tab 3: transmission (Stage 0 — corridor congestion + cost KPI over
    #      the already-solved ccx frontier; read-only, pre-extracted parquet) ----
    with tab_transmission:
        render_transmission_tab(
            _CORRIDORS_DIR, frontier, prices, years, price_colours,
            _price_label, _price_highlight_figure,
        )

    # ---- Tab 4: composition & fuels (capacity mix + fuel-input intensity) ----
    with tab_comp:
        _technology_mix_tab(prices, price_colours)
        st.markdown("---")
        st.plotly_chart(
            _fuel_intensity_figure(frontier, prices, price_colours, years),
            width="stretch",
        )
        st.caption(
            "Per-MWh-delivered fuel inputs — a **contract output** (the "
            "input_commodities / input_coefficients the Pass-2 orchestrator prices). "
            "One panel per commodity; lines are the carbon-price sweep."
        )

    # ---- Tab 5: methodology & provenance — the doc sections, verbatim ----
    with tab_method:
        st.info(
            "Rendered directly from METHODOLOGY.md (the menu's source of truth) "
            "— cannot drift from it."
        )
        with st.expander("Assumptions & methodology", expanded=True):
            st.markdown(_section(sections, "What a row is"))
            st.markdown(_section(sections, "cost column"))
            st.markdown(_section(sections, "Emissions"))
            st.markdown(_section(sections, "Solver provenance"))
        with st.expander("Sources & provenance"):
            st.markdown(_section(sections, "Provenance"))
            st.markdown(_section(sections, "Coverage"))
        with st.expander("Scope & limitations"):
            st.markdown(_section(sections, "Scope honesty"))
            st.markdown(_section(sections, "Compositions are non-contract"))
            st.markdown(_section(sections, "Named gaps"))


def _selected_trajectory_table(frontier: pd.DataFrame, selected: float) -> None:
    d = frontier[frontier["carbon_price_aud_per_tco2e"] == selected].sort_values("year")
    show = d[
        [
            "year",
            "cost_per_mwh_excl_fuel_carbon",
            "co2_t_per_mwh",
            "renewable_share_pct_bulk_grid",
            "tolerance_robust",
        ]
    ].copy()
    show.columns = [
        "Year",
        "Fleet cost excl. fuel & carbon ($/MWh)",
        "CO₂ (t/MWh)",
        "Renewable share % (bulk grid)",
        "tolerance_robust",
    ]
    st.dataframe(show.round(3), width="stretch", hide_index=True)


def _technology_mix_tab(prices: list[float], price_colours: dict[float, str]) -> None:
    if not _COMPOSITIONS_CSV.exists():
        st.warning("Composition diagnostic not available.")
        return
    st.error(
        "⚠ **Indicative only — NOT part of the certified menu.** At the "
        "solver tolerance the objective face is flat, so individual technology "
        "capacities are soft (a carrier can move ≈30–70% between "
        "near-equal-cost solutions). The certified quantities are the "
        "**coordinates** (cost, CO₂) shown in the other tabs — these "
        "mixes are a diagnostic you have chosen to open, not a contract output."
    )
    comp = _load_compositions()
    price = st.select_slider(
        "Carbon price",
        options=prices,
        value=prices[len(prices) // 2],
        format_func=_price_label,
        key="comp_price",
    )
    st.plotly_chart(_composition_figure(comp, price), width="stretch")
    st.caption(
        "**The 2025 column is the actual existing NEM fleet (~64.7 GW capacity "
        "data — not modelled, not soft);** 2030 onward is the modelled trajectory "
        "(indicative, per the banner above). Storage, gas, CCS, hydrogen and "
        "biomass appear here as **technologies**, not as named scenarios. "
        "Nuclear is **absent from the candidate set** (not in AEMO's IASR) — "
        "it is not a hidden zero, it was never available to build."
    )


if __name__ == "__main__":
    main()
