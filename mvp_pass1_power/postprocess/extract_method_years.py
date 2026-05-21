"""Aggregate a solved ISPyPSA network into simple-msm method_years rows.

For each (archetype, milestone year), produces one row of the simple-msm
method_years schema:
  - output_cost_per_unit (AUD/MWh, EXCLUDING endogenously-priced fuel)
  - input_commodities + input_coefficients (GJ/MWh)
  - energy_emissions_by_pollutant + process_emissions_by_pollutant (t/MWh)
  - max_share / min_share / max_activity (archetype-author supplied)
  - cost_basis_year, currency, source/assumption refs

Methodological notes:

  Denominator. "Per unit" is per MWh of electricity delivered to end uses (the
  Pass-2 orchestrator's commodity). We use the modelled system demand (sum of
  load × snapshot_weighting per period), not generator dispatch. This handles
  curtailment, storage round-trip losses and transmission losses correctly —
  every MWh of fuel/cost shows up in the per-MWh-delivered intensity.

  Cost decoupling. ISPyPSA's PyPSA network stores marginal_cost as
  (fuel_price × heat_rate + VOM) per snapshot. We recover VOM-only by reading
  the pypsa-friendly generators.csv columns (isp_heat_rate_gj/mwh,
  isp_vom_$/mwh_sent_out, isp_fuel_cost_mapping) plus the IASR fuel-price tables
  (coal_prices, gas_prices, liquid_fuel_prices, biomass_prices, hydrogen_prices,
  biomethane_prices), then subtract fuel cost from total opex.

  CONSEQUENCE OF DECOUPLING: ISPyPSA's LP minimises bundled cost; we report
  fuel-decoupled cost to simple-msm. The two diverge in capacity-mix decisions
  whenever Pass 2's endogenous fuel price differs from ISPyPSA's IASR fuel price.
  For Pass 1 (where ISPyPSA selects the *menu* of archetypes from which Pass 2
  chooses), this is acceptable provided IASR fuel prices remain a reasonable
  point estimate. For Pass 3, the LP must be re-solved with the orchestrator's
  fuel-price overrides for capacity decisions to be self-consistent.

  Capex bundling. ISPyPSA pre-annuitises CAPEX and adds FOM into capital_cost
  (translator/generators.py:546-548). We expose capital_cost × p_nom_opt as a
  single annualised number per period; CAPEX/FOM split is not recovered.

  Transmission cost. Each line/link capital_cost × p_nom_opt is summed into the
  method's annualised cost in the period it is active — transmission is part of
  delivering electricity to load.

  Storage cost. Storage capital_cost × p_nom_opt and storage VOM (always small
  in IASR) are summed into the method's cost. Storage charging electricity
  comes from the same generator pool, so no double-counting.

  Snapshot weighting. PyPSA's snapshot_weightings.generators (and .stores) is
  the hours-per-snapshot scaler. ISPyPSA's translator sets these so that
  Σ(weighting) per period ≈ 8760 even when using representative weeks. We
  multiply every per-snapshot quantity by this weighting to get annual totals.

  Hyblend. Generators on the "Hyblend" carrier consume a gas+H2 mix that varies
  per investment period; we use ISPyPSA's templated h2 fraction (from
  gpg_emissions_reduction_h2) per generator per year, and split the heat-rate
  draw into a "natural_gas" portion and a "hydrogen" portion.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pypsa

from .nger_factors import nger_factor_table, hyblend_factor

log = logging.getLogger(__name__)

# Carriers classified as renewable for share computation (matches extract_granular_outputs.py).
# Water excluded: ISPyPSA models hydro with p_max_pu=1.0 (no availability traces),
# producing ~60 TWh/year vs ~15-17 TWh realistic. Including it inflates renewable
# share by ~20 pp. Wind + Solar + Biomass only.
_RENEWABLE_CARRIERS = {"Wind", "Solar", "Biomass"}

# Map ISPyPSA fuel_type (the PyPSA carrier) to simple-msm commodity ids.
# Renewables / Storage / Water carry no commodity input.
_CARRIER_TO_COMMODITY = {
    "Black Coal": "coal",
    "Brown Coal": "coal",            # Pass-2 orchestrator does not distinguish coal grades.
    "Gas": "natural_gas",
    "Liquid Fuel": "diesel",
    "Biomass": "biomass",
    "Hydrogen": "hydrogen",
    "Biomethane": "biomethane",
    # Hyblend handled per-row: split into natural_gas + hydrogen by H2 fraction.
}


# ---------- helpers (orchestrator pattern: keep each ≤10 lines) ----------


def _load_pypsa_friendly_generators(pypsa_friendly_dir: Path) -> pd.DataFrame:
    """Load the pypsa-friendly generators.csv with isp_* metadata preserved."""
    gens = pd.read_csv(pypsa_friendly_dir / "generators.csv")
    return gens.set_index("name")


def _load_fuel_price_tables(workbook_cache: Path) -> dict[str, pd.DataFrame]:
    """Load all IASR fuel-price tables relevant to ISPyPSA carriers.

    Coal and gas tables are scenario-specific; try the Step Change variant
    first (the only scenario used in Pass 1 runs).
    """
    tables = {}
    _CANDIDATES = {
        "coal_prices":        ("coal_prices_step_change", "coal_prices"),
        "gas_prices":         ("gas_prices_step_change",  "gas_prices"),
        "liquid_fuel_prices": ("liquid_fuel_prices",),
        "biomass_prices":     ("biomass_prices",),
        "hydrogen_prices":    ("hydrogen_prices",),
        "biomethane_prices":  ("biomethane_prices",),
    }
    for name, candidates in _CANDIDATES.items():
        for candidate in candidates:
            path = workbook_cache / f"{candidate}.csv"
            if path.exists():
                tables[name] = pd.read_csv(path)
                break
    return tables


def _financial_year_start(period: int) -> int:
    """ISPyPSA fy 2030 means 2029-07-01 -> 2030-06-30. The IASR price tables
    are indexed by financial-year-start label (e.g. column '2029-30').
    """
    return period - 1


def _carrier_to_price_table(carrier: str) -> str | None:
    """Return the IASR fuel-price table name for a given PyPSA carrier."""
    mapping = {
        "Black Coal": "coal_prices",
        "Brown Coal": "coal_prices",
        "Gas":        "gas_prices",
        "Liquid Fuel":"liquid_fuel_prices",
        "Biomass":    "biomass_prices",
        "Hydrogen":   "hydrogen_prices",
        "Biomethane": "biomethane_prices",
        "Hyblend":    "gas_prices",  # handled separately for H2 component
    }
    return mapping.get(carrier)


def _per_period_total_load(network: pypsa.Network) -> pd.Series:
    """Annual MWh delivered to loads per investment period (= the denominator)."""
    weightings = network.snapshot_weightings["generators"]
    load_t = network.loads_t.p_set
    load_total = (load_t.sum(axis=1) * weightings).groupby(level=0).sum()
    return load_total


def _per_period_total_dispatch(network: pypsa.Network) -> pd.DataFrame:
    """Per (period, generator) annual MWh dispatched, excluding ISPyPSA's
    custom-constraint slack generators and the unserved-energy emergency slack."""
    real = _real_generator_names(network)
    weightings = network.snapshot_weightings["generators"]
    gen_t = network.generators_t.p[real].clip(lower=0)
    return gen_t.mul(weightings, axis=0).groupby(level=0).sum()


def _real_generator_names(network: pypsa.Network) -> list[str]:
    """Names of generators that represent real assets (not slack/relaxation gens)."""
    g = network.generators
    mask = (g["bus"] != "bus_for_custom_constraint_gens") & (g["carrier"] != "Unserved Energy")
    return list(g.index[mask])


def _per_period_total_marginal_cost(network: pypsa.Network) -> pd.DataFrame:
    """Per (period, generator) annual marginal cost (AUD bundled, with fuel),
    excluding slack/unserved-energy generators."""
    real = _real_generator_names(network)
    weightings = network.snapshot_weightings["generators"]
    mc_t = network.get_switchable_as_dense("Generator", "marginal_cost")[real]
    gen_t = network.generators_t.p[real].clip(lower=0)
    bundled = (mc_t * gen_t).mul(weightings, axis=0)
    return bundled.groupby(level=0).sum()


def _capex_per_period(network: pypsa.Network) -> pd.Series:
    """Annualised capital cost per investment period, summed over assets active in
    that period.

    A component is active in period p iff build_year <= p < build_year + lifetime.
    PyPSA's capital_cost is already AUD/MW/year (ISPyPSA pre-annuitises CAPEX +
    FOM into capital_cost in the translator)."""
    out = {}
    for period in network.investment_periods:
        out[int(period)] = (
            _active_capex(network.generators, period, "p_nom_opt")
            + _active_capex(network.lines, period, "s_nom_opt")
            + _active_capex(network.links, period, "p_nom_opt")
            + _active_capex(network.storage_units, period, "p_nom_opt")
            + _active_capex(network.stores, period,
                            "e_nom_opt" if "e_nom_opt" in network.stores.columns else None)
        )
    return pd.Series(out)


def _active_capex(df: pd.DataFrame, period: int, sizing_col: str | None) -> float:
    """Σ capital_cost × sizing for rows active in `period`. Excludes slack
    generators attached to bus_for_custom_constraint_gens."""
    if df.empty or sizing_col is None or sizing_col not in df.columns:
        return 0.0
    if "bus" in df.columns:
        df = df[df["bus"] != "bus_for_custom_constraint_gens"]
    if "carrier" in df.columns:
        df = df[df["carrier"] != "Unserved Energy"]
    active = df[
        (df["build_year"].fillna(0) <= period)
        & (df["build_year"].fillna(0) + df["lifetime"].fillna(0) > period)
    ]
    return float((active["capital_cost"].fillna(0) * active[sizing_col].fillna(0)).sum())


def _h2_blend_fraction_per_period(
    workbook_cache: Path, period: int
) -> float:
    """Return the H2 share-by-energy for Hyblend generators in a given period.
    ISPyPSA's gpg_emissions_reduction_h2 table is keyed by fy-label. Returns 0
    if not found (treat as pure natural gas)."""
    for fname in ("gpg_emissions_reduction_h2_kogan.csv",
                  "gpg_emissions_reduction_h2_sa_turbine.csv"):
        path = workbook_cache / fname
        if path.exists():
            df = pd.read_csv(path)
            fy_col = f"{_financial_year_start(period)}-{str(period)[-2:]}"
            for col in df.columns:
                if col.replace(" ", "") == fy_col.replace(" ", ""):
                    val = df[col].iloc[-1]
                    return float(val) if pd.notna(val) else 0.0
    return 0.0


def _annual_fuel_consumption_gj(
    dispatch_by_gen: pd.Series,
    gens: pd.DataFrame,
) -> pd.Series:
    """For each generator, annual fuel input in GJ = dispatch_MWh × heat_rate."""
    heat_rate = gens.get("isp_heat_rate_gj/mwh", pd.Series(0.0, index=gens.index))
    heat_rate = pd.to_numeric(heat_rate, errors="coerce").fillna(0.0)
    return dispatch_by_gen.mul(heat_rate.reindex(dispatch_by_gen.index).fillna(0.0))


def _carrier_of(gens: pd.DataFrame) -> pd.Series:
    """Carrier (fuel_type) per generator from the pypsa-friendly table."""
    return gens["carrier"]


def _renewable_share_pct(dispatch: pd.Series, gens: pd.DataFrame) -> float:
    """Fraction of total dispatch from renewable carriers, as percentage."""
    carrier = _carrier_of(gens).reindex(dispatch.index)
    renewable_mwh = float(dispatch[carrier.isin(_RENEWABLE_CARRIERS)].sum())
    total_mwh = float(dispatch.sum())
    return (renewable_mwh / total_mwh * 100.0) if total_mwh > 0 else 0.0


def _fuel_price_per_mwh(
    gen_row: pd.Series, fuel_tables: dict, period: int
) -> float:
    """Best-effort fuel-price lookup for a single generator-year.

    The IASR price tables differ in shape (some keyed by scenario, some by
    generator, some by fuel-class). For the MVP we take the table-wide median
    of the financial-year column as a representative central value. The mvp's
    cost decoupling reports this as the unfuel-cost number explicitly; precise
    per-generator lookups are a production task. Returns AUD/GJ; multiply by
    heat_rate to get AUD/MWh.
    """
    carrier = gen_row.get("carrier")
    table_name = _carrier_to_price_table(carrier)
    if table_name not in fuel_tables:
        return 0.0
    df = fuel_tables[table_name]
    fy_label = f"{_financial_year_start(period)}-{str(period)[-2:]}"
    # Find the right FY column (formats vary: "2029-30", "FY2029-30", etc.)
    candidates = [c for c in df.columns if fy_label.replace(" ", "") in str(c).replace(" ", "")]
    if not candidates:
        return 0.0
    col = candidates[0]
    return float(pd.to_numeric(df[col], errors="coerce").median())


# ---------- core orchestrators ----------


def extract_method_year_row(
    network: pypsa.Network,
    pypsa_friendly_dir: Path,
    workbook_cache: Path,
    period: int,
    archetype_id: str,
    archetype_bounds: dict,
) -> dict:
    """Produce one simple-msm method_years row for an archetype-period."""
    gens = _load_pypsa_friendly_generators(pypsa_friendly_dir)
    fuel_tables = _load_fuel_price_tables(workbook_cache)
    load_per_period = _per_period_total_load(network)
    dispatch_per_period = _per_period_total_dispatch(network)
    mcost_per_period = _per_period_total_marginal_cost(network)
    capex_per_period = _capex_per_period(network)

    annual_mwh = float(load_per_period.loc[period])
    if annual_mwh <= 0:
        return _empty_row(archetype_id, period, archetype_bounds)

    dispatch = dispatch_per_period.loc[period]
    fuel_gj = _annual_fuel_consumption_gj(dispatch, gens)

    fuel_cost = _annual_fuel_cost(dispatch, gens, fuel_tables, period)
    bundled_opex = float(mcost_per_period.loc[period].sum())
    non_fuel_opex = bundled_opex - fuel_cost
    capex = float(capex_per_period.loc[period])

    input_coeffs = _aggregate_fuel_coefficients(fuel_gj, gens, annual_mwh, workbook_cache, period)
    emissions = _aggregate_emissions(fuel_gj, gens, annual_mwh, workbook_cache, period)
    renewable_share = _renewable_share_pct(dispatch, gens)

    return _assemble_row(
        archetype_id, period,
        output_cost_per_unit=(capex + non_fuel_opex) / annual_mwh,
        bundled_cost_per_unit=(capex + bundled_opex) / annual_mwh,
        fuel_cost_per_unit=fuel_cost / annual_mwh,
        input_coeffs=input_coeffs,
        emissions=emissions,
        bounds=archetype_bounds,
        annual_mwh_delivered=annual_mwh,
        renewable_share=renewable_share,
    )


def _annual_fuel_cost(
    dispatch: pd.Series, gens: pd.DataFrame, fuel_tables: dict, period: int
) -> float:
    """Sum of dispatch × heat_rate × fuel_price across all generators."""
    total = 0.0
    for name, mwh in dispatch.items():
        if mwh <= 0 or name not in gens.index:
            continue
        row = gens.loc[name]
        hr = pd.to_numeric(row.get("isp_heat_rate_gj/mwh"), errors="coerce")
        if pd.isna(hr) or hr == 0:
            continue
        price = _fuel_price_per_mwh(row, fuel_tables, period)
        total += float(mwh) * float(hr) * float(price)
    return total


def _aggregate_fuel_coefficients(
    fuel_gj: pd.Series, gens: pd.DataFrame, annual_mwh: float,
    workbook_cache: Path, period: int,
) -> dict[str, float]:
    """Sum fuel_gj by commodity_id, divided by total annual MWh delivered."""
    coeffs: dict[str, float] = {}
    h2_frac = _h2_blend_fraction_per_period(workbook_cache, period)
    for gen_name, gj in fuel_gj.items():
        if gj <= 0 or gen_name not in gens.index:
            continue
        carrier = gens.loc[gen_name, "carrier"]
        if carrier == "Hyblend":
            coeffs["natural_gas"] = coeffs.get("natural_gas", 0.0) + (1 - h2_frac) * gj
            coeffs["hydrogen"]    = coeffs.get("hydrogen", 0.0)    + h2_frac * gj
        else:
            commodity = _CARRIER_TO_COMMODITY.get(carrier)
            if commodity:
                coeffs[commodity] = coeffs.get(commodity, 0.0) + float(gj)
    return {c: v / annual_mwh for c, v in coeffs.items()}


def _aggregate_emissions(
    fuel_gj: pd.Series, gens: pd.DataFrame, annual_mwh: float,
    workbook_cache: Path, period: int,
) -> dict[str, float]:
    """Sum NGER scope-1 emissions across pollutants per generator, divide by load.

    Returns CO2e pollutant totals plus physical-mass CH4 and N2O (in kg/MWh).
    Physical masses are derived by reversing AR5 GWP-100 from the CO2e values.
    """
    nger = nger_factor_table().set_index("carrier")
    h2_frac = _h2_blend_fraction_per_period(workbook_cache, period)
    em = {"CO2": 0.0, "CH4_CO2e": 0.0, "N2O_CO2e": 0.0,
          "CH4_physical_kg": 0.0, "N2O_physical_kg": 0.0}
    for gen_name, gj in fuel_gj.items():
        if gj <= 0 or gen_name not in gens.index:
            continue
        carrier = gens.loc[gen_name, "carrier"]
        factors = _carrier_emission_factors(carrier, nger, h2_frac)
        em["CO2"]      += float(gj) * factors["co2_kg_per_gj"]
        em["CH4_CO2e"] += float(gj) * factors["ch4_co2e_kg_per_gj"]
        em["N2O_CO2e"] += float(gj) * factors["n2o_co2e_kg_per_gj"]
        em["CH4_physical_kg"] += float(gj) * factors["ch4_co2e_kg_per_gj"] / 28
        em["N2O_physical_kg"] += float(gj) * factors["n2o_co2e_kg_per_gj"] / 265
    # Convert kg → tonnes per MWh for CO2e; keep physical as kg per MWh.
    return {
        "CO2":              em["CO2"]      / annual_mwh / 1000.0,
        "CH4_CO2e":         em["CH4_CO2e"] / annual_mwh / 1000.0,
        "N2O_CO2e":         em["N2O_CO2e"] / annual_mwh / 1000.0,
        "CH4_physical_kg_per_mwh": em["CH4_physical_kg"] / annual_mwh,
        "N2O_physical_kg_per_mwh": em["N2O_physical_kg"] / annual_mwh,
    }


def _carrier_emission_factors(carrier: str, nger: pd.DataFrame, h2_frac: float) -> dict:
    """NGER factors for an ISPyPSA carrier, with Hyblend handled by linear blend."""
    if carrier == "Hyblend":
        return hyblend_factor(h2_frac)
    if carrier in nger.index:
        return {
            "co2_kg_per_gj":      float(nger.loc[carrier, "co2_kg_per_gj"]),
            "ch4_co2e_kg_per_gj": float(nger.loc[carrier, "ch4_co2e_kg_per_gj"]),
            "n2o_co2e_kg_per_gj": float(nger.loc[carrier, "n2o_co2e_kg_per_gj"]),
        }
    return {"co2_kg_per_gj": 0.0, "ch4_co2e_kg_per_gj": 0.0, "n2o_co2e_kg_per_gj": 0.0}


def _assemble_row(
    archetype_id: str, period: int, *, output_cost_per_unit: float,
    bundled_cost_per_unit: float, fuel_cost_per_unit: float,
    input_coeffs: dict, emissions: dict, bounds: dict,
    annual_mwh_delivered: float,
    renewable_share: float = 0.0,
) -> dict:
    """Build one method_years row."""
    input_commodities = list(input_coeffs.keys())
    input_coefficients = [input_coeffs[c] for c in input_commodities]
    co2e_pollutants = {k: v for k, v in emissions.items()
                       if k in ("CO2", "CH4_CO2e", "N2O_CO2e")}
    return {
        "method_id": f"electricity__grid_supply__{archetype_id}",
        "year": period,
        "output_cost_per_unit": output_cost_per_unit,
        "cost_basis_year": 2024,
        "currency": "AUD_2024",
        "input_commodities": input_commodities,
        "input_coefficients": input_coefficients,
        "input_units": ["GJ/MWh"] * len(input_commodities),
        "energy_emissions_by_pollutant": {
            "CO2": emissions["CO2"],
            "CH4_CO2e": emissions["CH4_CO2e"],
            "N2O_CO2e": emissions["N2O_CO2e"],
            "total_CO2e": sum(co2e_pollutants.values()),
        },
        "process_emissions_by_pollutant": {"total_CO2e": 0.0},
        "emissions_units": "tCO2e/MWh",
        "max_share": bounds.get("max_share"),
        "min_share": bounds.get("min_share"),
        "max_activity": bounds.get("max_activity"),
        "availability_conditions": bounds.get("availability_conditions", "national_frontier"),
        "diagnostic_bundled_cost_per_unit": bundled_cost_per_unit,
        "diagnostic_fuel_cost_per_unit":   fuel_cost_per_unit,
        "diagnostic_annual_mwh_delivered": annual_mwh_delivered,
        "diagnostic_ch4_physical_kg_per_mwh": emissions.get("CH4_physical_kg_per_mwh", 0.0),
        "diagnostic_n2o_physical_kg_per_mwh": emissions.get("N2O_physical_kg_per_mwh", 0.0),
        "diagnostic_co2_kg_per_mwh": emissions["CO2"] * 1000.0,  # tonne → kg
        "renewable_share_pct": renewable_share,
        "source_ids": "AEMO IASR 2024 v6.0; NGA Factors 2024; ISPyPSA 0.1.3",
        "confidence_rating": "MVP_prototype",
        "review_notes": "Pass-1 MVP — fuel cost subtracted post-hoc from ISPyPSA bundled LP.",
    }


def _empty_row(archetype_id: str, period: int, bounds: dict) -> dict:
    return _assemble_row(
        archetype_id, period,
        output_cost_per_unit=float("nan"),
        bundled_cost_per_unit=float("nan"),
        fuel_cost_per_unit=float("nan"),
        input_coeffs={},
        emissions={
            "CO2": 0.0, "CH4_CO2e": 0.0, "N2O_CO2e": 0.0,
            "CH4_physical_kg_per_mwh": 0.0, "N2O_physical_kg_per_mwh": 0.0,
        },
        bounds=bounds, annual_mwh_delivered=0.0,
    )
