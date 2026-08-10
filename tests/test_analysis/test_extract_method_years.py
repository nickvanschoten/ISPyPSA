"""Tests for the cost-decoupling fuel-price lookup in extract_method_years.

These guard the Pass 1 ↔ Pass 2 cost-intensity separation: the post-processor
must subtract a *material* fuel cost from the bundled LP cost so the emitted
`output_cost_per_unit` is operator-controllable-only.

Phase 1 follow-up (l) context: the v6.0 → v7.4 schema normalisation renamed the
coal/gas/biomass price tables (coal_prices_step_change → coal_fuel_price, etc.).
The loader kept looking for the v6.0 names, silently found nothing, and every
coal/gas/biomass fuel cost computed as zero — while trace hydrogen/biomethane
kept a tiny non-zero fuel cost alive. The earlier verification ("fuel_cost > 0
and excl_fuel < bundled") was satisfied by that trace amount and did not catch
the regression. These tests assert the *dominant* fuels resolve to a material
price, which is the strict check that would have caught it.
"""

from pathlib import Path

import pandas as pd

from mvp_pass1_power.postprocess.extract_method_years import (
    _annual_fuel_cost,
    _fuel_price_per_mwh,
    _load_fuel_price_tables,
)


def _write_v74_price_tables(cache: Path) -> None:
    """Write minimal v7.4-shaped fuel-price CSVs (Generator/label, Scenario, FY cols)."""
    cache.mkdir(parents=True, exist_ok=True)
    # Coal: per-generator, Scenario column, Step Change + a decoy scenario.
    (cache / "coal_fuel_price.csv").write_text(
        "Generator,Scenario,2029-30,2049-50\n"
        "Bayswater,Step Change,3.0,2.4\n"
        "Eraring,Step Change,4.0,2.6\n"
        "Bayswater,Slower Growth,9.9,9.9\n"  # decoy: must be excluded
    )
    # Gas: split into existing + new entrants, "Gas price scenario" column.
    (cache / "gas_prices_existing_generators.csv").write_text(
        "Generator,Gas price scenario,2029-30,2049-50\n"
        "Torrens,Step Change,10.0,12.0\n"
    )
    (cache / "gas_prices_new_entrants.csv").write_text(
        "Generator,Gas price scenario,2029-30,2049-50\n"
        "Bairnsdale,Step Change,12.0,14.0\n"
    )
    # Biomass: single label col, Scenario column.
    (cache / "biomass_fuel_price.csv").write_text(
        "Biomass price,Scenario,2029-30,2049-50\n"
        "Biomass,Step Change,0.62,0.62\n"
    )


# ---------------------------------------------------------------------------
# _load_fuel_price_tables — resolves v7.4-canonical names
# ---------------------------------------------------------------------------


def test_load_fuel_price_tables_resolves_v74_names(tmp_path):
    _write_v74_price_tables(tmp_path)

    tables = _load_fuel_price_tables(tmp_path)

    assert "coal_prices" in tables
    assert "gas_prices" in tables
    assert "biomass_prices" in tables


def test_gas_prices_concatenates_existing_and_new_entrants(tmp_path):
    _write_v74_price_tables(tmp_path)

    tables = _load_fuel_price_tables(tmp_path)

    # Both the existing-generator and new-entrant rows must be present so the
    # representative median spans the whole fleet.
    assert set(tables["gas_prices"]["Generator"]) == {"Torrens", "Bairnsdale"}


# ---------------------------------------------------------------------------
# _fuel_price_per_mwh — Step-Change filter + FY column selection
# ---------------------------------------------------------------------------


def test_fuel_price_filters_to_step_change(tmp_path):
    _write_v74_price_tables(tmp_path)
    tables = _load_fuel_price_tables(tmp_path)

    # 2050 → FY "2049-50". Step Change coal rows are 2.4 and 2.6 → median 2.5.
    # The Slower Growth decoy (9.9) must be excluded.
    price = _fuel_price_per_mwh(pd.Series({"carrier": "Black Coal"}), tables, 2050)

    assert price == 2.5


def test_fuel_price_zero_when_table_missing(tmp_path):
    # Empty cache → no tables → price resolves to 0 (the failure mode the
    # loader fix prevents in production).
    tables = _load_fuel_price_tables(tmp_path)

    price = _fuel_price_per_mwh(pd.Series({"carrier": "Black Coal"}), tables, 2050)

    assert price == 0.0


# ---------------------------------------------------------------------------
# _annual_fuel_cost — STRICT check: dominant fuels yield a material cost
# ---------------------------------------------------------------------------


def test_annual_fuel_cost_is_material_for_coal(tmp_path):
    _write_v74_price_tables(tmp_path)
    tables = _load_fuel_price_tables(tmp_path)
    # One coal generator dispatching 1,000,000 MWh at 9 GJ/MWh heat rate.
    dispatch = pd.Series({"coal_gen": 1_000_000.0})
    gens = pd.DataFrame(
        {"carrier": ["Black Coal"], "isp_heat_rate_gj/mwh": [9.0]},
        index=["coal_gen"],
    )

    fuel_cost = _annual_fuel_cost(dispatch, gens, tables, 2050)

    # 1e6 MWh × 9 GJ/MWh × 2.5 AUD/GJ (Step-Change median) = 22,500,000 AUD.
    assert fuel_cost == 1_000_000.0 * 9.0 * 2.5


def test_annual_fuel_cost_zero_when_prices_missing(tmp_path):
    # The regression signature: tables absent → coal fuel cost silently zero.
    # This is what the loader fix prevents; the test pins the failure mode so a
    # future rename can't reintroduce it unnoticed.
    tables = _load_fuel_price_tables(tmp_path)  # empty cache
    dispatch = pd.Series({"coal_gen": 1_000_000.0})
    gens = pd.DataFrame(
        {"carrier": ["Black Coal"], "isp_heat_rate_gj/mwh": [9.0]},
        index=["coal_gen"],
    )

    fuel_cost = _annual_fuel_cost(dispatch, gens, tables, 2050)

    assert fuel_cost == 0.0
