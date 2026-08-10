"""Tests for the production frontier-point extraction.

The load-bearing behaviours are the full-fleet re-attribution rules:
vintage isolation (only the year's own extendable builds), the retirement
filter (build_year + lifetime > at_year), and accumulation across multiple
prior vintages — the 2050 leg of a production chain carries up to four.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pypsa
import pytest

from mvp_pass1_power.postprocess.extract_frontier_points import (
    _assemble_frontier_row,
    _carried_vintage_capex,
    _existing_fleet_fom,
    _solve_diagnostics,
    _surviving_new_builds,
)


# ---------------------------------------------------------------------------
# _surviving_new_builds — vintage isolation + retirement
# ---------------------------------------------------------------------------


def test_surviving_new_builds_isolates_vintage_and_applies_retirement():
    gens = pd.DataFrame({
        "bus": ["n", "n", "n", "n", "n"],
        "p_nom_extendable": [True, True, False, True, True],
        "build_year": [2040, 2035, 2040, 2040, 2040],
        "lifetime": [30.0, 30.0, 30.0, 5.0, 30.0],
        "p_nom_opt": [500.0, 400.0, 300.0, 200.0, 0.5],
        "capital_cost": [100.0, 100.0, 100.0, 100.0, 100.0],
    }, index=["own_vintage", "other_vintage", "carried_row",
              "retired_by_2050", "sub_threshold"])

    result = _surviving_new_builds(gens, vintage_year=2040, at_year=2050)

    # own_vintage only: other_vintage fails build_year, carried_row fails
    # extendable, retired (2040+5=2045 <= 2050) fails retirement,
    # sub_threshold fails p_nom_opt > 1 MW.
    assert list(result.index) == ["own_vintage"]


def test_surviving_new_builds_excludes_custom_constraint_bus():
    gens = pd.DataFrame({
        "bus": ["bus_for_custom_constraint_gens", "n"],
        "p_nom_extendable": [True, True],
        "build_year": [2040, 2040],
        "lifetime": [30.0, 30.0],
        "p_nom_opt": [500.0, 500.0],
        "capital_cost": [100.0, 100.0],
    }, index=["slack", "real"])

    result = _surviving_new_builds(gens, vintage_year=2040, at_year=2045)

    assert list(result.index) == ["real"]


# ---------------------------------------------------------------------------
# _carried_vintage_capex — accumulation across multiple prior vintages
# ---------------------------------------------------------------------------


def _save_vintage_network(tmp_path: Path, vintage_year: int,
                          gens: list[dict]) -> Path:
    n = pypsa.Network()
    n.investment_periods = [vintage_year]
    n.snapshots = pd.MultiIndex.from_tuples(
        [(vintage_year, pd.Timestamp(f"{vintage_year}-07-01 00:00"))]
    )
    n.add("Bus", "n")
    for g in gens:
        attrs = {k: v for k, v in g.items() if k not in ("name", "p_nom_opt")}
        n.add("Generator", g["name"], bus="n", **attrs)
    n.generators["p_nom_opt"] = [g["p_nom_opt"] for g in gens]
    path = tmp_path / f"v{vintage_year}.nc"
    n.export_to_netcdf(path)
    return path


def test_carried_vintage_capex_accumulates_across_vintages(tmp_path):
    """A 2040 solve carrying 2030 + 2035 vintages must sum BOTH, each at its
    own original capital_cost — not just the immediately-prior year."""
    nc_2030 = _save_vintage_network(tmp_path, 2030, [
        dict(name="wind_2030", p_nom_extendable=True, build_year=2030,
             lifetime=30.0, capital_cost=200.0, p_nom_opt=1000.0),
    ])
    nc_2035 = _save_vintage_network(tmp_path, 2035, [
        dict(name="solar_2035", p_nom_extendable=True, build_year=2035,
             lifetime=30.0, capital_cost=150.0, p_nom_opt=2000.0),
    ])

    result = _carried_vintage_capex(
        {2030: nc_2030, 2035: nc_2035}, at_year=2040
    )

    assert result["carried_capex_by_vintage"] == {
        2030: 200.0 * 1000.0, 2035: 150.0 * 2000.0,
    }
    assert result["carried_capex_aud_per_yr"] == 200.0 * 1000.0 + 150.0 * 2000.0
    assert result["carried_vintages"] == 2
    assert result["carried_gw"] == 3.0


def test_carried_vintage_capex_retires_expired_vintage(tmp_path):
    """A 15y battery-style asset built 2030 must contribute zero to a 2050
    re-attribution (2030 + 15 = 2045 <= 2050)."""
    nc_2030 = _save_vintage_network(tmp_path, 2030, [
        dict(name="short_lived_2030", p_nom_extendable=True, build_year=2030,
             lifetime=15.0, capital_cost=300.0, p_nom_opt=500.0),
        dict(name="long_lived_2030", p_nom_extendable=True, build_year=2030,
             lifetime=40.0, capital_cost=100.0, p_nom_opt=800.0),
    ])

    result = _carried_vintage_capex({2030: nc_2030}, at_year=2050)

    assert result["carried_capex_aud_per_yr"] == 100.0 * 800.0
    assert result["carried_gw"] == 0.8


# ---------------------------------------------------------------------------
# _assemble_frontier_row — the dual-cost-column identity
# ---------------------------------------------------------------------------


def _minimal_base_row() -> dict:
    return {
        "diagnostic_annual_mwh_delivered": 1_000_000.0,
        "diagnostic_cost_per_unit_excl_fuel_and_carbon": 50.0,
        "energy_emissions_by_pollutant": {
            "CO2": 0.10, "CH4_CO2e": 0.001, "N2O_CO2e": 0.001,
            "total_CO2e": 0.102,
        },
        "input_commodities": ["coal"],
        "input_coefficients": [1.5],
        "renewable_share_pct": 60.0,
        "diagnostic_bundled_cost_per_unit": 80.0,
        "diagnostic_fuel_cost_per_unit": 20.0,
        "diagnostic_carbon_cost_per_unit": 10.0,
    }


def test_primary_excl_fuel_carbon_is_year_t_plus_carried_plus_existing_fom():
    """The PRIMARY contract column is the full-fleet intensity: year-t incremental
    + carried-vintage capex+FOM + existing-fleet FOM, the three-way fixed-cost
    partition, all per MWh."""
    carried = {"carried_capex_aud_per_yr": 10_000_000.0,
               "carried_capex_by_vintage": {2030: 10_000_000.0},
               "carried_vintages": 1, "carried_gw": 5.0}
    existing_fom = {"existing_fleet_fom_aud_per_yr": 5_000_000.0,
                    "existing_fleet_active_gw": 3.0}
    diagnostics = {"solve_gap_rel": 1e-3, "solve_pinf_rel": 1e-3,
                   "solve_dinf_rel": 1e-6, "solve_iterations": 100,
                   "solve_wall_s": 10.0, "solve_model_status": "Unknown",
                   "tolerance_robust": True}

    row = _assemble_frontier_row(
        "c150", 2040, 150.0, 20.0, _minimal_base_row(), carried, existing_fom,
        diagnostics,
    )

    # primary = year_t + (carried + existing_fom)/annual_mwh = 50 + (1e7+5e6)/1e6 = 65
    assert row["diagnostic_cost_per_mwh_year_t_incremental"] == 50.0
    assert row["cost_per_mwh_excl_fuel_carbon"] == 65.0
    assert row["existing_fleet_fom_aud_per_yr"] == 5_000_000.0
    assert row["existing_fleet_active_gw"] == 3.0
    assert row["gj_per_mwh_coal"] == 1.5
    assert row["co2_t_per_mwh"] == 0.10
    assert row["annual_generation_twh"] == 1.0
    assert row["tolerance_robust"] is True


def test_existing_fleet_fom_partitions_roster_and_applies_retirement(tmp_path):
    """Existing-fleet FOM bills only ECAA-roster names still active at the year:
    carried/new-build rows (not in roster) are excluded, and a unit retired by
    the year (build_year + lifetime <= year) drops out."""
    import pypsa

    n = pypsa.Network()
    n.add("Bus", "n")
    # In-roster, active: build 2010 + life 40 = 2050 > 2045 -> billed
    n.add("Generator", "Bayswater", bus="n", p_nom=2000.0,
          p_nom_extendable=False, build_year=2010, lifetime=40.0)
    # In-roster, retired by 2045: 2010 + 30 = 2040 <= 2045 -> excluded
    n.add("Generator", "Liddell", bus="n", p_nom=1000.0,
          p_nom_extendable=False, build_year=2010, lifetime=30.0)
    # Carried tranche (NOT in roster): excluded even though active
    n.add("Generator", "Wind REZ_2030", bus="n", p_nom=500.0,
          p_nom_extendable=False, build_year=2030, lifetime=30.0)
    n.generators["p_nom_opt"] = n.generators["p_nom"]

    roster = tmp_path / "ecaa_generators.csv"
    roster.write_text(
        "generator,fom_$/kw/annum\nBayswater,99.5\nLiddell,64.5\n"
    )
    (tmp_path / "ecaa_batteries.csv").write_text("storage_name,fom_$/kw/annum\n")

    result = _existing_fleet_fom(n, tmp_path, at_year=2045)

    # only Bayswater: 99.5 $/kW/yr * 2000 MW * 1000 kW/MW = 199,000,000
    assert result["existing_fleet_fom_aud_per_yr"] == 99.5 * 2000.0 * 1000.0
    assert result["existing_fleet_active_gw"] == 2.0


# ---------------------------------------------------------------------------
# _solve_diagnostics — tolerance_robust semantics
# ---------------------------------------------------------------------------


def test_tolerance_robust_true_when_all_metrics_within(tmp_path):
    record = tmp_path / "rec.json"
    record.write_text(json.dumps({
        "pdlp_final_gap_rel": 2.9e-3, "pdlp_final_pinf_rel": 1.3e-3,
        "pdlp_final_dinf_rel": 6e-6, "pdlp_iterations": 15240,
        "solve_s": 14388.7, "model_status": "Unknown",
    }))

    result = _solve_diagnostics(record)

    assert result["tolerance_robust"] is True
    assert result["solve_iterations"] == 15240


def test_tolerance_robust_false_when_metrics_missing(tmp_path):
    """Missing convergence metrics must read as NOT robust (conservative),
    never silently True."""
    record = tmp_path / "rec.json"
    record.write_text(json.dumps({"solve_s": 100.0}))

    result = _solve_diagnostics(record)

    assert result["tolerance_robust"] is False


def test_tolerance_robust_false_when_metric_above_tolerance(tmp_path):
    record = tmp_path / "rec.json"
    record.write_text(json.dumps({
        "pdlp_final_gap_rel": 4e-3, "pdlp_final_pinf_rel": 1e-3,
        "pdlp_final_dinf_rel": 1e-6,
    }))

    result = _solve_diagnostics(record)

    assert result["tolerance_robust"] is False
