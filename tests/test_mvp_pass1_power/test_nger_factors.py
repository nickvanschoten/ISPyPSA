"""Tests for the NGER emission-factor cross-walk and GWP utilities."""

import pandas as pd
import pytest

from mvp_pass1_power.postprocess.nger_factors import (
    GWP_AR5_NGER,
    GWP_AR6_IPCC,
    co2e_per_mwh,
    hyblend_factor,
    nger_factor_table,
)


# ---------------------------------------------------------------------------
# nger_factor_table()
# ---------------------------------------------------------------------------


def test_nger_factor_table_contains_all_expected_carriers():
    df = nger_factor_table()

    expected_carriers = {
        "Black Coal", "Brown Coal", "Gas", "Liquid Fuel", "Biomass",
        "Hydrogen", "Biomethane", "Nuclear", "Wind", "Solar", "Water", "Storage",
    }
    assert set(df["carrier"]) == expected_carriers


def test_nger_factor_table_has_required_columns():
    df = nger_factor_table()

    expected_columns = {
        "carrier", "co2_kg_per_gj", "ch4_co2e_kg_per_gj", "n2o_co2e_kg_per_gj",
        "total_co2e_kg_per_gj", "ch4_physical_kg_per_gj", "n2o_physical_kg_per_gj",
        "nga_table", "nga_fuel_name",
    }
    assert set(df.columns) == expected_columns


def test_nger_factor_table_total_co2e_equals_sum_of_components():
    df = nger_factor_table()

    computed = df["co2_kg_per_gj"] + df["ch4_co2e_kg_per_gj"] + df["n2o_co2e_kg_per_gj"]
    pd.testing.assert_series_equal(
        df["total_co2e_kg_per_gj"], computed, check_names=False, rtol=1e-10,
    )


def test_nger_factor_table_physical_ch4_derived_from_ar5_gwp():
    # ch4_physical = ch4_co2e / GWP_AR5_NGER["CH4"]
    df = nger_factor_table().set_index("carrier")

    for carrier in ("Black Coal", "Brown Coal", "Gas", "Biomass", "Biomethane"):
        row = df.loc[carrier]
        expected = row["ch4_co2e_kg_per_gj"] / GWP_AR5_NGER["CH4"]
        assert row["ch4_physical_kg_per_gj"] == pytest.approx(expected)


def test_nger_factor_table_physical_n2o_derived_from_ar5_gwp():
    df = nger_factor_table().set_index("carrier")

    for carrier in ("Black Coal", "Brown Coal", "Gas", "Biomass", "Biomethane"):
        row = df.loc[carrier]
        expected = row["n2o_co2e_kg_per_gj"] / GWP_AR5_NGER["N2O"]
        assert row["n2o_physical_kg_per_gj"] == pytest.approx(expected)


def test_nger_factor_table_non_combustion_carriers_are_all_zero():
    df = nger_factor_table().set_index("carrier")

    for carrier in ("Wind", "Solar", "Water", "Hydrogen", "Nuclear", "Storage"):
        row = df.loc[carrier]
        assert row["co2_kg_per_gj"] == 0.0
        assert row["ch4_co2e_kg_per_gj"] == 0.0
        assert row["n2o_co2e_kg_per_gj"] == 0.0
        assert row["total_co2e_kg_per_gj"] == 0.0
        assert row["ch4_physical_kg_per_gj"] == 0.0
        assert row["n2o_physical_kg_per_gj"] == 0.0


def test_nger_factor_table_black_coal_matches_nga_2024():
    df = nger_factor_table().set_index("carrier")
    row = df.loc["Black Coal"]

    assert row["co2_kg_per_gj"] == pytest.approx(90.0)
    assert row["ch4_co2e_kg_per_gj"] == pytest.approx(0.04)
    assert row["n2o_co2e_kg_per_gj"] == pytest.approx(0.2)
    assert row["total_co2e_kg_per_gj"] == pytest.approx(90.24)


def test_nger_factor_table_gas_matches_nga_2024():
    df = nger_factor_table().set_index("carrier")
    row = df.loc["Gas"]

    assert row["co2_kg_per_gj"] == pytest.approx(51.4)
    assert row["ch4_co2e_kg_per_gj"] == pytest.approx(0.1)
    assert row["n2o_co2e_kg_per_gj"] == pytest.approx(0.03)
    assert row["total_co2e_kg_per_gj"] == pytest.approx(51.53)


# ---------------------------------------------------------------------------
# hyblend_factor()
# ---------------------------------------------------------------------------


def test_hyblend_factor_pure_gas_matches_natural_gas_factors():
    result = hyblend_factor(h2_fraction=0.0)

    assert result["co2_kg_per_gj"] == pytest.approx(51.4)
    assert result["ch4_co2e_kg_per_gj"] == pytest.approx(0.1)
    assert result["n2o_co2e_kg_per_gj"] == pytest.approx(0.03)
    assert result["total_co2e_kg_per_gj"] == pytest.approx(51.53)


def test_hyblend_factor_pure_hydrogen_gives_zero_emissions():
    result = hyblend_factor(h2_fraction=1.0)

    assert result["co2_kg_per_gj"] == 0.0
    assert result["ch4_co2e_kg_per_gj"] == 0.0
    assert result["n2o_co2e_kg_per_gj"] == 0.0
    assert result["total_co2e_kg_per_gj"] == 0.0


def test_hyblend_factor_50_percent_blend_is_half_of_pure_gas():
    pure_gas = hyblend_factor(h2_fraction=0.0)
    blend = hyblend_factor(h2_fraction=0.5)

    for key in pure_gas:
        assert blend[key] == pytest.approx(pure_gas[key] * 0.5)


def test_hyblend_factor_total_equals_sum_of_components():
    result = hyblend_factor(h2_fraction=0.3)

    expected_total = (
        result["co2_kg_per_gj"]
        + result["ch4_co2e_kg_per_gj"]
        + result["n2o_co2e_kg_per_gj"]
    )
    assert result["total_co2e_kg_per_gj"] == pytest.approx(expected_total)


# ---------------------------------------------------------------------------
# co2e_per_mwh()
# ---------------------------------------------------------------------------


def test_co2e_per_mwh_pure_co2_input():
    # No CH4 or N2O: result equals CO2
    result = co2e_per_mwh(
        physical_ch4_kg_per_mwh=0.0,
        physical_n2o_kg_per_mwh=0.0,
        co2_kg_per_mwh=500.0,
        gwp=GWP_AR5_NGER,
    )

    assert result == pytest.approx(500.0)


def test_co2e_per_mwh_ar5_applies_correct_gwp():
    # 1 kg/MWh physical CH4 with AR5 CH4 GWP=28 → 28 kg CO2e/MWh
    result = co2e_per_mwh(
        physical_ch4_kg_per_mwh=1.0,
        physical_n2o_kg_per_mwh=0.0,
        co2_kg_per_mwh=0.0,
        gwp=GWP_AR5_NGER,
    )

    assert result == pytest.approx(28.0)


def test_co2e_per_mwh_ar6_applies_different_gwp_than_ar5():
    # AR6 CH4 GWP=27 (vs AR5=28) → AR6 gives lower CO2e for same physical CH4
    ar5 = co2e_per_mwh(1.0, 0.0, 0.0, gwp=GWP_AR5_NGER)
    ar6 = co2e_per_mwh(1.0, 0.0, 0.0, gwp=GWP_AR6_IPCC)

    assert ar5 == pytest.approx(28.0)
    assert ar6 == pytest.approx(27.0)
    assert ar6 < ar5


def test_co2e_per_mwh_n2o_ar6_gives_higher_result_than_ar5():
    # AR6 N2O GWP=273 > AR5 N2O GWP=265 → AR6 gives higher CO2e for same physical N2O
    ar5 = co2e_per_mwh(0.0, 1.0, 0.0, gwp=GWP_AR5_NGER)
    ar6 = co2e_per_mwh(0.0, 1.0, 0.0, gwp=GWP_AR6_IPCC)

    assert ar5 == pytest.approx(265.0)
    assert ar6 == pytest.approx(273.0)
    assert ar6 > ar5


def test_co2e_per_mwh_sums_all_three_components():
    result = co2e_per_mwh(
        physical_ch4_kg_per_mwh=2.0,
        physical_n2o_kg_per_mwh=1.0,
        co2_kg_per_mwh=100.0,
        gwp=GWP_AR5_NGER,
    )

    expected = 100.0 + 2.0 * 28 + 1.0 * 265  # 100 + 56 + 265 = 421
    assert result == pytest.approx(expected)


# ---------------------------------------------------------------------------
# GWP constants
# ---------------------------------------------------------------------------


def test_gwp_constants_have_required_keys():
    assert "CH4" in GWP_AR5_NGER
    assert "N2O" in GWP_AR5_NGER
    assert "CH4" in GWP_AR6_IPCC
    assert "N2O" in GWP_AR6_IPCC


def test_gwp_ar5_values_match_nger_determination():
    assert GWP_AR5_NGER["CH4"] == 28
    assert GWP_AR5_NGER["N2O"] == 265


def test_gwp_ar6_values_match_ipcc_wg1():
    assert GWP_AR6_IPCC["CH4"] == 27
    assert GWP_AR6_IPCC["N2O"] == 273
