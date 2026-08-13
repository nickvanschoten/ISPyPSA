import pandas as pd
import pytest

from ispypsa.translator.fuel_supply_curve import _translate_fuel_supply_curve


def _write_curve_csv(tmp_path, text):
    curve_csv = tmp_path / "fuel_supply_curve.csv"
    curve_csv.write_text(text)
    return str(curve_csv)


def test_translate_fuel_supply_curve_filters_to_investment_periods(
    tmp_path, csv_str_to_df
):
    curve_csv = _write_curve_csv(
        tmp_path,
        "tranche,financial_year,cap_pj,adder_$/gj\n"
        "existing_market,2025,110,0.0\n"
        "lng_backstop,2025,,6.0\n"
        "existing_market,2030,110,0.0\n"
        "lng_backstop,2030,,6.0\n"
        "existing_market,2035,100,0.0\n"
        "lng_backstop,2035,,6.0\n",
    )

    result = _translate_fuel_supply_curve(curve_csv, [2025, 2030], "Gas")

    expected = csv_str_to_df("""
        investment_period,  tranche,          cap_pj,  adder_$/gj
        2025,               existing_market,  110,     0.0
        2025,               lng_backstop,     ,        6.0
        2030,               existing_market,  110,     0.0
        2030,               lng_backstop,     ,        6.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_translate_fuel_supply_curve_missing_period_raises(tmp_path):
    curve_csv = _write_curve_csv(
        tmp_path,
        "tranche,financial_year,cap_pj,adder_$/gj\n"
        "existing_market,2025,110,0.0\n"
        "lng_backstop,2025,,6.0\n",
    )

    with pytest.raises(
        ValueError,
        match=r"Gas supply curve CSV .* no tranche rows for investment periods: \[2030\]",
    ):
        _translate_fuel_supply_curve(curve_csv, [2025, 2030], "Gas")


def test_translate_fuel_supply_curve_missing_columns_raises(tmp_path):
    curve_csv = _write_curve_csv(
        tmp_path,
        "tranche,financial_year,price\nexisting_market,2025,12.0\n",
    )

    with pytest.raises(
        ValueError,
        match=r"Biomass supply curve CSV .* missing columns: \['adder_\$/gj', 'cap_pj'\]",
    ):
        _translate_fuel_supply_curve(curve_csv, [2025], "Biomass")


def test_translate_fuel_supply_curve_all_capped_period_raises(tmp_path):
    curve_csv = _write_curve_csv(
        tmp_path,
        "tranche,financial_year,cap_pj,adder_$/gj\n"
        "sustainable_residues,2025,30,0.0\n"
        "pellet_backstop,2025,,15.0\n"
        "sustainable_residues,2030,30,0.0\n"
        "energy_crops,2030,40,9.0\n",
    )

    with pytest.raises(
        ValueError,
        match=r"Biomass supply curve CSV .* uncapped backstop tranche .* missing for: \[2030\]",
    ):
        _translate_fuel_supply_curve(curve_csv, [2025, 2030], "Biomass")
