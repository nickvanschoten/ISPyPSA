from pathlib import Path

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.dynamic_generator_properties import (
    _add_new_entrant_wacc,
    _regulated_transmission_wacc,
    _template_generator_dynamic_properties,
    _wacc_by_technology,
)
from ispypsa.templater.lists import _ISP_SCENARIOS


def _wacc_table():
    # AEMO publishes WACC in percent; Step Change: CCGT 10.5%, Wind 7.5%,
    # batteries 8%. Battery row uses AEMO's lowercase "storage" spelling. The
    # two transmission rows distinguish regulated (3.0%) from unregulated (6.5%).
    return pd.DataFrame(
        {
            "Technology type": [
                "CCGT",
                "Wind",
                "Battery storage (4hrs storage)",
                "Electricity - Transmission and Distribution (Regulated)",
                "Electricity - Transmission and Distribution (Unregulated)",
            ],
            "Slower Growth": [9.5, 7.0, 7.5, 3.0, 6.0],
            "Step Change": [10.5, 7.5, 8.0, 3.0, 6.5],
            "Accelerated Transition": [12.0, 8.5, 9.5, 3.5, 7.5],
        }
    )


def test_regulated_transmission_wacc_selects_regulated_electricity_row():
    # Regulated (3.0%), not unregulated (6.5%); as a fraction.
    assert _regulated_transmission_wacc(_wacc_table(), "Step Change") == pytest.approx(
        0.03
    )
    assert _regulated_transmission_wacc(
        _wacc_table(), "Accelerated Transition"
    ) == pytest.approx(0.035)


def test_wacc_by_technology_maps_percent_to_fraction_for_scenario():
    result = _wacc_by_technology(_wacc_table(), "Step Change")

    assert result["CCGT"] == 0.105
    assert result["Wind"] == 0.075
    # storage-capitalisation standardised to match the summary's spelling.
    assert result["Battery Storage (4hrs storage)"] == 0.08


def test_wacc_by_technology_selects_mapped_scenario_column():
    # "Progressive Change" maps to the "Slower Growth" workbook column.
    result = _wacc_by_technology(_wacc_table(), "Progressive Change")

    assert result["CCGT"] == 0.095
    assert result["Wind"] == 0.07


def test_add_new_entrant_wacc_adds_per_technology_column(csv_str_to_df):
    new_entrant_table = csv_str_to_df(
        """
        technology_type,                 build_cost_$/mw
        CCGT,                            1950000
        Wind,                            1800000
        Battery__Storage__(4hrs__storage),  1400000
        """
    )
    iasr_tables = {"wacc": _wacc_table()}

    result = _add_new_entrant_wacc(new_entrant_table, iasr_tables, "Step Change")

    expected = csv_str_to_df(
        """
        technology_type,                 build_cost_$/mw,  wacc
        CCGT,                            1950000,          0.105
        Wind,                            1800000,          0.075
        Battery__Storage__(4hrs__storage),  1400000,       0.08
        """
    )
    pd.testing.assert_frame_equal(result, expected)


def test_add_new_entrant_wacc_noop_when_table_absent(csv_str_to_df):
    new_entrant_table = csv_str_to_df(
        """
        technology_type,  build_cost_$/mw
        CCGT,             1950000
        """
    )

    result = _add_new_entrant_wacc(new_entrant_table, {}, "Step Change")

    # No wacc table (v6.0/tests) -> table returned unchanged, translator falls
    # back to the scalar config rate.
    pd.testing.assert_frame_equal(result, new_entrant_table)


def test_add_new_entrant_wacc_raises_on_unmatched_technology(csv_str_to_df):
    new_entrant_table = csv_str_to_df(
        """
        technology_type,          build_cost_$/mw
        Fusion__Reactor,          9000000
        """
    )
    iasr_tables = {"wacc": _wacc_table()}

    with pytest.raises(ValueError, match="Fusion Reactor"):
        _add_new_entrant_wacc(new_entrant_table, iasr_tables, "Step Change")


def test_generator_dynamic_properties_templater(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    for scenario in _ISP_SCENARIOS:
        mapped_dfs = _template_generator_dynamic_properties(iasr_tables, scenario)
        for key, df in mapped_dfs.items():
            if "price" in key:
                if key == "liquid_fuel_prices" or key == "hydrogen_prices":
                    assert all("$/gj" in col for col in df.columns[:])
                    assert all(df.iloc[:, :].dtypes != "object")
                else:
                    assert all("$/gj" in col for col in df.columns[1:])
                    assert all(df.iloc[:, 1:].dtypes != "object")
                assert all(df.notna())
            elif "outage" in key:
                assert all(df.iloc[:, 1:].dtypes != "object")
                assert all(df.notna())
            elif "ratings" in key:
                assert all(df.iloc[:, 3:].dtypes != "object")
                assert all(df.notna())
