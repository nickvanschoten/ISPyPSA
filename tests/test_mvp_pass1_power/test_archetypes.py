"""Tests for Pass 1 power-sector archetype mutation functions.

Each test follows the strict ordering: inputs → function call → expected → assertion.
"""

import logging

import pandas as pd
import pytest

from mvp_pass1_power.archetypes.cost_optimal import apply as cost_optimal_apply
from mvp_pass1_power.archetypes.fast_fossil_exit import apply as fast_fossil_exit_apply
from mvp_pass1_power.archetypes.fossil_incumbent import apply as fossil_incumbent_apply
from mvp_pass1_power.archetypes.gas_bridge import apply as gas_bridge_apply
from mvp_pass1_power.archetypes.nuclear_included import apply as nuclear_included_apply
from mvp_pass1_power.archetypes.storage_led import apply as storage_led_apply

# Minimal empty DataFrames with the column schemas each archetype function accesses.
# Used in tests that focus on one table and need an inert placeholder for the other.
_EMPTY_ECAA = pd.DataFrame(columns=["fuel_type", "closure_year"])
_EMPTY_NE_TECH = pd.DataFrame(columns=["technology_type"])          # fast_fossil_exit
_EMPTY_NE_FUEL_TECH = pd.DataFrame(columns=["fuel_type", "technology_type"])  # storage_led
_EMPTY_NE_FUEL = pd.DataFrame(columns=["fuel_type"])                # fossil_incumbent


# ---------------------------------------------------------------------------
# cost_optimal — identity mutation
# ---------------------------------------------------------------------------


def test_cost_optimal_returns_ecaa_unchanged(sample_ispypsa_tables):
    ecaa_before = sample_ispypsa_tables["ecaa_generators"].copy()

    result = cost_optimal_apply(sample_ispypsa_tables, config=None)

    pd.testing.assert_frame_equal(result["ecaa_generators"], ecaa_before)


def test_cost_optimal_returns_new_entrant_generators_unchanged(sample_ispypsa_tables):
    ne_before = sample_ispypsa_tables["new_entrant_generators"].copy()

    result = cost_optimal_apply(sample_ispypsa_tables, config=None)

    pd.testing.assert_frame_equal(result["new_entrant_generators"], ne_before)


# ---------------------------------------------------------------------------
# fast_fossil_exit — coal clip to 2030; drop unabated gas new entrants
# ---------------------------------------------------------------------------


def test_fast_fossil_exit_clips_coal_closure_above_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2035
        GenB,       Brown__Coal,  2040
        GenC,       Wind,         2055
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": _EMPTY_NE_TECH.copy()}

    result = fast_fossil_exit_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2030
        GenB,       Brown__Coal,  2030
        GenC,       Wind,         2055
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_fast_fossil_exit_does_not_advance_coal_already_at_or_before_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2028
        GenB,       Black__Coal,  2030
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": _EMPTY_NE_TECH.copy()}

    result = fast_fossil_exit_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2028
        GenB,       Black__Coal,  2030
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_fast_fossil_exit_drops_ocgt_and_ccgt_new_entrants(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,    technology_type,  fuel_type
        Gas__Peaker,       OCGT__(small__GT), Gas
        Gas__Mid-merit,    OCGT__(large__GT), Gas
        Gas__Baseload,     CCGT,              Gas
        Wind__Farm,        Wind,              Wind
        Solar__Farm,       Large__scale__Solar__PV, Solar
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = fast_fossil_exit_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        Wind__Farm,      Wind,             Wind
        Solar__Farm,     Large__scale__Solar__PV, Solar
    """)
    pd.testing.assert_frame_equal(
        result["new_entrant_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_fast_fossil_exit_retains_ccgt_ccs_and_h2_new_entrants(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT,            CCGT,             Gas
        CCGT__CCS,       CCGT-CCS,         Gas
        H2__Engine,      Hydrogen__Recip,  Hydrogen
        Biomass__Plant,  Biomass,          Biomass
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = fast_fossil_exit_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT__CCS,       CCGT-CCS,         Gas
        H2__Engine,      Hydrogen__Recip,  Hydrogen
        Biomass__Plant,  Biomass,          Biomass
    """)
    pd.testing.assert_frame_equal(
        result["new_entrant_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


# ---------------------------------------------------------------------------
# gas_bridge — coal clip to 2030; new entrants unchanged
# ---------------------------------------------------------------------------


def test_gas_bridge_clips_coal_closure_to_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2037
        GenB,       Wind,         2055
    """)
    # gas_bridge does not access new_entrant_generators, so an empty DataFrame suffices.
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": pd.DataFrame()}

    result = gas_bridge_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2030
        GenB,       Wind,         2055
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_gas_bridge_retains_all_new_entrant_technologies(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT__Plant,     CCGT,             Gas
        OCGT__Plant,     OCGT__(small__GT), Gas
        Wind__Farm,      Wind,             Wind
        Solar__Farm,     Large__scale__Solar__PV, Solar
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}
    ne_before = ne.copy()

    result = gas_bridge_apply(tables, config=None)

    pd.testing.assert_frame_equal(result["new_entrant_generators"], ne_before)


# ---------------------------------------------------------------------------
# storage_led — coal clip to 2035; drop all gas new entrants (incl. CCS)
# ---------------------------------------------------------------------------


def test_storage_led_clips_coal_closure_to_2035(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2040
        GenB,       Brown__Coal,  2037
        GenC,       Wind,         2060
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": _EMPTY_NE_FUEL_TECH.copy()}

    result = storage_led_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2035
        GenB,       Brown__Coal,  2035
        GenC,       Wind,         2060
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_storage_led_drops_gas_fuel_type_new_entrants(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT__Plant,     CCGT,             Gas
        OCGT__Plant,     OCGT,             Gas
        H2__Engine,      Hydrogen__Recip,  Hydrogen
        Wind__Farm,      Wind,             Wind
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = storage_led_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        H2__Engine,      Hydrogen__Recip,  Hydrogen
        Wind__Farm,      Wind,             Wind
    """)
    pd.testing.assert_frame_equal(
        result["new_entrant_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_storage_led_drops_ccs_technology_rows_regardless_of_fuel(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        Gas__CCS,        CCGT-CCS,         Gas
        Biomass__CCS,    Biomass__CCS,     Biomass
        Wind__Farm,      Wind,             Wind
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = storage_led_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        Wind__Farm,      Wind,             Wind
    """)
    pd.testing.assert_frame_equal(
        result["new_entrant_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


# ---------------------------------------------------------------------------
# fossil_incumbent — coal +10 years; drop solar; thin wind by 75%
# ---------------------------------------------------------------------------


def test_fossil_incumbent_extends_coal_closure_by_10_years(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2033
        GenB,       Brown__Coal,  2029
        GenC,       Wind,         2050
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": _EMPTY_NE_FUEL.copy()}

    result = fossil_incumbent_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2043
        GenB,       Brown__Coal,  2039
        GenC,       Wind,         2050
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_fossil_incumbent_drops_all_solar_new_entrants(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,         fuel_type
        Solar__North,    Large__scale__Solar__PV, Solar
        Solar__South,    Large__scale__Solar__PV, Solar
        Gas__Plant,      CCGT,                    Gas
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = fossil_incumbent_apply(tables, config=None)

    expected = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        Gas__Plant,      CCGT,             Gas
    """)
    pd.testing.assert_frame_equal(
        result["new_entrant_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_fossil_incumbent_removes_75_percent_of_wind_new_entrants(csv_str_to_df):
    # With 4 wind rows and frac=0.75: round(4 * 0.75) = 3 dropped, 1 remains.
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        Wind__A,         Wind,             Wind
        Wind__B,         Wind,             Wind
        Wind__C,         Wind,             Wind
        Wind__D,         Wind,             Wind
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}

    result = fossil_incumbent_apply(tables, config=None)

    result_ne = result["new_entrant_generators"]
    remaining_wind = result_ne["fuel_type"].eq("Wind").sum()
    assert remaining_wind == 1
    assert result_ne["fuel_type"].iloc[0] == "Wind"


# ---------------------------------------------------------------------------
# nuclear_included — inject Advanced Nuclear from CCGT template
# ---------------------------------------------------------------------------


def test_nuclear_included_adds_one_nuclear_row_per_ccgt_sub_region(sample_ispypsa_tables):
    # sample_ispypsa_tables has CCGT only in CNSW → exactly one nuclear row expected.
    result = nuclear_included_apply(sample_ispypsa_tables, config=None)

    nuclear_rows = result["new_entrant_generators"][
        result["new_entrant_generators"]["generator_name"] == "Advanced Nuclear"
    ]
    assert len(nuclear_rows) == 1
    assert nuclear_rows.iloc[0]["sub_region_id"] == "CNSW"


def test_nuclear_included_sets_correct_overridden_parameters(sample_ispypsa_tables):
    result = nuclear_included_apply(sample_ispypsa_tables, config=None)

    row = result["new_entrant_generators"][
        result["new_entrant_generators"]["generator_name"] == "Advanced Nuclear"
    ].iloc[0]

    assert row["technology_type"] == "Advanced Nuclear"
    # fuel_type = "Nuclear" — listed in translator/generators.py:non_fuel_carriers,
    # so ISPyPSA treats it as a zero-fuel-cost, fully-dispatchable carrier.
    assert row["fuel_type"] == "Nuclear"
    assert row["heat_rate_gj/mwh"] == 0.0
    assert row["vom_$/mwh_sent_out"] == 10.0
    assert row["lifetime"] == 60
    assert row["minimum_stable_level_%"] == 53.0
    assert pd.isna(row["rez_id"])
    assert pd.isna(row["fuel_cost_mapping"])


def test_nuclear_included_adds_build_cost_row_at_csiro_gencost(sample_ispypsa_tables):
    result = nuclear_included_apply(sample_ispypsa_tables, config=None)

    bc = result["new_entrant_build_costs"]
    nuclear_bc = bc[bc["technology"] == "Advanced Nuclear"]
    assert len(nuclear_bc) == 1

    year_cols = [c for c in bc.columns if c.endswith("_$/mw")]
    assert len(year_cols) > 0
    for col in year_cols:
        assert nuclear_bc.iloc[0][col] == 31_100_000


def test_nuclear_included_does_not_duplicate_nuclear_across_same_sub_region(csv_str_to_df):
    # Two CCGT rows in CNSW → only one nuclear row for CNSW.
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type,  sub_region_id,  heat_rate_gj/mwh,  vom_$/mwh_sent_out,  minimum_stable_level_%,  lifetime,  fuel_cost_mapping,  rez_id,  connection_cost_technology
        CCGT__A,         CCGT,             Gas,        CNSW,           7.0,               4.0,                 46.0,                    40,        NSW__CCGT,          ,        CCGT
        CCGT__B,         CCGT,             Gas,        CNSW,           7.5,               4.5,                 46.0,                    40,        NSW__CCGT,          ,        CCGT
    """)
    bc = pd.DataFrame([{"technology": "CCGT", "2024_25_$/mw": 1_800_000}])
    tables = {"new_entrant_generators": ne, "new_entrant_build_costs": bc}

    result = nuclear_included_apply(tables, config=None)

    nuclear_rows = result["new_entrant_generators"][
        result["new_entrant_generators"]["generator_name"] == "Advanced Nuclear"
    ]
    assert len(nuclear_rows) == 1


def test_nuclear_included_skips_gracefully_when_no_thermal_template(csv_str_to_df, caplog):
    ne = csv_str_to_df("""
        generator_name,  technology_type,         fuel_type,  sub_region_id
        Wind__Farm,      Wind,                    Wind,       CNSW
        Solar__Farm,     Large__scale__Solar__PV, Solar,      CNSW
    """)
    bc = pd.DataFrame(columns=["technology", "2024_25_$/mw"])
    tables = {"new_entrant_generators": ne, "new_entrant_build_costs": bc}

    with caplog.at_level(logging.WARNING, logger="mvp_pass1_power.archetypes.nuclear_included"):
        result = nuclear_included_apply(tables, config=None)

    assert result is tables
    assert "no non-VRE thermal template rows found" in caplog.text


def test_nuclear_included_warns_and_skips_when_new_entrant_table_missing(caplog):
    tables = {"some_other_table": pd.DataFrame()}

    with caplog.at_level(logging.WARNING, logger="mvp_pass1_power.archetypes.nuclear_included"):
        result = nuclear_included_apply(tables, config=None)

    assert result is tables
    assert "new_entrant_generators table missing" in caplog.text
