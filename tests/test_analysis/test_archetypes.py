"""Tests for Pass 1 power-sector archetype mutation functions.

Each test follows the strict ordering: inputs → function call → expected → assertion.
"""

import logging

import pandas as pd
import pytest

from analysis.archetypes.cost_optimal import apply as cost_optimal_apply
from analysis.archetypes.rapid_coal_phaseout import apply as rapid_coal_phaseout_apply
from analysis.archetypes.fossil_incumbent import apply as fossil_incumbent_apply
from analysis.archetypes.gas_fleet_maintained import apply as gas_fleet_maintained_apply
from analysis.archetypes.nuclear_baseload import apply as nuclear_baseload_apply
from analysis.archetypes.storage_led import apply as storage_led_apply

# Minimal empty DataFrames with the column schemas each archetype function accesses.
# Used in tests that focus on one table and need an inert placeholder for the other.
_EMPTY_ECAA = pd.DataFrame(columns=["fuel_type", "closure_year"])
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
# rapid_coal_phaseout — coal clip to 2030; gas remains available
# ---------------------------------------------------------------------------


def test_rapid_coal_phaseout_clips_coal_closure_above_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2035
        GenB,       Brown__Coal,  2040
        GenC,       Wind,         2055
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": pd.DataFrame()}

    result = rapid_coal_phaseout_apply(tables, config=None)

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


def test_rapid_coal_phaseout_does_not_advance_coal_already_at_or_before_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2028
        GenB,       Black__Coal,  2030
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": pd.DataFrame()}

    result = rapid_coal_phaseout_apply(tables, config=None)

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


def test_rapid_coal_phaseout_retains_all_new_entrant_technologies(csv_str_to_df):
    # rapid_coal_phaseout differs from the prior fast_fossil_exit in that it
    # does NOT drop gas new entrants — gas remains available; the LP decides.
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT__Plant,     CCGT,             Gas
        OCGT__Plant,     OCGT__(small__GT), Gas
        Wind__Farm,      Wind,             Wind
        Solar__Farm,     Large__scale__Solar__PV, Solar
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}
    ne_before = ne.copy()

    result = rapid_coal_phaseout_apply(tables, config=None)

    pd.testing.assert_frame_equal(result["new_entrant_generators"], ne_before)


# ---------------------------------------------------------------------------
# gas_fleet_maintained — coal clip to 2030; new entrants unchanged
# (gas ≥ 12,500 MW @ 2030 & 2035 mandate is added in a follow-up commit)
# ---------------------------------------------------------------------------


def test_gas_fleet_maintained_clips_coal_closure_to_2030(csv_str_to_df):
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year
        GenA,       Black__Coal,  2037
        GenB,       Wind,         2055
    """)
    tables = {"ecaa_generators": ecaa, "new_entrant_generators": pd.DataFrame()}

    result = gas_fleet_maintained_apply(tables, config=None)

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


def test_gas_fleet_maintained_retains_all_new_entrant_technologies(csv_str_to_df):
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type
        CCGT__Plant,     CCGT,             Gas
        OCGT__Plant,     OCGT__(small__GT), Gas
        Wind__Farm,      Wind,             Wind
        Solar__Farm,     Large__scale__Solar__PV, Solar
    """)
    tables = {"ecaa_generators": _EMPTY_ECAA.copy(), "new_entrant_generators": ne}
    ne_before = ne.copy()

    result = gas_fleet_maintained_apply(tables, config=None)

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
# nuclear_baseload — inject Advanced Nuclear from CCGT template
# (≥2,000 MW @ 2045 and ≥4,000 MW @ 2050 deployment mandate is added in
# a follow-up commit alongside the gas / storage mandates.)
# ---------------------------------------------------------------------------


def test_nuclear_baseload_adds_one_nuclear_row_per_ccgt_sub_region(sample_ispypsa_tables):
    # sample_ispypsa_tables has CCGT only in CNSW → exactly one nuclear row expected.
    result = nuclear_baseload_apply(sample_ispypsa_tables, config=None)

    nuclear_rows = result["new_entrant_generators"][
        result["new_entrant_generators"]["generator_name"] == "Advanced Nuclear"
    ]
    assert len(nuclear_rows) == 1
    assert nuclear_rows.iloc[0]["sub_region_id"] == "CNSW"


def test_nuclear_baseload_sets_correct_overridden_parameters(sample_ispypsa_tables):
    result = nuclear_baseload_apply(sample_ispypsa_tables, config=None)

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


def test_nuclear_baseload_adds_build_cost_row_at_csiro_gencost(sample_ispypsa_tables):
    result = nuclear_baseload_apply(sample_ispypsa_tables, config=None)

    bc = result["new_entrant_build_costs"]
    nuclear_bc = bc[bc["technology"] == "Advanced Nuclear"]
    assert len(nuclear_bc) == 1

    year_cols = [c for c in bc.columns if c.endswith("_$/mw")]
    assert len(year_cols) > 0
    for col in year_cols:
        assert nuclear_bc.iloc[0][col] == 31_100_000


def test_nuclear_baseload_does_not_duplicate_nuclear_across_same_sub_region(csv_str_to_df):
    # Two CCGT rows in CNSW → only one nuclear row for CNSW.
    ne = csv_str_to_df("""
        generator_name,  technology_type,  fuel_type,  sub_region_id,  heat_rate_gj/mwh,  vom_$/mwh_sent_out,  minimum_stable_level_%,  lifetime,  fuel_cost_mapping,  rez_id,  connection_cost_technology
        CCGT__A,         CCGT,             Gas,        CNSW,           7.0,               4.0,                 46.0,                    40,        NSW__CCGT,          ,        CCGT
        CCGT__B,         CCGT,             Gas,        CNSW,           7.5,               4.5,                 46.0,                    40,        NSW__CCGT,          ,        CCGT
    """)
    bc = pd.DataFrame([{"technology": "CCGT", "2024_25_$/mw": 1_800_000}])
    tables = {"new_entrant_generators": ne, "new_entrant_build_costs": bc}

    result = nuclear_baseload_apply(tables, config=None)

    nuclear_rows = result["new_entrant_generators"][
        result["new_entrant_generators"]["generator_name"] == "Advanced Nuclear"
    ]
    assert len(nuclear_rows) == 1


def test_nuclear_baseload_skips_gracefully_when_no_thermal_template(csv_str_to_df, caplog):
    ne = csv_str_to_df("""
        generator_name,  technology_type,         fuel_type,  sub_region_id
        Wind__Farm,      Wind,                    Wind,       CNSW
        Solar__Farm,     Large__scale__Solar__PV, Solar,      CNSW
    """)
    bc = pd.DataFrame(columns=["technology", "2024_25_$/mw"])
    tables = {"new_entrant_generators": ne, "new_entrant_build_costs": bc}

    with caplog.at_level(logging.WARNING, logger="analysis.archetypes.nuclear_baseload"):
        result = nuclear_baseload_apply(tables, config=None)

    assert result is tables
    assert "no non-VRE thermal template rows found" in caplog.text


def test_nuclear_baseload_warns_and_skips_when_new_entrant_table_missing(caplog):
    tables = {"some_other_table": pd.DataFrame()}

    with caplog.at_level(logging.WARNING, logger="analysis.archetypes.nuclear_baseload"):
        result = nuclear_baseload_apply(tables, config=None)

    assert result is tables
    assert "new_entrant_generators table missing" in caplog.text


# ---------------------------------------------------------------------------
# AEMO-anchored deployment-mandate constraints (custom_constraints rows).
# Tests use _StubConfig to inject investment_periods without depending on the
# full ModelConfig schema. Each test fixes minimal templater inputs so the
# generated constraint rows can be asserted against a full expected DataFrame.
# ---------------------------------------------------------------------------


class _StubConfig:
    """Minimal stand-in for ModelConfig — exposes only the chains the helpers read."""

    def __init__(self, investment_periods, biomass_supply_curve_csv=None):
        self.temporal = type("T", (), {
            "capacity_expansion": type("C", (), {"investment_periods": investment_periods})()
        })()
        self.biomass_supply_curve = type(
            "B", (), {"curve_csv": biomass_supply_curve_csv}
        )()


_EMPTY_CC_LHS = pd.DataFrame(columns=["constraint_id", "term_type", "term_id", "coefficient"])
_EMPTY_CC_RHS = pd.DataFrame(columns=["constraint_id", "constraint_type", "rhs"])


def test_gas_fleet_maintained_adds_floor_constraint_at_binding_year(csv_str_to_df):
    # 2030 is in the investment periods AND there are no existing gas units;
    # the entire 12,500 MW floor flows through to the new-entrant LHS.
    ecaa = csv_str_to_df("""
        generator, fuel_type,   closure_year, maximum_capacity_mw
        GenA,      Wind,        2055,         2000
    """)
    ne = csv_str_to_df("""
        generator,    fuel_type,   technology_type, lifetime
        ccgt_cnsw,    Gas,         CCGT,            40
        ocgt_nnsw,    Gas,         OCGT,            30
        wind_cnsw,    Wind,        Wind,            25
    """)
    tables = {
        "ecaa_generators": ecaa,
        "new_entrant_generators": ne,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2030])

    result = gas_fleet_maintained_apply(tables, config)

    expected_lhs = csv_str_to_df("""
        constraint_id,   term_type,            term_id,         coefficient
        gas_floor_2030,  generator_capacity,   ccgt_cnsw_2030,  1.0
        gas_floor_2030,  generator_capacity,   ocgt_nnsw_2030,  1.0
    """)
    expected_rhs = csv_str_to_df("""
        constraint_id,   constraint_type, rhs
        gas_floor_2030,  >=,              12500
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].reset_index(drop=True),
        expected_lhs.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].reset_index(drop=True),
        expected_rhs.reset_index(drop=True),
        check_dtype=False,
    )


def test_gas_fleet_maintained_subtracts_existing_gas_from_floor(csv_str_to_df):
    # 5,000 MW of existing gas surviving past 2030 → residual on RHS is 7,500 MW.
    ecaa = csv_str_to_df("""
        generator, fuel_type, closure_year, maximum_capacity_mw
        ExistGas1, Gas,       2035,         3000
        ExistGas2, Gas,       2040,         2000
        ExistGas3, Gas,       2029,         9999
    """)
    ne = csv_str_to_df("""
        generator,    fuel_type, technology_type, lifetime
        ccgt_cnsw,    Gas,       CCGT,            40
    """)
    tables = {
        "ecaa_generators": ecaa,
        "new_entrant_generators": ne,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2030])

    result = gas_fleet_maintained_apply(tables, config)

    # ExistGas3 closure_year 2029 — not active at 2030, excluded from existing total.
    # ExistGas1 + ExistGas2 = 5,000 MW. Residual floor at 2030: 12,500 - 5,000 = 7,500.
    assert float(result["custom_constraints_rhs"].iloc[0]["rhs"]) == 7500.0


def test_gas_fleet_maintained_skips_year_when_existing_already_meets_floor(csv_str_to_df, caplog):
    ecaa = csv_str_to_df("""
        generator, fuel_type, closure_year, maximum_capacity_mw
        ExistGas,  Gas,       2040,         13000
    """)
    ne = csv_str_to_df("""
        generator,  fuel_type, technology_type, lifetime
        ccgt_cnsw,  Gas,       CCGT,            40
    """)
    tables = {
        "ecaa_generators": ecaa,
        "new_entrant_generators": ne,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2030])

    with caplog.at_level(logging.INFO, logger="analysis.archetypes._capacity_floor"):
        result = gas_fleet_maintained_apply(tables, config)

    assert result["custom_constraints_lhs"].empty
    assert result["custom_constraints_rhs"].empty
    assert (
        "gas_floor_2030: existing 13000 MW already meets floor 12500 MW; skipping"
        in caplog.text
    )


def test_storage_led_floor_uses_storage_capacity_term_type(csv_str_to_df):
    # Storage mandate should produce storage_capacity term_type rows on batteries.
    ecaa_batt = csv_str_to_df("""
        storage_name,   closure_year, maximum_capacity_mw
        ExistBatt,      2050,         500
    """)
    ne_batt = csv_str_to_df("""
        storage_name,         lifetime
        battery_1h_cnsw,      20
        battery_8h_nnsw,      20
    """)
    tables = {
        "ecaa_generators": _EMPTY_ECAA.copy(),
        "new_entrant_generators": _EMPTY_NE_FUEL_TECH.copy(),
        "ecaa_batteries": ecaa_batt,
        "new_entrant_batteries": ne_batt,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2030])

    result = storage_led_apply(tables, config)

    # storage_floor_2030: floor 33,926 - existing 500 = 33,426 residual.
    assert (result["custom_constraints_lhs"]["term_type"] == "storage_capacity").all()
    assert set(result["custom_constraints_lhs"]["term_id"]) == {
        "battery_1h_cnsw_2030",
        "battery_8h_nnsw_2030",
    }
    assert float(result["custom_constraints_rhs"].iloc[0]["rhs"]) == 33_926 - 500


def test_capacity_floor_warns_when_mandate_year_not_in_investment_periods(csv_str_to_df, caplog):
    # 2035 is not in the configured investment periods; the helper logs a warning
    # and skips that mandate year while still applying others.
    ecaa = csv_str_to_df("""
        generator, fuel_type, closure_year, maximum_capacity_mw
        ExistGas,  Gas,       2050,         1000
    """)
    ne = csv_str_to_df("""
        generator,  fuel_type, technology_type, lifetime
        ccgt_cnsw,  Gas,       CCGT,            40
    """)
    tables = {
        "ecaa_generators": ecaa,
        "new_entrant_generators": ne,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2030])

    with caplog.at_level(logging.WARNING, logger="analysis.archetypes._capacity_floor"):
        result = gas_fleet_maintained_apply(tables, config)

    assert (
        "gas_floor: mandate years [2035] not in investment_periods [2030]; skipping those years"
        in caplog.text
    )
    # 2030 floor still applied (one constraint id present).
    assert set(result["custom_constraints_rhs"]["constraint_id"]) == {"gas_floor_2030"}


# ---------------------------------------------------------------------------
# Option B maintenance overlay — adds ageing premium to fom_$/kw/annum for
# ECAA thermal generators inside the EOL window. Applied as a pre-pass to all
# six archetypes via the _with_pre_passes wrapper in __init__.py.
# ---------------------------------------------------------------------------


def test_maintenance_overlay_adds_premium_to_coal_within_eol_window(csv_str_to_df):
    from analysis.archetypes._maintenance_overlay import apply as overlay_apply

    # Coal closing in 5 years from first period 2030 -> closes 2035.
    # Premium = (10 - 5) / 10 * 50 = 25.0 AUD/kW/yr.
    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year,  fom_$/kw/annum
        CoalEOL,    Black__Coal,  2035,          60.0
        CoalFar,    Black__Coal,  2050,          60.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030, 2040, 2050])

    result = overlay_apply(tables, config)

    expected = csv_str_to_df("""
        generator,  fuel_type,    closure_year,  fom_$/kw/annum
        CoalEOL,    Black__Coal,  2035,          85.0
        CoalFar,    Black__Coal,  2050,          60.0
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_maintenance_overlay_uses_separate_gas_window_and_max(csv_str_to_df):
    from analysis.archetypes._maintenance_overlay import apply as overlay_apply

    # Gas closing 2 years from first period 2030 -> 2032.
    # Premium = (5 - 2) / 5 * 20 = 12.0 AUD/kW/yr.
    ecaa = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        GasEOL,     Gas,        2032,          15.0
        GasFar,     Gas,        2040,          15.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = overlay_apply(tables, config)

    expected = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        GasEOL,     Gas,        2032,          27.0
        GasFar,     Gas,        2040,          15.0
    """)
    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_maintenance_overlay_leaves_non_thermal_units_unchanged(csv_str_to_df):
    from analysis.archetypes._maintenance_overlay import apply as overlay_apply

    ecaa = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        Wind1,      Wind,       2032,          25.0
        Solar1,     Solar,      2032,          18.0
        Hydro1,     Water,      2032,          15.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = overlay_apply(tables, config)

    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        ecaa.reset_index(drop=True),
        check_dtype=False,
    )


def test_maintenance_overlay_pre_pass_runs_for_every_archetype(csv_str_to_df):
    # End-to-end: invoke the wrapped APPLY_ARCHETYPE entry for cost_optimal and
    # confirm the overlay's coal premium has been applied before the archetype
    # mutation runs.
    from analysis.archetypes import APPLY_ARCHETYPE

    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year,  fom_$/kw/annum
        CoalEOL,    Black__Coal,  2035,          60.0
    """)
    tables = {
        "ecaa_generators": ecaa,
        "ecaa_batteries": pd.DataFrame(columns=["storage_name"]),
        # Real templater output always includes a biomass price table; the
        # feedstock-cost pre-pass re-prices it.
        "biomass_prices": csv_str_to_df("""
            2029_30_$/gj,  2049_50_$/gj
            0.661895,      0.661895
        """),
    }
    config = _StubConfig(investment_periods=[2030, 2040, 2050])

    result = APPLY_ARCHETYPE["cost_optimal"](tables, config)

    # Premium 25.0 added (see test_maintenance_overlay_adds_premium_to_coal_within_eol_window).
    assert float(result["ecaa_generators"].iloc[0]["fom_$/kw/annum"]) == 85.0


# ---------------------------------------------------------------------------
# Phase 2.4 pre-solve integration check — exercise every wrapped archetype in
# PRODUCTION_ARCHETYPES against the shared sample fixture; assert no crashes
# and that each runs the three pre-passes (pumped storage / Option B / repowering)
# before the per-archetype mutation.
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_templater_output(csv_str_to_df):
    """Minimal post-templater inputs in the templater-output schema (term_id, not
    variable_name) so the pumped_storage_fix and downstream archetype mutations
    can run end-to-end without column-name surprises."""
    return {
        "ecaa_generators": csv_str_to_df("""
            generator,  technology_type,        sub_region_id,  fuel_type,    fom_$/kw/annum,  vom_$/mwh_sent_out,  heat_rate_gj/mwh,  closure_year,  maximum_capacity_mw
            Bayswater,  Steam__Sub__Critical,   CNSW,           Black__Coal,  60.0,            5.0,                 10.0,              2033,          2640
            Tarong,     Steam__Sub__Critical,   CNSW,           Black__Coal,  55.0,            5.0,                 10.0,              2038,          1843
            Tallawarra, CCGT,                   CNSW,           Gas,          15.0,            4.0,                 7.0,               2042,          400
            Bodangora,  Wind,                   CNSW,           Wind,         25.0,            0.0,                 0.0,               2045,          250
            Moree,      Large__scale__Solar__PV,CNSW,           Solar,        18.0,            0.0,                 0.0,               2046,          320
        """),
        "new_entrant_generators": csv_str_to_df("""
            generator,     generator_name, technology_type, fuel_type, sub_region_id, fuel_cost_mapping, heat_rate_gj/mwh, vom_$/mwh_sent_out, lifetime, minimum_stable_level_%, connection_cost_technology, rez_id
            ccgt_cnsw,     CCGT,           CCGT,            Gas,       CNSW,          NSW__new__CCGT,    7.0,              4.0,                40,       40,                     CCGT,                       N3
            ocgt_cnsw,     OCGT,           OCGT,            Gas,       CNSW,          NSW__new__OCGT,    10.0,             8.0,                30,       40,                     OCGT,                       N3
            wind_n3,       Wind,           Wind,            Wind,      CNSW,          ,                  0.0,              0.0,                25,       0,                      ,                           N3
            solar_n3,      Solar__PV,      Large__scale__Solar__PV,  Solar, CNSW,     ,                  0.0,              0.0,                25,       0,                      ,                           N3
        """),
        "new_entrant_build_costs": pd.DataFrame([
            {"technology": "CCGT", "2024_25_$/mw": 1_800_000},
            {"technology": "OCGT", "2024_25_$/mw": 900_000},
            {"technology": "Wind", "2024_25_$/mw": 2_000_000},
            {"technology": "Large scale Solar PV", "2024_25_$/mw": 1_400_000},
        ]),
        "ecaa_batteries": pd.DataFrame(columns=[
            "storage_name", "sub_region_id", "closure_year",
            "maximum_capacity_mw", "storage_duration_hours",
        ]),
        "new_entrant_batteries": csv_str_to_df("""
            storage_name,        sub_region_id, lifetime, storage_duration_hours
            battery_8h_cnsw,     CNSW,          20,       8
        """),
        "custom_constraints_lhs": pd.DataFrame(columns=["constraint_id", "term_type", "term_id", "coefficient"]),
        "custom_constraints_rhs": pd.DataFrame(columns=["constraint_id", "constraint_type", "rhs"]),
        # The real templater always emits a single-row biomass price table
        # (IASR $0.66/GJ, flat). Included so the biomass feedstock-cost
        # pre-pass has a table to re-price.
        "biomass_prices": csv_str_to_df("""
            2029_30_$/gj,  2039_40_$/gj,  2049_50_$/gj
            0.661895,      0.661895,      0.661895
        """),
    }


@pytest.mark.parametrize("archetype_id", [
    "cost_optimal",
    "rapid_coal_phaseout",
    "gas_fleet_maintained",
    "storage_led",
    "fossil_incumbent",
    "nuclear_baseload",
])
def test_every_wrapped_archetype_runs_against_minimal_inputs(archetype_id, minimal_templater_output):
    from analysis.archetypes import APPLY_ARCHETYPE

    config = _StubConfig(investment_periods=[2030, 2040, 2050])

    result = APPLY_ARCHETYPE[archetype_id](minimal_templater_output, config)

    # Pre-pass evidence: repowering extends every VRE closure_year by 20 yr.
    # Bodangora 2045 -> 2065; Moree 2046 -> 2066. (fossil_incumbent thins wind
    # in new_entrants but doesn't touch ecaa.)
    vre = result["ecaa_generators"][result["ecaa_generators"]["fuel_type"].isin(["Wind", "Solar"])]
    assert int(vre[vre["generator"] == "Bodangora"]["closure_year"].iloc[0]) == 2065
    assert int(vre[vre["generator"] == "Moree"]["closure_year"].iloc[0]) == 2066

    # Pre-pass evidence: Bayswater (Black Coal, 2033 closure, 3 yr to first
    # period 2030) gets a maintenance overlay premium of 35 AUD/kW/yr.
    bayswater = result["ecaa_generators"][result["ecaa_generators"]["generator"] == "Bayswater"]
    expected_premium = (10 - 3) / 10 * 50
    assert abs(float(bayswater.iloc[0]["fom_$/kw/annum"]) - (60.0 + expected_premium)) < 1e-6


def test_production_archetypes_registry_lists_six_entries():
    from analysis.archetypes import APPLY_ARCHETYPE, PRODUCTION_ARCHETYPES

    expected = {
        "cost_optimal",
        "rapid_coal_phaseout",
        "gas_fleet_maintained",
        "storage_led",
        "fossil_incumbent",
        "nuclear_baseload",
    }
    assert set(PRODUCTION_ARCHETYPES) == expected
    assert set(APPLY_ARCHETYPE.keys()) == expected


# ---------------------------------------------------------------------------
# EOL renewable repowering — extends VRE closure_year by 20y and adds an
# annualised repowering capex premium to fom_$/kw/annum. Applied as pre-pass.
# ---------------------------------------------------------------------------


def test_repowering_extends_wind_closure_year_by_20_years_and_adds_fom_premium(csv_str_to_df):
    from analysis.archetypes._repowering import apply as repowering_apply

    # Wind closing 2045 — 15 years from first period 2030. After repowering:
    # closure_year = 2065. fom premium = 1000 / (15 + 20) = 28.5714... AUD/kW/yr.
    ecaa = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        WindA,      Wind,       2045,          25.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = repowering_apply(tables, config)

    result_row = result["ecaa_generators"].iloc[0]
    assert int(result_row["closure_year"]) == 2065
    expected_premium = 1000.0 / (15 + 20)
    assert abs(float(result_row["fom_$/kw/annum"]) - (25.0 + expected_premium)) < 1e-6


def test_repowering_uses_separate_solar_capex(csv_str_to_df):
    from analysis.archetypes._repowering import apply as repowering_apply

    # Solar closing 2040 — 10 years from first period 2030.
    # premium = 800 / (10 + 20) = 26.6667 AUD/kW/yr.
    ecaa = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        SolarA,     Solar,      2040,          12.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = repowering_apply(tables, config)

    result_row = result["ecaa_generators"].iloc[0]
    assert int(result_row["closure_year"]) == 2060
    expected_premium = 800.0 / (10 + 20)
    assert abs(float(result_row["fom_$/kw/annum"]) - (12.0 + expected_premium)) < 1e-6


def test_repowering_leaves_thermal_and_hydro_unchanged(csv_str_to_df):
    from analysis.archetypes._repowering import apply as repowering_apply

    ecaa = csv_str_to_df("""
        generator,  fuel_type,    closure_year,  fom_$/kw/annum
        Coal1,      Black__Coal,  2040,          60.0
        Gas1,       Gas,          2040,          15.0
        Hydro1,     Water,        2040,          15.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = repowering_apply(tables, config)

    pd.testing.assert_frame_equal(
        result["ecaa_generators"].reset_index(drop=True),
        ecaa.reset_index(drop=True),
        check_dtype=False,
    )


def test_repowering_skips_already_retired_vre(csv_str_to_df):
    from analysis.archetypes._repowering import apply as repowering_apply

    ecaa = csv_str_to_df("""
        generator,  fuel_type,  closure_year,  fom_$/kw/annum
        WindOld,    Wind,       2025,          25.0
    """)
    tables = {"ecaa_generators": ecaa}
    config = _StubConfig(investment_periods=[2030])

    result = repowering_apply(tables, config)

    # Years to close = -5; no premium applied. (Closure year still extends by
    # 20, but the asset has already retired before first_period — translator
    # will filter it out via lifetime <= 0.)
    assert float(result["ecaa_generators"].iloc[0]["fom_$/kw/annum"]) == 25.0


def test_nuclear_baseload_adds_floor_at_2045_and_2050(csv_str_to_df):
    # The injected Advanced Nuclear rows must be the ones constrained by the
    # nuclear floor. Use a CCGT template so injection succeeds.
    ne = csv_str_to_df("""
        generator,    generator_name,  technology_type,  fuel_type,  sub_region_id,  heat_rate_gj/mwh,  vom_$/mwh_sent_out,  minimum_stable_level_%,  lifetime,  fuel_cost_mapping,  rez_id,  connection_cost_technology
        ccgt_cnsw,    CCGT__plant,     CCGT,             Gas,        CNSW,           7.0,               4.0,                 46.0,                    40,        NSW__CCGT,          ,        CCGT
    """)
    bc = pd.DataFrame([{"technology": "CCGT", "2024_25_$/mw": 1_800_000}])
    tables = {
        "new_entrant_generators": ne,
        "new_entrant_build_costs": bc,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2045, 2050])

    result = nuclear_baseload_apply(tables, config)

    rhs = result["custom_constraints_rhs"].set_index("constraint_id")
    assert float(rhs.loc["nuclear_floor_2045", "rhs"]) == 2_000.0
    assert float(rhs.loc["nuclear_floor_2050", "rhs"]) == 4_000.0
    # All LHS terms reference the injected nuclear generator across build_years.
    lhs = result["custom_constraints_lhs"]
    assert (lhs["term_type"] == "generator_capacity").all()
    assert set(lhs["term_id"]).issubset({"Advanced Nuclear CNSW_2045", "Advanced Nuclear CNSW_2050"})


# ---------------------------------------------------------------------------
# Phase 7.0 biomass availability cap — applied as a pre-pass on every
# archetype. Methodology improvement post-Phase 6 surfacing of biomass
# dispatch ~70x current Australian usage.
# ---------------------------------------------------------------------------


def test_biomass_cap_adds_ceiling_at_each_binding_year(csv_str_to_df):
    from analysis.archetypes._biomass_cap import apply as cap_apply

    ne = csv_str_to_df("""
        generator,    fuel_type,  technology_type, lifetime
        biomass_cnsw, Biomass,    Biomass,         30
        biomass_vic,  Biomass,    Biomass,         30
        ccgt_cnsw,    Gas,        CCGT,            40
    """)
    tables = {
        "ecaa_generators": pd.DataFrame(columns=["fuel_type", "closure_year", "maximum_capacity_mw"]),
        "new_entrant_generators": ne,
        "custom_constraints_lhs": _EMPTY_CC_LHS.copy(),
        "custom_constraints_rhs": _EMPTY_CC_RHS.copy(),
    }
    config = _StubConfig(investment_periods=[2025, 2030, 2050])

    result = cap_apply(tables, config)

    rhs = result["custom_constraints_rhs"].set_index("constraint_id")
    # Per the cap dict: 2025=1000, 2030=1500, 2050=5000
    assert float(rhs.loc["biomass_cap_2025", "rhs"]) == 1_000.0
    assert float(rhs.loc["biomass_cap_2030", "rhs"]) == 1_500.0
    assert float(rhs.loc["biomass_cap_2050", "rhs"]) == 5_000.0
    assert (rhs["constraint_type"] == "<=").all()

    # LHS targets only biomass new entrants. Each year's LHS sums every
    # biomass cohort active at that year (build_year <= year < build_year
    # + lifetime). With investment_periods=[2025, 2030, 2050] and biomass
    # lifetime=30: year 2050 sees the 2025/2030/2050 build cohorts all
    # still active (2025+30=2055, 2030+30=2060, 2050+30=2080, all > 2050).
    lhs = result["custom_constraints_lhs"]
    biomass_lhs = lhs[lhs["constraint_id"].str.startswith("biomass_cap_")]
    assert (biomass_lhs["term_type"] == "generator_capacity").all()
    # CCGT must NOT appear in any biomass_cap LHS.
    assert not biomass_lhs["term_id"].str.contains("ccgt").any()
    # 2025 cap: only 2025-build cohort active.
    assert set(biomass_lhs[biomass_lhs.constraint_id == "biomass_cap_2025"]["term_id"]) == {
        "biomass_cnsw_2025", "biomass_vic_2025",
    }
    # 2030 cap: 2025+2030 cohorts active.
    assert set(biomass_lhs[biomass_lhs.constraint_id == "biomass_cap_2030"]["term_id"]) == {
        "biomass_cnsw_2025", "biomass_vic_2025",
        "biomass_cnsw_2030", "biomass_vic_2030",
    }
    # 2050 cap: all three cohorts active.
    assert set(biomass_lhs[biomass_lhs.constraint_id == "biomass_cap_2050"]["term_id"]) == {
        "biomass_cnsw_2025", "biomass_vic_2025",
        "biomass_cnsw_2030", "biomass_vic_2030",
        "biomass_cnsw_2050", "biomass_vic_2050",
    }


def test_biomass_cap_runs_under_every_wrapped_archetype(minimal_templater_output):
    from analysis.archetypes import APPLY_ARCHETYPE
    # Add a biomass new-entrant row so the cap has something to constrain.
    minimal_templater_output["new_entrant_generators"] = pd.concat([
        minimal_templater_output["new_entrant_generators"],
        pd.DataFrame([{
            "generator": "biomass_cnsw",
            "generator_name": "Biomass",
            "technology_type": "Biomass",
            "fuel_type": "Biomass",
            "sub_region_id": "CNSW",
            "fuel_cost_mapping": "Biomass",
            "heat_rate_gj/mwh": 12.0,
            "vom_$/mwh_sent_out": 8.0,
            "lifetime": 30,
            "minimum_stable_level_%": 50,
            "connection_cost_technology": "",
            "rez_id": "N3",
        }])
    ], ignore_index=True)
    config = _StubConfig(investment_periods=[2030, 2040, 2050])

    for arch in ["cost_optimal", "rapid_coal_phaseout", "gas_fleet_maintained",
                 "storage_led", "fossil_incumbent", "nuclear_baseload"]:
        tables = {k: v.copy() if hasattr(v, "copy") else v for k, v in minimal_templater_output.items()}
        result = APPLY_ARCHETYPE[arch](tables, config)
        rhs = result["custom_constraints_rhs"]
        biomass_caps = rhs[rhs.constraint_id.str.startswith("biomass_cap_")]
        assert not biomass_caps.empty, f"{arch}: biomass_cap_* not present in rhs"
        assert set(biomass_caps.constraint_id) == {"biomass_cap_2030", "biomass_cap_2040", "biomass_cap_2050"}


# ---------------------------------------------------------------------------
# Phase 8.x biomass feedstock cost — re-prices biomass feedstock from the
# IASR $0.66/GJ residue-tier value to the scale-appropriate beyond-residue
# delivered cost ($6.0/GJ, IRENA locally-collected tier). Corrects the
# running economics the capacity cap left untouched. See
# _biomass_feedstock_cost.py for sourcing.
# ---------------------------------------------------------------------------


def test_biomass_feedstock_cost_reprices_every_financial_year(csv_str_to_df):
    from analysis.archetypes._biomass_feedstock_cost import (
        _SCALED_BIOMASS_FEEDSTOCK_COST_GJ,
        apply as feedstock_apply,
    )

    biomass_prices = csv_str_to_df("""
        2029_30_$/gj,  2039_40_$/gj,  2049_50_$/gj
        0.661895,      0.661895,      0.661895
    """)
    tables = {"biomass_prices": biomass_prices}

    result = feedstock_apply(tables, _StubConfig(investment_periods=[2030]))

    expected = csv_str_to_df(f"""
        2029_30_$/gj,                        2039_40_$/gj,                        2049_50_$/gj
        {_SCALED_BIOMASS_FEEDSTOCK_COST_GJ}, {_SCALED_BIOMASS_FEEDSTOCK_COST_GJ}, {_SCALED_BIOMASS_FEEDSTOCK_COST_GJ}
    """)
    pd.testing.assert_frame_equal(
        result["biomass_prices"], expected, check_dtype=False
    )


def test_biomass_feedstock_cost_leaves_other_price_tables_untouched(csv_str_to_df):
    from analysis.archetypes._biomass_feedstock_cost import apply as feedstock_apply

    biomass_prices = csv_str_to_df("""
        2029_30_$/gj,  2049_50_$/gj
        0.661895,      0.661895
    """)
    gas_prices = csv_str_to_df("""
        generator,  2029_30_$/gj,  2049_50_$/gj
        CNSW__CCGT, 12.5,          13.0
    """)
    tables = {"biomass_prices": biomass_prices, "gas_prices": gas_prices.copy()}

    result = feedstock_apply(tables, _StubConfig(investment_periods=[2030]))

    # Gas price table (also has $/gj columns) must be left untouched — the
    # pre-pass only re-prices the biomass table it is handed by name.
    pd.testing.assert_frame_equal(result["gas_prices"], gas_prices)


def test_biomass_feedstock_cost_logs_the_reprice(csv_str_to_df, caplog):
    from analysis.archetypes._biomass_feedstock_cost import apply as feedstock_apply

    biomass_prices = csv_str_to_df("""
        2029_30_$/gj,  2039_40_$/gj,  2049_50_$/gj
        0.661895,      0.661895,      0.661895
    """)

    with caplog.at_level("INFO"):
        feedstock_apply(
            {"biomass_prices": biomass_prices}, _StubConfig(investment_periods=[2030])
        )

    assert (
        "Biomass feedstock re-priced to 6.0 $/GJ "
        "(IRENA locally-collected tier) across 3 financial years"
    ) in caplog.text


def test_biomass_feedstock_cost_stands_down_when_supply_curve_configured(
    csv_str_to_df, caplog
):
    from analysis.archetypes._biomass_feedstock_cost import apply as feedstock_apply

    biomass_prices = csv_str_to_df("""
        2029_30_$/gj,  2049_50_$/gj
        0.661895,      0.661895
    """)
    config = _StubConfig(
        investment_periods=[2030],
        biomass_supply_curve_csv="bioenergy_market/biomass_supply_curve_central.csv",
    )

    with caplog.at_level("INFO"):
        result = feedstock_apply({"biomass_prices": biomass_prices.copy()}, config)

    # The IASR residue-tier baseline must survive untouched — it is the
    # supply curve's tranche-1 price.
    pd.testing.assert_frame_equal(result["biomass_prices"], biomass_prices)
    assert (
        "Biomass feedstock supply curve configured; leaving the IASR "
        "residue-tier baseline price in place (the curve prices scaled "
        "feedstock above it), so the flat re-price pre-pass is skipped"
    ) in caplog.text
