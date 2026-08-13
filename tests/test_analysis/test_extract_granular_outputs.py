"""Tests for per-period granular output extraction helpers.

Tests cover pure computation functions and storage-unit summarisation.
Network-dependent orchestrators (extract_granular_period, emit_granular_outputs)
require a full solved PyPSA network and are integration-tested separately.
"""

from unittest.mock import Mock

import pandas as pd
import pytest

from analysis.postprocess.extract_granular_outputs import (
    _capacity_factors,
    _storage_totals,
)


# ---------------------------------------------------------------------------
# _capacity_factors(generation_mwh, capacity_mw, period_hours)
# ---------------------------------------------------------------------------


def test_capacity_factors_basic_computation():
    gen = pd.Series({"Wind": 8760.0, "Gas": 4380.0})
    cap = pd.Series({"Wind": 2.0, "Gas": 1.0})
    period_hours = 8760.0

    result = _capacity_factors(gen, cap, period_hours)

    # Wind: 8760 / (2 * 8760) = 0.5; Gas: 4380 / (1 * 8760) = 0.5
    expected = pd.Series({"Gas": 0.5, "Wind": 0.5})
    pd.testing.assert_series_equal(result.sort_index(), expected.sort_index(), rtol=1e-5)


def test_capacity_factors_high_utilisation_carrier():
    gen = pd.Series({"Coal": 8_760_000.0})
    cap = pd.Series({"Coal": 1_000.0})
    period_hours = 8760.0

    result = _capacity_factors(gen, cap, period_hours)

    # 8_760_000 / (1000 * 8760) = 1.0
    assert result["Coal"] == pytest.approx(1.0)


def test_capacity_factors_excludes_carriers_with_zero_capacity():
    gen = pd.Series({"Wind": 100.0, "Solar": 50.0})
    cap = pd.Series({"Wind": 1.0, "Solar": 0.0})

    result = _capacity_factors(gen, cap, 8760.0)

    assert "Solar" not in result.index
    assert "Wind" in result.index


def test_capacity_factors_handles_zero_generation_on_positive_capacity():
    gen = pd.Series({"Wind": 0.0, "Gas": 4380.0})
    cap = pd.Series({"Wind": 1.0, "Gas": 1.0})

    result = _capacity_factors(gen, cap, 8760.0)

    expected = pd.Series({"Gas": 0.5, "Wind": 0.0})
    pd.testing.assert_series_equal(result.sort_index(), expected.sort_index(), rtol=1e-5)


def test_capacity_factors_empty_inputs_return_empty():
    result = _capacity_factors(pd.Series(dtype=float), pd.Series(dtype=float), 8760.0)

    assert result.empty


# ---------------------------------------------------------------------------
# _storage_totals(network)
# ---------------------------------------------------------------------------


def test_storage_totals_empty_storage_units_returns_zeros():
    network = Mock()
    network.storage_units = pd.DataFrame(columns=["p_nom_opt", "max_hours"])

    power_gw, energy_gwh = _storage_totals(network)

    assert power_gw == 0.0
    assert energy_gwh == 0.0


def test_storage_totals_sums_power_correctly():
    network = Mock()
    network.storage_units = pd.DataFrame({
        "p_nom_opt": [1000.0, 2000.0],
        "max_hours": [4.0, 8.0],
    })

    power_gw, _ = _storage_totals(network)

    assert power_gw == pytest.approx(3.0)   # (1000 + 2000) / 1000


def test_storage_totals_sums_energy_correctly():
    network = Mock()
    network.storage_units = pd.DataFrame({
        "p_nom_opt": [1000.0, 2000.0],
        "max_hours": [4.0, 8.0],
    })

    _, energy_gwh = _storage_totals(network)

    assert energy_gwh == pytest.approx(20.0)  # (1000*4 + 2000*8) / 1000


def test_storage_totals_without_p_nom_opt_column_returns_zeros():
    network = Mock()
    network.storage_units = pd.DataFrame({"max_hours": [4.0, 8.0]})

    power_gw, energy_gwh = _storage_totals(network)

    assert power_gw == 0.0
    assert energy_gwh == 0.0


def test_storage_totals_without_max_hours_column_gives_zero_energy():
    # max_hours absent → energy capacity cannot be computed → 0.0
    network = Mock()
    network.storage_units = pd.DataFrame({"p_nom_opt": [1000.0, 2000.0]})

    power_gw, energy_gwh = _storage_totals(network)

    assert power_gw == pytest.approx(3.0)
    assert energy_gwh == 0.0


def test_storage_totals_single_unit():
    network = Mock()
    network.storage_units = pd.DataFrame({
        "p_nom_opt": [500.0],
        "max_hours": [6.0],
    })

    power_gw, energy_gwh = _storage_totals(network)

    assert power_gw == pytest.approx(0.5)   # 500 / 1000
    assert energy_gwh == pytest.approx(3.0)  # 500 * 6 / 1000
