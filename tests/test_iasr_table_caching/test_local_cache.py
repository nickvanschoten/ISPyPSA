from unittest.mock import patch

from ispypsa.iasr_table_caching.local_cache import _build_required_tables


def test_build_required_tables_new_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = _build_required_tables()
    assert result == [
        "sub_regional_reference_nodes",
        "renewable_energy_zones",
        "flow_path_transfer_capability",
        "initial_transmission_limits",
    ]


def test_build_required_tables_old_format():
    with patch(
        "ispypsa.iasr_table_caching.local_cache.FEATURE_FLAGS",
        {"use_new_table_format": False},
    ):
        result = _build_required_tables()
    assert "sub_regional_reference_nodes" in result
    assert "initial_build_limits" in result
    assert "existing_generators_summary" in result
    assert "battery_properties" in result
    assert "vic_renewable_target_trajectory" in result
    assert "build_costs_current_policies" in result
    assert "expected_closure_years" in result
    assert "maximum_capacity_existing_generators" in result


# ---------------------------------------------------------------------------
# Phase 1 follow-up (j): backfill empty early-FY columns in fuel-price tables
# at cache-load time. Hydrogen_prices is the only v6.0 IASR table with the
# empty-early-FY pattern, but the function is conservative across all known
# fuel-price tables.
# ---------------------------------------------------------------------------


def test_backfill_early_fy_fills_empty_columns_from_first_populated(tmp_path):
    import pandas as pd
    from ispypsa.iasr_table_caching.schema_normalisation import (
        backfill_early_fy_fuel_prices,
    )

    # Two-row hydrogen_prices with empty FY 2022-23, 2023-24 and populated 2024-25+.
    df = pd.DataFrame({
        "Hydrogen price": ["Hydrogen", "Hydrogen"],
        "Hydrogen price scenario": ["Step Change", "Progressive Change"],
        "2022-23": [None, None],
        "2023-24": [None, None],
        "2024-25": [42.9, 31.7],
        "2025-26": [41.5, 30.8],
    })
    df.to_csv(tmp_path / "hydrogen_prices.csv", index=False)

    backfill_early_fy_fuel_prices(tmp_path)

    backfilled = pd.read_csv(tmp_path / "hydrogen_prices.csv")
    # 2022-23 and 2023-24 should equal 2024-25 (the earliest populated FY).
    assert list(backfilled["2022-23"]) == [42.9, 31.7]
    assert list(backfilled["2023-24"]) == [42.9, 31.7]
    # 2024-25 and 2025-26 unchanged.
    assert list(backfilled["2024-25"]) == [42.9, 31.7]
    assert list(backfilled["2025-26"]) == [41.5, 30.8]


def test_backfill_early_fy_is_noop_when_all_populated(tmp_path):
    import pandas as pd
    from ispypsa.iasr_table_caching.schema_normalisation import (
        backfill_early_fy_fuel_prices,
    )

    # gas_prices_existing_generators with all FY columns populated.
    df = pd.DataFrame({
        "Generator": ["Bayswater"],
        "2022-23": [10.0],
        "2023-24": [10.5],
        "2024-25": [11.0],
    })
    df.to_csv(tmp_path / "gas_prices_existing_generators.csv", index=False)

    backfill_early_fy_fuel_prices(tmp_path)

    after = pd.read_csv(tmp_path / "gas_prices_existing_generators.csv")
    assert list(after["2022-23"]) == [10.0]
    assert list(after["2023-24"]) == [10.5]
    assert list(after["2024-25"]) == [11.0]


def test_backfill_early_fy_skips_missing_tables(tmp_path):
    """No error when a configured table isn't in the cache (defensive)."""
    from ispypsa.iasr_table_caching.schema_normalisation import (
        backfill_early_fy_fuel_prices,
    )
    # Empty tmp_path; nothing to do. Should not raise.
    backfill_early_fy_fuel_prices(tmp_path)
