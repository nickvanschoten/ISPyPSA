"""Tests for the recursive-dynamic capacity roll-forward.

The load-bearing test is `test_inject_mutates_in_memory_dict_not_csv` — the
gotcha that build_pypsa_network reads the in-memory pypsa_friendly dict and
not the CSV on disk caused the persistence probe's first run to no-op.
A future regression on the injection seam would silently produce greenfield
trajectories that look like brownfield from the file system.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pypsa
import pytest

from analysis.benchmarks.recursive_dynamic import (
    _extract_new_built_generators,
    _extract_new_built_storage_units,
    adjust_capacity_caps_for_carried,
    inject_carried_tranches,
    load_tranches,
    save_tranche,
)


def _make_solved_network_one_period(
    period: int,
    generator_rows: list[dict],
    storage_rows: list[dict] | None = None,
) -> pypsa.Network:
    """Build a minimal multi-investment-period network and stamp p_nom_opt
    onto each generator row to mimic a solved-network state."""
    n = pypsa.Network()
    n.investment_periods = [period]
    n.snapshots = pd.MultiIndex.from_tuples(
        [(period, pd.Timestamp(f"{period}-07-01 00:00"))]
    )
    n.add("Bus", "node")
    n.add("Bus", "bus_for_custom_constraint_gens")
    for row in generator_rows:
        attrs = {k: v for k, v in row.items() if k not in ("p_nom_opt", "name")}
        n.add("Generator", row["name"], **attrs)
    n.generators["p_nom_opt"] = [
        r.get("p_nom_opt", r.get("p_nom", 0.0)) for r in generator_rows
    ]
    for row in storage_rows or []:
        attrs = {k: v for k, v in row.items() if k not in ("p_nom_opt", "name")}
        n.add("StorageUnit", row["name"], **attrs)
    if storage_rows:
        n.storage_units["p_nom_opt"] = [
            r.get("p_nom_opt", r.get("p_nom", 0.0)) for r in storage_rows
        ]
    return n


# ---------------------------------------------------------------------------
# Gotcha test: in-memory dict mutation, NOT CSV roundtrip
# ---------------------------------------------------------------------------


def test_inject_mutates_in_memory_dict_not_csv(tmp_path):
    """The persistence probe's first run no-op'd because it mutated the CSV
    only. build_pypsa_network reads the in-memory dict; the injection must
    leave the in-memory `pypsa_friendly["generators"]` DataFrame with the new
    row, regardless of what is on disk."""
    pypsa_friendly = {
        "generators": pd.DataFrame(
            {
                "name": ["live_extendable_2045"],
                "bus": ["CNSW"],
                "p_nom": [0.0],
                "p_nom_extendable": [True],
                "carrier": ["Gas"],
                "build_year": [2045],
                "lifetime": [30],
                "capital_cost": [200000.0],
            }
        ),
        "batteries": pd.DataFrame(
            columns=[
                "name",
                "bus",
                "p_nom",
                "p_nom_extendable",
                "carrier",
                "max_hours",
                "build_year",
                "lifetime",
                "capital_cost",
            ]
        ),
    }
    carried = {
        "generators": pd.DataFrame(
            {
                "name": ["ocgt_small_gt_nq_2030"],
                "bus": ["CNSW"],
                "p_nom": [500.0],
                "p_nom_extendable": [False],
                "carrier": ["Gas"],
                "build_year": [2030],
                "lifetime": [40],
                "capital_cost": [0.0],
            }
        ),
        "batteries": pd.DataFrame(),
    }

    diag = inject_carried_tranches(pypsa_friendly, carried)

    assert "ocgt_small_gt_nq_2030" in pypsa_friendly["generators"]["name"].values
    assert diag["carried_generators"] == 1
    assert diag["carried_generator_mw"] == 500.0
    carried_row = pypsa_friendly["generators"][
        pypsa_friendly["generators"]["name"] == "ocgt_small_gt_nq_2030"
    ].iloc[0]
    assert carried_row["p_nom"] == 500.0
    assert (
        carried_row["p_nom_extendable"] is False
        or carried_row["p_nom_extendable"] == False
    )  # noqa
    assert carried_row["capital_cost"] == 0.0


# ---------------------------------------------------------------------------
# Schema invariants: carried rows are ECAA-shaped
# ---------------------------------------------------------------------------


def test_extract_marks_carried_generator_as_fixed_capacity():
    """Carried rows must be `p_nom_extendable=False` with capital_cost=0 —
    the 2030-vintage paying 2030 capex once is the brownfield contract."""
    n = _make_solved_network_one_period(
        period=2030,
        generator_rows=[
            {
                "name": "ocgt_small_gt_nq_2030",
                "bus": "node",
                "carrier": "Gas",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 40,
                "capital_cost": 250000.0,
                "p_nom_opt": 720.0,
            }
        ],
    )

    pf_gens = pd.DataFrame(
        {
            "name": ["ocgt_small_gt_nq_2030"],
            "bus": ["node"],
            "carrier": ["Gas"],
            "p_nom": [0.0],
            "p_nom_extendable": [True],
            "build_year": [2030],
            "lifetime": [40.0],
            "capital_cost": [250000.0],
            "marginal_cost": ["ocgt_nq"],
            "isp_heat_rate_gj/mwh": [11.0],
            "isp_capture_rate": [0.0],
            "isp_residual_co2_t_per_mwh": [0.561],
            "isp_fuel_cost_mapping": ["NSW new OCGT"],
        }
    ).set_index("name")

    built = _extract_new_built_generators(n, pf_gens, year=2030)

    expected = pd.DataFrame(
        {
            "name": ["ocgt_small_gt_nq_2030"],
            "p_nom": [720.0],
            "p_nom_extendable": [False],
            "capital_cost": [0.0],
            "build_year": [2030],
            "lifetime": [40.0],
        }
    )
    pd.testing.assert_frame_equal(
        built[
            [
                "name",
                "p_nom",
                "p_nom_extendable",
                "capital_cost",
                "build_year",
                "lifetime",
            ]
        ].reset_index(drop=True),
        expected,
        check_dtype=False,
    )
    # Regression guard for the carry-forward attribute-drop class defect:
    # physically-meaningful isp_* attributes MUST survive on the carried row
    # (they feed the next solve's marginal cost and extraction's emissions/fuel).
    row = built.set_index("name").loc["ocgt_small_gt_nq_2030"]
    assert row["isp_heat_rate_gj/mwh"] == 11.0
    assert row["isp_residual_co2_t_per_mwh"] == 0.561
    assert row["isp_capture_rate"] == 0.0
    assert row["isp_fuel_cost_mapping"] == "NSW new OCGT"
    assert row["marginal_cost"] == "ocgt_nq"


def test_extract_drops_below_threshold_and_existing_fleet():
    """Only newly-extendable rows with non-trivial p_nom_opt carry forward.
    The pre-existing ECAA fleet (p_nom_extendable=False) must NOT be re-carried
    — that would double-count when next year's templater rebuilds it from
    IASR. Custom-constraint dummy bus is also excluded."""
    n = _make_solved_network_one_period(
        period=2030,
        generator_rows=[
            {
                "name": "ocgt_nq_2030",
                "bus": "node",
                "carrier": "Gas",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 40,
                "capital_cost": 250000.0,
                "p_nom_opt": 720.0,
            },
            {
                "name": "subthreshold_2030",
                "bus": "node",
                "carrier": "Gas",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 40,
                "capital_cost": 250000.0,
                "p_nom_opt": 0.5,
            },
            {
                "name": "Bayswater",
                "bus": "node",
                "carrier": "Black Coal",
                "p_nom": 2715.0,
                "p_nom_extendable": False,
                "build_year": 1985,
                "lifetime": 48,
                "capital_cost": 0.0,
                "p_nom_opt": 2715.0,
            },
            {
                "name": "dummy_in_cc_bus",
                "bus": "bus_for_custom_constraint_gens",
                "carrier": "Gas",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 40,
                "capital_cost": 0.0,
                "p_nom_opt": 9999.0,
            },
        ],
    )

    pf_gens = pd.DataFrame(
        {
            "name": ["ocgt_nq_2030", "subthreshold_2030", "Bayswater"],
            "carrier": ["Gas", "Gas", "Black Coal"],
            "p_nom": [0.0, 0.0, 2715.0],
            "p_nom_extendable": [True, True, False],
            "build_year": [2030, 2030, 1985],
            "lifetime": [40.0, 40.0, 48.0],
            "capital_cost": [250000.0, 250000.0, 0.0],
            "isp_heat_rate_gj/mwh": [11.0, 11.0, 9.4],
        }
    ).set_index("name")

    built = _extract_new_built_generators(n, pf_gens, year=2030)

    assert list(built["name"]) == ["ocgt_nq_2030"]


def test_extract_excludes_reducible_existing_fleet():
    """Regression for the retirement × carry-forward interaction.

    The original existing-fleet guard relied on the existing fleet being
    `p_nom_extendable=False`. The retirement seam (`make_existing_reducible`)
    breaks that premise: it turns each ECAA unit into a downward capacity
    decision — `p_nom_extendable=True` with `build_year` at the period. Such a
    retained existing unit then matches the extendable/build-year filter and,
    without the `existing_names` exclusion, was carried forward and collided
    with next period's re-templated ECAA row (the 2035 duplicate-index halt).
    Only the genuine new-entrant build may carry."""
    n = _make_solved_network_one_period(
        period=2030,
        generator_rows=[
            {
                "name": "ocgt_nq_2030",
                "bus": "node",
                "carrier": "Gas",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 40,
                "capital_cost": 250000.0,
                "p_nom_opt": 720.0,
            },
            # Reducible ECAA: made extendable by retirement, build_year==period,
            # retained at full capacity. Must NOT carry.
            {
                "name": "Gawara Baya Wind Farm",
                "bus": "node",
                "carrier": "Wind",
                "p_nom": 600.0,
                "p_nom_extendable": True,
                "build_year": 2030,
                "lifetime": 45,
                "capital_cost": 80000.0,
                "p_nom_opt": 600.0,
            },
        ],
    )

    pf_gens = pd.DataFrame(
        {
            "name": ["ocgt_nq_2030", "Gawara Baya Wind Farm"],
            "carrier": ["Gas", "Wind"],
            "p_nom": [0.0, 600.0],
            "p_nom_extendable": [True, True],
            "build_year": [2030, 2030],
            "lifetime": [40.0, 45.0],
            "capital_cost": [250000.0, 80000.0],
            "isp_heat_rate_gj/mwh": [11.0, 0.0],
        }
    ).set_index("name")

    built = _extract_new_built_generators(
        n,
        pf_gens,
        year=2030,
        existing_names={"Gawara Baya Wind Farm"},
    )

    assert list(built["name"]) == ["ocgt_nq_2030"]


def test_extract_storage_carries_capacity_not_soc():
    """Sharpening point #2: carried storage pins capacity (p_nom, max_hours)
    and preserves cyclic_state_of_charge=True. PyPSA's column for
    state_of_charge_initial is always present and refers to the INPUT seed
    (default 0); the cyclic flag is what guarantees the next year's solve
    re-derives SOC fresh per snapshot range, so the cross-year SOC trajectory
    is not preserved across milestone years."""
    n = _make_solved_network_one_period(
        period=2030,
        generator_rows=[],
        storage_rows=[
            {
                "name": "bess_4h_cnsw_2030",
                "bus": "node",
                "carrier": "Storage",
                "p_nom": 0.0,
                "p_nom_extendable": True,
                "max_hours": 4.0,
                "build_year": 2030,
                "lifetime": 15,
                "capital_cost": 400000.0,
                "cyclic_state_of_charge": True,
                "p_nom_opt": 300.0,
            }
        ],
    )

    pf_bats = pd.DataFrame(
        {
            "name": ["bess_4h_cnsw_2030"],
            "bus": ["node"],
            "carrier": ["Storage"],
            "p_nom": [0.0],
            "p_nom_extendable": [True],
            "max_hours": [4.0],
            "build_year": [2030],
            "lifetime": [15.0],
            "capital_cost": [400000.0],
            "cyclic_state_of_charge": [True],
        }
    ).set_index("name")

    built = _extract_new_built_storage_units(n, pf_bats, year=2030)

    assert len(built) == 1
    row = built.iloc[0]
    assert row["p_nom"] == 300.0
    assert row["max_hours"] == 4.0
    assert row["p_nom_extendable"] == False  # noqa
    assert row["cyclic_state_of_charge"] == True  # noqa
    # SOC is NOT carried: the pypsa_friendly battery row has no
    # state_of_charge_initial column, so the carried row doesn't propagate it —
    # year (t+1) seeds SOC fresh (PyPSA default 0) under cyclic_state_of_charge.
    # The prior solve's terminal SOC (network.storage_units_t.state_of_charge)
    # is intentionally not propagated.
    assert "state_of_charge_initial" not in built.columns


# ---------------------------------------------------------------------------
# Save / load roundtrip + retirement
# ---------------------------------------------------------------------------


def test_save_load_roundtrip_preserves_tranche(tmp_path):
    tranche = {
        "generators": pd.DataFrame(
            {
                "name": ["wind_high_cwo_2030"],
                "bus": ["CWO"],
                "carrier": ["Wind"],
                "p_nom": [400.0],
                "p_nom_extendable": [False],
                "build_year": [2030],
                "lifetime": [25],
                "capital_cost": [0.0],
            }
        ),
        "batteries": pd.DataFrame(
            {
                "name": ["bess_4h_cnsw_2030"],
                "bus": ["CNSW"],
                "carrier": ["Storage"],
                "p_nom": [200.0],
                "p_nom_extendable": [False],
                "max_hours": [4.0],
                "build_year": [2030],
                "lifetime": [15],
                "capital_cost": [0.0],
                "cyclic_state_of_charge": [True],
            }
        ),
    }

    save_tranche(tranche, tmp_path, year=2030)
    loaded = load_tranches(tmp_path, before_year=2035)

    pd.testing.assert_frame_equal(
        loaded["generators"].reset_index(drop=True),
        tranche["generators"].reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded["batteries"].reset_index(drop=True),
        tranche["batteries"].reset_index(drop=True),
        check_dtype=False,
    )


def test_load_tranches_retires_expired_vintages(tmp_path):
    """Retirement filter mirrors PyPSA's active_assets: a row survives iff
    build_year + lifetime > period. A 2030-built 15y battery is alive in
    2044 (30+15=45 > 44) but retired by 2045 (45 > 45 is False)."""
    short_lived = pd.DataFrame(
        {
            "name": ["bess_4h_2030"],
            "bus": ["CNSW"],
            "carrier": ["Storage"],
            "p_nom": [200.0],
            "p_nom_extendable": [False],
            "max_hours": [4.0],
            "build_year": [2030],
            "lifetime": [15],
            "capital_cost": [0.0],
        }
    )
    long_lived = pd.DataFrame(
        {
            "name": ["wind_high_cwo_2030"],
            "bus": ["CWO"],
            "carrier": ["Wind"],
            "p_nom": [400.0],
            "p_nom_extendable": [False],
            "build_year": [2030],
            "lifetime": [25],
            "capital_cost": [0.0],
        }
    )
    save_tranche(
        {"generators": long_lived, "batteries": short_lived},
        tmp_path,
        year=2030,
    )

    alive_2044 = load_tranches(tmp_path, before_year=2044)
    retired_2045 = load_tranches(tmp_path, before_year=2045)
    retired_2055 = load_tranches(tmp_path, before_year=2055)

    assert len(alive_2044["batteries"]) == 1
    assert len(alive_2044["generators"]) == 1
    assert len(retired_2045["batteries"]) == 0
    assert len(retired_2045["generators"]) == 1
    assert len(retired_2055["batteries"]) == 0
    assert len(retired_2055["generators"]) == 0


# ---------------------------------------------------------------------------
# Sharpening point #3: additive vintage accumulation across the chain
# ---------------------------------------------------------------------------


def test_load_tranches_accumulates_multiple_prior_vintages(tmp_path):
    """A 2040 solve must see BOTH a 2030-built and a 2035-built tranche, not
    just the most recent one. Same base technology built in two years yields
    two distinct carried rows that both persist subject to retirement."""
    save_tranche(
        {
            "generators": pd.DataFrame(
                {
                    "name": ["wind_high_cwo_2030"],
                    "bus": ["CWO"],
                    "carrier": ["Wind"],
                    "p_nom": [400.0],
                    "p_nom_extendable": [False],
                    "build_year": [2030],
                    "lifetime": [25],
                    "capital_cost": [0.0],
                }
            ),
            "batteries": pd.DataFrame(),
        },
        tmp_path,
        year=2030,
    )
    save_tranche(
        {
            "generators": pd.DataFrame(
                {
                    "name": ["wind_high_cwo_2035"],
                    "bus": ["CWO"],
                    "carrier": ["Wind"],
                    "p_nom": [350.0],
                    "p_nom_extendable": [False],
                    "build_year": [2035],
                    "lifetime": [25],
                    "capital_cost": [0.0],
                }
            ),
            "batteries": pd.DataFrame(),
        },
        tmp_path,
        year=2035,
    )

    loaded = load_tranches(tmp_path, before_year=2040)

    assert sorted(loaded["generators"]["name"].tolist()) == [
        "wind_high_cwo_2030",
        "wind_high_cwo_2035",
    ]
    assert loaded["generators"]["p_nom"].sum() == 750.0


def test_load_tranches_excludes_current_year_and_future(tmp_path):
    """A year-T solve must not inherit its own previously-attempted output —
    only strictly-prior tranches."""
    save_tranche(
        {
            "generators": pd.DataFrame(
                {
                    "name": ["ocgt_2040"],
                    "bus": ["x"],
                    "carrier": ["Gas"],
                    "p_nom": [100.0],
                    "p_nom_extendable": [False],
                    "build_year": [2040],
                    "lifetime": [40],
                    "capital_cost": [0.0],
                }
            ),
            "batteries": pd.DataFrame(),
        },
        tmp_path,
        year=2040,
    )
    save_tranche(
        {
            "generators": pd.DataFrame(
                {
                    "name": ["ocgt_2045"],
                    "bus": ["x"],
                    "carrier": ["Gas"],
                    "p_nom": [200.0],
                    "p_nom_extendable": [False],
                    "build_year": [2045],
                    "lifetime": [40],
                    "capital_cost": [0.0],
                }
            ),
            "batteries": pd.DataFrame(),
        },
        tmp_path,
        year=2045,
    )

    loaded = load_tranches(tmp_path, before_year=2040)

    assert loaded["generators"].empty


def test_load_tranches_returns_empty_when_no_priors(tmp_path):
    """First year of a chain has no prior tranches; load returns empty
    DataFrames so inject_carried_tranches no-ops cleanly."""
    loaded = load_tranches(tmp_path, before_year=2030)

    assert loaded["generators"].empty
    assert loaded["batteries"].empty


# ---------------------------------------------------------------------------
# Capacity cap/floor cumulative-scope under carry-forward (the biomass-class fix)
#
# These guard the CUMULATIVE behaviour — carried + new ≤ cap_year — NOT the
# per-period behaviour the bug passed (each period ≤ its own cap while the
# carried-forward fleet blew past the ceiling).
# ---------------------------------------------------------------------------


def _caps_pypsa_friendly(
    generators: pd.DataFrame, lhs: pd.DataFrame, rhs: pd.DataFrame
) -> dict:
    empty_bat = pd.DataFrame(
        columns=["name", "p_nom", "p_nom_extendable", "build_year"]
    )
    return {
        "generators": generators,
        "batteries": empty_bat,
        "custom_constraints_lhs": lhs,
        "custom_constraints_rhs": rhs,
    }


def test_capacity_cap_nets_carried_so_cumulative_bound_holds():
    """A cap listing only the current vintage (biomass_*_2050) must have its RHS
    reduced by the carried earlier vintages (biomass_*_2030/2035) so the CUMULATIVE
    fleet is bounded. The buggy per-period version left RHS at 5000, letting
    carried (3000) + new (≤5000) reach 8000 > the 5000 ceiling — this guards it."""
    pf = _caps_pypsa_friendly(
        pd.DataFrame(
            {
                "name": [
                    "biomass_nq_2050",
                    "biomass_cq_2050",  # current new (extendable)
                    "biomass_nq_2030",
                    "biomass_cq_2035",
                ],  # carried (fixed)
                "p_nom": [0.0, 0.0, 1800.0, 1200.0],
                "p_nom_extendable": [True, True, False, False],
                "build_year": [2050, 2050, 2030, 2035],
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2050", "biomass_cap_2050"],
                "variable_name": ["biomass_nq_2050", "biomass_cq_2050"],
                "coefficient": [1.0, 1.0],
                "component": ["Generator", "Generator"],
                "attribute": ["p_nom", "p_nom"],
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2050"],
                "constraint_type": ["<="],
                "rhs": [5000.0],
            }
        ),
    )

    adjustments = adjust_capacity_caps_for_carried(pf, current_year=2050)

    rhs = pf["custom_constraints_rhs"]
    adjusted = float(
        rhs.loc[rhs["constraint_name"] == "biomass_cap_2050", "rhs"].iloc[0]
    )
    assert (
        adjusted == 2000.0
    )  # 5000 ceiling − 3000 carried → new head-room; cumulative ≤ 5000
    assert adjustments == {"biomass_cap_2050": 3000.0}


def test_only_capacity_constraints_adjusted_and_matched_by_base_name():
    """Output/flow security constraints (attribute 'p') are NOT adjusted — only
    capacity (p_nom/e_nom) caps — and carried capacity is matched to a cap by
    base-name, so a carried generator of an unrelated technology is never netted."""
    pf = _caps_pypsa_friendly(
        pd.DataFrame(
            {
                "name": ["biomass_nq_2050", "biomass_nq_2030", "wind_n3_2030"],
                "p_nom": [0.0, 1000.0, 2000.0],
                "p_nom_extendable": [True, False, False],
                "build_year": [2050, 2030, 2030],
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2050", "CNSW1", "CNSW1"],
                "variable_name": ["biomass_nq_2050", "Bodangora Wind Farm", "DN1-CNSW"],
                "coefficient": [1.0, 1.0, 1.0],
                "component": ["Generator", "Generator", "Link"],
                "attribute": [
                    "p_nom",
                    "p",
                    "p",
                ],  # CNSW1 sums OUTPUT/flow, not capacity
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2050", "CNSW1"],
                "constraint_type": ["<=", "<="],
                "rhs": [5000.0, 1800.0],
            }
        ),
    )

    adjustments = adjust_capacity_caps_for_carried(pf, current_year=2050)

    rhs = pf["custom_constraints_rhs"].set_index("constraint_name")["rhs"]
    assert (
        rhs["biomass_cap_2050"] == 4000.0
    )  # netted by carried biomass (1000), NOT carried wind
    assert rhs["CNSW1"] == 1800.0  # output/flow security constraint untouched
    assert adjustments == {"biomass_cap_2050": 1000.0}


def test_no_carried_first_year_leaves_caps_unchanged():
    """First chain year: no carried tranches, so the per-period build IS the
    cumulative fleet and every cap RHS is unchanged."""
    pf = _caps_pypsa_friendly(
        pd.DataFrame(
            {
                "name": ["biomass_nq_2030"],
                "p_nom": [0.0],
                "p_nom_extendable": [True],
                "build_year": [2030],
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2030"],
                "variable_name": ["biomass_nq_2030"],
                "coefficient": [1.0],
                "component": ["Generator"],
                "attribute": ["p_nom"],
            }
        ),
        pd.DataFrame(
            {
                "constraint_name": ["biomass_cap_2030"],
                "constraint_type": ["<="],
                "rhs": [1500.0],
            }
        ),
    )

    adjustments = adjust_capacity_caps_for_carried(pf, current_year=2030)

    assert float(pf["custom_constraints_rhs"]["rhs"].iloc[0]) == 1500.0
    assert adjustments == {}
