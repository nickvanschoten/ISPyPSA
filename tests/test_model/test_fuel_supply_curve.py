import numpy as np
import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.fuel_supply_curve import _add_fuel_supply_curve

# 100 MW served all year by a 10 GJ/MWh generator burns
# 100 * 8760 * 10 = 8.76e6 GJ = 8.76 PJ a year.
_LOAD_MW = 100.0
_HEAT_RATE_GJ_PER_MWH = 10.0
_ANNUAL_BURN_PJ = 8.76
_MARGINAL_COST = 50.0


def _build_fuel_network(carrier="Gas", periods=(2025,)):
    """Minimal solvable network: one bus, a flat load, one fuelled generator per
    period setup, four snapshots per period each weighted to a quarter of the year."""
    snapshots = pd.MultiIndex.from_tuples(
        [
            (period, timestamp)
            for period in periods
            for timestamp in pd.date_range(f"{period}-01-01", periods=4, freq="h")
        ],
        names=["period", "timestep"],
    )
    network = pypsa.Network(snapshots=snapshots, investment_periods=list(periods))
    weightings = pd.DataFrame(
        {"objective": 8760 / 4, "generators": 8760 / 4, "stores": 1.0},
        index=snapshots,
    )
    network.snapshot_weightings = weightings
    network.add("Bus", "bus")
    network.add("Carrier", carrier)
    network.add("Load", "load", bus="bus", p_set=_LOAD_MW)
    network.add(
        "Generator",
        f"{carrier.lower()}_gen",
        bus="bus",
        carrier=carrier,
        p_nom=200.0,
        marginal_cost=_MARGINAL_COST,
    )
    return network


def _generators_table(carrier="Gas"):
    return pd.DataFrame(
        {
            "name": [f"{carrier.lower()}_gen"],
            "carrier": [carrier],
            "isp_heat_rate_gj/mwh": [_HEAT_RATE_GJ_PER_MWH],
        }
    )


def _curve(periods, cap_pj_by_period, adder=2.0):
    rows = []
    for period in periods:
        rows.append(
            {
                "investment_period": period,
                "tranche": "first_tranche",
                "cap_pj": cap_pj_by_period[period],
                "adder_$/gj": 0.0,
            }
        )
        rows.append(
            {
                "investment_period": period,
                "tranche": "backstop",
                "cap_pj": np.nan,
                "adder_$/gj": adder,
            }
        )
    return pd.DataFrame(rows)


def _solve(network):
    network.optimize.solve_model(solver_name="highs")


def _tranche_solution_pj(network, period, carrier="Gas"):
    solution = network.model.variables[
        f"{carrier.lower()}_supply_purchases_tj_{period}"
    ].solution
    tranche_dim = f"{carrier.lower()}_tranche"
    return {
        str(tranche): float(solution.sel({tranche_dim: tranche})) / 1.0e3
        for tranche in solution[tranche_dim].values
    }


@pytest.mark.parametrize("carrier", ["Gas", "Biomass"])
def test_burn_beyond_first_tranche_pays_backstop_premium(carrier):
    network = _build_fuel_network(carrier)
    network.optimize.create_model(multi_investment_periods=True)

    _add_fuel_supply_curve(
        network, _curve([2025], {2025: 5.0}), _generators_table(carrier), carrier
    )
    _solve(network)

    fills = _tranche_solution_pj(network, 2025, carrier)
    assert fills["first_tranche"] == pytest.approx(5.0, rel=1e-6)
    assert fills["backstop"] == pytest.approx(_ANNUAL_BURN_PJ - 5.0, rel=1e-6)

    dispatch_cost = _MARGINAL_COST * _LOAD_MW * 8760
    premium_cost = (_ANNUAL_BURN_PJ - 5.0) * 1.0e6 * 2.0
    assert network.objective == pytest.approx(dispatch_cost + premium_cost, rel=1e-6)


@pytest.mark.parametrize("carrier", ["Gas", "Biomass"])
def test_burn_within_first_tranche_pays_no_premium(carrier):
    network = _build_fuel_network(carrier)
    network.optimize.create_model(multi_investment_periods=True)

    _add_fuel_supply_curve(
        network, _curve([2025], {2025: 20.0}), _generators_table(carrier), carrier
    )
    _solve(network)

    fills = _tranche_solution_pj(network, 2025, carrier)
    assert fills["backstop"] == pytest.approx(0.0, abs=1e-9)
    assert network.objective == pytest.approx(
        _MARGINAL_COST * _LOAD_MW * 8760, rel=1e-6
    )


def test_gas_and_biomass_curves_meter_their_own_carriers_independently():
    network = _build_fuel_network("Gas")
    network.add("Carrier", "Biomass")
    network.add(
        "Generator",
        "biomass_gen",
        bus="bus",
        carrier="Biomass",
        p_nom=60.0,
        marginal_cost=_MARGINAL_COST - 10.0,
    )
    network.optimize.create_model(multi_investment_periods=True)
    generators = pd.DataFrame(
        {
            "name": ["gas_gen", "biomass_gen"],
            "carrier": ["Gas", "Biomass"],
            "isp_heat_rate_gj/mwh": [_HEAT_RATE_GJ_PER_MWH, _HEAT_RATE_GJ_PER_MWH],
        }
    )

    _add_fuel_supply_curve(
        network, _curve([2025], {2025: 2.0}, adder=2.0), generators, "Gas"
    )
    _add_fuel_supply_curve(
        network, _curve([2025], {2025: 3.0}, adder=4.0), generators, "Biomass"
    )
    _solve(network)

    # Marginal costs with premiums at heat rate 10: biomass $40 in-tranche /
    # $80 on its backstop; gas $50 in-tranche / $70 on its backstop. So the
    # LP runs biomass only to its 3.0 PJ cheap tranche (300 GWh = 34.25 MW
    # average) and gas serves the rest (576 GWh = 5.76 PJ burn) — the curve
    # premiums feed back into the dispatch merit order per carrier.
    biomass_energy_mwh = 3.0e6 / _HEAT_RATE_GJ_PER_MWH
    gas_energy_mwh = _LOAD_MW * 8760 - biomass_energy_mwh
    gas_burn_pj = gas_energy_mwh * _HEAT_RATE_GJ_PER_MWH / 1.0e6
    gas_fills = _tranche_solution_pj(network, 2025, "Gas")
    biomass_fills = _tranche_solution_pj(network, 2025, "Biomass")
    assert gas_fills["first_tranche"] == pytest.approx(2.0, rel=1e-6)
    assert gas_fills["backstop"] == pytest.approx(gas_burn_pj - 2.0, rel=1e-6)
    assert biomass_fills["first_tranche"] == pytest.approx(3.0, rel=1e-6)
    assert biomass_fills["backstop"] == pytest.approx(0.0, abs=1e-9)

    dispatch_cost = (
        _MARGINAL_COST - 10.0
    ) * biomass_energy_mwh + _MARGINAL_COST * gas_energy_mwh
    premium_cost = (gas_burn_pj - 2.0) * 1.0e6 * 2.0
    assert network.objective == pytest.approx(dispatch_cost + premium_cost, rel=1e-6)


def test_annual_budgets_reset_each_investment_period():
    network = _build_fuel_network(periods=(2025, 2026))
    network.optimize.create_model(multi_investment_periods=True)

    _add_fuel_supply_curve(
        network,
        _curve([2025, 2026], {2025: 5.0, 2026: 8.0}),
        _generators_table(),
        "Gas",
    )
    _solve(network)

    fills_2025 = _tranche_solution_pj(network, 2025)
    fills_2026 = _tranche_solution_pj(network, 2026)
    assert fills_2025["first_tranche"] == pytest.approx(5.0, rel=1e-6)
    assert fills_2025["backstop"] == pytest.approx(3.76, rel=1e-6)
    assert fills_2026["first_tranche"] == pytest.approx(8.0, rel=1e-6)
    assert fills_2026["backstop"] == pytest.approx(0.76, rel=1e-6)


def test_premium_scales_with_investment_period_objective_weight():
    baseline = _build_fuel_network()
    baseline.optimize.create_model(multi_investment_periods=True)
    weighted = _build_fuel_network()
    weighted.investment_period_weightings["objective"] = 5.0
    weighted.optimize.create_model(multi_investment_periods=True)

    _add_fuel_supply_curve(
        weighted, _curve([2025], {2025: 5.0}), _generators_table(), "Gas"
    )
    _solve(baseline)
    _solve(weighted)

    premium_cost = (_ANNUAL_BURN_PJ - 5.0) * 1.0e6 * 2.0
    assert weighted.objective == pytest.approx(
        5.0 * baseline.objective + 5.0 * premium_cost, rel=1e-6
    )


@pytest.mark.parametrize("carrier", ["Gas", "Biomass"])
def test_no_fuel_generators_logs_warning_and_adds_nothing(carrier, caplog):
    network = _build_fuel_network(carrier)
    network.optimize.create_model(multi_investment_periods=True)
    generators = pd.DataFrame(
        {
            "name": [f"{carrier.lower()}_gen"],
            "carrier": ["Solar"],
            "isp_heat_rate_gj/mwh": [np.nan],
        }
    )

    with caplog.at_level("WARNING"):
        _add_fuel_supply_curve(
            network, _curve([2025], {2025: 5.0}), generators, carrier
        )

    assert (
        f"{carrier} supply curve configured but the network has no "
        f"{carrier.lower()} generators, so no {carrier.lower()} supply curve "
        f"constraints were added."
    ) in caplog.text
    assert f"{carrier.lower()}_supply_purchases_tj_2025" not in network.model.variables


def test_missing_heat_rate_with_no_peer_raises():
    network = _build_fuel_network()
    network.optimize.create_model(multi_investment_periods=True)
    generators = _generators_table()
    generators["isp_heat_rate_gj/mwh"] = np.nan
    generators["isp_technology_type"] = "OCGT (small GT)"

    with pytest.raises(
        ValueError, match=r"no same-technology peer to fill from.*\['gas_gen'\]"
    ):
        _add_fuel_supply_curve(network, _curve([2025], {2025: 5.0}), generators, "Gas")


def test_missing_heat_rate_filled_from_technology_median(caplog):
    network = _build_fuel_network()
    # Second gas generator with the missing heat rate; same technology type as
    # gas_gen so its heat rate is the median donor.
    network.add(
        "Generator",
        "gas_gen_no_heat_rate",
        bus="bus",
        carrier="Gas",
        p_nom=200.0,
        marginal_cost=_MARGINAL_COST + 1.0,
    )
    network.optimize.create_model(multi_investment_periods=True)
    generators = pd.DataFrame(
        {
            "name": ["gas_gen", "gas_gen_no_heat_rate"],
            "carrier": ["Gas", "Gas"],
            "isp_heat_rate_gj/mwh": [_HEAT_RATE_GJ_PER_MWH, np.nan],
            "isp_technology_type": ["OCGT (small GT)", "OCGT (small GT)"],
        }
    )

    with caplog.at_level("WARNING"):
        _add_fuel_supply_curve(network, _curve([2025], {2025: 5.0}), generators, "Gas")
    _solve(network)

    assert (
        "Gas generators missing heat rates in the IASR data; metering their gas "
        "supply curve fuel use at their technology-type median heat rate: "
        "['gas_gen_no_heat_rate']"
    ) in caplog.text
    # The cheaper generator serves the whole load, so total burn and premium are
    # unchanged by the second unit; the constraint still binds through it.
    fills = _tranche_solution_pj(network, 2025)
    assert fills["backstop"] == pytest.approx(_ANNUAL_BURN_PJ - 5.0, rel=1e-6)
