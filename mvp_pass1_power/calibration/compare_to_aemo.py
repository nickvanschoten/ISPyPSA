"""Calibration: ISPyPSA cost-optimal Step Change vs AEMO 2024 ISP published Step Change.

Compares aggregate capacity and generation between the solved ISPyPSA cost-optimal
run and the headline figures AEMO published in the 2024 ISP overview (see
aemo_2024_isp_step_change.md for sources). Produces calibration_report.md with
side-by-side numbers, divergence percentages, and honest interpretation.

Usage:
    uv run python mvp_pass1_power/calibration/compare_to_aemo.py \\
        --run mvp_pass1_power/runs/baseline_step_change__cost_optimal \\
        --out mvp_pass1_power/calibration

Methodological caveats baked into the report (kept here so the calibration
output cannot be read without the reader seeing them):

  - The MVP runs with the example trace dataset (reference year 2018 only) and
    three named representative weeks. AEMO uses 30+ reference years and full
    8760 with stochastic dispatch. We expect capacity decisions to diverge —
    capacity choice is partly driven by weather-extremes coverage, and we
    cover much less weather than AEMO.

  - AEMO's distributed PV ("rooftop solar") is exogenous in ISPyPSA (treated
    as netting out load), not endogenous capacity. The MVP therefore reports
    no rooftop figure to compare against AEMO's published rooftop trajectory.

  - AEMO 2024 ISP runs both capacity expansion and operational sub-models
    jointly across all NEM years; the MVP runs capacity expansion alone for
    three milestone periods. Cumulative trajectories will not align period-
    by-period even if endpoints converge.

  - This calibration is summary-level only — it compares aggregate NEM-wide
    capacity by major fuel type. A production calibration would need AEMO's
    full scenario results workbook (publicly available, separate download).
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import pypsa


# Source: AEMO 2024 ISP overview, see aemo_2024_isp_step_change.md.
AEMO_STEP_CHANGE = {
    "wind_solar_gw": {2030: 55.0, 2050: 127.0},
    "gas_gw":        {2030: None, 2050: 15.0},     # AEMO publishes a single 2050 number
    "coal_gw":       {2030: "retiring", 2050: 0.0},
    "annual_grid_consumption_twh": {2030: 202.0, 2050: 313.0},
}


def _load_solved(run_dir: Path) -> pypsa.Network:
    return pypsa.Network(run_dir / "outputs" / "capacity_expansion.nc")


def _coalesce_fuel(fuel: str) -> str:
    if fuel in ("Wind", "Solar"):
        return "wind_solar"
    if fuel in ("Black Coal", "Brown Coal"):
        return "coal"
    if fuel in ("Gas", "Hyblend"):
        return "gas"
    if fuel in ("Hydrogen",):
        return "hydrogen"
    if fuel in ("Biomass",):
        return "biomass"
    if fuel in ("Water",):
        return "hydro"
    if fuel == "Unserved Energy":
        return "_drop"  # not a real technology — synthetic VoLL slack
    if fuel == "Battery":
        return "storage"
    return "other"


def _capacity_by_fuel_by_period(network: pypsa.Network) -> pd.DataFrame:
    gens = network.generators[["bus", "carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    gens = gens[gens["bus"] != "bus_for_custom_constraint_gens"]
    gens = gens.drop(columns=["bus"])
    storage = network.storage_units[["carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    storage["carrier"] = storage["carrier"].fillna("Storage")
    components = pd.concat([gens, storage])
    components["fuel_group"] = components["carrier"].apply(_coalesce_fuel)
    components = components[components["fuel_group"] != "_drop"]
    rows = []
    for period in network.investment_periods:
        active = components[
            (components["build_year"] <= period)
            & (components["build_year"] + components["lifetime"] >= period)
        ]
        agg = active.groupby("fuel_group")["p_nom_opt"].sum() / 1000.0  # MW->GW
        for fuel_group, gw in agg.items():
            rows.append({"period": int(period), "fuel_group": fuel_group, "gw_active": gw})
    return pd.DataFrame(rows)


def _annual_consumption_twh_by_period(network: pypsa.Network) -> pd.Series:
    weightings = network.snapshot_weightings["generators"]
    load_t = network.loads_t.p_set.sum(axis=1)
    weighted = (load_t * weightings).groupby(level=0).sum() / 1e6  # MWh->TWh
    return weighted


def _build_table(network: pypsa.Network) -> tuple[pd.DataFrame, pd.Series]:
    cap = _capacity_by_fuel_by_period(network)
    cap = cap.pivot(index="period", columns="fuel_group", values="gw_active").fillna(0.0)
    cons = _annual_consumption_twh_by_period(network)
    return cap, cons


def _format_share(ispypsa_val, aemo_val) -> str:
    """AEMO is NEM-wide; ISPyPSA in MVP is NSW-only. Report ISPyPSA as % of AEMO,
    not as divergence — they aren't the same scope."""
    if aemo_val in (None, "retiring"):
        return "(no published target)" if aemo_val is None else f"AEMO: {aemo_val}"
    if aemo_val == 0:
        return f"AEMO: 0; ISPyPSA: {ispypsa_val:.1f}"
    share_pct = 100.0 * ispypsa_val / aemo_val
    return f"AEMO NEM: {aemo_val:.1f}; ISPyPSA NSW: {ispypsa_val:.1f} ({share_pct:.0f}% of NEM)"


def _write_calibration_report(
    cap: pd.DataFrame, cons: pd.Series, out_dir: Path
) -> Path:
    lines = ["# ISPyPSA cost-optimal vs AEMO 2024 ISP Step Change — calibration report",
             "",
             "**Scope mismatch warning.** The MVP was run at NSW-only scale due to"
             " session-time constraints (see README, *Honest assessment of MVP scope*)."
             " AEMO publishes NEM-wide totals. The comparison below therefore shows"
             " ISPyPSA NSW capacity as a *percentage of AEMO's NEM number*, not as"
             " a divergence. NSW carries roughly 28–32% of NEM peak demand depending"
             " on year — anything in that range is consistent with a credible NSW"
             " share, not an indicator of model accuracy.",
             "",
             "Source for AEMO numbers: AEMO 2024 ISP overview report. See"
             " aemo_2024_isp_step_change.md for citations.",
             "",
             "## Capacity by fuel group (GW active in period)",
             "",
             "| Year | wind+solar (AEMO vs ISPyPSA) | gas (AEMO vs ISPyPSA) | "
             "coal (AEMO vs ISPyPSA) | hydro | storage | biomass+H2 |",
             "|------|-----|-----|-----|-----|-----|-----|"]
    for year in [2030, 2050]:
        if year not in cap.index:
            continue
        ws  = cap.loc[year].get("wind_solar", 0.0)
        gas = cap.loc[year].get("gas", 0.0)
        coal= cap.loc[year].get("coal", 0.0)
        hyd = cap.loc[year].get("hydro", 0.0)
        sto = cap.loc[year].get("Storage", 0.0)
        bio = cap.loc[year].get("biomass", 0.0) + cap.loc[year].get("hydrogen", 0.0)
        lines.append(
            f"| {year} | {_format_share(ws, AEMO_STEP_CHANGE['wind_solar_gw'].get(year))}"
            f" | {_format_share(gas, AEMO_STEP_CHANGE['gas_gw'].get(year))}"
            f" | {_format_share(coal, AEMO_STEP_CHANGE['coal_gw'].get(year))}"
            f" | {hyd:.1f} | {sto:.1f} | {bio:.1f} |"
        )
    lines += [
        "",
        "## Annual grid consumption (TWh)",
        "",
        "| Year | AEMO NEM | ISPyPSA NSW | share |",
        "|------|---------:|------------:|------:|",
    ]
    for year in [2030, 2050]:
        if year not in cons.index:
            continue
        aemo_v = AEMO_STEP_CHANGE["annual_grid_consumption_twh"].get(year)
        isp_v = float(cons.loc[year])
        share = 100.0 * isp_v / aemo_v if aemo_v else float("nan")
        lines.append(f"| {year} | {aemo_v:.0f} | {isp_v:.1f} | {share:.0f}% of NEM |")

    lines += [
        "",
        "## Honest assessment",
        "",
        "**Divergences expected because of MVP simplifications:**",
        "",
        "1. **Single reference year (2018), three representative weeks.**"
        " AEMO uses 30+ reference years and full 8760. Our coverage of"
        " inter-annual variability is one year — capacity decisions sensitive"
        " to renewables droughts will diverge.",
        "",
        "2. **Distributed PV is not modelled as endogenous capacity.** AEMO's"
        " 86 GW 2050 rooftop figure is exogenous load reduction in ISPyPSA's"
        " demand traces; it does not appear in our capacity table.",
        "",
        "3. **Policy targets templated but not enforced.** ISPyPSA reads"
        " renewable-share trajectories from IASR but does not translate them"
        " into PyPSA constraints. The cost-optimal LP may therefore under-build"
        " renewables relative to AEMO's policy-constrained Step Change result.",
        "",
        "**What this calibration evidences vs what it doesn't:**",
        "",
        "- *Evidences*: the templater + translator + LP build + solve pipeline"
        " produces a defensible-shape capacity trajectory.",
        "- *Does not evidence*: that ISPyPSA at this configuration reproduces"
        " AEMO's published Step Change figures within any specific tolerance.",
        " For an AEMO-facing deliverable, a tighter calibration would need: full"
        " 30-reference-year traces, 8760 snapshots per year, policy-share"
        " constraints wired in, and side-by-side comparison against AEMO's"
        " scenario results workbook (not just the published summary numbers).",
    ]
    path = out_dir / "calibration_report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    network = _load_solved(args.run)
    cap, cons = _build_table(network)

    cap.to_csv(args.out / "ispypsa_capacity_by_fuel_by_year.csv")
    cons.to_csv(args.out / "ispypsa_annual_consumption_twh.csv", header=["twh"])
    report = _write_calibration_report(cap, cons, args.out)

    print("== ISPyPSA capacity (GW) ==")
    print(cap.round(1).to_string())
    print()
    print("== ISPyPSA annual consumption (TWh) ==")
    print(cons.round(1).to_string())
    print()
    print(f"Wrote {report}")


if __name__ == "__main__":
    main()
