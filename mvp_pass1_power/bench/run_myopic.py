"""Myopic period-decomposition driver for ISPyPSA.

Runs N sequential single-period ISPyPSA capacity-expansion solves. Two modes:

  default (independent-static): each year is a greenfield-on-IASR-baseline
  solve with no memory of prior years. Cheap and parallelisable, but the
  greenfield trajectories are not realisable (persistence probe established
  ~17% emissions / -7.7 pp renewable-share shift when 2040 capacity is
  forced into 2045).

  --recursive-dynamic: each year's new-build tranche is extracted after the
  solve and accumulated into a per-chain tranche directory. The next year's
  solve loads all surviving prior tranches and threads them into the
  in-memory pypsa_friendly dict between translation and timeseries
  generation (recursive_dynamic.inject_carried_tranches). The chain
  accumulates vintages — a 2050 solve carries 2030's + 2035's + 2040's +
  2045's surviving stock, each named after the originating new-entrant row
  so the same base tech built in different years yields distinct carried
  rows. Retirement is enforced by PyPSA's active-assets check
  (build_year + lifetime > period). See recursive_dynamic.py for the three
  correctness traps the design clears.

Per-period the script builds an ISPyPSA workflow at year T as the single
investment period, optionally injects carried tranches (if
--recursive-dynamic), solves, extracts dispatch + capacity + (in RD mode)
the newly-built tranche, then moves to the next period.

Usage:
    uv run python mvp_pass1_power/bench/run_myopic.py \\
        --run-id nsw_6p_myopic \\
        --filter NSW \\
        --periods 2025 2030 2035 2040 2045 2050 \\
        --archetype cost_optimal \\
        --budget-min 600 \\
        --recursive-dynamic
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import psutil

# Add the project root to sys.path so `from mvp_pass1_power.bench.recursive_dynamic
# import ...` resolves at runtime. `uv run python mvp_pass1_power/bench/run_myopic.py`
# only puts the script's directory (mvp_pass1_power/bench/) on sys.path; without this
# the recursive-dynamic tranche extraction silently fails with ImportError caught by
# the try/except below, and the chain proceeds as a no-op carrying nothing forward.
# instrumented_runner.py:30 and probe_persistence.py:43 use the same idiom.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BENCH = Path(__file__).parent
LOGS = BENCH / "logs"
RECORDS = BENCH / "records"
CONFIG_TEMPLATE_DIR = BENCH / "configs"


def _write_period_config(run_id: str, year: int, regions: list[str] | None,
                         rep_weeks: list[int] | None = None,
                         full_year: bool = False,
                         resolution_min: int = 30,
                         carbon_price: float = 0.0,
                         tns_price: float = 0.0,
                         parsed_traces_directory: str = "mvp_pass1_power/data/traces",
                         dataset_year: int = 2024) -> Path:
    """Synthesise a single-period config for this milestone year.

    Callers pass `run_id` already containing the year suffix (sub_run_id from
    main()). We do NOT append the year again — earlier versions did, producing
    paths like `..._gas_fleet_maintained_2025_2025__gas_fleet_maintained/...`
    which exceeded Windows' 260-char MAX_PATH on the longer archetype names
    (gas_fleet_maintained / rapid_coal_phaseout) when written under
    `pypsa_friendly/capacity_expansion_timeseries/marginal_cost_timeseries/`
    with the longest generator parquet name in the cache
    (`morgan_to_whyalla_pipeline_no_1_ps_and_water_filtration_plant.parquet`,
    66 chars).
    """
    cfg_dir = BENCH / "configs_myopic"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / f"{run_id}.yaml"
    filter_line = f"filter_by_nem_regions: {regions}\n" if regions else "# Full NEM\n"
    # Vintage wiring: dataset_year 2026 -> Draft 2026 ISP economics (v7.5 cache +
    # the Draft 2026 workbook + the 7.5/ manual tables); otherwise 2025 IASR
    # (v7.4). Keyed on dataset_year so the trace vintage and economics vintage
    # stay coherent.
    if dataset_year == 2026:
        workbook_path = "iasr inputs/Draft 2026 ISP Inputs and Assumptions workbook.xlsx"
        workbook_cache = "mvp_pass1_power/data/workbook_cache_v75"
        iasr_version = "7.5"
    else:
        workbook_path = "iasr inputs/2025-inputs-and-assumptions-workbook.xlsm"
        workbook_cache = "mvp_pass1_power/data/workbook_cache"
        iasr_version = "7.4"
    cfg_text = f"""# Auto-generated myopic config for {run_id} year {year}
paths:
  run_directory: "mvp_pass1_power/bench/runs_myopic"
  ispypsa_run_name: {run_id}
  parsed_traces_directory: "{parsed_traces_directory}"
  workbook_path: "{workbook_path}"
  parsed_workbook_cache: "{workbook_cache}"
trace_data:
  dataset_type: example
  dataset_year: {dataset_year}
iasr_workbook_version: "{iasr_version}"
scenario: Step Change
wacc: 0.07
discount_rate: 0.05
unserved_energy:
  cost: 10000.0
  max_per_node: 100000.0
{filter_line}network:
  transmission_expansion: True
  rez_transmission_expansion: True
  annuitisation_lifetime: 30
  nodes:
    regional_granularity: sub_regions
    rezs: discrete_nodes
  rez_to_sub_region_transmission_default_limit: 1e5
temporal:
  year_type: fy
  range:
    start_year: {year}
    end_year: {year}
  capacity_expansion:
    resolution_min: {resolution_min}
    reference_year_cycle: [2018]
    investment_periods: [{year}]
    aggregation:
      # Phase 7.2 (revised): 3-week sampling. Reduced from 4 to 3 weeks
      # after the 4-week smoke landed PDLP duality gap at 1.5e-3
      # (asymptoting above the 1e-3 tolerance target). 3-week LP is
      # ~25 % smaller in nonzeros, which should improve PDLP gap
      # convergence while still providing seasonal coverage. Off-peak
      # week dropped as least informative (largely a scaled-down
      # shoulder).
      #
      # Selection:
      #   peak winter (named residual-peak-demand): mid-June for 2018
      #     reference data — heating-driven evening residual peak, low
      #     solar resource. Team's existing single-rep-week pattern.
      #   peak summer (named peak-demand): data-driven highest
      #     instantaneous demand — Australia's summer afternoon cooling
      #     peak with high solar resource.
      #   spring shoulder (numbered week 42): mid-October — rising
      #     solar resource, moderate demand. Captures the VRE-favouring
      #     economic regime that single-rep-week sampling misses.
      representative_weeks: {"~" if full_year else (rep_weeks if rep_weeks is not None else [42])}
      named_representative_weeks: {"~" if full_year else "[residual-peak-demand, peak-demand]"}
  operational:
    resolution_min: 30
    reference_year_cycle: [2018]
    horizon: 336
    overlap: 48
    aggregation:
      representative_weeks: ~
      named_representative_weeks: [residual-peak-demand]
solver: highs
create_plots: False
carbon_pricing:
  carbon_price: {carbon_price}
  tns_price: {tns_price}
"""
    cfg_path.write_text(cfg_text)
    return cfg_path


def _run_one_period(
    cfg: Path,
    run_id: str,
    budget_min: float,
    archetype: str,
    use_pdlp: bool = False,
    pdlp_tolerance: float | None = None,
    use_gurobi: bool = False,
    gurobi_bar_conv_tol: float | None = None,
    gurobi_opt_tol: float | None = None,
    gurobi_feas_tol: float | None = None,
    gurobi_method: int | None = None,
    gurobi_crossover: int | None = None,
    carried_tranches_dir: Path | None = None,
    current_year: int | None = None,
) -> dict:
    """Run a single period via the instrumented runner and return its record."""
    log_path = LOGS / f"{run_id}.log"
    record_path = RECORDS / f"{run_id}.json"
    if record_path.exists():
        record_path.unlink()
    if log_path.exists():
        log_path.unlink()

    solver_flags = ""
    if use_pdlp:
        solver_flags = " --use-pdlp"
        if pdlp_tolerance is not None:
            solver_flags += f" --pdlp-tolerance {pdlp_tolerance}"
    elif use_gurobi:
        solver_flags = " --use-gurobi"
        if gurobi_bar_conv_tol is not None:
            solver_flags += f" --gurobi-bar-conv-tol {gurobi_bar_conv_tol}"
        if gurobi_opt_tol is not None:
            solver_flags += f" --gurobi-opt-tol {gurobi_opt_tol}"
        if gurobi_feas_tol is not None:
            solver_flags += f" --gurobi-feas-tol {gurobi_feas_tol}"
        if gurobi_method is not None:
            solver_flags += f" --gurobi-method {gurobi_method}"
        if gurobi_crossover is not None:
            solver_flags += f" --gurobi-crossover {gurobi_crossover}"
    if carried_tranches_dir is not None and current_year is not None:
        solver_flags += (
            f' --carried-tranches-dir "{carried_tranches_dir}"'
            f" --current-year {current_year}"
        )

    cmd_str = (
        f'"{sys.executable}" -u "{BENCH / "instrumented_runner.py"}" '
        f'--config "{cfg}" --run-id "{run_id}" --archetype {archetype}{solver_flags} '
        f'> "{log_path}" 2>&1'
    )
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(cmd_str, shell=True, env=env)
    psproc = psutil.Process(proc.pid)
    started = time.time()
    peak_rss = 0
    budget_s = budget_min * 60
    while True:
        rc = proc.poll()
        try:
            rss = psproc.memory_info().rss
            for ch in psproc.children(recursive=True):
                try:
                    rss += ch.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak_rss = max(peak_rss, rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        if rc is not None:
            break
        if time.time() - started > budget_s:
            for ch in psproc.children(recursive=True):
                try:
                    ch.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            psproc.kill()
            proc.wait(timeout=30)
            return {"status": "timed_out", "wall_clock_s": time.time() - started,
                    "peak_rss_gib": peak_rss / (1024**3)}
        time.sleep(5)

    if record_path.exists():
        rec = json.loads(record_path.read_text())
        rec["peak_rss_gib"] = peak_rss / (1024**3)
        return rec
    return {"status": "failed", "wall_clock_s": time.time() - started,
            "peak_rss_gib": peak_rss / (1024**3)}


def _extract_built_capacities(run_id: str, year: int, archetype: str) -> pd.DataFrame:
    """Extract capacity built in this period — generators with build_year=year."""
    import pypsa
    nc_path = Path("mvp_pass1_power/bench/runs_myopic") / f"{run_id}_{year}__{archetype}" \
              / "outputs" / "capacity_expansion.nc"
    if not nc_path.exists():
        return pd.DataFrame()
    n = pypsa.Network(nc_path)
    gens = n.generators[["bus", "carrier", "p_nom_opt", "build_year", "lifetime"]].copy()
    gens = gens[gens["bus"] != "bus_for_custom_constraint_gens"]
    # Filter to new entrants that were built this period
    built = gens[(gens["build_year"] == year) & (gens["p_nom_opt"] > 1.0)]
    return built


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--filter", default=None,
                    help="NEM region filter (e.g. 'NSW'); omit for full NEM")
    ap.add_argument("--periods", type=int, nargs="+",
                    default=[2025, 2030, 2035, 2040, 2045, 2050])
    ap.add_argument("--archetype", default="cost_optimal",
                    help="Archetype id to apply (default: cost_optimal)")
    ap.add_argument("--budget-min", type=float, default=720,
                    help="Per-period wall-clock budget (default 12h)")
    ap.add_argument("--use-pdlp", action="store_true",
                    help="Solve with HiGHS PDLP instead of default simplex.")
    ap.add_argument("--pdlp-tolerance", type=float, default=None,
                    help="Set pdlp_optimality_tolerance + primal/dual feasibility "
                         "tolerances; only used with --use-pdlp.")
    ap.add_argument("--use-gurobi", action="store_true",
                    help="Solve with Gurobi instead of HiGHS (overrides config.solver).")
    ap.add_argument("--gurobi-bar-conv-tol", type=float, default=None,
                    help="Set Gurobi BarConvTol (default 1e-8); only used with --use-gurobi.")
    ap.add_argument("--gurobi-opt-tol", type=float, default=None,
                    help="Set Gurobi OptimalityTol (default 1e-6); only used with --use-gurobi.")
    ap.add_argument("--gurobi-feas-tol", type=float, default=None,
                    help="Set Gurobi FeasibilityTol (default 1e-6); only used with --use-gurobi.")
    ap.add_argument("--gurobi-method", type=int, default=None,
                    help="Set Gurobi Method (2=barrier); only used with --use-gurobi.")
    ap.add_argument("--gurobi-crossover", type=int, default=None,
                    help="Set Gurobi Crossover (0=off -> interior solution, fast); only used with --use-gurobi.")
    ap.add_argument("--rep-weeks", type=int, nargs="+", default=None,
                    help="Override representative_weeks list (default [42]). "
                         "Named weeks (residual-peak-demand, peak-demand) remain. "
                         "E.g. --rep-weeks 42 33 gives 4-week sampling.")
    ap.add_argument("--full-year", action="store_true",
                    help="Disable all rep-week sampling (numbered AND named) so the LP "
                         "covers the full reference year. Default resolution_min=60 (hourly); "
                         "override with --resolution-min.")
    ap.add_argument("--resolution-min", type=int, default=None,
                    help="Override temporal resolution in minutes (default 30 for "
                         "rep-week modes; default 60 for --full-year).")
    ap.add_argument("--recursive-dynamic", action="store_true",
                    help="Enable recursive-dynamic capacity roll-forward: after each "
                         "year's solve, extract the new-build tranche and inject all "
                         "surviving prior tranches into subsequent solves. Default "
                         "(independent-static) is unchanged when this flag is absent.")
    ap.add_argument("--carbon-price", type=float, default=0.0,
                    help="AUD/tCO2e adder on residual emissions, threaded into "
                         "config.carbon_pricing.carbon_price for every period in the "
                         "chain. Default 0 (no carbon adder).")
    ap.add_argument("--tns-price", type=float, default=0.0,
                    help="AUD/tCO2 T&S cost on captured tonnes for CCS plants. "
                         "Threaded into config.carbon_pricing.tns_price. Default 0.")
    ap.add_argument("--parsed-traces-directory", default="mvp_pass1_power/data/traces",
                    help="Base parsed-traces dir (isp_<dataset_year> is appended). Use "
                         "'data/trace_data' with --dataset-year 2026 for the Draft 2026 store.")
    ap.add_argument("--dataset-year", type=int, default=2024,
                    help="Trace dataset year; selects isp_<year> and (==2026) triggers the "
                         "flagged-new-entrant exclusion in instrumented_runner. Default 2024.")
    args = ap.parse_args()

    regions = [args.filter] if args.filter else None
    # Per-chain tranche directory: independent-static runs never touch it
    # (carried_tranches_dir stays None), so default behaviour is bit-identical
    # to the pre-recursive-dynamic code path.
    tranches_dir = (
        Path("mvp_pass1_power/bench/runs_myopic") / args.run_id / "tranches"
        if args.recursive_dynamic
        else None
    )
    if tranches_dir is not None:
        # Fresh chain → empty tranche directory. A re-run that wants to resume
        # from a partially-completed chain should preserve prior tranches; the
        # explicit `--recursive-dynamic` invocation starts clean here so a
        # rerun does not accidentally inherit stale state from a different
        # configuration.
        if tranches_dir.exists():
            shutil.rmtree(tranches_dir)
        tranches_dir.mkdir(parents=True, exist_ok=True)

    seq_record = {
        "run_id": args.run_id,
        "kind": "myopic_sequential",
        "archetype": args.archetype,
        "regions_filter": regions,
        "periods": args.periods,
        "recursive_dynamic": args.recursive_dynamic,
        "tranches_dir": str(tranches_dir) if tranches_dir else None,
        "carbon_price": args.carbon_price,
        "tns_price": args.tns_price,
        "started_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "per_period": {},
        "cumulative_wall_clock_s": 0.0,
        "peak_rss_gib_observed": 0.0,
    }

    seq_started = time.time()
    for year in args.periods:
        sub_run_id = f"{args.run_id}_{year}"
        print(f"\n=== Myopic period {year} ({sub_run_id}) archetype={args.archetype} ===")
        # capacity_expansion.resolution_min must match operational.resolution_min
        # (validator constraint in src/ispypsa/config/validators.py). Operational
        # is fixed at 30 in the template, so capacity_expansion stays 30 unless
        # the caller knows what they are doing.
        resolution_min = args.resolution_min if args.resolution_min is not None else 30
        cfg = _write_period_config(sub_run_id, year, regions,
                                    rep_weeks=args.rep_weeks,
                                    full_year=args.full_year,
                                    resolution_min=resolution_min,
                                    carbon_price=args.carbon_price,
                                    tns_price=args.tns_price,
                                    parsed_traces_directory=args.parsed_traces_directory,
                                    dataset_year=args.dataset_year)
        per_started = time.time()
        rec = _run_one_period(
            cfg, sub_run_id, args.budget_min, args.archetype,
            use_pdlp=args.use_pdlp, pdlp_tolerance=args.pdlp_tolerance,
            use_gurobi=args.use_gurobi,
            gurobi_bar_conv_tol=args.gurobi_bar_conv_tol,
            gurobi_opt_tol=args.gurobi_opt_tol,
            gurobi_feas_tol=args.gurobi_feas_tol,
            gurobi_method=args.gurobi_method,
            gurobi_crossover=args.gurobi_crossover,
            carried_tranches_dir=tranches_dir,
            current_year=year if tranches_dir is not None else None,
        )
        per_wall = time.time() - per_started
        rec["per_period_wall_s"] = per_wall
        rec["per_period_peak_gib"] = rec.get("peak_rss_gib", 0)
        # capacity_built per fuel_type
        try:
            built = _extract_built_capacities(args.run_id, year, args.archetype)
            by_fuel_gw = (built.groupby("carrier")["p_nom_opt"].sum() / 1000.0).to_dict()
            rec["capacity_built_gw_by_fuel"] = by_fuel_gw
            rec["total_capacity_built_gw"] = float(built["p_nom_opt"].sum() / 1000.0)
        except Exception as e:
            rec["capacity_extract_error"] = str(e)
        if tranches_dir is not None and rec.get("status") == "completed":
            try:
                from mvp_pass1_power.bench.recursive_dynamic import (
                    extract_new_built_tranche, save_tranche,
                )
                nc_path = (
                    Path("mvp_pass1_power/bench/runs_myopic")
                    / f"{sub_run_id}__{args.archetype}"
                    / "outputs" / "capacity_expansion.nc"
                )
                tranche = extract_new_built_tranche(nc_path, year)
                save_tranche(tranche, tranches_dir, year)
                rec["tranche_extracted"] = {
                    "generators_rows": int(len(tranche["generators"])),
                    "batteries_rows": int(len(tranche["batteries"])),
                    "generator_mw": float(
                        tranche["generators"]["p_nom"].sum()
                        if not tranche["generators"].empty else 0.0
                    ),
                    "battery_mw": float(
                        tranche["batteries"]["p_nom"].sum()
                        if not tranche["batteries"].empty else 0.0
                    ),
                }
            except Exception as e:
                rec["tranche_extract_error"] = str(e)
        seq_record["per_period"][year] = rec
        seq_record["cumulative_wall_clock_s"] = time.time() - seq_started
        seq_record["peak_rss_gib_observed"] = max(
            seq_record["peak_rss_gib_observed"], rec.get("peak_rss_gib", 0)
        )
        # Save partial after each period in case we die early.
        (RECORDS / f"{args.run_id}.json").write_text(json.dumps(seq_record, indent=2, default=str))
        if rec.get("status") != "completed":
            print(f"  Period {year} status: {rec.get('status')}; aborting sequence")
            break
        # Recursive-dynamic-specific halt: a tranche extraction failure means the
        # next year would inherit an empty floor and run as bit-identical to a
        # greenfield standalone — corrupting the chain silently. The pre-fix bug
        # at step3_chain_p200_8760 logged `tranche_extract_error: "No module named
        # 'mvp_pass1_power'"` and the chain proceeded anyway because the solve
        # itself succeeded; here we halt instead so a failed write-back surfaces
        # immediately rather than poisoning the downstream chain.
        if tranches_dir is not None and rec.get("tranche_extract_error"):
            print(f"  Period {year} tranche extraction FAILED: "
                  f"{rec.get('tranche_extract_error')}; halting recursive-dynamic "
                  f"chain (next year would carry no floor and run as no-op).")
            break

    seq_record["ended_at_iso"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    seq_record["cumulative_wall_clock_s"] = time.time() - seq_started
    (RECORDS / f"{args.run_id}.json").write_text(json.dumps(seq_record, indent=2, default=str))
    print(f"\n=== Done. Cumulative wall: {seq_record['cumulative_wall_clock_s']:.0f}s ===")


if __name__ == "__main__":
    main()
