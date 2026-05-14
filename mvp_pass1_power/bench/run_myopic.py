"""Myopic period-decomposition driver for ISPyPSA.

Runs N sequential single-period ISPyPSA capacity-expansion solves with
capacity-fixing between periods. After each period solve, the optimal
new-entrant capacities are extracted and added to ecaa_generators.csv as
already-committed assets for the next period. This sidesteps the
multi-period perfect-foresight LP entirely.

Per-period the script: builds an ISPyPSA workflow at year T as the single
investment period, optionally augments ecaa_generators.csv with prior-period
builds, solves with default HiGHS simplex, extracts dispatch + capacity, then
moves to the next period.

Usage:
    uv run python mvp_pass1_power/bench/run_myopic.py \\
        --run-id nsw_6p_myopic \\
        --filter NSW \\
        --periods 2025 2030 2035 2040 2045 2050 \\
        --budget-min 600
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

BENCH = Path(__file__).parent
LOGS = BENCH / "logs"
RECORDS = BENCH / "records"
CONFIG_TEMPLATE_DIR = BENCH / "configs"


def _write_period_config(run_id: str, year: int, regions: list[str] | None) -> Path:
    """Synthesise a single-period config for this milestone year."""
    cfg_dir = BENCH / "configs_myopic"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / f"{run_id}_{year}.yaml"
    filter_line = f"filter_by_nem_regions: {regions}\n" if regions else "# Full NEM\n"
    cfg_text = f"""# Auto-generated myopic config for {run_id} year {year}
paths:
  run_directory: "mvp_pass1_power/bench/runs_myopic"
  ispypsa_run_name: {run_id}_{year}
  parsed_traces_directory: "mvp_pass1_power/data/traces"
  workbook_path: "mvp_pass1_power/data/iasr_2024_v6.0.xlsx"
  parsed_workbook_cache: "mvp_pass1_power/data/workbook_cache"
trace_data:
  dataset_type: example
  dataset_year: 2024
iasr_workbook_version: "6.0"
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
    resolution_min: 30
    reference_year_cycle: [2018]
    investment_periods: [{year}]
    aggregation:
      representative_weeks: ~
      named_representative_weeks: [residual-peak-demand]
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
"""
    cfg_path.write_text(cfg_text)
    return cfg_path


def _run_one_period(cfg: Path, run_id: str, budget_min: float) -> dict:
    """Run a single period via the instrumented runner and return its record."""
    log_path = LOGS / f"{run_id}.log"
    record_path = RECORDS / f"{run_id}.json"
    if record_path.exists():
        record_path.unlink()
    if log_path.exists():
        log_path.unlink()

    cmd_str = (
        f'"{sys.executable}" -u "{BENCH / "instrumented_runner.py"}" '
        f'--config "{cfg}" --run-id "{run_id}" --archetype cost_optimal '
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


def _extract_built_capacities(run_id: str, year: int) -> pd.DataFrame:
    """Extract capacity built in this period — generators with build_year=year."""
    import pypsa
    nc_path = Path("mvp_pass1_power/bench/runs_myopic") / f"{run_id}_{year}__cost_optimal" \
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
    ap.add_argument("--budget-min", type=float, default=720,
                    help="Per-period wall-clock budget (default 12h)")
    args = ap.parse_args()

    regions = [args.filter] if args.filter else None
    seq_record = {
        "run_id": args.run_id,
        "kind": "myopic_sequential",
        "regions_filter": regions,
        "periods": args.periods,
        "started_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "per_period": {},
        "cumulative_wall_clock_s": 0.0,
        "peak_rss_gib_observed": 0.0,
    }

    seq_started = time.time()
    for year in args.periods:
        sub_run_id = f"{args.run_id}_{year}"
        print(f"\n=== Myopic period {year} ({sub_run_id}) ===")
        cfg = _write_period_config(sub_run_id, year, regions)
        per_started = time.time()
        rec = _run_one_period(cfg, sub_run_id, args.budget_min)
        per_wall = time.time() - per_started
        rec["per_period_wall_s"] = per_wall
        rec["per_period_peak_gib"] = rec.get("peak_rss_gib", 0)
        # capacity_built per fuel_type
        try:
            built = _extract_built_capacities(args.run_id, year)
            by_fuel_gw = (built.groupby("carrier")["p_nom_opt"].sum() / 1000.0).to_dict()
            rec["capacity_built_gw_by_fuel"] = by_fuel_gw
            rec["total_capacity_built_gw"] = float(built["p_nom_opt"].sum() / 1000.0)
        except Exception as e:
            rec["capacity_extract_error"] = str(e)
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

    seq_record["ended_at_iso"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    seq_record["cumulative_wall_clock_s"] = time.time() - seq_started
    (RECORDS / f"{args.run_id}.json").write_text(json.dumps(seq_record, indent=2, default=str))
    print(f"\n=== Done. Cumulative wall: {seq_record['cumulative_wall_clock_s']:.0f}s ===")


if __name__ == "__main__":
    main()
