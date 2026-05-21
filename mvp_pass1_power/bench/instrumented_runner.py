"""Instrumented ISPyPSA runner for compute-envelope characterisation.

Wraps the standard run_workflow pipeline with:
  - per-stage wall-clock timing
  - peak RSS memory tracked via a sibling psutil poller
  - HiGHS log parsing for LP problem size and convergence status

Writes a JSON record summarising the run to bench/records/<run_id>.json.

Usage:
    uv run python mvp_pass1_power/bench/instrumented_runner.py \
        --config mvp_pass1_power/bench/configs/nem_3period.yaml \
        --run-id nem_3period \
        --archetype cost_optimal
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# ----- memory poller -----------------------------------------------------

class MemoryPoller(threading.Thread):
    """Polls current process RSS at fixed interval, keeps peak."""

    def __init__(self, interval_s: float = 1.0):
        super().__init__(daemon=True)
        self.interval = interval_s
        self.peak_rss_bytes = 0
        self.samples = []
        self._stop_event = threading.Event()
        self._proc = psutil.Process(os.getpid())

    def run(self):
        while not self._stop_event.wait(self.interval):
            try:
                rss = self._proc.memory_info().rss
                # Also include child Python processes if any
                for child in self._proc.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                self.samples.append((time.time(), rss))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

    def stop(self):
        self._stop_event.set()
        self.join(timeout=2.0)


# ----- HiGHS log parser --------------------------------------------------

_LP_SIZE_RE = re.compile(
    r"has\s+(\d+)\s+rows;\s+(\d+)\s+cols;\s+(\d+)\s+nonzeros",
)
# Gurobi: "Optimize a model with 100 rows, 200 columns and 20000 nonzeros"
_GUROBI_LP_SIZE_RE = re.compile(
    r"Optimize a model with\s+(\d+)\s+rows?,\s+(\d+)\s+columns?\s+and\s+(\d+)\s+nonzeros",
)
_MODEL_STATUS_RE = re.compile(r"Model status\s*:\s*(\S.*?)\s*$", re.MULTILINE)
_HIGHS_RUN_TIME_RE = re.compile(r"HiGHS run time\s*:\s*([\d.]+)")
# Gurobi solve-time lines (one of):
#   "Solved in 148 iterations and 0.02 seconds (0.02 work units)"           [simplex]
#   "Barrier solved model in 12 iterations and 5.31 seconds (3.20 work units)" [barrier]
#   "Concurrent spin time: 0.00s"                                            [concurrent]
_GUROBI_SIMPLEX_TIME_RE = re.compile(
    r"Solved in\s+(\d+)\s+iterations? and\s+([\d.]+)\s+seconds"
)
_GUROBI_BARRIER_TIME_RE = re.compile(
    r"Barrier solved model in\s+(\d+)\s+iterations? and\s+([\d.]+)\s+seconds"
)
# Gurobi status: "Optimal objective <value>" or terminal status line
_GUROBI_OPTIMAL_OBJ_RE = re.compile(r"Optimal objective\s+([-\d.eE+]+)")
_GUROBI_STATUS_RE = re.compile(
    r"^\s*(Optimal|Infeasible|Unbounded|Sub-?optimal|Time limit|Iteration limit|"
    r"Numerical trouble)\s+(?:solution|reached|encountered)?",
    re.MULTILINE,
)
_HIGHS_SIMPLEX_ITER_RE = re.compile(r"Simplex\s+iterations:\s*(\d+)")
_HIGHS_IPM_ITER_RE = re.compile(r"IPM\s+iterations:\s*(\d+)")
_HIGHS_PDLP_ITER_RE = re.compile(r"PDLP\s+iterations:\s*(\d+)")
# PDLP summary lines:
#   Primal infeas (abs/rel): 1.98e-07 / 3.99e-10
#   Dual infeas (abs/rel): 0.00e+00 / 0.00e+00
#   Duality gap (abs/rel): 1.20e-05 / 6.28e-08
_PDLP_PINF_RE = re.compile(r"Primal infeas \(abs/rel\):\s*([\d.eE+-]+)\s*/\s*([\d.eE+-]+)")
_PDLP_DINF_RE = re.compile(r"Dual infeas \(abs/rel\):\s*([\d.eE+-]+)\s*/\s*([\d.eE+-]+)")
_PDLP_GAP_RE = re.compile(r"Duality gap \(abs/rel\):\s*([\d.eE+-]+)\s*/\s*([\d.eE+-]+)")
_HIGHS_OBJ_RE = re.compile(r"Objective value\s*:\s*([-\d.eE+]+)")
# Per-iteration IPX line: "   42    8.56205215e+11  -1.39610557e+16   8.70e-02   4.24e-02  2.00e+00      10s"
# Columns: iter, primal_obj, dual_obj, pinf, dinf, gap, time
_IPM_ITER_LINE_RE = re.compile(
    r"^\s*(\d+)\s+"
    r"([-\d.eE+]+)\s+"
    r"([-\d.eE+]+)\s+"
    r"([\d.eE+-]+)\s+"
    r"([\d.eE+-]+)\s+"
    r"([\d.eE+-]+)\s+"
    r"(\d+)s\s*$",
    re.MULTILINE,
)


def _parse_highs_log(log_text: str) -> dict:
    """Pull out LP size and convergence info from a captured HiGHS log."""
    out = {
        "lp_rows": None,
        "lp_cols": None,
        "lp_nonzeros": None,
        "model_status": None,
        "highs_run_time_s": None,
        "simplex_iterations": None,
        "ipm_iterations": None,
        "ipm_final_pinf": None,
        "ipm_final_dinf": None,
        "ipm_final_gap": None,
        "objective_value": None,
    }
    m = _LP_SIZE_RE.search(log_text)
    if m:
        out["lp_rows"] = int(m.group(1))
        out["lp_cols"] = int(m.group(2))
        out["lp_nonzeros"] = int(m.group(3))
    else:
        m = _GUROBI_LP_SIZE_RE.search(log_text)
        if m:
            out["lp_rows"] = int(m.group(1))
            out["lp_cols"] = int(m.group(2))
            out["lp_nonzeros"] = int(m.group(3))
    m = _GUROBI_SIMPLEX_TIME_RE.search(log_text)
    if m:
        out["gurobi_iterations"] = int(m.group(1))
        out["gurobi_solver_time_s"] = float(m.group(2))
    m = _GUROBI_BARRIER_TIME_RE.search(log_text)
    if m:
        out["gurobi_barrier_iterations"] = int(m.group(1))
        out["gurobi_barrier_time_s"] = float(m.group(2))
    m = _GUROBI_OPTIMAL_OBJ_RE.search(log_text)
    if m:
        out["objective_value"] = float(m.group(1))
        out["model_status"] = out.get("model_status") or "Optimal"
    m = _GUROBI_STATUS_RE.search(log_text)
    if m and not out.get("model_status"):
        out["model_status"] = m.group(1).strip()
    m = _MODEL_STATUS_RE.search(log_text)
    if m:
        out["model_status"] = m.group(1).strip()
    m = _HIGHS_RUN_TIME_RE.search(log_text)
    if m:
        out["highs_run_time_s"] = float(m.group(1))
    m = _HIGHS_SIMPLEX_ITER_RE.search(log_text)
    if m:
        out["simplex_iterations"] = int(m.group(1))
    m = _HIGHS_IPM_ITER_RE.search(log_text)
    if m:
        out["ipm_iterations"] = int(m.group(1))
    m = _HIGHS_PDLP_ITER_RE.search(log_text)
    if m:
        out["pdlp_iterations"] = int(m.group(1))
    m = _PDLP_PINF_RE.search(log_text)
    if m:
        out["pdlp_final_pinf_abs"] = float(m.group(1))
        out["pdlp_final_pinf_rel"] = float(m.group(2))
    m = _PDLP_DINF_RE.search(log_text)
    if m:
        out["pdlp_final_dinf_abs"] = float(m.group(1))
        out["pdlp_final_dinf_rel"] = float(m.group(2))
    m = _PDLP_GAP_RE.search(log_text)
    if m:
        out["pdlp_final_gap_abs"] = float(m.group(1))
        out["pdlp_final_gap_rel"] = float(m.group(2))
    # Last IPM-style iteration row: extract final pinf, dinf, gap.
    ipm_lines = list(_IPM_ITER_LINE_RE.finditer(log_text))
    if ipm_lines:
        last = ipm_lines[-1]
        out["ipm_iterations_observed"] = int(last.group(1))
        out["ipm_final_pinf"] = float(last.group(4))
        out["ipm_final_dinf"] = float(last.group(5))
        out["ipm_final_gap"] = float(last.group(6))
    m = _HIGHS_OBJ_RE.search(log_text)
    if m:
        out["objective_value"] = float(m.group(1))
    return out


# ----- staged pipeline runner --------------------------------------------

def _run_staged_pipeline(
    config_path: Path, archetype: str, log_path: Path,
    solver_options: dict | None = None,
    solver_name_override: str | None = None,
) -> dict:
    """Run the ISPyPSA pipeline with per-stage timing. Returns timings dict."""
    from io import StringIO
    import contextlib

    from ispypsa.config import load_config
    from ispypsa.data_fetch import read_csvs, write_csvs
    from ispypsa.iasr_table_caching import build_local_cache
    from ispypsa.logging import configure_logging
    from ispypsa.pypsa_build import build_pypsa_network, save_pypsa_network
    from ispypsa.results import (
        extract_regions_and_zones_mapping, extract_tabular_results,
    )
    from ispypsa.templater import (
        create_ispypsa_inputs_template, load_manually_extracted_tables,
    )
    from ispypsa.translator import (
        create_pypsa_friendly_inputs, create_pypsa_friendly_timeseries_inputs,
    )

    from mvp_pass1_power.archetypes import APPLY_ARCHETYPE

    configure_logging()
    config = load_config(config_path)

    archetype_run_name = f"{config.paths.ispypsa_run_name}__{archetype}"
    run_root = Path(config.paths.run_directory) / archetype_run_name
    ispypsa_inputs_dir = run_root / "ispypsa_inputs"
    pypsa_inputs_dir = run_root / "pypsa_friendly"
    ce_ts_dir = pypsa_inputs_dir / "capacity_expansion_timeseries"
    outputs_dir = run_root / "outputs"
    tables_dir = outputs_dir / "capacity_expansion_tables"
    for d in (ispypsa_inputs_dir, pypsa_inputs_dir, ce_ts_dir, outputs_dir, tables_dir):
        d.mkdir(parents=True, exist_ok=True)

    parsed_workbook_cache = Path(config.paths.parsed_workbook_cache)
    parsed_traces_directory = (
        Path(config.paths.parsed_traces_directory) / f"isp_{config.trace_data.dataset_year}"
    )
    workbook_path = Path(config.paths.workbook_path)
    parsed_workbook_cache.mkdir(parents=True, exist_ok=True)

    timings = {}

    t = time.perf_counter()
    if not (parsed_workbook_cache / "existing_generators_summary.csv").exists():
        build_local_cache(parsed_workbook_cache, workbook_path, config.iasr_workbook_version)
    iasr_tables = read_csvs(parsed_workbook_cache)
    manually_extracted_tables = load_manually_extracted_tables(config.iasr_workbook_version)
    timings["iasr_load_s"] = time.perf_counter() - t

    t = time.perf_counter()
    ispypsa_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
        config.filter_by_nem_regions,
        config.filter_by_isp_sub_regions,
    )
    ispypsa_tables = APPLY_ARCHETYPE[archetype](ispypsa_tables, config)
    write_csvs(ispypsa_tables, ispypsa_inputs_dir)
    timings["templating_s"] = time.perf_counter() - t

    t = time.perf_counter()
    pypsa_friendly = create_pypsa_friendly_inputs(config, ispypsa_tables)
    pypsa_friendly["snapshots"] = create_pypsa_friendly_timeseries_inputs(
        config, "capacity_expansion", ispypsa_tables, pypsa_friendly["generators"],
        parsed_traces_directory, ce_ts_dir,
    )
    write_csvs(pypsa_friendly, pypsa_inputs_dir)
    timings["translation_s"] = time.perf_counter() - t

    t = time.perf_counter()
    network = build_pypsa_network(pypsa_friendly, ce_ts_dir)
    timings["pypsa_build_s"] = time.perf_counter() - t

    # HiGHS C++ writes directly to OS fd 1. When this runner is launched by
    # run_chain.py, fd 1 is the per-run log file — so HiGHS output is captured
    # without any in-process redirect. When run standalone, HiGHS output goes
    # to the terminal and the log parser will simply not find an LP-size line.
    print("\n=== SOLVE START ===", flush=True)
    if solver_options:
        print(f"solver_options: {solver_options}", flush=True)
    t = time.perf_counter()
    try:
        kwargs = {"solver_name": solver_name_override or config.solver}
        if solver_options:
            kwargs["solver_options"] = solver_options
        network.optimize.solve_model(**kwargs)
        solve_ok = True
    except Exception as e:
        print(f"\n=== SOLVE EXCEPTION === {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        solve_ok = False
    print("\n=== SOLVE END ===", flush=True)
    timings["solve_s"] = time.perf_counter() - t
    timings["solve_ok"] = solve_ok

    if not solve_ok:
        return timings

    t = time.perf_counter()
    save_pypsa_network(network, outputs_dir, "capacity_expansion")
    timings["save_network_s"] = time.perf_counter() - t

    t = time.perf_counter()
    results = extract_tabular_results(network, ispypsa_tables)
    results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(ispypsa_tables)
    write_csvs(results, tables_dir)
    timings["extract_results_s"] = time.perf_counter() - t

    # Sanity check: total annual generation per period (MWh).
    gen_dispatch = results["generator_dispatch"]
    weightings = network.snapshot_weightings["generators"]
    by_period = {}
    for period in network.investment_periods:
        period_snaps = [s for s in network.snapshots if s[0] == period]
        if not period_snaps:
            continue
        df_p = gen_dispatch[gen_dispatch["investment_period"] == period]
        # Weight each dispatch_mw by snapshot weighting (h/snapshot)
        gen_t = network.generators_t.p
        period_p = gen_t.loc[period_snaps].clip(lower=0).sum(axis=1)
        weighted_mwh = (period_p * weightings.loc[period_snaps]).sum()
        by_period[int(period)] = float(weighted_mwh)
    timings["annual_generation_mwh_by_period"] = by_period

    return timings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument("--use-ipm", action="store_true",
                    help="Use HiGHS interior-point method instead of simplex")
    ap.add_argument("--no-crossover", action="store_true",
                    help="Disable IPM crossover (returns interior solution)")
    ap.add_argument("--use-pdlp", action="store_true",
                    help="Use HiGHS PDLP (primal-dual hybrid gradient) solver")
    ap.add_argument("--pdlp-tolerance", type=float, default=None,
                    help="Set pdlp_optimality_tolerance + primal/dual feasibility "
                         "tolerances all to this value (default uses HiGHS defaults)")
    ap.add_argument("--use-gurobi", action="store_true",
                    help="Use Gurobi (overrides config.solver = highs); default Gurobi settings")
    ap.add_argument("--gurobi-bar-conv-tol", type=float, default=None,
                    help="Set Gurobi BarConvTol (default 1e-8); e.g. 1e-3 for relaxed run")
    args = ap.parse_args()
    solver_options = None
    solver_name_override = None
    if args.use_ipm:
        solver_options = {"solver": "ipm"}
        if args.no_crossover:
            solver_options["run_crossover"] = "off"
    elif args.use_pdlp:
        solver_options = {"solver": "pdlp"}
        if args.pdlp_tolerance is not None:
            solver_options["pdlp_optimality_tolerance"] = args.pdlp_tolerance
            solver_options["primal_feasibility_tolerance"] = args.pdlp_tolerance
            solver_options["dual_feasibility_tolerance"] = args.pdlp_tolerance
    elif args.use_gurobi:
        solver_name_override = "gurobi"
        if args.gurobi_bar_conv_tol is not None:
            solver_options = {"BarConvTol": args.gurobi_bar_conv_tol}

    bench_dir = Path(__file__).parent
    log_path = bench_dir / "logs" / f"{args.run_id}.log"
    record_path = bench_dir / "records" / f"{args.run_id}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.parent.mkdir(parents=True, exist_ok=True)
    # Do NOT truncate the log file: when this runner is invoked from
    # run_chain.py, the parent has already opened the log for append and
    # rebinding the inode here would orphan the parent's file descriptor.

    poller = MemoryPoller(interval_s=2.0)
    poller.start()

    record = {
        "run_id": args.run_id,
        "config": str(args.config),
        "archetype": args.archetype,
        "solver_options": solver_options,
        "started_at": time.time(),
        "started_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    t_total = time.perf_counter()
    try:
        timings = _run_staged_pipeline(args.config, args.archetype, log_path,
                                       solver_options=solver_options,
                                       solver_name_override=solver_name_override)
        record.update(timings)
        record["wall_clock_s"] = time.perf_counter() - t_total
        record["status"] = "completed"
    except Exception as e:
        record["status"] = "failed"
        record["exception"] = f"{type(e).__name__}: {e}"
        record["traceback"] = traceback.format_exc()
        record["wall_clock_s"] = time.perf_counter() - t_total
    finally:
        poller.stop()
        record["peak_rss_bytes"] = poller.peak_rss_bytes
        record["peak_rss_gib"] = poller.peak_rss_bytes / (1024**3)

    if log_path.exists():
        log_text = log_path.read_text(errors="replace")
        record.update(_parse_highs_log(log_text))

    record_path.write_text(json.dumps(record, indent=2, default=str))
    print(json.dumps({k: v for k, v in record.items() if k != "traceback"}, indent=2, default=str))


if __name__ == "__main__":
    main()
