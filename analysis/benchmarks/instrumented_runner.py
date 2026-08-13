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
import random
import re
import sys
import threading
import time
import traceback
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# ----- Gurobi license-retry ----------------------------------------------

# The CSIRO Gurobi token server (sc-license1-cdc.it.csiro.au) allows only 2
# concurrent seats. When more chains run at once, Gurobi raises "use limit (2)
# exceeded"; a seat frees at a running chain's next period boundary, so a
# bounded retry lets surplus chains queue on the 2 seats instead of halting the
# recursive-dynamic chain. Kept under run_myopic's --budget-min wall-clock cap.
_LICENSE_RETRY_MAX_WAIT_S = 36000  # 10 h


def extract_gas_supply_curve_usage(network, gas_supply_curve):
    """Reads solved tranche purchases into a tidy per-period usage table (PJ).

    The tranche variables live only in the in-process linopy model (not the
    saved NetCDF), so this must run in the same process as the solve.
    """
    import pandas as pd

    rows = []
    for period in network.investment_periods:
        variable_name = f"gas_supply_purchases_tj_{period}"
        if variable_name not in network.model.variables:
            continue
        solution = network.model.variables[variable_name].solution
        tranches = gas_supply_curve[gas_supply_curve["investment_period"] == period]
        for _, tranche in tranches.iterrows():
            used_pj = float(solution.sel(gas_tranche=tranche["tranche"])) / 1.0e3
            rows.append(
                {
                    "investment_period": period,
                    "tranche": tranche["tranche"],
                    "adder_$/gj": tranche["adder_$/gj"],
                    "cap_pj": tranche["cap_pj"],
                    "used_pj": used_pj,
                    "premium_cost_$m": used_pj * tranche["adder_$/gj"],
                }
            )
    return pd.DataFrame(rows)


def _solve_with_license_retry(network, kwargs) -> bool:
    """Solve, retrying only while the token server reports no free seat.

    Returns True on success, False on a genuine solve failure or once the retry
    budget (`_LICENSE_RETRY_MAX_WAIT_S`) is exhausted. Any non-license exception
    is reported and returned as a failure immediately — never retried.
    """
    deadline = time.perf_counter() + _LICENSE_RETRY_MAX_WAIT_S
    while True:
        try:
            network.optimize.solve_model(**kwargs)
            return True
        except Exception as e:
            seat_busy = "use limit" in str(e).lower()
            if not (seat_busy and time.perf_counter() < deadline):
                print(f"\n=== SOLVE EXCEPTION === {type(e).__name__}: {e}", flush=True)
                traceback.print_exc()
                return False
            wait = 60 + random.uniform(0, 30)  # jitter avoids a thundering herd
            print(f"\n=== LICENSE BUSY (retry in {wait:.0f}s) === {e}", flush=True)
            time.sleep(wait)


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
_PDLP_PINF_RE = re.compile(
    r"Primal infeas \(abs/rel\):\s*([\d.eE+-]+)\s*/\s*([\d.eE+-]+)"
)
_PDLP_DINF_RE = re.compile(
    r"Dual infeas \(abs/rel\):\s*([\d.eE+-]+)\s*/\s*([\d.eE+-]+)"
)
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
    config_path: Path,
    archetype: str,
    log_path: Path,
    solver_options: dict | None = None,
    solver_name_override: str | None = None,
    carried_tranches_dir: Path | None = None,
    current_year: int | None = None,
    reducible_existing: bool = False,
    retention_floor_dir: Path | None = None,
    existing_keeping_cost: float = 0.0,
    existing_fom_keeping: bool = False,
    span_weight_years: int | None = None,
    disestablishment_cost: float = 0.0,
) -> dict:
    """Run the ISPyPSA pipeline with per-stage timing. Returns timings dict.

    If `carried_tranches_dir` is set, recursive-dynamic mode loads all
    surviving prior tranches (build_year < current_year, retirement-filtered)
    and injects them into the in-memory pypsa_friendly dict between
    translation and timeseries generation. This makes year-(t+1)'s solve see
    the accumulated brownfield stock built across the chain.
    """
    import contextlib
    from io import StringIO

    from ispypsa.config import load_config
    from ispypsa.data_fetch import read_csvs, write_csvs
    from ispypsa.iasr_table_caching import build_local_cache
    from ispypsa.logging import configure_logging
    from ispypsa.pypsa_build import build_pypsa_network, save_pypsa_network
    from ispypsa.results import (
        extract_regions_and_zones_mapping,
        extract_tabular_results,
    )
    from ispypsa.templater import (
        create_ispypsa_inputs_template,
        load_manually_extracted_tables,
    )
    from ispypsa.translator import (
        create_pypsa_friendly_inputs,
        create_pypsa_friendly_timeseries_inputs,
    )
    from mvp_pass1_power.archetypes import APPLY_ARCHETYPE
    from mvp_pass1_power.bench.flagged_exclusions_2026 import (
        exclude_ecaa_without_trace,
        exclude_flagged_new_entrants,
        normalize_2026_rez_ids,
    )

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
        Path(config.paths.parsed_traces_directory)
        / f"isp_{config.trace_data.dataset_year}"
    )
    workbook_path = Path(config.paths.workbook_path)
    parsed_workbook_cache.mkdir(parents=True, exist_ok=True)

    timings = {}

    t = time.perf_counter()
    # Cache sentinel uses the v7.4 canonical name; schema normalisation
    # produces this file for both v6.0 and v7.4 source workbooks.
    cache_sentinel = (
        parsed_workbook_cache
        / "existing_committed_anticipated_additional_generator_summary.csv"
    )
    if not cache_sentinel.exists():
        build_local_cache(
            parsed_workbook_cache,
            workbook_path,
            config.iasr_workbook_version,
            trace_directory=Path(config.paths.parsed_traces_directory),
        )
    iasr_tables = read_csvs(parsed_workbook_cache)
    manually_extracted_tables = load_manually_extracted_tables(
        config.iasr_workbook_version
    )
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
    # REQUIRED for the Draft 2026 trace store: drop VRE new entrants whose
    # (rez_id, isp_resource_type) has no 2026 trace (Q8 split; N10/N11 fixed
    # offshore). Left in, each crashes create_pypsa_friendly_timeseries at
    # _check_time_series. Gated on the 2026 dataset so it can't wrongly fire on
    # the 2024 store. See flagged_exclusions_2026 and isp_2026/TRACE_BASIS.md.
    if config.trace_data.dataset_year == 2026:
        # Normalize component rez_ids to 2026 REZ codes + connect the split Q8
        # sub-zones (data-determined), THEN drop only the genuinely-no-trace
        # candidates. Order matters: normalization maps Q8->Q8a (which has traces),
        # so the exclusion is left with only the N10/N11 fixed-offshore gap.
        ispypsa_tables = normalize_2026_rez_ids(ispypsa_tables)
        ispypsa_tables["new_entrant_generators"] = exclude_flagged_new_entrants(
            ispypsa_tables["new_entrant_generators"]
        )
        # ECAA VRE trace-coverage filter. The upstream filter_v74_ecaa_to_trace_coverage
        # is hardcoded to isp_2024 and no-ops for the 2026 traces, so 2026 ECAA VRE
        # trace filtering lives here. Runs AFTER normalize_2026_rez_ids (which applies
        # GENERATOR_NAME_2026_NORMALIZATION, e.g. Goyder North Wind Farm 1 -> Goyder
        # North Wind Farm) so name-mismatched generators are matched, not dropped.
        ispypsa_tables["ecaa_generators"] = exclude_ecaa_without_trace(
            ispypsa_tables["ecaa_generators"], parsed_traces_directory
        )
    write_csvs(ispypsa_tables, ispypsa_inputs_dir)
    timings["templating_s"] = time.perf_counter() - t

    t = time.perf_counter()
    pypsa_friendly = create_pypsa_friendly_inputs(config, ispypsa_tables)

    # Endogenous-retirement seam (Phase 1). Make existing (ECAA) generators a
    # downward-only continuous capacity decision BEFORE carried tranches enter,
    # so it targets only the templated existing fleet (carried new-build
    # vintages, also non-extendable, stay fixed and are managed by
    # recursive_dynamic). keeping_cost=0 is Phase-1 strict (mechanism without
    # economics); the monotone retention floor caps this period at the prior
    # period's retained level. Runs before timeseries so the now-extendable rows
    # still get their p_max_pu / marginal_cost wiring downstream unchanged.
    reducible_existing_names: list = []
    if reducible_existing:
        from mvp_pass1_power.bench.retirement import (
            load_retention_floor,
            make_existing_reducible,
        )

        floor = {}
        if retention_floor_dir is not None and current_year is not None:
            floor = load_retention_floor(retention_floor_dir, current_year)
        ecaa = ispypsa_tables["ecaa_generators"]
        existing_names = ecaa["generator"].tolist()
        reducible_existing_names = existing_names
        if existing_fom_keeping:
            # Per-unit FOM ($/kW/yr -> $/MW/yr) becomes capital_cost on the
            # reducible existing units: the recurring keeping cost retirement
            # saves. Phase-2 driver (Phase 0 showed the coal is idle, so the
            # keeping cost -- not operation savings -- is what makes shedding pay).
            keeping_cost = (
                ecaa.set_index("generator")["fom_$/kw/annum"] * 1000.0
            ).to_dict()
        else:
            keeping_cost = existing_keeping_cost
        timings["retirement"] = make_existing_reducible(
            pypsa_friendly["generators"], existing_names, floor, keeping_cost
        )
        print(f"\n=== REDUCIBLE EXISTING === {timings['retirement']}", flush=True)

    # Recursive-dynamic injection seam. Carried rows must enter pypsa_friendly
    # BEFORE timeseries generation so marginal_cost parquets cover any carrier
    # that exists only in carried tranches, and so the parquet generated for
    # the base-tech mapping is computed at the CURRENT year's carbon price
    # (not the vintage year's). The carried row references the base mapping
    # by sharing the original new-entrant's marginal_cost field value.
    if carried_tranches_dir is not None and current_year is not None:
        from mvp_pass1_power.bench.recursive_dynamic import (
            adjust_capacity_caps_for_carried,
            inject_carried_tranches,
            load_tranches,
        )

        carried = load_tranches(carried_tranches_dir, before_year=current_year)
        timings["recursive_dynamic"] = inject_carried_tranches(pypsa_friendly, carried)
        # Net the carried capacity off any per-period capacity cap/floor RHS so the
        # constraint bounds the cumulative active fleet (carried + new), not just
        # the current vintage — fixes the recursive-dynamic × per-period-cap leak
        # (biomass cap reaching ~2x its ceiling; the storage/nuclear/gas floors).
        timings["capacity_cap_carried_adjust"] = adjust_capacity_caps_for_carried(
            pypsa_friendly, current_year
        )
        print(
            f"\n=== RECURSIVE-DYNAMIC INJECTION === {timings['recursive_dynamic']} "
            f"| capacity-cap carried adjust: {timings['capacity_cap_carried_adjust']}",
            flush=True,
        )

    # Capacity-expansion modeling choice (2026-06-22): zero p_min_pu so AEMO's
    # min-stable-level (a unit-commitment concept = the floor WHEN a unit is on)
    # is NOT enforced as a hard floor in this no-unit-commitment investment LP.
    # Applied to fresh AND carried generators. Without it, must-run floors (e.g.
    # new-entrant OCGT at 0.5) force overgeneration at high-solar low-demand hours
    # in the VRE-rich out-years -> infeasible (2045 IIS: SA/VIC summer-midday,
    # Generator-fix/ext-p-lower). Min-load belongs in the operational stage; the
    # AEMO values remain in the data, just not enforced as a hard LP floor here.
    pypsa_friendly["generators"]["p_min_pu"] = 0.0

    # Span-weighting (retirement accounting fix). The myopic single-period config
    # makes the investment period 1 year long, so each milestone weights as 1 year.
    # A milestone represents ~span_weight_years; weighting it as its (discounted)
    # span makes the recurring FOM keeping-cost count over the span (so it can
    # outweigh the one-off disestablishment), WITHOUT changing the new-build optimum
    # (a uniform objective scalar for a single-period solve). PyPSA multiplies this
    # objective weight onto BOTH capital and operational costs (optimize.py:151,181).
    # MUST be applied to pypsa_friendly["investment_period_weights"] BEFORE
    # build_pypsa_network -- build.py:92 calls create_model() at build time, so a
    # post-build override of network.investment_period_weightings never reaches the LP.
    if span_weight_years is not None:
        r = config.discount_rate
        obj_w = sum(1.0 / (1.0 + r) ** yr for yr in range(span_weight_years))
        ipw = pypsa_friendly["investment_period_weights"]
        ipw["years"] = span_weight_years
        ipw["objective"] = obj_w
        timings["span_weighting"] = {"years": span_weight_years, "objective": obj_w}
        print(
            f"\n=== SPAN-WEIGHTING (pre-build) === years={span_weight_years} "
            f"objective={obj_w:.3f}",
            flush=True,
        )

    pypsa_friendly["snapshots"] = create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_friendly["generators"],
        parsed_traces_directory,
        ce_ts_dir,
    )
    write_csvs(pypsa_friendly, pypsa_inputs_dir)
    timings["translation_s"] = time.perf_counter() - t

    t = time.perf_counter()
    network = build_pypsa_network(pypsa_friendly, ce_ts_dir)
    timings["pypsa_build_s"] = time.perf_counter() - t

    # One-off disestablishment cost (Phase 2b). The economic cost of retiring is
    # D*(floor - p_nom): a one-off, paid when capacity is shed. Its variable part
    # is -D*p_nom (the D*floor constant doesn't affect the per-period retirement
    # decision and is dropped). Added at x1 -- NOT span-weighted -- so a unit
    # retires only when the span-weighted FOM saving (FOM*span, in capital_cost)
    # exceeds the one-off D. Added directly to the linopy model built by
    # build_pypsa_network (build.py:92), the same seam _add_custom_constraints
    # uses. Sign: -D*p_nom rewards higher p_nom (discourages retiring), so the net
    # coefficient on p_nom is (FOM*span - D); the LP sheds iff FOM*span > D.
    if disestablishment_cost and reducible_existing_names:
        m = network.model
        # Only the reducible existing units that are actually extendable in the
        # model carry a Generator-p_nom variable. Vectorised .loc[...].sum() gives
        # a LinearExpression (a scalar .at[] loop yields a ScalarLinearExpression,
        # which Objective.__add__ rejects).
        ext = set(network.generators.index[network.generators["p_nom_extendable"]])
        names_present = [n for n in reducible_existing_names if n in ext]
        if names_present:
            sub = m.variables.Generator_p_nom.loc[names_present]
            m.objective = m.objective + (-float(disestablishment_cost)) * sub.sum()
            timings["disestablishment"] = {
                "cost_per_mw": float(disestablishment_cost),
                "units": len(names_present),
            }
            print(
                f"\n=== DISESTABLISHMENT === D={disestablishment_cost}/MW "
                f"on {len(names_present)} reducible existing units",
                flush=True,
            )

    # HiGHS C++ writes directly to OS fd 1. When this runner is launched by
    # run_chain.py, fd 1 is the per-run log file — so HiGHS output is captured
    # without any in-process redirect. When run standalone, HiGHS output goes
    # to the terminal and the log parser will simply not find an LP-size line.
    print("\n=== SOLVE START ===", flush=True)
    if solver_options:
        print(f"solver_options: {solver_options}", flush=True)
    t = time.perf_counter()
    kwargs = {"solver_name": solver_name_override or config.solver}
    if solver_options:
        kwargs["solver_options"] = solver_options
    solve_ok = _solve_with_license_retry(network, kwargs)
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
    results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(
        ispypsa_tables
    )
    if "gas_supply_curve" in pypsa_friendly:
        usage = extract_gas_supply_curve_usage(
            network, pypsa_friendly["gas_supply_curve"]
        )
        results["gas_supply_curve_usage"] = usage
        print("\n=== GAS SUPPLY CURVE USAGE (PJ by tranche) ===", flush=True)
        print(usage.to_string(index=False), flush=True)
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

    # Disestablishment cost-reporting (Phase 2b). The objective term added a
    # variable-only -D*p_nom and DROPPED the +D*cap constant (it doesn't affect
    # the retirement decision). So the solved objective UNDERSTATES the true
    # system cost by D*sum(cap). Recover both the actually-incurred one-off
    # (D*(cap - retained), the cost of capacity shed THIS period) and the
    # constant add-back so the reported cost is right. cap = p_nom_max (the
    # floor-or-installed cap make_existing_reducible set); retained = p_nom_opt.
    if disestablishment_cost and reducible_existing_names:
        g = network.generators
        red = g[
            g.index.astype(str).isin(set(map(str, reducible_existing_names)))
            & g["p_nom_extendable"].astype(bool)
        ]
        cap = red["p_nom_max"].clip(upper=1e12)  # guard against inf on any stray row
        retired_mw = float((cap - red["p_nom_opt"]).clip(lower=0).sum())
        D = float(disestablishment_cost)
        timings["disestablishment_cost_report"] = {
            "retired_mw": retired_mw,
            "incurred_one_off_cost": D * retired_mw,
            "objective_constant_add_back_DxCap": D * float(cap.sum()),
            "note": "true_objective = solved_objective + objective_constant_add_back_DxCap",
        }

    return timings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument(
        "--use-ipm",
        action="store_true",
        help="Use HiGHS interior-point method instead of simplex",
    )
    ap.add_argument(
        "--no-crossover",
        action="store_true",
        help="Disable IPM crossover (returns interior solution)",
    )
    ap.add_argument(
        "--use-pdlp",
        action="store_true",
        help="Use HiGHS PDLP (primal-dual hybrid gradient) solver",
    )
    ap.add_argument(
        "--pdlp-tolerance",
        type=float,
        default=None,
        help="Set pdlp_optimality_tolerance + primal/dual feasibility "
        "tolerances all to this value (default uses HiGHS defaults)",
    )
    ap.add_argument(
        "--use-gurobi",
        action="store_true",
        help="Use Gurobi (overrides config.solver = highs); default Gurobi settings",
    )
    ap.add_argument(
        "--gurobi-bar-conv-tol",
        type=float,
        default=None,
        help="Set Gurobi BarConvTol (default 1e-8); e.g. 1e-3 for relaxed run",
    )
    ap.add_argument(
        "--gurobi-opt-tol",
        type=float,
        default=None,
        help="Set Gurobi OptimalityTol (default 1e-6); reduced-cost / dual tolerance",
    )
    ap.add_argument(
        "--gurobi-feas-tol",
        type=float,
        default=None,
        help="Set Gurobi FeasibilityTol (default 1e-6); primal feasibility tolerance",
    )
    ap.add_argument(
        "--gurobi-threads",
        type=int,
        default=None,
        help="Set Gurobi Threads (cores per solve). Cap when running multiple "
        "trajectories concurrently so concurrent x threads <= cores with "
        "headroom; default (unset) lets Gurobi use all cores (right for a "
        "single solve).",
    )
    ap.add_argument(
        "--gurobi-method",
        type=int,
        default=None,
        help="Set Gurobi Method (0=primal simplex, 1=dual simplex, 2=barrier, "
        "3=concurrent, 4=det concurrent). For barrier-only solves "
        "(no crossover), pair --gurobi-method 2 with --gurobi-crossover 0.",
    )
    ap.add_argument(
        "--gurobi-crossover",
        type=int,
        default=None,
        help="Set Gurobi Crossover (-1=auto, 0=disabled, 1-4=specific strategies). "
        "0 keeps Gurobi at the barrier interior solution and avoids the "
        "superlinear crossover scaling that was impractical at 8760 in "
        "Phase 8.1 Test 2.",
    )
    ap.add_argument(
        "--gurobi-numeric-focus",
        type=int,
        default=None,
        help="Set Gurobi NumericFocus (0=auto, 1-3=increasing numerical care). "
        "Buildable new-entrant storage widens the objective coefficient range "
        "(~[1, 1e7]); Gurobi then terminates the barrier Sub-optimal with a "
        "'set NumericFocus' warning. 2-3 trades speed for numerical robustness.",
    )
    ap.add_argument(
        "--gurobi-bar-homogeneous",
        type=int,
        default=None,
        help="Set Gurobi BarHomogeneous (-1=auto, 0=off, 1=on). 1 forces the "
        "homogeneous self-dual barrier algorithm, more robust to "
        "ill-conditioning and infeasibility detection. Phase 8 finding: "
        "Gurobi-barrier-crossover-off with default BarHomogeneous=auto "
        "stalled identically at iter 178 for two BarConvTol settings on "
        "the 2045 8760 LP — HSD is the next-class-of-barrier test.",
    )
    ap.add_argument(
        "--gurobi-obj-scale",
        type=float,
        default=None,
        help="Set Gurobi ObjScale (>0 divides objective by this value, -1=auto, "
        "0=off). Phase 8 LP has cost range 4e0-3e6 and RHS down to 3e-5; "
        "both HiGHS and Gurobi flagged 'consider scaling the objective by "
        "1e-1'. ObjScale=10 implements that suggestion (divides obj by 10).",
    )
    ap.add_argument(
        "--carried-tranches-dir",
        type=Path,
        default=None,
        help="Recursive-dynamic mode: directory holding per-year tranche "
        "parquets from prior solves in this chain. Requires --current-year.",
    )
    ap.add_argument(
        "--current-year",
        type=int,
        default=None,
        help="The investment year being solved. Used by recursive-dynamic "
        "mode to retirement-filter carried tranches, and by "
        "reducible-existing mode to load the prior-period retention floor.",
    )
    ap.add_argument(
        "--reducible-existing",
        action="store_true",
        help="Phase-1 retirement: make existing (ECAA) generators a "
        "downward-only continuous capacity decision (extendable, "
        "p_nom_min=0, p_nom_max=installed, capital_cost=keeping-cost). "
        "p_nom_opt < installed is economic retirement. LP, not MIP.",
    )
    ap.add_argument(
        "--retention-floor-dir",
        type=Path,
        default=None,
        help="Directory holding per-year retained-existing floors from prior "
        "solves in this chain; caps this period's existing capacity at the "
        "prior retained level (monotone retirement). Requires --current-year.",
    )
    ap.add_argument(
        "--existing-keeping-cost",
        type=float,
        default=0.0,
        help="Per-MW cost of KEEPING existing capacity each period, set as "
        "capital_cost on the reducible existing generators. 0 = Phase-1 "
        "strict (no economics); Phase-2 passes the AEMO fixed-OPEX.",
    )
    ap.add_argument(
        "--existing-fom-keeping",
        action="store_true",
        help="Phase-2: route each existing unit's OWN FOM (ecaa fom_$/kw/annum "
        "-> $/MW/yr) as its capital_cost keeping-cost, instead of the scalar "
        "--existing-keeping-cost. This is the recurring cost retirement saves.",
    )
    ap.add_argument(
        "--span-weight-years",
        type=int,
        default=None,
        help="Phase-2 accounting fix: weight the (1-year) myopic investment period "
        "as an N-year milestone span, so recurring FOM counts over the span vs "
        "the one-off disestablishment. Does NOT change the new-build optimum "
        "(uniform objective scalar for a single-period solve). Default: off.",
    )
    ap.add_argument(
        "--disestablishment-cost",
        type=float,
        default=0.0,
        help="Phase-2b: one-off disestablishment/decommissioning cost ($/MW) on "
        "retired existing capacity, added as -D*p_nom to the objective (x1, NOT "
        "span-weighted). A unit sheds only when FOM*span > D. Default 0 (off).",
    )
    args = ap.parse_args()
    if args.carried_tranches_dir is not None and args.current_year is None:
        ap.error("--carried-tranches-dir requires --current-year.")
    if args.retention_floor_dir is not None and args.current_year is None:
        ap.error("--retention-floor-dir requires --current-year.")
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
        gurobi_opts = {}
        if args.gurobi_bar_conv_tol is not None:
            gurobi_opts["BarConvTol"] = args.gurobi_bar_conv_tol
        if args.gurobi_opt_tol is not None:
            gurobi_opts["OptimalityTol"] = args.gurobi_opt_tol
        if args.gurobi_feas_tol is not None:
            gurobi_opts["FeasibilityTol"] = args.gurobi_feas_tol
        if args.gurobi_threads is not None:
            gurobi_opts["Threads"] = args.gurobi_threads
        if args.gurobi_method is not None:
            gurobi_opts["Method"] = args.gurobi_method
        if args.gurobi_crossover is not None:
            gurobi_opts["Crossover"] = args.gurobi_crossover
        if args.gurobi_numeric_focus is not None:
            gurobi_opts["NumericFocus"] = args.gurobi_numeric_focus
        if args.gurobi_bar_homogeneous is not None:
            gurobi_opts["BarHomogeneous"] = args.gurobi_bar_homogeneous
        if args.gurobi_obj_scale is not None:
            gurobi_opts["ObjScale"] = args.gurobi_obj_scale
        if gurobi_opts:
            solver_options = gurobi_opts

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
        timings = _run_staged_pipeline(
            args.config,
            args.archetype,
            log_path,
            solver_options=solver_options,
            solver_name_override=solver_name_override,
            carried_tranches_dir=args.carried_tranches_dir,
            current_year=args.current_year,
            reducible_existing=args.reducible_existing,
            retention_floor_dir=args.retention_floor_dir,
            existing_keeping_cost=args.existing_keeping_cost,
            existing_fom_keeping=args.existing_fom_keeping,
            span_weight_years=args.span_weight_years,
            disestablishment_cost=args.disestablishment_cost,
        )
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
    print(
        json.dumps(
            {k: v for k, v in record.items() if k != "traceback"}, indent=2, default=str
        )
    )


if __name__ == "__main__":
    main()
