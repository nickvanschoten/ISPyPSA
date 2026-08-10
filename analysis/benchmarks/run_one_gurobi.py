"""Run a single config under Gurobi at default settings with an external budget.

Mirrors run_one_simplex.py / run_one_pdlp.py / run_one_ipm.py. The only change
vs run_one_simplex.py is `--use-gurobi` is passed through to the instrumented
runner, which calls `solve_model(solver_name="gurobi")` and lets Gurobi pick
its default algorithm.

Usage:
    uv run python mvp_pass1_power/bench/run_one_gurobi.py \\
        --run-id 02_gurobi_nsw_2period \\
        --config mvp_pass1_power/bench/configs/02_nsw_2period.yaml \\
        --budget-min 60
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil

BENCH = Path(__file__).parent
LOGS = BENCH / "logs"
RECORDS = BENCH / "records"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--budget-min", type=float, default=60)
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument("--gurobi-bar-conv-tol", type=float, default=None,
                    help="Set Gurobi BarConvTol (default 1e-8); e.g. 1e-3 for relaxed run")
    ap.add_argument("--gurobi-method", type=int, default=None,
                    help="Gurobi Method (2=barrier); passthrough to instrumented_runner")
    ap.add_argument("--gurobi-crossover", type=int, default=None,
                    help="Gurobi Crossover (0=off -> interior solution)")
    ap.add_argument("--gurobi-numeric-focus", type=int, default=None,
                    help="Gurobi NumericFocus (0=auto, 1-3 increasing care)")
    ap.add_argument("--gurobi-bar-homogeneous", type=int, default=None,
                    help="Gurobi BarHomogeneous (-1 auto, 0 off, 1 on) — robust on ill-conditioned LPs")
    args = ap.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    RECORDS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / f"{args.run_id}.log"

    flags = "--use-gurobi"
    if args.gurobi_bar_conv_tol is not None:
        flags += f" --gurobi-bar-conv-tol {args.gurobi_bar_conv_tol}"
    if args.gurobi_method is not None:
        flags += f" --gurobi-method {args.gurobi_method}"
    if args.gurobi_crossover is not None:
        flags += f" --gurobi-crossover {args.gurobi_crossover}"
    if args.gurobi_numeric_focus is not None:
        flags += f" --gurobi-numeric-focus {args.gurobi_numeric_focus}"
    if args.gurobi_bar_homogeneous is not None:
        flags += f" --gurobi-bar-homogeneous {args.gurobi_bar_homogeneous}"
    solver_label = f"Gurobi [{flags}]"
    cmd_str = (
        f'"{sys.executable}" -u "{BENCH / "instrumented_runner.py"}" '
        f"--config \"{args.config}\" --run-id \"{args.run_id}\" "
        f"--archetype {args.archetype} {flags} "
        f'> "{log_path}" 2>&1'
    )
    print(f"Launching {args.run_id} (budget {args.budget_min:.0f} min, {solver_label})")

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(cmd_str, shell=True, env=env)
    psproc = psutil.Process(proc.pid)
    started = time.time()
    peak_rss = 0
    budget_s = args.budget_min * 60

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

        elapsed = time.time() - started
        if rc is not None:
            print(f"  {args.run_id} exited rc={rc}; wall={elapsed:.1f}s peak={peak_rss/(1024**3):.2f} GiB")
            return
        if elapsed > budget_s:
            print(f"  {args.run_id} hit budget {args.budget_min:.0f} min; killing")
            try:
                for ch in psproc.children(recursive=True):
                    try:
                        ch.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                psproc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            proc.wait(timeout=30)
            sys.path.insert(0, str(BENCH))
            from instrumented_runner import _parse_highs_log
            log_text = log_path.read_text(errors="replace") if log_path.exists() else ""
            info = _parse_highs_log(log_text)
            text_clean = log_text.replace("\r", "\n")
            iters = [ln.strip() for ln in text_clean.split("\n")
                     if "iteration" in ln.lower() or "objective" in ln.lower()]
            record = {
                "run_id": args.run_id,
                "status": "timed_out",
                "wall_clock_s": time.time() - started,
                "wall_clock_budget_s": budget_s,
                "peak_rss_bytes": peak_rss,
                "peak_rss_gib": peak_rss / (1024**3),
                **info,
                "last_solver_line": iters[-1][:220] if iters else None,
            }
            (RECORDS / f"{args.run_id}.json").write_text(json.dumps(record, indent=2, default=str))
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
