"""Run a single benchmark configuration under HiGHS PDLP with external budget.

Mirrors run_one_ipm.py but passes --use-pdlp to the instrumented runner.

Usage:
    uv run python analysis/benchmarks/run_one_pdlp.py \\
        --run-id 02_pdlp_nsw_2period \\
        --config analysis/benchmarks/configs/02_nsw_2period.yaml \\
        --budget-min 180
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
    ap.add_argument("--budget-min", type=float, default=180)
    ap.add_argument("--archetype", default="cost_optimal")
    ap.add_argument("--pdlp-tolerance", type=float, default=None,
                    help="Relax PDLP tolerance from HiGHS defaults")
    args = ap.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    RECORDS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / f"{args.run_id}.log"

    tol_flag = f"--pdlp-tolerance {args.pdlp_tolerance}" if args.pdlp_tolerance else ""
    cmd_str = (
        f'"{sys.executable}" -u "{BENCH / "instrumented_runner.py"}" '
        f"--config \"{args.config}\" --run-id \"{args.run_id}\" "
        f"--archetype {args.archetype} --use-pdlp {tol_flag} "
        f'> "{log_path}" 2>&1'
    )
    tol_str = f"tol={args.pdlp_tolerance}" if args.pdlp_tolerance else "default tol"
    print(f"Launching {args.run_id} (budget {args.budget_min:.0f} min, PDLP, {tol_str})")

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
            _write_timeout_record(args.run_id, started, budget_s, peak_rss)
            return
        time.sleep(5)


def _write_timeout_record(run_id, started, budget_s, peak_rss):
    sys.path.insert(0, str(BENCH))
    from instrumented_runner import _parse_highs_log
    log_path = LOGS / f"{run_id}.log"
    text = log_path.read_text(errors="replace") if log_path.exists() else ""
    info = _parse_highs_log(text)
    text_clean = text.replace("\r", "\n")
    iters = [ln.strip() for ln in text_clean.split("\n")
             if "Primal" in ln or "PDLP" in ln or "[A]" in ln or "[L]" in ln]
    record = {
        "run_id": run_id,
        "status": "timed_out",
        "wall_clock_s": time.time() - started,
        "wall_clock_budget_s": budget_s,
        "peak_rss_bytes": peak_rss,
        "peak_rss_gib": peak_rss / (1024**3),
        **info,
        "last_solver_line": iters[-1][:220] if iters else None,
        "note": f"Killed after {budget_s/60:.0f} min budget. See log for solver progress.",
    }
    (RECORDS / f"{run_id}.json").write_text(json.dumps(record, indent=2, default=str))


if __name__ == "__main__":
    main()
