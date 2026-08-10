"""Run a chain of benchmark configurations sequentially.

Each configuration is run as a separate subprocess so that memory measurement
is clean (Python heap from a prior run doesn't contaminate the next). If a
configuration times out at the wall-clock budget given, capture-on-timeout
records a "timed_out" record so the curve has a data point.

Usage:
    uv run python mvp_pass1_power/bench/run_chain.py 02 03 04 05 06 07
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil

BENCH_DIR = Path(__file__).parent
CONFIGS_DIR = BENCH_DIR / "configs"
RECORDS_DIR = BENCH_DIR / "records"
LOGS_DIR = BENCH_DIR / "logs"

# Per-config wall-clock budgets (seconds). Generous — captures the data point
# even at the slow end. If a run hits the budget without converging, we record
# a partial result with the HiGHS iteration count and last objective so the
# team can see whether the solver was making progress.
TIMEOUT_S = {
    # Revised after run 02 observation: HiGHS at default settings cycles on
    # multi-investment-period LPs (Pr-infeasibility oscillates, objective
    # drifts, no monotonic convergence). Multi-period budgets shortened so
    # the full envelope can be characterised within a session — "did not
    # converge in 30 min" is just as informative as "did not converge in 4h".
    "01_nsw_1period":  10 * 60,    # 10 min (76s expected baseline)
    "02_nsw_2period":  30 * 60,    # 30 min (killed externally at 7 min after observing degeneracy)
    "03_nsw_3period":  30 * 60,
    "04_nem_1period":  60 * 60,    # 1 h — single-period, expected to succeed
    # 06/07 explicitly LP-size-only: 10 min is enough for HiGHS to print
    # LP rows/cols/nonzeros before any meaningful simplex progress. Convergence
    # on these LPs at default HiGHS is empirically impossible within
    # session time — see runs 02–05.
    "05_nem_2period":  30 * 60,
    "06_nem_3period":  10 * 60,
    "07_nem_6period":  10 * 60,
}


def _config_path(run_id: str) -> Path:
    return CONFIGS_DIR / f"{run_id}.yaml"


def _record_path(run_id: str) -> Path:
    return RECORDS_DIR / f"{run_id}.json"


def _capture_timeout_partial(run_id: str, started_at: float, budget_s: float,
                             proc_peak_rss: int) -> None:
    """When we kill a run, still write a record so it appears on the curve."""
    log_path = LOGS_DIR / f"{run_id}.log"
    log_text = log_path.read_text(errors="replace") if log_path.exists() else ""
    sys.path.insert(0, str(BENCH_DIR))
    from instrumented_runner import _parse_highs_log
    info = _parse_highs_log(log_text)
    record = {
        "run_id": run_id,
        "status": "timed_out",
        "wall_clock_s": time.time() - started_at,
        "wall_clock_budget_s": budget_s,
        "peak_rss_bytes": proc_peak_rss,
        "peak_rss_gib": proc_peak_rss / (1024**3),
        **info,
        "note": f"Killed after {budget_s/60:.0f} min wall-clock budget. "
                f"See last HiGHS Iteration line in logs for progress.",
    }
    _record_path(run_id).write_text(json.dumps(record, indent=2, default=str))


def _run_one(run_id: str) -> None:
    cfg = _config_path(run_id)
    if not cfg.exists():
        print(f"  SKIP: {cfg} not found")
        return
    if _record_path(run_id).exists():
        print(f"  SKIP: {run_id} already has a record")
        return

    budget_s = TIMEOUT_S.get(run_id, 60 * 60)
    print(f"  Launching {run_id} (budget {budget_s/60:.0f} min)")
    started = time.time()

    log_path = LOGS_DIR / f"{run_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    # Use shell redirection — on Windows, Popen(stdout=file_obj) does not
    # reliably flush C-extension (HiGHS) output to disk. Letting cmd.exe own
    # the redirection is more robust.
    cmd_str = (
        f'"{sys.executable}" -u "{BENCH_DIR / "instrumented_runner.py"}" '
        f'--config "{cfg}" --run-id "{run_id}" --archetype cost_optimal '
        f'> "{log_path}" 2>&1'
    )
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(cmd_str, shell=True, env=env)
    psproc = psutil.Process(proc.pid)
    peak_rss = 0

    try:
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
                print(f"  {run_id} exited rc={rc}; wall={time.time()-started:.1f}s peak_rss={peak_rss/(1024**3):.2f} GiB")
                return
            if time.time() - started > budget_s:
                print(f"  {run_id} hit budget {budget_s/60:.0f} min; killing")
                try:
                    psproc.kill()
                    for ch in psproc.children(recursive=True):
                        try:
                            ch.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                proc.wait(timeout=30)
                _capture_timeout_partial(run_id, started, budget_s, peak_rss)
                return
            time.sleep(5)
    except KeyboardInterrupt:
        print("  interrupted; killing subprocess")
        try:
            psproc.kill()
        except psutil.NoSuchProcess:
            pass
        raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_ids", nargs="+", help="Run ids like '02_nsw_2period'")
    args = ap.parse_args()

    for run_id in args.run_ids:
        # Allow short ids like "02" -> "02_nsw_2period"
        if "_" not in run_id:
            matches = sorted(CONFIGS_DIR.glob(f"{run_id}_*.yaml"))
            if not matches:
                print(f"No config matches '{run_id}'")
                continue
            run_id = matches[0].stem
        print(f"\n=== {run_id} ===")
        _run_one(run_id)


if __name__ == "__main__":
    main()
