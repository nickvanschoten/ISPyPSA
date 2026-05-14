"""Run a single config under default HiGHS simplex with external budget.

Mirrors run_one_ipm.py / run_one_pdlp.py but uses HiGHS defaults (primal
simplex). Used for Test 2: extended-budget NEM 2035 single-period.

Usage:
    uv run python mvp_pass1_power/bench/run_one_simplex.py \\
        --run-id 09_nem_1period_2035_extended \\
        --config mvp_pass1_power/bench/configs/09_nem_1period_2035.yaml \\
        --budget-min 240
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
    ap.add_argument("--budget-min", type=float, default=240)
    ap.add_argument("--archetype", default="cost_optimal")
    args = ap.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    RECORDS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / f"{args.run_id}.log"

    cmd_str = (
        f'"{sys.executable}" -u "{BENCH / "instrumented_runner.py"}" '
        f"--config \"{args.config}\" --run-id \"{args.run_id}\" "
        f"--archetype {args.archetype} "
        f'> "{log_path}" 2>&1'
    )
    print(f"Launching {args.run_id} (budget {args.budget_min:.0f} min, default simplex)")

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
            print(f"  {args.run_id} hit budget; killing")
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
                     if "Pr:" in ln or "Du:" in ln]
            record = {
                "run_id": args.run_id,
                "status": "timed_out",
                "wall_clock_s": time.time() - started,
                "wall_clock_budget_s": budget_s,
                "peak_rss_bytes": peak_rss,
                "peak_rss_gib": peak_rss / (1024**3),
                **info,
                "last_solver_line": iters[-1][:200] if iters else None,
            }
            (RECORDS / f"{args.run_id}.json").write_text(json.dumps(record, indent=2, default=str))
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
