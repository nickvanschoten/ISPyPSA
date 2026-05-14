"""Manually write a timed_out/killed record from a partially-completed run.

Used when a benchmark run is interrupted externally (chain killed, OS signal)
so the log file has data but no record was emitted by instrumented_runner.

Usage:
    uv run python mvp_pass1_power/bench/capture_partial.py 02_nsw_2period \
        --reason "killed externally — HiGHS not converging at default settings"
"""

import argparse
import json
import sys
import time
from pathlib import Path

BENCH = Path(__file__).parent
sys.path.insert(0, str(BENCH))
from instrumented_runner import _parse_highs_log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id")
    ap.add_argument("--reason", required=True)
    ap.add_argument("--wall-clock-s", type=float, default=None)
    ap.add_argument("--peak-rss-gib", type=float, default=None)
    args = ap.parse_args()

    log = BENCH / "logs" / f"{args.run_id}.log"
    rec_path = BENCH / "records" / f"{args.run_id}.json"
    if rec_path.exists():
        print(f"Record already exists at {rec_path}; refusing to overwrite")
        return

    info = {}
    if log.exists():
        text = log.read_text(errors="replace")
        info.update(_parse_highs_log(text))
        # Last iteration line — useful when status==killed since model_status
        # won't be set.
        text_clean = text.replace("\r", "\n")
        iters = [ln.strip() for ln in text_clean.split("\n")
                 if "Pr:" in ln or "Du:" in ln]
        if iters:
            info["last_iter_line"] = iters[-1][:200]

    record = {
        "run_id": args.run_id,
        "status": "killed",
        "kill_reason": args.reason,
        "started_at_iso": "(unknown; chain log was overwritten)",
        "wall_clock_s": args.wall_clock_s,
        "peak_rss_gib": args.peak_rss_gib,
        **info,
        "note": "Record reconstructed from log file after external kill. "
                "Per-stage timings are not available; HiGHS markers are from the "
                "captured log if any were emitted before the kill.",
    }
    rec_path.write_text(json.dumps(record, indent=2, default=str))
    print(f"Wrote {rec_path}")
    print(json.dumps(record, indent=2, default=str))


if __name__ == "__main__":
    main()
