"""Quick status snapshot of the bench chain.

Prints which configurations have records, which are in flight (have a log file
but no record), and the last interesting HiGHS marker for each.
"""

import json
import re
import time
from pathlib import Path

BENCH = Path(__file__).parent
RECORDS = BENCH / "records"
LOGS = BENCH / "logs"
CONFIGS_DIR = BENCH / "configs"


def _last_highs_marker(log_path: Path) -> str:
    if not log_path.exists():
        return "(no log)"
    try:
        text = log_path.read_text(errors="replace")
    except Exception:
        return "(unreadable)"
    text_clean = re.sub(r"\r", "\n", text)
    interesting = []
    for line in text_clean.split("\n"):
        if any(k in line for k in (
            "Model status", "HiGHS run time", "LP linopy-problem",
            "Simplex   iterations", "Objective value", "SOLVE EXCEPTION",
            "Iteration ", "Pr:", "Du:")):
            interesting.append(line.strip())
    if not interesting:
        return "(no HiGHS markers yet)"
    return interesting[-1][:120]


def _file_age_min(p: Path) -> float | None:
    if not p.exists():
        return None
    return (time.time() - p.stat().st_mtime) / 60


def main():
    run_ids = sorted(p.stem for p in CONFIGS_DIR.glob("*.yaml"))
    print(f"=== Bench chain status @ {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
    for rid in run_ids:
        rec = RECORDS / f"{rid}.json"
        log = LOGS / f"{rid}.log"
        if rec.exists():
            d = json.loads(rec.read_text())
            wall = d.get("wall_clock_s", 0)
            peak = d.get("peak_rss_gib", 0)
            status = d.get("status", "?")
            lp_rows = d.get("lp_rows")
            iters = d.get("simplex_iterations")
            print(f"  [DONE]  {rid}: status={status}; wall={wall:.0f}s; "
                  f"peak={peak:.1f} GiB; LP rows={lp_rows}; iters={iters}")
        elif log.exists():
            age = _file_age_min(log)
            print(f"  [LIVE]  {rid}: log age {age:.1f} min; last marker: {_last_highs_marker(log)}")
        else:
            print(f"  [WAIT]  {rid}: not started")
    print()


if __name__ == "__main__":
    main()
