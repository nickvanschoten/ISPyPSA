"""Parallel production runner — launches all archetypes as concurrent myopic sequences.

Spawns one run_myopic.py subprocess per archetype and monitors them until
completion or timeout. At the end, reads all JSON records and prints a summary
table. Any archetype with a failed period is flagged for manual Gurobi fallback.

Usage:
    uv run python mvp_pass1_power/bench/run_production.py \\
        --archetypes cost_optimal fast_fossil_exit gas_bridge \\
                     storage_led fossil_incumbent nuclear_included \\
        --periods 2025 2030 2035 2040 2045 2050 \\
        --budget-min 720 \\
        --gurobi-fallback

    # Default archetypes = all six production archetypes.
    uv run python mvp_pass1_power/bench/run_production.py --budget-min 720
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BENCH = Path(__file__).parent
RECORDS = BENCH / "records"
RUN_MYOPIC = BENCH / "run_myopic.py"

_STATUS_INTERVAL_S = 30


def _launch_archetype(
    archetype: str,
    run_id_prefix: str,
    periods: list[int],
    budget_min: float,
    regions_filter: str | None,
) -> subprocess.Popen:
    """Spawn run_myopic.py for one archetype and return the process handle."""
    run_id = f"{run_id_prefix}__{archetype}"
    cmd = [
        sys.executable, "-u", str(RUN_MYOPIC),
        "--run-id", run_id,
        "--archetype", archetype,
        "--periods", *[str(p) for p in periods],
        "--budget-min", str(budget_min),
    ]
    if regions_filter:
        cmd += ["--filter", regions_filter]
    log_path = BENCH / "logs" / f"{run_id}_production.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fh = open(log_path, "w")
    return subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh)


def _poll_all(procs: dict[str, subprocess.Popen]) -> dict[str, bool]:
    """Return {archetype: finished} for all tracked processes."""
    return {arch: proc.poll() is not None for arch, proc in procs.items()}


def _read_record(run_id_prefix: str, archetype: str) -> dict | None:
    """Load the JSON record written by run_myopic.py, or None if missing."""
    record_path = RECORDS / f"{run_id_prefix}__{archetype}.json"
    if not record_path.exists():
        return None
    return json.loads(record_path.read_text())


def _summarise_record(record: dict | None) -> dict:
    """Extract summary fields from a myopic sequence record."""
    if record is None:
        return {"total_wall_s": None, "periods_solved": 0, "periods_failed": 0,
                "peak_rss_gib": None, "failed_periods": []}
    per_period = record.get("per_period", {})
    solved = [y for y, r in per_period.items() if r.get("status") == "completed"]
    failed = [y for y, r in per_period.items() if r.get("status") != "completed"]
    return {
        "total_wall_s": record.get("cumulative_wall_clock_s"),
        "periods_solved": len(solved),
        "periods_failed": len(failed),
        "peak_rss_gib": record.get("peak_rss_gib_observed"),
        "failed_periods": failed,
    }


def _print_status(procs: dict[str, subprocess.Popen], finished: dict[str, bool]) -> None:
    """Print a one-line status for each archetype."""
    running = [a for a, done in finished.items() if not done]
    done = [a for a, done in finished.items() if done]
    print(f"  Running ({len(running)}): {running}")
    print(f"  Finished ({len(done)}): {done}")


def _print_summary_table(run_id_prefix: str, archetypes: list[str]) -> list[str]:
    """Print and return a list of archetypes with failed periods."""
    print("\n" + "=" * 72)
    print(f"{'Archetype':<22} {'Wall (s)':>10} {'Solved':>8} {'Failed':>8} {'Peak GiB':>10}")
    print("-" * 72)
    archetypes_needing_fallback = []
    for arch in archetypes:
        rec = _read_record(run_id_prefix, arch)
        s = _summarise_record(rec)
        wall = f"{s['total_wall_s']:.0f}" if s["total_wall_s"] is not None else "—"
        peak = f"{s['peak_rss_gib']:.1f}" if s["peak_rss_gib"] is not None else "—"
        print(f"{arch:<22} {wall:>10} {s['periods_solved']:>8} {s['periods_failed']:>8} {peak:>10}")
        if s["periods_failed"]:
            archetypes_needing_fallback.append(arch)
    print("=" * 72)
    return archetypes_needing_fallback


def _print_fallback_instructions(
    run_id_prefix: str, archetypes_needing_fallback: list[str], archetypes: list[str]
) -> None:
    """Print manual Gurobi retry commands for each archetype with failed periods."""
    if not archetypes_needing_fallback:
        return
    print("\nGurobi fallback required for:")
    for arch in archetypes_needing_fallback:
        rec = _read_record(run_id_prefix, arch)
        s = _summarise_record(rec)
        for year in s["failed_periods"]:
            run_id = f"{run_id_prefix}__{arch}_{year}"
            cfg = BENCH / "configs_myopic" / f"{run_id_prefix}__{arch}_{year}.yaml"
            print(f"  uv run python {BENCH / 'instrumented_runner.py'} \\")
            print(f"      --config \"{cfg}\" --run-id \"{run_id}\" \\")
            print(f"      --archetype {arch} --use-gurobi")


def _write_aggregate_record(
    run_id_prefix: str,
    archetypes: list[str],
    periods: list[int],
    wall_s: float,
) -> Path:
    """Write a top-level JSON record summarising the production run."""
    aggregate = {
        "run_id_prefix": run_id_prefix,
        "archetypes": archetypes,
        "periods": periods,
        "total_wall_s": wall_s,
        "started_at_iso": run_id_prefix.split("__")[-1] if "__" in run_id_prefix else None,
        "ended_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "per_archetype": {},
    }
    for arch in archetypes:
        rec = _read_record(run_id_prefix, arch)
        aggregate["per_archetype"][arch] = _summarise_record(rec)
    timestamp = run_id_prefix
    out_path = RECORDS / f"production_run_{timestamp}.json"
    out_path.write_text(json.dumps(aggregate, indent=2, default=str))
    return out_path


def main():
    from mvp_pass1_power.archetypes import PRODUCTION_ARCHETYPES

    ap = argparse.ArgumentParser()
    ap.add_argument("--archetypes", nargs="+", default=None,
                    help="Archetypes to run (default: all PRODUCTION_ARCHETYPES)")
    ap.add_argument("--periods", type=int, nargs="+",
                    default=[2025, 2030, 2035, 2040, 2045, 2050])
    ap.add_argument("--budget-min", type=float, default=720,
                    help="Per-period budget per archetype (default 12h)")
    ap.add_argument("--filter", default=None,
                    help="NEM region filter (e.g. 'NSW'); omit for full NEM")
    ap.add_argument("--gurobi-fallback", action="store_true",
                    help="Print Gurobi fallback commands for failed periods")
    args = ap.parse_args()

    archetypes = args.archetypes or PRODUCTION_ARCHETYPES
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id_prefix = timestamp

    print(f"Production run {run_id_prefix}")
    print(f"  Archetypes: {archetypes}")
    print(f"  Periods:    {args.periods}")
    print(f"  Budget/period: {args.budget_min} min")
    print(f"  Filter: {args.filter or 'full NEM'}")

    procs: dict[str, subprocess.Popen] = {}
    for arch in archetypes:
        print(f"  Launching {arch}...")
        procs[arch] = _launch_archetype(
            arch, run_id_prefix, args.periods, args.budget_min, args.filter
        )

    wall_start = time.time()
    print(f"\nAll {len(archetypes)} archetypes launched. Monitoring every {_STATUS_INTERVAL_S}s...\n")

    while True:
        time.sleep(_STATUS_INTERVAL_S)
        finished = _poll_all(procs)
        elapsed = time.time() - wall_start
        print(f"[{elapsed/60:.1f} min elapsed]")
        _print_status(procs, finished)
        if all(finished.values()):
            break

    total_wall_s = time.time() - wall_start
    print(f"\nAll archetypes completed in {total_wall_s/3600:.2f} h.")

    archetypes_needing_fallback = _print_summary_table(run_id_prefix, archetypes)
    out_path = _write_aggregate_record(run_id_prefix, archetypes, args.periods, total_wall_s)
    print(f"\nAggregate record: {out_path}")

    if args.gurobi_fallback:
        _print_fallback_instructions(run_id_prefix, archetypes_needing_fallback, archetypes)
    elif archetypes_needing_fallback:
        print(f"\nNote: {len(archetypes_needing_fallback)} archetype(s) have failed periods. "
              f"Re-run with --gurobi-fallback to see retry commands.")


if __name__ == "__main__":
    main()
