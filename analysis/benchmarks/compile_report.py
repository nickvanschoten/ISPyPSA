"""Compile the benchmark records into a characterisation report.

Reads every bench/records/*.json and produces:
  - characterisation_report.md  — human-readable, with the data table
  - characterisation_summary.csv — machine-readable row per run

Usage:
    uv run python analysis/benchmarks/compile_report.py
"""

from __future__ import annotations

import csv
import json
import platform
from pathlib import Path

import psutil

BENCH = Path(__file__).parent
RECORDS = BENCH / "records"
OUT_MD = BENCH / "characterisation_report.md"
OUT_CSV = BENCH / "characterisation_summary.csv"


def _machine_specs() -> dict:
    mem_total = psutil.virtual_memory().total
    cpu_count_logical = psutil.cpu_count(logical=True)
    cpu_count_physical = psutil.cpu_count(logical=False)
    try:
        cpu_freq_mhz = psutil.cpu_freq().max
    except Exception:
        cpu_freq_mhz = None
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or "(unknown)",
        "physical_cores": cpu_count_physical,
        "logical_cores": cpu_count_logical,
        "max_clock_mhz": cpu_freq_mhz,
        "total_ram_bytes": mem_total,
        "total_ram_gib": mem_total / (1024**3),
    }


def _load_records() -> list[dict]:
    records = []
    for p in sorted(RECORDS.glob("*.json")):
        try:
            rec = json.loads(p.read_text())
            # For timed-out runs, extract last simplex iteration line from log for
            # the team to see what HiGHS was doing when killed.
            if rec.get("status") in ("timed_out", "failed"):
                log_path = BENCH / "logs" / f"{rec['run_id']}.log"
                if log_path.exists():
                    text = log_path.read_text(errors="replace").replace("\r", "\n")
                    iter_lines = [ln for ln in text.split("\n")
                                  if "Pr:" in ln or "Du:" in ln]
                    if iter_lines:
                        rec["last_iter_line"] = iter_lines[-1][:200]
            records.append(rec)
        except Exception as e:
            print(f"  warning: could not parse {p}: {e}")
    return records


def _fmt(v, fmt="{:.1f}", default="—"):
    if v is None:
        return default
    try:
        return fmt.format(v)
    except (TypeError, ValueError):
        return str(v)


def _short_status(rec: dict) -> str:
    """Compact status label for the table."""
    status = rec.get("status", "?")
    if status == "completed":
        m = rec.get("model_status")
        return f"completed ({m})" if m else "completed"
    if status == "timed_out":
        return f"timed_out @ {rec.get('wall_clock_budget_s', 0)/60:.0f} min"
    if status == "failed":
        return f"failed: {rec.get('exception', 'unknown')}"[:60]
    return status


def _config_label(run_id: str) -> str:
    """Friendlier name for a config id like 02_nsw_2period."""
    parts = run_id.split("_", 1)
    if len(parts) != 2:
        return run_id
    rest = parts[1].replace("_", " ").replace("nem", "NEM").replace("nsw", "NSW")
    return rest.replace("period", "-period")


def _table_rows(records: list[dict]) -> list[list[str]]:
    rows = []
    for r in records:
        rows.append([
            r["run_id"],
            _config_label(r["run_id"]),
            _fmt(r.get("lp_rows"), "{:,d}"),
            _fmt(r.get("lp_cols"), "{:,d}"),
            _fmt(r.get("lp_nonzeros"), "{:,d}"),
            _fmt(r.get("wall_clock_s"), "{:.0f}s"),
            _fmt(r.get("solve_s"), "{:.0f}s"),
            _fmt(r.get("highs_run_time_s"), "{:.0f}s"),
            _fmt(r.get("simplex_iterations"), "{:,d}"),
            _fmt(r.get("peak_rss_gib"), "{:.1f} GiB"),
            _short_status(r),
        ])
    return rows


def _write_csv(records: list[dict]) -> None:
    """One row per run with the columns we'd want to analyse externally."""
    columns = [
        "run_id", "status", "wall_clock_s", "solve_s",
        "iasr_load_s", "templating_s", "translation_s", "pypsa_build_s",
        "save_network_s", "extract_results_s",
        "peak_rss_gib", "lp_rows", "lp_cols", "lp_nonzeros",
        "highs_run_time_s", "simplex_iterations", "model_status",
        "objective_value", "annual_generation_mwh_by_period",
    ]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = {c: r.get(c) for c in columns}
            if isinstance(row.get("annual_generation_mwh_by_period"), dict):
                row["annual_generation_mwh_by_period"] = json.dumps(row["annual_generation_mwh_by_period"])
            w.writerow(row)


def _write_md(machine: dict, records: list[dict]) -> None:
    lines = []
    lines += [
        "# ISPyPSA compute-envelope characterisation",
        "",
        "Systematic measurement of ISPyPSA solve runtime, memory, LP size,",
        "and convergence across seven progressively larger configurations on the",
        "user's local hardware.",
        "",
        "All runs: `cost_optimal` archetype (Step Change, no archetype mutation);",
        "HiGHS solver at default settings; 30-min snapshot resolution;",
        "single representative week (`residual-peak-demand`); reference year 2018.",
        "Only spatial extent and number of investment periods vary across configs.",
        "",
        "## Hardware",
        "",
        f"- Platform: {machine['platform']}",
        f"- Processor: {machine['processor']}",
        f"- Physical cores: {machine['physical_cores']}; logical: {machine['logical_cores']}",
        f"- Max clock: {_fmt(machine['max_clock_mhz'], '{:.0f}')} MHz",
        f"- RAM: {machine['total_ram_gib']:.1f} GiB total",
        "",
        "## Results",
        "",
        "| run_id | config | LP rows | LP cols | LP nonzeros | wall | solve | HiGHS | simplex iters | peak RSS | status |",
        "|--------|--------|--------:|--------:|------------:|-----:|------:|------:|--------------:|---------:|--------|",
    ]
    for row in _table_rows(records):
        lines.append("| " + " | ".join(row) + " |")

    lines += [
        "",
        "## Per-stage timing",
        "",
        "Wall-clock seconds spent in each ISPyPSA pipeline stage. `solve` is the",
        "linopy/HiGHS LP solve; everything else is data plumbing.",
        "",
        "| run_id | iasr_load | templating | translation | pypsa_build | solve | save | extract |",
        "|--------|----------:|-----------:|------------:|------------:|------:|-----:|--------:|",
    ]
    for r in records:
        lines.append("| " + " | ".join([
            r["run_id"],
            _fmt(r.get("iasr_load_s"), "{:.1f}s"),
            _fmt(r.get("templating_s"), "{:.1f}s"),
            _fmt(r.get("translation_s"), "{:.1f}s"),
            _fmt(r.get("pypsa_build_s"), "{:.1f}s"),
            _fmt(r.get("solve_s"), "{:.0f}s"),
            _fmt(r.get("save_network_s"), "{:.1f}s"),
            _fmt(r.get("extract_results_s"), "{:.1f}s"),
        ]) + " |")

    lines += [
        "",
        "## Output sanity check",
        "",
        "Total annual generation (TWh) at each investment period — confirms each",
        "run reached an output that's recognisable as a NEM-scale electricity system",
        "rather than terminating partway.",
        "",
    ]
    for r in records:
        gen = r.get("annual_generation_mwh_by_period")
        if isinstance(gen, dict) and gen:
            twh = {int(k): v / 1e6 for k, v in gen.items()}
            twh_str = ", ".join(f"{y}: {v:.1f} TWh" for y, v in sorted(twh.items()))
            lines.append(f"- **{r['run_id']}**: {twh_str}")
        elif r.get("status") == "timed_out":
            lines.append(f"- **{r['run_id']}**: did not produce output (timed out)")
        elif r.get("status") == "failed":
            lines.append(f"- **{r['run_id']}**: did not produce output (failed)")
        else:
            lines.append(f"- **{r['run_id']}**: (no sanity-check data)")

    lines += [
        "",
        "## Scaling commentary",
        "",
        "_See the `Honest envelope answer` section below for the team-facing summary._",
        "",
        "## Where time and memory go",
        "",
        "Inspection of per-stage timing (see table above) shows where the runtime",
        "budget is actually spent at each problem scale.",
        "",
        "## Honest envelope answer",
        "",
        "_Populated by hand after the runs complete — see the final section of the",
        "report file_.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main():
    machine = _machine_specs()
    records = _load_records()
    if not records:
        print("No records yet. Run benchmarks first.")
        return
    _write_csv(records)
    _write_md(machine, records)
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
