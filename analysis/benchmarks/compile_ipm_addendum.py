"""Compile the IPM-vs-primal-simplex side-by-side addendum.

Reads bench/records/*.json (both simplex baseline and *_ipm_* variants) and
emits ipm_addendum.md with a unified comparison table.
"""

import json
import re
from pathlib import Path

BENCH = Path(__file__).parent
RECORDS = BENCH / "records"
OUT = BENCH / "ipm_addendum.md"


def _short_label(rid: str) -> str:
    # 02_nsw_2period -> NSW 2-period; 02_ipm_nsw_2period -> NSW 2-period (IPM)
    is_ipm = "_ipm_" in rid
    base = rid.replace("_ipm_", "_")
    parts = base.split("_", 1)[1] if "_" in base else base
    parts = parts.replace("_", " ").replace("nem", "NEM").replace("nsw", "NSW")
    parts = parts.replace("period", "-period")
    return parts + (" (IPM)" if is_ipm else "")


def _fmt(v, fmt="{:.0f}"):
    if v is None:
        return "—"
    try:
        return fmt.format(v)
    except (TypeError, ValueError):
        return str(v)


def _short_status(r: dict) -> str:
    status = r.get("status", "?")
    if status == "completed":
        m = r.get("model_status")
        return f"OPTIMAL" if m == "Optimal" else f"completed ({m})"
    if status == "timed_out":
        return f"timed_out @ {r.get('wall_clock_budget_s', 0)/60:.0f} min"
    if status in ("killed", "failed"):
        return status
    return status


def _last_marker(rid: str) -> str | None:
    log = BENCH / "logs" / f"{rid}.log"
    if not log.exists():
        return None
    text = log.read_text(errors="replace").replace("\r", "\n")
    keys = ("Pr:", "Du:", "barrier", "IPM iter", "crossover", "Model status",
            "HiGHS run", "Objective value")
    matches = [ln.strip() for ln in text.split("\n") if any(k in ln for k in keys)]
    return matches[-1][:160] if matches else None


def _classify(rid: str) -> str:
    """Return 'simplex' / 'ipm' for sorting/grouping."""
    return "ipm" if "_ipm_" in rid else "simplex"


def _config_id(rid: str) -> str:
    """Strip the IPM marker for direct comparison: 02_ipm_nsw_2period -> 02_nsw_2period."""
    return rid.replace("_ipm_", "_")


def main():
    records = {}
    for p in sorted(RECORDS.glob("*.json")):
        try:
            records[p.stem] = json.loads(p.read_text())
        except Exception:
            continue

    # Group: each config has a baseline (simplex) record and an IPM record.
    pairs = {}
    for rid, rec in records.items():
        cid = _config_id(rid)
        kind = _classify(rid)
        pairs.setdefault(cid, {})[kind] = rec

    lines = [
        "# Addendum to ISPyPSA compute-envelope characterisation",
        "## IPM vs primal simplex — same configs, only solver algorithm varied",
        "",
        "**Question:** does HiGHS interior-point method (IPM) resolve the EKK",
        "primal-simplex degeneracy observed on multi-investment-period ISPyPSA LPs?",
        "",
        "**Setup:** identical configurations to the primal-simplex baseline runs",
        "(cost_optimal archetype, AEMO 2024 IASR Step Change, 30-min snapshots,",
        "single representative week, same Dell Precision 5490 / 64 GiB RAM /",
        "HiGHS 1.12). Only the HiGHS solver-algorithm option changed:",
        "default simplex → `solver=\"ipm\"`. Crossover left on (default).",
        "",
        "## Direct comparison",
        "",
        "| Config | Solver | LP rows | wall | HiGHS solve | iters/IPM-iters | peak RSS | status |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for cid in sorted(pairs):
        for kind in ("simplex", "ipm"):
            r = pairs[cid].get(kind)
            if r is None:
                continue
            label = "primal simplex" if kind == "simplex" else "IPM"
            lines.append(
                f"| {_short_label(cid)} | {label} | "
                f"{_fmt(r.get('lp_rows'), '{:,d}')} | "
                f"{_fmt(r.get('wall_clock_s'), '{:.0f}s')} | "
                f"{_fmt(r.get('highs_run_time_s'), '{:.0f}s')} | "
                f"{_fmt(r.get('simplex_iterations'), '{:,d}')} | "
                f"{_fmt(r.get('peak_rss_gib'), '{:.1f} GiB')} | "
                f"{_short_status(r)} |"
            )

    # Per-IPM-run last solver marker (the team needs this when status is not Optimal).
    ipm_runs = {rid: rec for rid, rec in records.items() if "_ipm_" in rid}
    lines += [
        "",
        "## Last solver line seen (for non-Optimal IPM runs)",
        "",
    ]
    for rid in sorted(ipm_runs):
        r = ipm_runs[rid]
        if r.get("model_status") == "Optimal":
            continue
        last = _last_marker(rid)
        lines.append(f"- **{rid}** ({_short_status(r)}): `{last}`" if last
                     else f"- **{rid}** ({_short_status(r)}): no solver line captured")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
