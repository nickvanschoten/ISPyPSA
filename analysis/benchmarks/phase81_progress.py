"""Phase 8.1 live progress dashboard.

Reads:
  - records/*.json — completed runs
  - logs/*.log — in-progress runs (parses cuPDLP-C iteration table)

Prints a snapshot of where Test 2 + variance sub-study tracks stand.
"""
import json
import re
import time
from datetime import datetime
from pathlib import Path

BENCH = Path("analysis/benchmarks")
LOGS = BENCH / "logs"
RECORDS = BENCH / "records"

PDLP_ITER_RE = re.compile(
    r"^\s*(\d+)\s+([+-]?\d\.\d+e[+-]?\d+)\s+([+-]?\d\.\d+e[+-]?\d+)\s+"
    r"([+-]?\d\.\d+e[+-]?\d+)\s+([+-]?\d\.\d+e[+-]?\d+)\s+([+-]?\d\.\d+e[+-]?\d+)\s+(\S+)"
)
GUROBI_BARRIER_RE = re.compile(
    r"^\s*(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)s"
)
GUROBI_SOLVED_RE = re.compile(
    r"Solved in (\d+) iterations and ([\d.]+) seconds"
)


def _parse_pdlp_latest(log_path: Path) -> dict | None:
    if not log_path.exists():
        return None
    text = log_path.read_text(errors="replace")
    # Find latest iter line
    iters = []
    for line in text.split("\n"):
        m = PDLP_ITER_RE.match(line)
        if m:
            try:
                iters.append({
                    "iter": int(m.group(1)),
                    "pobj": float(m.group(2)),
                    "dobj": float(m.group(3)),
                    "gap": float(m.group(4)),
                    "pinf": float(m.group(5)),
                    "dinf": float(m.group(6)),
                })
            except ValueError:
                pass
    if not iters:
        return None
    return iters[-1]


def _parse_gurobi_latest(log_path: Path) -> dict | None:
    if not log_path.exists():
        return None
    text = log_path.read_text(errors="replace")
    # Find latest barrier iter (lines like "  60   1.34e+10  1.34e+10  3.08e-04 ...    29s")
    barr_iters = []
    in_barrier = False
    for line in text.split("\n"):
        if "Barrier statistics" in line:
            in_barrier = True; continue
        if in_barrier and "Barrier solved" in line:
            in_barrier = False; continue
        if in_barrier:
            m = GUROBI_BARRIER_RE.match(line)
            if m:
                barr_iters.append({"iter": int(m.group(1)), "time_s": int(m.group(7))})
    out = {"phase": "running"}
    if barr_iters:
        out["barrier_latest_iter"] = barr_iters[-1]["iter"]
        out["barrier_latest_time_s"] = barr_iters[-1]["time_s"]
    if "Barrier solved" in text:
        out["barrier"] = "complete"
    if "Crossover log" in text:
        out["phase"] = "crossover"
    m = GUROBI_SOLVED_RE.search(text)
    if m:
        out["phase"] = "complete"
        out["crossover_iters"] = int(m.group(1))
        out["crossover_time_s"] = float(m.group(2))
    return out


def _show_completed(run_id: str, label: str, kind: str = "pdlp") -> None:
    rec_path = RECORDS / f"{run_id}_2040.json"
    if not rec_path.exists():
        return None
    r = json.loads(rec_path.read_text())
    obj = r.get("objective_value")
    return {
        "label": label,
        "status": r.get("model_status"),
        "wall_s": r.get("wall_clock_s"),
        "solver_s": r.get("highs_run_time_s") if kind == "pdlp" else r.get("gurobi_solver_time_s"),
        "iters": r.get("pdlp_iterations") if kind == "pdlp" else r.get("gurobi_iterations"),
        "gap_rel": r.get("pdlp_final_gap_rel"),
        "objective": obj,
        "completed": True,
    }


def _show_running(run_id: str, label: str, kind: str = "pdlp") -> dict | None:
    log_path = LOGS / f"{run_id}_2040.log"
    if not log_path.exists():
        return None
    mtime_age = time.time() - log_path.stat().st_mtime
    if kind == "pdlp":
        latest = _parse_pdlp_latest(log_path)
        if latest is None:
            return {"label": label, "phase": "pre-iter", "log_age_s": mtime_age, "completed": False}
        return {
            "label": label,
            "phase": "PDLP iterating",
            "iter": latest["iter"],
            "gap_rel": latest["gap"],
            "pinf_rel": latest["pinf"],
            "dinf_rel": latest["dinf"],
            "log_age_s": mtime_age,
            "completed": False,
        }
    else:
        s = _parse_gurobi_latest(log_path)
        if s is None:
            return {"label": label, "phase": "pre-iter", "log_age_s": mtime_age, "completed": False}
        s["label"] = label
        s["log_age_s"] = mtime_age
        s["completed"] = (s.get("phase") == "complete")
        return s


def main():
    now = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'='*92}")
    print(f"Phase 8.1 Test 2 + variance sub-study — progress at {now}")
    print(f"{'='*92}")

    # Test 2 — Track A
    print("\n┌─ Track A: Test 2 (4-week LP) ──────────────────────────────────────────────────────────────┐")
    pdlp = _show_completed("p81t2_pdlp", "PDLP-1e-3 4-week", kind="pdlp")
    if pdlp:
        print(f"  ✓ {pdlp['label']:<28} {pdlp['status']:<10} obj=${pdlp['objective']:>14,.0f}  wall={pdlp['wall_s']:.0f}s  iters={pdlp['iters']}  gap={pdlp['gap_rel']:.2e}")
    else:
        p = _show_running("p81t2_pdlp", "PDLP-1e-3 4-week", kind="pdlp")
        if p:
            if p.get("iter") is not None:
                print(f"  ◌ {p['label']:<28} PDLP iter {p['iter']:<6}  gap_rel={p['gap_rel']:.2e}  pinf_rel={p['pinf_rel']:.2e}  dinf_rel={p['dinf_rel']:.2e}  log_age={p['log_age_s']:.0f}s")
            else:
                print(f"  ◌ {p['label']:<28} {p['phase']}  log_age={p['log_age_s']:.0f}s")
        else:
            print(f"  · PDLP-1e-3 4-week              not started")

    gurobi = _show_completed("p81t2_gurobi", "Gurobi 4-week", kind="gurobi")
    if gurobi:
        print(f"  ✓ {gurobi['label']:<28} {gurobi['status']:<10} obj=${gurobi['objective']:>14,.0f}  wall={gurobi['wall_s']:.0f}s  iters={gurobi['iters']}")
    else:
        g = _show_running("p81t2_gurobi", "Gurobi 4-week", kind="gurobi")
        if g:
            print(f"  ◌ {g['label']:<28} phase={g.get('phase','?')}  barrier_iter={g.get('barrier_latest_iter','-')}  log_age={g.get('log_age_s',0):.0f}s")
        else:
            print(f"  · Gurobi 4-week                 not started (queued after PDLP)")
    print("└─────────────────────────────────────────────────────────────────────────────────────────────┘")

    # Variance sub-study — Track B
    print("\n┌─ Track B: PDLP variance sub-study (5× PDLP-1e-3 on Test 1 v2 LP) ───────────────────────────┐")
    for i in range(1, 6):
        rid = f"p81vs_pdlp_r{i}"
        done = _show_completed(rid, f"Run {i}/5", kind="pdlp")
        if done:
            print(f"  ✓ {done['label']:<8} {done['status']:<10} obj=${done['objective']:>14,.0f}  wall={done['wall_s']:.0f}s  iters={done['iters']}  gap_rel={done['gap_rel']:.2e}")
        else:
            p = _show_running(rid, f"Run {i}/5", kind="pdlp")
            if p and p.get("iter") is not None:
                print(f"  ◌ {p['label']:<8} PDLP iter {p['iter']:<6}  gap_rel={p['gap_rel']:.2e}  log_age={p['log_age_s']:.0f}s")
            elif p:
                print(f"  ◌ {p['label']:<8} {p['phase']}  log_age={p['log_age_s']:.0f}s")
            else:
                print(f"  · Run {i}/5                  pending")
    print("└─────────────────────────────────────────────────────────────────────────────────────────────┘")

    # Reference: Test 1 v2 for orientation
    print("\nReference — Test 1 v2 (3-week LP) baseline:")
    pdlp_ref = _show_completed("p81t1_pdlp_v2", "PDLP-1e-3 3-week", kind="pdlp")
    gur_ref = _show_completed("p81t1_gurobi_v2", "Gurobi 3-week", kind="gurobi")
    if pdlp_ref:
        print(f"  PDLP-1e-3 3-week:    obj=${pdlp_ref['objective']:>14,.0f}  wall={pdlp_ref['wall_s']:.0f}s  iters={pdlp_ref['iters']}  gap={pdlp_ref['gap_rel']:.2e}")
    if gur_ref:
        print(f"  Gurobi 3-week:       obj=${gur_ref['objective']:>14,.0f}  wall={gur_ref['wall_s']:.0f}s  crossover_iters={gur_ref['iters']}")

if __name__ == "__main__":
    main()
