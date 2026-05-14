---
name: ispypsa-bench
description: Run an ISPyPSA compute-envelope benchmark configuration and interpret HiGHS solver output. Use when asked to characterise solver performance, run a new bench config, debug an LP that doesn't converge, or extend the compute envelope tables. Covers primal simplex, IPM (with and without crossover), and PDLP (with relaxed tolerance).
---

Run and interpret ISPyPSA benchmark configurations under
[`mvp_pass1_power/bench/`](../../../mvp_pass1_power/bench/).

# When to use

- Re-running a bench configuration to verify a prior result.
- Adding a new spatial-extent / period-count configuration.
- Debugging an LP that doesn't converge: distinguish "still progressing"
  from "genuinely stuck".
- Extending the compute envelope tables in the addenda.
- Picking a solver path for a new use case based on the cumulative
  characterisation evidence.

# Layout

- **Configs**: [`bench/configs/*.yaml`](../../../mvp_pass1_power/bench/configs/)
  numbered 01–09. Each is a standalone ISPyPSA config; differences are
  scenario filter (NSW vs full NEM) and investment-period list.
- **Runner scripts**:
  - `run_one_simplex.py` — default HiGHS simplex
  - `run_one_ipm.py --no-crossover` — IPM, optionally without crossover
  - `run_one_pdlp.py --pdlp-tolerance 1e-3` — PDLP at relaxed tolerance
  - `run_chain.py` — sequence of configs with budget enforcement
  - `run_myopic.py` — sequential single-period chain with year-by-year
    capacity buildouts
- **Records**: `bench/records/*.json` — wall-clock, peak RSS, LP size,
  convergence status, sanity-check generation totals per run.
- **Logs**: `bench/logs/*.log` — full HiGHS solver stdout per run.

All three solver-specific runners share
[`instrumented_runner.py`](../../../mvp_pass1_power/bench/instrumented_runner.py)
which wraps the ISPyPSA workflow with per-stage timing and HiGHS log
parsing.

# Running a config

```bash
# Default simplex (HiGHS dual)
uv run python mvp_pass1_power/bench/run_one_simplex.py \
    --run-id 04_nem_1period \
    --config mvp_pass1_power/bench/configs/04_nem_1period.yaml \
    --budget-min 240

# IPM, default with crossover
uv run python mvp_pass1_power/bench/run_one_ipm.py \
    --run-id 02_ipm_nsw_2period \
    --config mvp_pass1_power/bench/configs/02_nsw_2period.yaml \
    --budget-min 60

# IPM without crossover
uv run python mvp_pass1_power/bench/run_one_ipm.py \
    --run-id 02_ipm_nocross_nsw_2period \
    --config mvp_pass1_power/bench/configs/02_nsw_2period.yaml \
    --no-crossover --budget-min 60

# PDLP at 1e-3 tolerance — the production-scale path
uv run python mvp_pass1_power/bench/run_one_pdlp.py \
    --run-id 05_pdlp_tol_3_nem_2period \
    --config mvp_pass1_power/bench/configs/05_nem_2period.yaml \
    --pdlp-tolerance 1e-3 --budget-min 180

# Myopic sequence (default simplex per period)
uv run python mvp_pass1_power/bench/run_myopic.py \
    --run-id nem_6p_myopic_v2 \
    --periods 2025 2030 2035 2040 2045 2050 \
    --budget-min 360
```

Each runner writes a JSON record to `bench/records/<run_id>.json` with
wall-clock, peak RSS, LP problem size, model status, solver iter count,
final pinf/dinf/gap, and annual-generation-by-period sanity check.

# Reading the HiGHS log

The solver-specific output that distinguishes "still progressing" from
"stuck" varies by algorithm.

## Primal simplex (default)

Phase 1 ("Ph1") and Phase 2 ("Pr:") iteration lines:

```
0    -2.7025830770e+10 Ph1: 41550(5.21496e+07); Du: 12096(2.70258e+07) 2s
50283    1.8072399019e+09 Pr: 21567(6.60216e+07); Du: 0(1.6512e-06) 7s
```

Three key signals:

1. **`Du: N(value)` where N decreases monotonically** — Phase 1 (dual
   infeasibility reduction) is working. Once `Du: 0(<1e-6)` appears,
   Phase 2 has started.
2. **`Pr:` value steadily decreasing** — Phase 2 converging.
3. **`Pr:` value oscillating between e.g. 1e8 and 1e11 with objective
   drifting** — degenerate Phase 2 on a multi-period LP. **This pattern
   does eventually converge on NEM single-period LPs given enough time
   (2035 in 22 min, 2050 in 2 h), but does NOT converge in practical
   wall-clock on NSW 2-period, NEM 2-period, or anything larger.**

**Rule of thumb**: If `Pr:` has been oscillating between two orders of
magnitude for >15 minutes, this is the degenerate pattern, and waiting
longer doesn't help for multi-period LPs.

## IPM (interior point)

IPM iteration table:

```
Iter       primal obj         dual obj       pinf       dinf       gap     time
0    8.56205215e+11  -1.39610557e+16   8.70e-02   4.24e-02  2.00e+00      10s
1    5.63549695e+12  -1.27288223e+16   7.93e-02   3.77e-02  2.00e+00      58s
```

Three IPM-specific signals:

1. **`Constructing starting basis...` followed by `Start factorization N`** —
   this is **IPX's internal basis identification during barrier**, NOT
   the final crossover. **`run_crossover=off` does NOT skip this step**
   on ISPyPSA LPs. Verified in
   [`ipm_nocrossover_addendum.md`](../../../mvp_pass1_power/bench/ipm_nocrossover_addendum.md).
2. **Factorization advancing** — `Start factorization 1` → `2` → ... over
   minutes is healthy. **`Start factorization 7` then silence for >30
   minutes** is the classic stall on this LP family; the basis matrix
   is numerically pathological.
3. **`Iter N` lines advancing with monotonic pinf/dinf decrease** —
   barrier is converging. On NSW 2p IPM ran 2 iters (10s, 58s) cleanly,
   then stalled at factorization 7 of the basis-id phase.

**Rule of thumb**: If you see `Start factorization 7` and no further log
output for >15 minutes, it's not going to finish. Kill and pivot to
PDLP or myopic.

## PDLP (primal-dual hybrid gradient)

PDLP iteration table:

```
Iter       Primal.Obj         Dual.Obj        Gap  Primal.Inf  Dual.Inf    Time
0  +1.75183138e+10  +1.75183138e+10  +0.00e+00    7.51e-01  9.29e-04    194s [L]
4000  +1.39856627e+11  +1.36033575e+11  +1.39e-02    2.85e-04  5.99e-04 612476s [A]
```

Format notes:

- **The `Time` column is in milliseconds, not seconds**, despite the
  `s` suffix. So `612476s` = 612 seconds = 10 min.
- **`[L]` vs `[A]`** — PDLP's progress metric mode. Both are normal.
- **Output is batch-buffered every 4000 iterations**. Between iter 0
  and iter 4000, PDLP runs silently for several minutes (depends on
  LP size). If the log seems frozen at iter 0 but the Python process
  is alive with stable RSS, PDLP is iterating internally — just wait.
- **NEM 2p iter rate is ~10 iter/sec**; NSW 2p is ~30 iter/sec.
  Expect first batch (iter 4000) at 200–400s.

PDLP convergence criterion (the option set this runner uses):

- `pdlp_optimality_tolerance = 1e-3`
- `primal_feasibility_tolerance = 1e-3`
- `dual_feasibility_tolerance = 1e-3`

PDLP declares Optimal when **all three** of pinf_rel, dinf_rel, gap_rel
are below the tolerance. Setting only `pdlp_optimality_tolerance` is
insufficient — HiGHS reports `kUnknown` and runs further. Verified on
a 100×200 control LP.

**Status reporting quirk**: even when PDLP converges all three metrics
below the tolerance on the NEM 2p ISPyPSA LP, HiGHS may still report
`model_status: Unknown` rather than `Optimal`. The solution is
mathematically converged — values in `network.generators_t.p` are
correct — but downstream code that gates on
`status == "Optimal"` will fail. Either override the status check or
expect this and treat `Unknown` with converged metrics as Optimal.

## Memory signal across all three solvers

The Python subprocess RSS, observed via
`Get-Process python | Select-Object WorkingSet64`:

- **Templating + translation phase**: 1–5 GB depending on config.
- **PyPSA `build_pypsa_network` + linopy `create_model`**: peaks
  during constraint serialisation; can hit 15–25 GB on NEM 2p+.
- **HiGHS solve phase**: typically drops after presolve, stable
  through iterations.

**If RSS is stable and HiGHS is silent**, the solver is compute-bound,
not waiting on I/O — wait longer or kill if past budget.

**If RSS is growing slowly**, HiGHS may be allocating fill-in for a
matrix factorization — likely will resolve in minutes if it's going
to resolve at all.

# Capturing partial records when a run is killed

If a budget hits or you kill manually, use:

```bash
uv run python mvp_pass1_power/bench/capture_partial.py \
    <run_id> --reason "..." --wall-clock-s <s> --peak-rss-gib <gib>
```

This parses the log file for the LP-size header, last solver-iter line,
and final pinf/dinf/gap, then writes a record consistent with the
runner-emitted records so `compile_report.py` produces a consistent
table.

# Compiling addenda

```bash
# Main characterisation report from all records
uv run python mvp_pass1_power/bench/compile_report.py

# IPM-vs-simplex addendum side-by-side
uv run python mvp_pass1_power/bench/compile_ipm_addendum.py
```

The other addenda (`ipm_nocrossover_addendum.md`,
`phase1_2_addendum.md`, `test1_test2_addendum.md`) are written by hand
because they include qualitative interpretation that doesn't auto-
generate well.

# What's already characterised

Don't re-run these unless you specifically need fresh data:

| Run | Solver | Outcome | File |
|---|---|---|---|
| NSW 1p | default simplex | Optimal in 75 s | `01_nsw_1period.json` |
| NSW 2p | simplex / IPM / IPM-no-cross / PDLP-default | all fail or partial | `02_*.json` |
| NSW 3p | simplex | killed @ 30 min | `03_nsw_3period.json` |
| NEM 1p (2050) | simplex | Optimal in 2 h | `04_nem_1period.json` |
| NEM 1p (2035) | simplex | Optimal in 22 min | `09_nem_1period_2035_extended.json` |
| NEM 2p | simplex / IPM / IPM-no-cross | all fail | `05_*.json` |
| **NEM 2p** | **PDLP at 1e-3** | **Optimal in 31 min** | `05_pdlp_tol_3_nem_2period.json` |
| NEM 3p | simplex / IPM | killed @ budget | `06_*.json` |
| NEM 6p | not run | extrapolated intractable | `07_*.json` |
| NSW 6p | myopic (6 × 1p) | ~9 min cumulative | `nsw_6p_myopic*.json` |
| NEM 6p | myopic 2025/2030 only | 2 of 6 periods completed | `nem_6p_myopic*.json` |

The gap to "production characterisation" is: NEM 6p PDLP at 1e-3 (not
yet run), and NEM 6p myopic full sequence (only 2 of 6 periods
completed at the time of writing).
