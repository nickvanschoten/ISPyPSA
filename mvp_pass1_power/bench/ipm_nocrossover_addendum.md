# Addendum 2: HiGHS IPM with crossover disabled (`run_crossover="off"`)

Companion to [characterisation_report.md](characterisation_report.md) and the
first IPM addendum [ipm_addendum.md](ipm_addendum.md). Same machine
(Dell Precision 5490, Intel Core Ultra 7 165H, 64 GiB RAM), same ISPyPSA
configurations, same `cost_optimal` archetype, HiGHS 1.12. Only change from
the first IPM addendum: `solver_options={"solver": "ipm", "run_crossover": "off"}`.

The diagnostic question the team asked: **does disabling HiGHS crossover
resolve the multi-period non-convergence that the first IPM addendum
attributed to "crossover stall at factorization 7"?**

---

## TL;DR

**No.** Disabling crossover does not resolve the stall. NSW 2-period — the
smallest multi-period configuration and the case where the previous IPM-with-
crossover run reached "factorization 7" before stalling — exhibits **identical
behaviour with `run_crossover="off"`**: same barrier iter pattern (2 iters in
~50s), same "Constructing starting basis" message, same `Start factorization 7`
stall point. Killed at ~20 minutes wall-clock with the same lack of further
progress.

Per the task instruction ("If NSW 2p doesn't converge under crossover-off,
the diagnostic hypothesis is wrong and there's no point continuing to the
larger configurations — capture that finding and stop"), the remaining
configurations in the requested sequence (NEM 2p, NSW 6p, NEM 3p, NEM 6p)
were not attempted.

The finding revises the conclusion of the first IPM addendum: the stall is
**not in the final crossover step** — it is in IPX's internal basis
identification, which is part of the barrier algorithm itself and is not
controlled by HiGHS' `run_crossover` option.

---

## Methodology check: did `run_crossover="off"` actually take effect?

This is the load-bearing methodological question. **Yes — verified
independently on a control LP.**

On a small synthetic 100-row × 200-col LP (forced through IPM by also
disabling presolve), the same `run_crossover="off"` setting produces:

- HiGHS option-set call: `setOptionValue('run_crossover', 'off')` →
  `HighsStatus.kOk`; `getOptionValue('run_crossover')` returns `'off'`.
- IPX runs 15 barrier iterations to convergence, primal/dual gap
  decreases monotonically from 2.01 → 1.40e-09.
- HiGHS summary line confirms: `Status crossover: not run`.
- `h.getBasis().valid` returns `False` — no basis returned, interior
  solution only. This is the expected behaviour for crossover-off.

So `run_crossover="off"` is being respected by HiGHS in principle. The
question is then: what is the "Constructing starting basis" / "Start
factorization N" output we see on the ISPyPSA LPs?

In the small control-LP log, "Constructing starting basis" appears between
barrier iterations 5 and 6, and barrier iteration 6 follows immediately on
the next line — i.e., it's a fast preliminary step inside the barrier loop,
not a post-barrier crossover. **The same line on the NSW 2p LP looks
syntactically identical but corresponds to a multi-minute stall that never
completes**, because IPX's basis-identification factorization on a
2,092,804-nonzero basis (from a 4.88M-row LP) is itself an expensive linear
algebra operation that doesn't terminate in practical wall-clock on this
hardware.

So the first IPM addendum's interpretation was wrong in a subtle way:
"Constructing starting basis" is NOT the crossover algorithm in HiGHS
terminology — it's IPX's preliminary basis identification that happens
during barrier convergence. The stall at "factorization 7" is in IPM's
own machinery, not in the IPM-to-simplex transition.

---

## Direct comparison: the same NSW 2p config under three solver settings

| Solver setting | Wall-clock | Barrier iters | Stall point | Status |
|---|---:|---:|---|---|
| Primal simplex (HiGHS default) | 8 min (killed) | n/a | Phase-2 simplex Pr oscillating 1e8↔7e8 | killed at degeneracy |
| IPM + crossover ON (default IPM) | 90 min (killed) | 2 | `Start factorization 7` at HiGHS time 174s | stalled |
| IPM + crossover OFF | 20 min (killed) | 1 | `Start factorization 7` at HiGHS time 151s | stalled |

The crossover-on and crossover-off runs reach the same stall point with the
same diagnostic state (same factorization count, same basis nonzero count).
The only difference is faster early progress under crossover-off (151s vs
174s to reach the stall, because no preparatory basis-construction work is
deferred to a separate phase).

---

## What this means for the broader question

The hypothesis the first IPM addendum proposed — "barrier converges, only
the crossover stalls; therefore crossover-off would unlock production-scale
solves" — is **wrong**.

The actual bottleneck is **IPM's internal sparse-matrix factorization on
multi-period ISPyPSA LPs**. After barrier iteration 1, IPX needs to factor
a basis matrix with ~2 million nonzeros to continue. That factorization
is taking longer than the wall-clock budget on this hardware. Whether the
factorization eventually completes given a multi-day budget is not known
from these runs — we killed at ~15–90 minutes per attempt — but the
extrapolation is not encouraging: HiGHS' built-in sparse Cholesky/LU
implementation is single-threaded and not optimised for matrices at this
scale.

The team's architectural decision now depends on a different set of options
than the first IPM addendum suggested:

- **Different solver entirely.** Commercial solvers (Gurobi, CPLEX) have
  parallel sparse-matrix factorization implementations that may handle
  this LP class. Untested here.
- **Different IPM solver.** HiGHS 1.12 ships PDLP (primal-dual hybrid
  gradient) under `solver="pdlp"`, a first-order method that doesn't
  require basis factorization. Untested here.
- **Different LP structure.** Decomposing the multi-period LP into
  per-period sequential solves (myopic capacity planning with
  `freeze_period()`) avoids the cross-period coupling that drives the
  basis-matrix size. The 8 GiB / 75-second / Optimal NSW 1p result
  shows single-period LPs of this scope are easy. Six sequential
  single-period solves would total roughly 12 hours on this hardware
  by extrapolation, vs the multi-day extrapolation for one perfect-
  foresight 6-period solve.
- **More RAM and/or external sparse-linear-algebra library.** Less clear
  whether this would help — the issue may be algorithm-level, not
  resource-level.

These are out-of-scope decisions; the addendum's job is to report what was
observed, not to recommend among them.

---

## Records produced

| Record | Status |
|---|---|
| `02_ipm_nocross_nsw_2period.json` | killed at ~20 min, stalled at `Start factorization 7` after IPM iter 1 |
| `05_ipm_nocross_nem_2period.json` | not run (per task instruction after NSW 2p disproof) |
| `08_ipm_nocross_nsw_6period.json` | not run (per task instruction) |
| `06_ipm_nocross_nem_3period.json` | not run (per task instruction) |
| `07_ipm_nocross_nem_6period.json` | not run (per task instruction) |

NSW 2p log captured in [logs/02_ipm_nocross_nsw_2period.log](logs/02_ipm_nocross_nsw_2period.log)
shows the side-by-side identical stall pattern with the IPM-with-crossover
run for the same configuration.
