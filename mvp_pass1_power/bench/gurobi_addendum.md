# Addendum 5: Gurobi at default settings on the production-scale LPs

Companion to [characterisation_report.md](characterisation_report.md), the
first [ipm_addendum.md](ipm_addendum.md), the second
[ipm_nocrossover_addendum.md](ipm_nocrossover_addendum.md), the third
[phase1_2_addendum.md](phase1_2_addendum.md), and the fourth
[test1_test2_addendum.md](test1_test2_addendum.md).

Same ISPyPSA configurations, same `cost_optimal` archetype. Two material
differences from prior addenda:

- **Solver**: Gurobi 11.0.3 at default settings (default algorithm choice,
  default tolerances, no parameter tuning) instead of HiGHS.
- **Hardware** *(unavoidable, see caveat below)*: a CSIRO Windows Server
  2019 node — dual Intel Xeon Platinum 8260 (24 cores / 48 threads × 2 =
  **48 physical / 96 logical**), **1,024 GiB RAM**, Gurobi using up to 32
  threads by default. Prior addenda ran on a Dell Precision 5490 laptop
  (Intel Core Ultra 7 165H, 16 physical / 22 logical, 64 GiB RAM).

The hardware change is significant. The Xeon machine has ~3× the logical
core count and ~16× the RAM of the laptop, and Gurobi parallelises barrier
and concurrent solves across all available threads by default. The
wall-clock numbers reported here cannot be directly attributed to "Gurobi
vs HiGHS" — they reflect "Gurobi-on-server vs HiGHS-on-laptop". This
addendum makes the comparison anyway because it is the comparison the team
asked for, but reads any wall-clock claim with that caveat in mind. The
qualitative findings (convergence behaviour, model-status reporting,
numerical solution agreement) are not hardware-dependent.

The three configurations match the previously-characterised paths so the
comparison is direct rather than reconstructed:

- **NSW 2-period** (4.88M-row LP) — where HiGHS primal simplex degenerated
  and HiGHS IPM stalled at IPX basis-factor 7.
- **NEM 2-period** (16.2M-row LP) — where PDLP at 1e-3 tolerance solved in
  31 min wall but reported `model_status: Unknown`.
- **NEM 6-period** (production-equivalent stretch goal) — PDLP-1e-3
  extrapolated to 90–120 min, myopic-sequential extrapolated to ~4 h.

---

## TL;DR

**Mixed result.** Gurobi at default settings solves NSW 2-period (~9 min)
and NEM 2-period (~43 min) to formal `Optimal` status, resolving both
methodological friction points (the PDLP `Unknown` status quirk and the
perfect-foresight multi-period tractability dead-end). **However, Gurobi
default did NOT converge on NEM 6-period within a 4 h wall budget** — it
reached barrier iteration 13 with primal infeasibility dropped 5 orders
of magnitude (4.55e+7 → 8.86e+2) and complementarity gap dropped 5
orders (8.36e+12 → 2.09e+8), but primal/dual objectives were still
spread by 3.87e+15 — relative gap ratio ~3.7, eight orders of magnitude
above Gurobi's default 1e-8 convergence threshold. The solution at the
kill point is *not* a usable basic-optimal — Gurobi never reached
crossover, never emitted a basic solution, and the runner did not save a
NetCDF. The per-iteration cost grew dramatically with each barrier
iteration (4 min → 32 min) as the Cholesky factorisations approached the
central path boundary.

**Numerical agreement is strong where Gurobi did converge.** NEM 2-period
Gurobi vs PDLP-1e-3: objective within 0.02 %, NEM 2030 TWh within 0.3 %,
NEM 2050 TWh within 0.5 %. **The 2030 NEM 36 % consumption over-shoot vs
AEMO published Step Change replicates under Gurobi** (274.8 TWh) at
essentially the same level as under PDLP-1e-3 (274.1 TWh) — confirming
that finding is data-side (IASR projections vs AEMO Overview rounded
headlines), not solver-side (PDLP precision).

The wall-clock-on-hardware combination here is not directly comparable to
the prior-addendum laptop numbers — the bench server is substantially
faster (96-thread Xeon vs 22-thread Core Ultra 7, 1024 GiB vs 64 GiB) —
so the wall-clock numbers are best read as "what Gurobi can do on
production-class hardware". The qualitative findings (formal Optimal status,
numerical agreement, tolerance behaviour) are not hardware-dependent.

---

## Methodology verification

### Solver binding

Gurobi accessed via `gurobipy==11.0.3` installed into the uv-managed venv
via `uv add 'gurobipy>=11,<12'`. The Gurobi 11.x version was chosen because
the CSIRO token-server licence (`license2-d61-cdc.it.csiro.au`, port 41954,
config at `c:\gurobi\gurobi.lic`) returned `Request denied: license not
valid for Gurobi version 13` against the PyPI-default `gurobipy==13.0.2`,
but issued a valid token for `gurobipy==11.0.3`. The system also has
Gurobi 9.5.2 installed natively at `C:\gurobi\gurobi952`; that install was
not invoked by the Python pipeline since gurobipy 11.x ships its own
self-contained Gurobi shared library.

### Verified that Gurobi is actually being invoked

The same discipline as the prior addenda's PDLP and IPM-no-crossover
verification (control LP before production LP):

```
import linopy, numpy as np, pandas as pd
# Build a 100-row × 200-col random dense LP, solve via linopy
m = linopy.Model(); x = m.add_variables(...); m.add_constraints((A*x).sum("i") >= b); m.add_objective(c*x.sum())
m.solve(solver_name="gurobi")
```

produces:

```
Read LP format model from file C:\Users\.../linopy-problem-...lp
Reading time = 0.02 seconds
obj: 100 rows, 200 columns, 20000 nonzeros
Gurobi Optimizer version 11.0.3 build v11.0.3rc0 (win64 - Windows Server 2019.0)
CPU model: Intel(R) Xeon(R) Platinum 8260 CPU @ 2.40GHz
Thread count: 48 physical cores, 96 logical processors, using up to 32 threads
Optimal objective -1.351493463e+03
Solved in 148 iterations and 0.02 seconds
Status: ok
```

Gurobi 11.0.3 is the binary doing the solve (not HiGHS); the token server
authenticated; the CPU and thread-count line is captured in every
production-LP log so this can be re-confirmed per run.

### Runner setup

`mvp_pass1_power/bench/run_one_gurobi.py` mirrors the existing
`run_one_simplex.py` / `run_one_pdlp.py` / `run_one_ipm.py` pattern. The
instrumented runner gained a `--use-gurobi` flag that overrides
`config.solver` to `"gurobi"`. No `solver_options` are passed — pure Gurobi
defaults. The HiGHS log parser was extended with Gurobi-format regexes
(`Optimize a model with N rows`, `Barrier solved model in N iterations and
X seconds`, `Solved in N iterations and X seconds`, `Optimal objective`).
The same `instrumented_runner.py` per-stage timing and the same
`run_one_*.py` budget-guard / peak-RSS poller pattern is used so the
metrics line up with prior addenda.

---

## Configuration 1 — NSW 2-period

**LP**: 4,877,350 rows × 2,250,776 cols × 8,935,253 nonzeros — *exactly*
the same LP as in [characterisation_report.md](characterisation_report.md)
and [ipm_addendum.md](ipm_addendum.md).

**Result**: **Optimal in 518 s wall (291 s solve, 216 s Gurobi solver
time)** after barrier + crossover.

| Metric | Value |
|---|---:|
| Wall-clock | 518 s (8.6 min) |
| Solve stage wall | 291 s (4.8 min) |
| Gurobi solver time | 216 s (3.6 min) |
| Barrier iterations | 80 |
| Barrier time | 178 s |
| Crossover (simplex) iterations | 501,999 |
| Crossover time | ~38 s |
| Peak RSS | 6.4 GiB |
| Model status | **Optimal** |
| Objective value | 5.629310123e+10 AUD |
| Final pinf | 5.3e-10 |
| Final dinf | 3.24e-7 |
| Final gap | 9.04e-11 |
| Annual generation NSW 2030 | 93.1 TWh |
| Annual generation NSW 2050 | 104.0 TWh |

**Capacity by fuel (GW active in period)**:

| Year | wind+solar | gas | coal | storage | hydro | biomass |
|---:|---:|---:|---:|---:|---:|---:|
| 2030 | 12.82 | 3.11 | 5.42 | 5.68 | 2.53 | 0.00 |
| 2050 | 29.60 | 1.79 | 0.00 | 13.33 | 2.53 | 1.64 |

**Comparison to prior addenda for the same LP**:

| Solver setting | Wall-clock | Iters | Final convergence | Status |
|---|---:|---:|---|---|
| HiGHS primal simplex (laptop) | 8 min (killed) | n/a | Pr oscillating 1e8↔7e8 | killed at degeneracy |
| HiGHS IPM + crossover ON (laptop) | 90 min (killed) | 2 barrier iters | stalled at `Start factorization 7` | stalled |
| HiGHS IPM + crossover OFF (laptop) | 20 min (killed) | 1 barrier iter | same `factorization 7` stall | stalled |
| HiGHS PDLP default tolerance (laptop) | 180 min budget / 61 min active | 100,000 PDLP iters | pinf 2.06e-6 / dinf 8.81e-6 / gap 2.50e-4 | timed_out (gap above 1e-4 threshold) |
| **Gurobi default (server)** | **8.6 min** | **80 barrier + 502K simplex** | **pinf 5.3e-10 / dinf 3.2e-7 / gap 9.0e-11** | **Optimal** |

Gurobi's NSW 2050 capacity result (wind+solar 29.60 GW, gas 1.79 GW, coal
0, storage 13.33 GW, hydro 2.53 GW, biomass 1.64 GW) is **identical to
within 0.1 GW per fuel** to the myopic-single-period NSW 2050 result
reported in [phase1_2_addendum.md](phase1_2_addendum.md). Annual generation
NSW 2050 = 104.0 TWh matches the myopic figure (104.0 TWh) and the original
single-period 2050 bench (106.4 TWh) within 2.3 %.

---

## Configuration 2 — NEM 2-period

**LP**: 16,234,604 rows × 7,505,085 cols × 30,017,864 nonzeros — *exactly*
the same LP that defeated primal simplex, IPM-with-crossover, IPM-no-
crossover, and default-tolerance PDLP in prior addenda, and that PDLP-1e-3
in [test1_test2_addendum.md](test1_test2_addendum.md) handled in 31 min
wall.

**Result**: **Optimal in 2589 s wall (2137 s solve, 1878 s Gurobi solver
time)** after barrier + crossover.

| Metric | Value |
|---|---:|
| Wall-clock | 2589 s (43.2 min) |
| Solve stage wall | 2137 s (35.6 min) |
| Gurobi solver time | 1878 s (31.3 min) |
| Barrier iterations | 148 |
| Barrier time | 1401 s (23.3 min) |
| Crossover (simplex) iterations | 1,829,010 |
| Crossover time | ~478 s (7.97 min) |
| Peak RSS | 22.2 GiB |
| Model status | **Optimal** |
| Objective value | 1.400268290e+11 AUD |
| Final pinf | 4.16e-5 |
| Final dinf | 4.2e-7 |
| Final gap | 8.02e-5 |
| Annual generation NEM 2030 | 274.8 TWh |
| Annual generation NEM 2050 | 305.6 TWh |

**Capacity by fuel (NEM-wide GW active in period)**:

| Year | wind+solar | gas | coal | storage | hydro | biomass | other |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2030 | 39.68 | 11.05 | 16.23 | 14.62 |  7.65 | 0.00 | 0.53 |
| 2050 | 92.00 |  2.81 |  1.70 | 33.32 |  9.50 | 2.31 | 0.10 |

**Comparison to prior addenda for the same LP**:

| Solver setting | Wall | HiGHS/Gurobi solver time | Iters | Convergence | Status |
|---|---:|---:|---:|---|---|
| HiGHS primal simplex (laptop) | 30 min (killed) | killed mid-Phase-2 | n/a | Pr 1e8↔1e9 oscillating | killed |
| HiGHS IPM + crossover ON (laptop) | 60 min (killed) | stuck in presolve | 0 | never reached barrier | killed |
| HiGHS PDLP at 1e-3 tolerance (laptop) | 31 min | 23 min | 8,840 PDLP | pinf 2.56e-4 / dinf 1.96e-4 / gap 9.84e-4 | **`Unknown`** (mathematically converged, status mis-reported) |
| **Gurobi default (server)** | **43 min** | **31 min** | **148 barrier + 1.83M simplex** | **pinf 4.16e-5 / dinf 4.2e-7 / gap 8.02e-5** | **`Optimal`** |

**Direct numerical agreement with PDLP-1e-3**:

| Quantity | PDLP at 1e-3 | Gurobi default | Δ |
|---|---:|---:|---:|
| Objective value | 1.40050e+11 | 1.40027e+11 | −0.016 % |
| NEM 2030 TWh | 274.1 | 274.8 | +0.26 % |
| NEM 2050 TWh | 304.1 | 305.6 | +0.49 % |
| Final pinf | 2.56e-4 (rel) | 4.16e-5 (abs) | — |
| Final dinf | 1.96e-4 (rel) | 4.2e-7 (abs) | — |
| Final gap | 9.84e-4 (rel) | 8.02e-5 (abs) | — |

The objective and per-period generation totals agree to within ~0.5 % —
strong consistency. Gurobi's final convergence metrics are 3–4 orders of
magnitude tighter than PDLP-1e-3's (which was relaxed deliberately). This
is expected: Gurobi delivers a fully-converged basic-optimal solution at
standard tolerance; PDLP-1e-3 delivered an interior solution at relaxed
tolerance. Different work products at different costs.

**The 2030 NEM consumption over-shoot vs AEMO published Step Change is
reproduced under Gurobi**. AEMO publishes 202 TWh; Gurobi gives 274.8 TWh
(+36 %), within 0.3 % of PDLP-1e-3's 274.1 TWh. **This confirms the 2030
discrepancy is data-side (IASR projections diverging from AEMO's published
Overview values) and not solver-side (PDLP precision quirk).** NEM 2050:
Gurobi 305.6 TWh vs AEMO 313 TWh (−2.4 %), well within engineering
tolerance and consistent with PDLP-1e-3's 304.1 TWh.

---

## Configuration 3 — NEM 6-period

**LP**: 38,123,031 rows × 17,963,800 cols × 76,069,533 nonzeros — first
direct measurement of the production-equivalent NEM 6-period LP. (Prior
addenda had marked this configuration "not run" or "extrapolated".)
Gurobi's presolve reduced the active LP to **12,957,515 rows × 11,347,542
cols × 40,626,813 nonzeros** in 122 s.

**Result**: **Timed out at the 4 h (240 min) wall-clock budget** during
barrier iterations. Did not formally converge to `Optimal`. Made
substantial progress (complementarity gap dropped 5 orders of magnitude
from initial; primal/dual infeasibilities dropped 4–5 orders) but was
still far from Gurobi's default convergence criteria at kill.

| Metric | Value |
|---|---:|
| Wall-clock | 14,404 s (240 min, budget hit) |
| LP-file read | 125 s |
| Presolve | 122 s |
| Ordering | 1,541 s (25.7 min) |
| Barrier iterations completed | **13** (of unknown total — extrapolation suggests 22–28 for default 1e-8 tolerance) |
| Crossover | not reached |
| Peak RSS | 116 GiB (of 1024 GiB available) |
| Model status | `null` (kill before terminal status emitted) |
| Final pinf | 886 absolute (~9e-3 relative to RHS scale 1e+5) |
| Final dinf | 52,800 absolute |
| Final compl gap | 2.09e+8 absolute |
| Current primal obj at iter 13 | 1.035e+15 (far from converged — true optimum likely ~1.4–1.5e+11) |
| Current dual obj at iter 13 | −2.836e+15 (still spread by 3.87e+15 from primal) |
| Predicted Factor NZ | 2.28e+9 (~30 GB Cholesky factor) |
| Predicted Factor Ops per iter | 5.33e+13 (Gurobi estimate ~80 s/iter; observed 270 s → 1944 s as iterations progressed) |

**Barrier-iteration trajectory**: per-iteration cost grew from ~4 minutes
(early iters, easy Newton steps far from optimum) to **32 minutes (iter
12 → 13)** as IPM approached the central path boundary and the Cholesky
refactorisations became progressively denser.

| Iter | Cumulative solve time | Per-iter wall | Pinf (abs) | Dinf (abs) | Compl (abs) |
|---:|---:|---:|---:|---:|---:|
|  0 | 2,002 s | — | 4.55e+07 | 7.10e+06 | 8.36e+12 |
|  1 | 2,257 s | 255 s | 4.25e+07 | 6.16e+06 | 7.56e+12 |
|  4 | 3,074 s | 286 s | 3.02e+07 | 1.66e+06 | 3.87e+12 |
|  7 | 4,249 s | 471 s | 4.76e+06 | 1.30e+07 | 5.39e+11 |
|  9 | 5,766 s | 900 s | 3.58e+05 | 6.29e+03 | 3.80e+10 |
| 10 | 7,085 s | 1,319 s | 8.45e+04 | 6.40e+04 | 9.08e+09 |
| 11 | 8,481 s | 1,396 s | 1.41e+04 | 3.92e+05 | 2.08e+09 |
| 12 | 9,962 s | 1,481 s | 5.42e+03 | 1.30e+05 | 8.47e+08 |
| **13** | **11,906 s** | **1,944 s** | **8.86e+02** | **5.28e+04** | **2.09e+08** |

The convergence trajectory shows IPM doing its job — pinf dropped 5
orders of magnitude (4.55e+7 → 8.86e+2), dinf dropped 2 orders (with
some non-monotonic bounces between iters 4–7 which is normal for
IPM Newton steps), and compl gap dropped 4.6 orders (8.36e+12 →
2.09e+8). **The Newton steps were getting larger per iteration (typical
super-linear regime) but per-iter cost was also growing fast (4 min
early → 32 min by iter 13) as the Cholesky refactorisations approached
the central-path boundary.**

**How far from convergence was Gurobi at the kill point?** Default
`BarConvTol` requires `|primal_obj − dual_obj| / max(1, |primal_obj|)
< 1e-8`. At iter 13: primal_obj 1.035e+15, dual_obj −2.836e+15, ratio
= 3.87e+15 / 1.035e+15 ≈ 3.7 — **still 8 orders of magnitude above the
1e-8 default convergence threshold**. Both primal_obj and dual_obj are
4 orders of magnitude above the expected true optimum (~1.4e+11 per
NEM 2p anchor), so the iterates have not yet collapsed toward the
optimum.

**Extrapolating remaining iterations is non-trivial** — IPM convergence
in the super-linear regime accelerates dramatically per iteration, but
per-iteration Cholesky cost also grows. A naïve linear extrapolation
on the compl reduction rate (~4× per iter at iters 11–13) suggests
8–10 more iterations to drop compl from 2.09e+8 to ~1e0 (default
absolute convergence floor) — adding 4–7 hours at the late-iter cost
of 30–60 min each. The 4 h budget covered the first 65 % of barrier
progress in compl reduction but probably less than 50 % of the
wall-clock to formal Optimal.

**The status of the partial solution**: Gurobi was killed during barrier,
so no `save_pypsa_network()` call was issued and no `capacity_expansion.nc`
was written. There is no extractable per-period capacity or generation
to compare against the prior PDLP-1e-3 and myopic outputs for the 6-period
configuration. The solution vector at iter 13 was in Gurobi's process
memory and is lost. **And the iter-13 state would not be a usable
engineering solution even if recovered** — primal infeas was still 886
(absolute), dinf 52,800, and primal/dual objectives were spread by
3.87e+15 (relative gap ratio ≈ 3.7, i.e. 8 orders of magnitude above
Gurobi's default 1e-8 convergence threshold). PDLP at 1e-3 produced a
near-feasible interior at much tighter relative infeasibility than this;
the two states are not equivalent.

If a follow-up run is commissioned, the simplest interventions are:
- **Larger wall budget (~8–10 h)**: accept the overnight pattern;
  Gurobi at default 1e-8 likely lands in iter ~22 + crossover, around
  6–8 h server time per the extrapolation.
- **Relaxed tolerance (`BarConvTol=1e-3`)**: trades the deep-precision
  tail for predictable convergence. Would likely declare Optimal in
  3–4 h server time, producing a basic solution (and saveable NetCDF)
  with engineering-quality dispatch aggregates per the NEM 2p
  Gurobi-default-vs-PDLP-1e-3 agreement evidence.
- **`Crossover=0` + relaxed tolerance**: interior solution only,
  no basic; same Pass-1 aggregates usable; saves the crossover cost
  but loses Status=Optimal in some Gurobi versions.

**Comparison to prior addenda for NEM 6-period**:

| Approach | Wall-clock | Status | Notes |
|---|---|---|---|
| HiGHS primal simplex (laptop) | not run | (extrapolated intractable from NEM 2p degeneracy) | |
| HiGHS IPM either crossover-mode (laptop) | not run | (extrapolated intractable from NEM 2p IPX stall) | |
| HiGHS PDLP at 1e-3 (laptop) | not directly run | extrapolated 90–120 min from NEM 2p scaling | Engineering-quality interior solution, status `Unknown` |
| Myopic 6× single-period (laptop) | extrapolated ~4 h overnight | each period `Optimal` | No cross-period state-passing in driver as tested |
| **Gurobi default (server)** | **4 h (budget hit)** | **timed_out at iter 13** | Barrier still 8 orders of magnitude above 1e-8 convergence threshold; iter-13 state not engineering-usable (pinf 886, primal-dual obj spread 3.87e+15) |

The Gurobi-default-1e-8 path at NEM 6p **is not faster than PDLP-1e-3
extrapolation** on this hardware-pair comparison (90–120 min for PDLP-
relaxed vs >4 h for Gurobi-default-1e-8 and a partial state at kill).
It is also **not faster than the myopic ~4 h overnight pattern** (which
delivers 6 Optimal sub-runs at default tolerance). The Gurobi 6p run was
server-side, which would advantage it vs laptop runs; even so, Gurobi
default-tolerance did not beat the established free-solver paths on
this benchmark, and the partial state recovered from the kill is not a
usable Pass-1 output. With a tolerance tweak (`BarConvTol=1e-3`)
matching what PDLP-1e-3 effectively delivers, Gurobi would likely fit
within a 5–7 h overnight budget on this server; that was not commissioned
here.

---

## Combined updated envelope

| Approach | Hardware | NSW 2p | NEM 2p | NEM 6p |
|---|---|:---:|:---:|:---:|
| Primal simplex (default) | laptop | ✗ degenerate (killed @8m) | ✗ degenerate (killed @30m) | ✗ degenerate |
| IPM + crossover ON | laptop | ✗ stall @factor 7 (killed @90m) | ✗ stuck in presolve (killed @60m) | not attempted |
| IPM + crossover OFF | laptop | ✗ same stall (killed @20m) | not attempted | not attempted |
| PDLP at HiGHS default tolerance | laptop | ◐ near-Optimal interior (61m active) | not attempted | not attempted |
| PDLP at 1e-3 tolerance | laptop | not attempted | **✓ 31 min, `Unknown` status** | extrapolated 90–120 min |
| Myopic (6× single-period) | laptop | n/a | n/a | ◐ ~4 h overnight, each period `Optimal` |
| **Gurobi default (1e-8 tolerance)** | **server** | **✓ 9 min `Optimal`** | **✓ 43 min `Optimal`** | **✗ timed_out @4h at iter 13/~25; no usable solution recovered** |

Legend: ✓ = converges to Optimal within practical time. ✗ = does not.
◐ = converges with caveat (interior-only solution, partial sequence, or
extrapolated multi-hour completion).

---

## Answers to the four team questions

### 1. Does Gurobi resolve the methodological friction points?

**On NSW 2p and NEM 2p, yes — both. On NEM 6p, not within the 4 h budget
at default tolerance.**

- The **`Unknown` model status** under PDLP-1e-3 is replaced by Gurobi's
  standard `Optimal` on NSW 2p and NEM 2p. Gurobi reaches default IPM
  convergence (NEM 2p: pinf 4e-5, dinf 4e-7, gap 8e-5) and emits
  crossover-clean basic-optimal status. Upstream code that gates on
  `status == "Optimal"` runs through cleanly without any post-processing
  override or HiGHS patch.
- The **perfect-foresight multi-period LP tractability** dead-end under
  default HiGHS settings is resolved at NSW 2p and NEM 2p: Gurobi solves
  the LPs where HiGHS primal simplex degenerated and HiGHS IPM stalled,
  without parameter tuning. No tolerance relaxation needed; no
  decomposition pattern needed.
- At **NEM 6p**, Gurobi default did not converge within 4 h. The Unknown-
  status quirk and tractability question both *would* be resolved if either
  (a) a longer budget (~8–10 h overnight) were granted, or (b)
  `BarConvTol=1e-3` were set to match the precision PDLP-1e-3 delivers
  (estimated 5–7 h on this server) — but at default 1e-8 tolerance and
  4 h budget on the server, Gurobi delivered no Optimal declaration and
  no saved solution. **For NEM 6p the friction points are not resolved
  out-of-the-box; they require either a tolerance tweak or a budget
  extension that the team needs to accept as part of the solver choice.**

### 2. How does Gurobi wall-clock at NEM 6-period compare?

**Gurobi default at NEM 6p is *not* faster than the established
free-solver paths on this hardware-pair comparison.** It hit the 4 h
budget at barrier iter 13 — substantial IPM progress (5 orders of
magnitude reduction in pinf and compl) but still ~8 orders of magnitude
above default convergence threshold, with primal/dual objectives spread
by 3.87e+15 and no basic solution emitted.

| Approach | Hardware | Wall-clock at NEM 6p | Status |
|---|---|---:|---|
| PDLP-1e-3 (extrapolated from NEM 2p 31 min) | laptop | 90–120 min | Engineering-quality interior, `Unknown` status |
| Myopic 6 × single-period (extrapolated) | laptop | ~4 h overnight | Each period `Optimal` at default tol |
| **Gurobi default (this addendum)** | **server** | **>4 h (budget hit at iter 13)** | **No formal Optimal; iter-13 state not engineering-usable** |
| Gurobi `BarConvTol=1e-3` (estimated, not run) | server | ~5–7 h estimated | Would land basic-optimal at iter ~17–18 |

This is the Pass-3 fleet-capacity question's data point: **for production-
equivalent NEM 6p, Gurobi at default tolerance does not provide a faster
single-shot perfect-foresight path on this hardware than the existing
PDLP-1e-3 (relaxed-tolerance) or myopic (period-decomposition) paths**.
A tolerance-tuned Gurobi run (`BarConvTol=1e-3`) would likely converge
in 5–7 h server-time but was not commissioned (task explicitly excluded
parameter tuning). The hardware advantage of the server over the laptop
appears to be **insufficient to overcome the cost of Gurobi default
1e-8 tolerance** at this LP scale — Gurobi-default-server hits 4-h
budget without converging, while PDLP-1e-3-laptop extrapolates to
90–120 min. Even tolerance-matched (Gurobi-1e-3-server vs PDLP-1e-3-
laptop), Gurobi's barrier is heavier per-iter than PDLP's first-order
steps, so PDLP at relaxed tolerance is plausibly *faster* than Gurobi
at relaxed tolerance for this LP family.
Whether **NEM 6p Gurobi default fits in a 6–10 h overnight budget on
the server, with crossover and basic-optimal status**, is bounded
follow-up — extrapolation says yes, but the empirical measurement was
not made in this addendum.

### 3. Are Gurobi's outputs numerically consistent with the free-solver paths?

**On NSW 2-period and NEM 2-period: yes, strongly so.**

- NSW 2050 capacity by fuel: Gurobi 2-period perfect-foresight matches the
  prior myopic-single-period 2050 solve **to within 0.1 GW per fuel** and
  matches annual generation to within 2.3 %.
- NEM 2-period: Gurobi's objective, NEM 2030 TWh, and NEM 2050 TWh agree
  with PDLP-1e-3 to **within 0.5 %**. The two solvers are clearly hitting
  the same LP optimum to within numerical precision; the difference is
  Gurobi's tolerance is tighter than PDLP-1e-3's.

**On NEM 6-period: not directly verifiable because no Gurobi solution was
saved** — the budget kill happened during barrier, before the
`save_pypsa_network()` call. The Gurobi objective at iter 13 was visible
in the log range (compl-side) but the solution vector is lost. A follow-
up run with `BarConvTol=1e-3` would produce a saveable solution that
could then be compared per-period against PDLP-1e-3 extrapolation. From
the NEM 2-period agreement (within 0.5 %), there is no prior reason to
expect NEM 6p Gurobi capacity-by-fuel to disagree materially with PDLP-
1e-3 at the same tolerance — but this is inference, not measurement.

### 4. Does Gurobi reproduce the 2030 NEM over-shoot?

**Yes — on NEM 2-period, where Gurobi did converge.** AEMO publishes 202
TWh for NEM 2030 Step Change. PDLP-1e-3 gave 274.1 TWh (+36 %). Gurobi
default gives 274.8 TWh (+36 %). The two independent solver paths landing
within 0.3 % of each other on this quantity means the over-shoot cannot
be attributed to PDLP tolerance relaxation or first-order method precision
quirks — **it is in the input data** (IASR 2024 v6.0 demand projections vs
AEMO's published Overview 2024 ISP rounded headline values), or in how
ISPyPSA's templater interprets the IASR demand traces.

This narrows the follow-up substantially: it is no longer a solver-vs-
solver question but an IASR-input-vs-AEMO-published-result question.
Resolution should compare ISPyPSA's NEM 2030 dispatched load against
AEMO's full scenario-results workbook (downloadable separately), not
against the public-Overview headline number.

NEM 2050 vs AEMO: Gurobi 305.6 vs AEMO 313 TWh (−2.4 %), within
engineering tolerance — no discrepancy to investigate here.

For NEM 6p specifically: no Gurobi solution was saved (see Q3), so the
2030 comparison at the 6-period perfect-foresight scope cannot be done
from this addendum's runs alone. The NEM 2-period agreement makes
solver-side explanation of the 2030 over-shoot effectively ruled out
regardless.

---

## Remaining considerations

Items the team should know that don't fit elsewhere in this addendum.

### Default tolerance vs convergence speed trade-off

The NEM 6 p result surfaces a real consideration: **Gurobi's default
`BarConvTol=1e-8` is much tighter than what the simple-msm contract
actually requires**. PDLP-1e-3 at gap_rel ≈ 1e-3 produced engineering-
acceptable dispatch-weighted aggregates per the previous addendum, and
the NEM 2p Gurobi-default-vs-PDLP-1e-3 agreement within 0.5 % suggests
that 1e-3 vs 1e-8 makes no practical difference to the contract outputs.
The team's choice is essentially:

- **Default Gurobi (1e-8 + crossover + basic solution)**: maximal
  numerical fidelity, formal `Optimal` status, fits in budget on NEM 2 p
  (43 min) but **not on NEM 6 p in 4 h on this hardware**.
- **Gurobi at relaxed tolerance (`BarConvTol=1e-3`, possibly `Crossover=0`)**:
  trades the deep-precision tail (which the NEM 2p agreement evidence
  suggests is not engineering-material for the simple-msm contract) for
  earlier termination. Extrapolating from the NEM 6p barrier trajectory,
  IPM would need ~3–5 additional iterations beyond iter 13 (where pobj /
  dobj are still 3.7e+0 apart relative) to reach the 1e-3 relative gap.
  At late-iter cost (30–45 min each) that's ~2–3 h additional, total
  wall ~5–7 h server time on NEM 6p. Status would be `OPTIMAL` from
  barrier; whether crossover is also done is configurable.

Both paths produce valid Pass-1 outputs. The first is "the rigorous one";
the second is "the matched-to-engineering-need one". This wasn't on the
team's list of architectural options before; it is now.

### Memory footprint at NEM 6 p — direct measurement

NEM 6p Gurobi peak RSS was **116 GiB** (11 % of the 1024 GiB server). This
is the first direct measurement of the production-equivalent LP's memory
demand — prior addenda extrapolated from NEM 2 p / NEM 3 p partial-build
data. Concrete implications:

- **A 64 GiB workstation is insufficient** for NEM 6 p under Gurobi
  default (which uses ~30 GB for the Cholesky factor alone, plus barrier
  vectors, plus presolve and LP data structures). The 64 GiB laptop
  prior addenda used would have OOMed before barrier started.
- **A 128 GiB workstation is the practical minimum** for this configuration
  under any IPM/barrier solver path. RAM upgrade is cheap relative to
  compute; the team should plan for at least 128 GiB on whatever machine
  runs production Pass-1 perfect-foresight 6-period solves.
- **The 1024 GiB server has ~9× headroom**. There is no scenario in this
  characterisation where RAM was the bottleneck for Gurobi on the server.

### Hardware-attribution caveat

Reiterating the upfront caveat: the wall-clock numbers in this addendum
were captured on a substantially more powerful machine than the laptop
that ran the prior addenda. **Re-running the prior HiGHS configurations
on the same server would also produce different wall-clock times** than
the previous addenda reported — likely substantially faster, since HiGHS
1.12's PDLP is also parallelisable. The fairest "Gurobi vs HiGHS" comparison
would re-run HiGHS-PDLP-1e-3 on this server alongside Gurobi default —
that comparison was not commissioned for this addendum but is bounded
follow-up work if the team wants a hardware-controlled comparison.

### Gurobi version

Gurobi 11.0.3 (not the latest — 13.x is current). Reason: the CSIRO
token-server licence does not currently authenticate Gurobi 13. If the
CSIRO licence is upgraded to issue Gurobi 12 or 13 tokens, the team may
see further wall-clock improvements (Gurobi's release-over-release LP
speed-ups average 5–15 % per major version). At Gurobi 11.0.3, on this
hardware, the numbers above are what the team can plan against.

### Solver-time vs wall-clock distinction

In the records, three time measurements matter:

- *Gurobi solver time* — the seconds inside Gurobi's optimize() call, what
  Gurobi reports.
- *Solve stage wall* — solve_s in the staged-pipeline timings; includes
  LP-file write (linopy → .lp → Gurobi), Gurobi optimize(), and
  result-reading back into linopy.
- *Wall-clock total* — full pipeline wall (templating, translation, PyPSA
  build, solve, save, extract).

NEM 2p numbers: Gurobi solver 31.3 min, solve stage wall 35.6 min, total
wall 43.2 min. The LP-file marshalling layer (linopy ↔ Gurobi) takes
~4 min for the 30M-nonzero LP. For shorter solves the marshalling can be
a significant fraction; the team should be aware that the
linopy↔solver round-trip is a real cost, not just instrumentation overhead.

### Solver-options were intentionally left default

No `solver_options` were passed. The previous addenda established the
discipline of "change one variable at a time" — this addendum changes only
the solver. Gurobi has many tuning parameters (`Method`, `Crossover`,
`Threads`, `BarHomogeneous`, `Presolve`, etc.) that could plausibly reduce
wall-clock further but were deliberately not explored. If the team wants
to lean further on Gurobi, a separate one-evening parameter-tuning study
on NEM 6p would identify whether default is already optimal or whether
e.g. `Method=2` (force barrier) or `Crossover=0` (interior solution only,
analogous to HiGHS IPM-no-crossover but on a solver that actually
converges) cuts wall-clock further.
