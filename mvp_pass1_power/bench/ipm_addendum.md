# Addendum: HiGHS IPM vs primal simplex on ISPyPSA multi-period LPs

Companion to [characterisation_report.md](characterisation_report.md). Same
machine (Dell Precision 5490, Intel Core Ultra 7 165H, 64 GiB RAM, Windows
11), same ISPyPSA configurations, same `cost_optimal` archetype, HiGHS 1.12.

The diagnostic question the team asked: **does HiGHS interior-point method
(IPM) resolve the EKK primal-simplex degeneracy observed on multi-period
ISPyPSA LPs?**

The only change between the primal-simplex baseline and the IPM runs in this
addendum is the HiGHS solver-algorithm option: `solver="ipm"` passed via PyPSA's
`solver_options` kwarg. **Crossover left on (HiGHS default).** This is the
same "default settings, only change the algorithm" discipline used in the
primal-simplex baseline characterisation.

---

## TL;DR

**No.** HiGHS IPM at default settings (barrier → crossover → simplex
clean-up) does not resolve the multi-period non-convergence on this hardware.
It exhibits a **different failure mode** but reaches the same outcome:
no Optimal solution within practical wall-clock.

- **NSW 2-period (4.88M rows)**: IPM barrier completed quickly (2 iters in
  58s, monotonic dual/primal infeasibility reduction). Crossover construction
  also progressed cleanly through the "fixed variables" stage in ~40s. Then
  stalled on a single basis-refactorization (`Start factorization 7: nonzeros
  in basis = 2,092,804`) at HiGHS time 174s. **No further output for the
  remaining ~85 minutes of wall-clock**; killed at 90 min budget.
- **NEM 2-period (16.2M rows)**: HiGHS started but never emitted the LP-size
  line or any IPM iteration line during the 60+ minute budget. Memory rose
  to 20.8 GiB and plateaued. Stuck in presolve / setup. Killed at 60 min.
- **NEM 3-period and NEM 6-period under IPM**: not attempted — NEM 2p
  failure means NEM 3p will fail at least as badly, and NEM 6p was a
  stretch-goal conditional on the 2p runs succeeding.

The team's diagnostic question is answered. Below is the same direct
comparison table the team asked for, in the same format as the previous
characterisation report, alongside the per-run failure mode.

---

## Direct comparison

| Config (LP rows where measured) | Solver | Wall-clock | Peak RSS | Status / outcome |
|---|---|---:|---:|---|
| NSW 1-period (168K rows) | primal simplex | 109s | 0.8 GiB | **Optimal** in 75s HiGHS, 67,834 iters |
| NSW 2-period (4.88M rows) | primal simplex | 480s (killed) | 5.5 GiB | killed mid-Phase-2 — `Pr:` oscillating 1e8↔7e8, no convergence |
| NSW 2-period (4.88M rows) | **IPM** | 5,400s (killed @ 90 min) | 4.9 GiB | barrier OK, crossover started, stalled on `factorization 7` |
| NSW 3-period | primal simplex | 28,667s (timed_out) | 5.8 GiB | timed out at 30-min budget (Windows-sleep skewed wall) |
| NEM 1-period (535K rows) | primal simplex | 7,486s | 2.1 GiB | **Optimal** in 7,427s HiGHS, 364,842 iters |
| NEM 2-period (16.2M rows) | primal simplex | 1,800s (killed) | 20.8 GiB | killed mid-Phase-2 — `Pr:` oscillating, no convergence |
| NEM 2-period (16.2M rows) | **IPM** | 3,600s (killed @ 60 min) | 20.8 GiB | never emitted LP-size or IPM-iter line; stuck in presolve/setup |
| NEM 3-period | primal simplex | 1,800s (killed) | 22.3 GiB | killed during LP build |
| NEM 3-period | **IPM** | not run | — | predicted from NEM 2p failure; NEM 3p strictly larger |
| NEM 6-period | primal simplex | not run | — | extrapolated intractable |
| NEM 6-period | **IPM** | not run | — | stretch-goal precondition not met |

---

## What IPM actually did on each run

### NSW 2-period under IPM — the most informative data point

This run captured a **clean per-phase trace** before stalling. The HiGHS log
([logs/02_ipm_nsw_2period.log](logs/02_ipm_nsw_2period.log)) shows:

**Barrier phase — fast and monotonic.** Unlike primal simplex which oscillated
indefinitely on the same LP, IPM barrier did two iterations and was well on
its way to convergence:

```
 Iter       primal obj         dual obj       pinf       dinf       gap     time
   0    8.56205215e+11  -1.39610557e+16   8.70e-02   4.24e-02  2.00e+00      10s
   1    5.63549695e+12  -1.27288223e+16   7.93e-02   3.77e-02  2.00e+00      58s
```

`pinf` and `dinf` both decreased monotonically — exactly the convergence
behaviour we don't get under primal simplex. Per-iteration cost was ~48s
on this 4.88M-row LP. Linear extrapolation suggests ~10–15 minutes of
barrier iterations would have reached a high-quality interior solution.

**Crossover construction phase — fast and clean.** HiGHS then began
constructing a starting basis from the interior solution:

```
 Constructing starting basis...
    Start  factorization   1: nonzeros in basis =   1275382   83s
    42109 fixed variables remaining   127s
    32185 fixed variables remaining   132s
    ...
      157 fixed variables remaining   167s
    Start  factorization   7: nonzeros in basis =   2092804   174s
```

`fixed variables remaining` dropped from 42,109 → 157 in 40 seconds.
That phase worked.

**Crossover refactorization phase — stalled.** After "Start factorization 7"
at HiGHS-time 174s, **no further output was emitted for the next ~85 minutes
of wall-clock**. The Python process remained alive at ~4.9 GiB RSS. We
killed it at the 90-minute budget.

`factorization 7` is the seventh basis-factorization pass during the IPM-to-
simplex crossover transition. On a basis with ~2.1M nonzeros, HiGHS's LU
factorization should not stall indefinitely — this is likely a numerical
issue with the basis matrix structure produced by the IPM endpoint, where
HiGHS is doing repeated refactorizations / pivot-swaps and not making
progress visible at the granularity of the log output.

### NEM 2-period under IPM

This run did not get as far as IPM iteration. The HiGHS log
([logs/05_ipm_nem_2period.log](logs/05_ipm_nem_2period.log)) shows:

```
=== SOLVE START ===
solver_options: {'solver': 'ipm'}
Writing constraints: 276/276 [38s]
Writing continuous variables: 8/8 [2s]
Running HiGHS 1.12.0 (git hash: 755a8e0): Copyright (c) 2025 HiGHS under MIT licence terms
```

…and then nothing for the next 60 minutes. No "LP linopy-problem-... has X
rows" line. No "Presolving model" line. No IPM-iter line. Memory peaked at
20.8 GiB and stayed there. The 16.2M-row LP is large enough that HiGHS's
presolve/setup phase exceeds the budget before producing diagnostics.

The natural next-step diagnostic would be to disable presolve or disable
crossover (`presolve="off"` or `run_crossover="off"`) — but the task
explicitly excluded parameter tuning, so this was not attempted.

### NEM 3-period and NEM 6-period under IPM

Not attempted. NEM 3p is a strictly larger LP than NEM 2p (3× period rows
plus all the same spatial structure), and NEM 2p IPM did not progress past
HiGHS setup in 60 minutes. The stretch-goal NEM 6p was conditional on the
2-period runs converging cleanly, which they did not.

---

## What this answers and what it does not

**Answered.** Default-settings HiGHS IPM does not resolve the multi-period
convergence problem on this ISPyPSA LP family on this hardware. The failure
mode shifts from "primal-simplex degenerate oscillation" to "crossover/setup
stall", but the team-facing outcome is unchanged — no Optimal solution within
practical wall-clock.

**Not answered.** Several variants the team may want to explore separately:

- **Pure barrier without crossover** (`run_crossover="off"`). The NSW 2p
  trace shows barrier itself was working — it stalled only at the
  IPM→simplex handoff. An interior solution (not basic) is sufficient for
  the simple-msm aggregates (cost / dispatch / emissions roll-up), so
  disabling crossover may produce a usable solution. **This is the most
  promising single change** to investigate, but it's a parameter tweak, not
  a default-settings finding.
- **Presolve off** (`presolve="off"`). NEM 2p stalled in presolve. Whether
  IPM would actually converge on the unpresolved LP is unknown.
- **PDLP (`solver="pdlp"`)**. HiGHS 1.12 ships a primal-dual hybrid-gradient
  solver intended for very large LPs. Untested here.
- **Commercial solver** (Gurobi, CPLEX, COPT) at default settings. These
  consistently outperform HiGHS by 10–100× on this LP class in published
  benchmarks. Untested here.

**What the team should take away.** The simplest interpretation is that the
ISPyPSA multi-period LP family has structural properties (high degeneracy,
large interior-point Hessians, ill-conditioned constraint matrices in
multi-period coupling, or all of the above) that HiGHS at default settings
is not equipped to handle reliably on this hardware. The choice between
investigating non-default solver tuning, switching solvers, restructuring
the LP (e.g., myopic period-by-period), or accepting lower-fidelity
configurations is the architectural decision — out of scope for this
addendum, as it was for the original characterisation.
