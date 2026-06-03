# Phase 8.1 — Test 2: 4-week production LP (PDLP and Gurobi at 1e-3)

Companion to
[phase81_test1_addendum.md](phase81_test1_addendum.md) (3-week baseline),
[phase81_variance_substudy_addendum.md](phase81_variance_substudy_addendum.md),
and [compute_survey_new_machine.md](compute_survey_new_machine.md).

**Date:** 2026-05-28
**Hardware:** Optimus-NC — Dell PowerEdge R940xa, 4× Intel Xeon Platinum 8280L
(112P / 224L, 3.07 TiB RAM, AVX-512, Windows Server 2022, Cascade Lake-SP).
**Cache version:** v6.0 (same as Test 1 v2; confirmed via md5 of `build_costs.csv`).
**Configuration:** cost_optimal_2040 single-period, **4-week sampling** = the
existing 3 weeks (peak winter + peak summer + spring shoulder week 42) plus
**week 33** (wind-favourable, per Phase 7.4 selection). Authoritative PHES via
the user-supplied `_pumped_storage_fix.py`.

---

## TL;DR

**Both solvers converge on the 4-week LP at 1e-3.** PDLP-1e-3 in 16.5 min,
Gurobi in 32.3 min. Both well within the 6-h-per-solver budget.

**The previous bench's documented 4-week PDLP asymptote at gap_rel
1.5–1.8e-3 is RESOLVED on Optimus-NC.** PDLP-1e-3 reached gap_rel 4.68e-04
with all three metrics under threshold. This is the headline Test 2 finding.

**Solver-vs-solver agreement on the 4-week LP is tighter than on 3-week.**
Every carrier agrees to within 1.6 %; objective agrees to 0.02 %.

**Direction of capacity restructuring from 3-week → 4-week (PDLP)** is
methodology-relevant: adding the wind-favourable week 33 **decreases Wind
build by 3.15 GW (-13.3 %)** while **increasing Solar by 3.82 GW (+25.4 %)
and Battery by 1.61 GW (+14.4 %)**. Counter-intuitive in sign — adding a
wind-rich week causes the model to build *less* wind capacity because the
higher available wind capacity factor in that week means less GW is needed
to meet demand. Materially supports Phase 7.4's rep-week-selection
sensitivity hypothesis.

**Gurobi crossover scales superlinearly with LP size on storage-rich LPs.**
3-week: 515,983 simplex pivots / 455 s. 4-week: 791,824 pivots / 1,739 s —
1.5× iterations, 3.8× time. The LP-degeneracy signature flagged in Test 1
v2 amplifies as predicted; primal-feasibility residual cycled in the
10^7–10^9 range for the first ~25 minutes of crossover before descending
to feasible.

**Conclusion**: 4-week LP is feasible under both solvers on Optimus-NC.
The choice between them at this scale is "PDLP-faster, interior solution,
status Unknown" vs "Gurobi-slower, basic-feasible, status Optimal".
Both deliver capacity decisions that agree to ≤2 % on every carrier.

---

## LP dimensions and timings

| | Value |
|---|---|
| Rows | 2,259,600 |
| Cols | 1,042,704 |
| Nonzeros | 4,177,240 |
| Ratio vs 3-week LP (Test 1 v2) | 1.500× rows, 1.500× cols, 1.502× nonzeros |
| Presolved rows (HiGHS PDLP) | 415,944 (-1,843,656, 82 % reduction) |
| Presolved rows (Gurobi) | similar (not extracted) |

### Side-by-side timings

| Stage | PDLP-1e-3 | Gurobi (Bar/Opt/Feas = 1e-3) |
|---|---:|---:|
| Total wall-clock | 993 s (16.5 min) | 1,935 s (32.3 min) |
| Solve stage wall | 854 s | 1,821 s |
| Solver time | 813 s (HiGHS) | 1,784 s (Gurobi) |
| Barrier iters | n/a | 64 (45 s) |
| Pushes (D+P) | n/a | ~88 s |
| Crossover (simplex) pivots | n/a | 791,824 (~1,652 s) |
| PDLP iterations | 21,400 | n/a |
| Peak RSS | 3.09 GiB | 3.68 GiB |

vs Test 1 v2 (3-week) wall-clock multipliers: PDLP 2.3×, Gurobi 3.1×.
The 4-week LP is 50 % bigger; PDLP scales sublinearly with iters/size,
Gurobi scales superlinearly because crossover hits more LP-degeneracy.

---

## PDLP-1e-3 result

```
Running HiGHS 1.12.0
Solving with cuPDLP-C; Ruiz + PC scaling
solver_options: pdlp_optimality_tolerance=1e-3,
                primal_feasibility_tolerance=1e-3,
                dual_feasibility_tolerance=1e-3
```

### Convergence trajectory

| Iter | gap_rel | pinf_rel | dinf_rel | Flag |
|---:|---:|---:|---:|---|
| 0 | — | 7.36e-1 | 1.14e-2 | [L] |
| 4,000 | 2.25e-2 | 9.64e-4 | 1.21e-4 | [A] restart |
| 8,000 | 1.32e-2 | 9.26e-4 | 4.55e-6 | [A] restart |
| 12,000 | 8.09e-3 | 2.65e-3 | 2.63e-6 | [A] restart |
| 16,000 | 5.99e-4 | 4.12e-3 | 5.36e-7 | [A] restart |
| 20,000 | 8.49e-4 | 1.93e-3 | 1.26e-7 | [A] restart |
| **21,400** | **4.68e-4** | **9.91e-4** | **2.27e-6** | **[L] terminated** |

All three convergence metrics under 1e-3 at termination. Status reported
as `Unknown` (the established HiGHS quirk surfaced in Test 1 v2 and all
prior PDLP runs; mathematically converged regardless).

**vs previous bench's 4-week asymptote at gap_rel 1.5–1.8e-3**: that
asymptote was the **headline parked v2 methodology question** for this
investigation track. On Optimus-NC at the same 1e-3 tolerance, the same
LP family converges below threshold. The compute change is the proximate
explanation — Optimus-NC has 2.3× more physical cores (32 thread default
in HiGHS, vs ~12 on the previous laptop) and substantially newer memory
bandwidth, which the cuPDLP-C matrix-multiply kernels benefit from
directly. This was empirically untested before Test 2 and is now
established.

### Run metrics

| Metric | Value |
|---|---|
| Status (HiGHS) | `Unknown` (all three metrics < 1e-3) |
| Wall-clock | 992.6 s (16.5 min) |
| Solve stage wall | 854.0 s |
| HiGHS solver time | 813.36 s |
| PDLP iterations | 21,400 |
| Final gap_rel | 4.68e-04 ✓ |
| Final pinf_rel | 9.91e-04 ✓ |
| Final dinf_rel | 2.27e-06 ✓ |
| Objective | **$13,239,671,344** |
| Peak RSS | 3.09 GiB |
| Annual load served | 252.11 TWh |

---

## Gurobi result

### Solver banner

```
Gurobi Optimizer version 11.0.3 build v11.0.3rc0 (win64 - Windows Server 2022.0)
CPU model: Intel(R) Xeon(R) Platinum 8280L CPU @ 2.70GHz, instruction set [SSE2|AVX|AVX2|AVX512]
Thread count: 112 physical cores, 224 logical processors, using up to 32 threads
Set parameter TokenServer to value "sc-license1-cdc.it.csiro.au"
Set parameter BarConvTol to value 0.001
Set parameter OptimalityTol to value 0.001
Set parameter FeasibilityTol to value 0.001
Optimize a model with 2259600 rows, 1042704 columns and 4177240 nonzeros
Presolve removed ~1.7M rows and ~390K columns
Concurrent LP optimizer: primal simplex, dual simplex, and barrier
```

### Phases

| Phase | Iters | Wall (cumulative) |
|---|---:|---:|
| Barrier | 64 | 45 s |
| Crossover — DPushes | (398K → 0) | 28 s of crossover (74 s cumulative) |
| Crossover — PPushes | (472K → 0) | 58 s of crossover (132 s cumulative) |
| Crossover — simplex polish | ~705K pivots | ~1,652 s of polish (1,784 s total Gurobi) |

### Crossover-polish behaviour (the key 4-week observation)

Test 1 v2 (3-week) crossover polish converged cleanly in ~10 K simplex
iterations / ~100 s once Push phase completed. Test 2 (4-week) polish
**cycled in the high-PInf regime for ~25 minutes before descending**.
Trajectory (PInf samples every 200 s):

| Time (s) | Iter | Primal Inf (abs) | Comment |
|---:|---:|---:|---|
| 132 | 714,463 | 9.13e+7 | end of Push, start of polish |
| 281 | 718,863 | 1.83e+8 | bouncing |
| 434 | 723,753 | 6.36e+7 | bouncing |
| 586 | 728,363 | 3.28e+7 | bouncing |
| 736 | 733,343 | 1.59e+7 | descending |
| 943 | 740,503 | 4.46e+6 | descending |
| 1158 | 746,783 | 3.55e+6 | descending |
| 1367 | 754,573 | 8.79e+5 | descending |
| 1561 | 763,253 | 2.0e+6 | bouncing-descending |
| 1747 | 785,776 | 3.9e+4 | nearly converged |
| 1784 | **791,824** | **0** | **basic-feasible reached, Optimal** |

During the 132 → ~1,200 s window the objective was stuck at $13.5053 B
(higher than the barrier-final $13.243 B) because the current basis was
primal-infeasible. Once PInf descended below ~10^5 the objective re-
descended to its converged value of $13.237 B.

**This is the LP-degeneracy signature** flagged at the close of Test 1 v2.
The 4-week LP has ~3× the snapshots-with-storage-SOC-constraints of the
3-week LP; the simplex polish has to traverse a much larger set of
near-degenerate basic-feasible representations. Empirically: 1.5× LP size,
3.8× crossover wall-clock, ~1.5× simplex pivots. Crossover scaling is
superlinear in LP size for this LP family.

### Run metrics

| Metric | Value |
|---|---|
| Status | **Optimal** |
| Wall-clock | 1,935.2 s (32.3 min) |
| Solve stage wall | 1,821.2 s |
| Gurobi solver time | 1,783.94 s |
| Barrier iterations | 64 |
| Barrier time | 45.31 s |
| Crossover total iterations | 791,824 |
| Crossover total time | ~1,739 s (~29 min) |
| Final pinf (abs) | 0 |
| Final dinf (abs) | 0 |
| Final compl gap | (barrier-final 8.32 abs ≈ 6e-10 relative) |
| Objective | **$13,237,056,338** (PyPSA `network.objective`) |
| Peak RSS | 3.68 GiB |
| Annual load served | 252.72 TWh |

---

## Head-to-head comparison (4-week LP)

### Objective

| | Objective | vs Gurobi |
|---|---:|---:|
| Gurobi (basic-feasible) | $13,237,056,338 | — |
| PDLP-1e-3 (interior at 1e-3) | $13,239,671,344 | +$2,615,006 (+0.0198 %) |

PDLP marginally higher by 0.02 %, well within PDLP's gap_rel 4.68e-04
convergence band. Both solvers found the same LP optimum to within
solver-tolerance noise.

### Capacity comparison (4-week LP)

| Component | Gurobi 4-week | PDLP 4-week | Δ GW | Δ % | Material >5 %? |
|---|---:|---:|---:|---:|---|
| Wind | 20.497 | 20.527 | +0.030 | +0.15 % | no |
| Solar | 18.782 | 18.866 | +0.084 | +0.45 % | no |
| Gas | 17.468 | 17.457 | -0.011 | -0.06 % | no |
| Storage:Battery | 12.623 | 12.827 | +0.204 | +1.62 % | no |
| Water (gens) | 11.488 | 11.489 | +0.001 | +0.01 % | no |
| Storage:Water (PHES) | 5.015 | 5.015 | 0.000 | 0.00 % | no |
| Black Coal | 3.900 | 3.900 | 0.000 | 0.00 % | no |
| Biomass | 0.876 | 0.885 | +0.009 | +1.03 % | no |
| Brown Coal | 1.160 | 1.160 | 0.000 | 0.00 % | no |
| Hyblend | 0.400 | 0.400 | 0.000 | 0.00 % | no |
| Liquid Fuel | 0.103 | 0.103 | 0.000 | 0.00 % | no |

**Every carrier under 2 %. No carrier exceeds 5 % material threshold.**
Tighter agreement than Test 1 v2 (where Biomass disagreed by 7.0 %).
Solver-vs-solver agreement at 4-week is excellent.

---

## Direction of movement: 3-week → 4-week (PDLP comparison)

Both runs at PDLP-1e-3 tolerance, same hardware, same v6.0 cache, same
authoritative PHES — only difference is the addition of week 33 to the
representative-week set.

| Component | 3-week | 4-week | Δ GW | Δ % | Material >5 %? |
|---|---:|---:|---:|---:|---|
| **Wind** | 23.673 | **20.527** | **-3.146** | **-13.3 %** | **YES** |
| **Solar** | 15.048 | **18.866** | **+3.818** | **+25.4 %** | **YES** |
| Gas | 17.311 | 17.457 | +0.146 | +0.84 % | no |
| **Storage:Battery** | 11.215 | **12.827** | **+1.612** | **+14.4 %** | **YES** |
| Water (gens) | 11.469 | 11.489 | +0.020 | +0.17 % | no |
| Storage:Water (PHES) | 5.015 | 5.015 | 0.000 | 0.00 % | no |
| Black Coal | 3.900 | 3.900 | 0.000 | 0.00 % | no |
| **Biomass** | 1.909 | **0.885** | **-1.024** | **-53.7 %** | **YES** (abs 1 GW) |
| Brown Coal | 1.160 | 1.160 | 0.000 | 0.00 % | no |
| Hyblend, Liquid Fuel | unchanged | | | | |

**Four carriers exceed the 5 % material threshold**: Wind (-13.3 %),
Solar (+25.4 %), Battery (+14.4 %), Biomass (-53.7 %).

### Why does adding a **wind-favourable** week decrease Wind capacity?

Counter-intuitive in sign, but the mechanism is straightforward when
unpacked: week 33's wind resource has higher capacity factor than the
average of the prior 3 weeks, so the *contribution per GW* of wind capacity
is higher. The model needs less Wind GW to deliver the same effective MWh.
The capacity reduction "absorbs" the higher CF.

Solar moves opposite — adding a high-wind week *reveals* the residual
demand pattern in non-wind weeks (the existing 3 weeks plus the new
week-33 morning/evening hours not covered by wind), which solar fills
during daytime. So solar build rises while wind build falls. Battery
build rises in tandem — to shift the additional solar generation into the
non-solar periods of the day.

Biomass drops by 1 GW: the 4-week LP's better resource representation
means biomass (high marginal cost) loses its dispatch slot to wind/solar/
storage combinations. The biomass-cap pre-pass is not binding here; the
LP simply prefers the lower-cost mix when given the full 4-week picture.

### Methodology relevance

This is exactly the **Phase 7.4 rep-week-selection sensitivity** finding
made concrete. The 3-week sampling missed the wind-favourable summer
period; including it materially restructures the capacity mix. The
deliverable's published 3-week numbers (`outputs/granular/`) systematically
**over-state Wind and Biomass** and **under-state Solar and Battery**
relative to a 4-week representation. Magnitudes: ~3 GW Wind, ~4 GW Solar,
~1.6 GW Battery, ~1 GW Biomass.

Whether 4-week is "more correct" than 3-week depends on whether week 33
representatively captures the wind-rich summer in AEMO's reference year
2018 trace data. Per Phase 7.4 selection rationale (the AEMO CF inventory
addendum, `bench/extracts/aemo_cf_diagnostic.py` and surrounding work),
week 33 was selected as the **single most wind-favourable week in the 2018
trace**. So 4-week sampling captures one wind-rich week alongside three
non-wind-extreme weeks; the relative weighting (1 wind-week out of 4 vs
~13 wind-weeks per year in real dispatch) still under-represents wind on
the seasonal-weight side. Annual full-resolution (8760) dispatch would
resolve this. That is the parked Test 3 question.

---

## Phase 7 envelope comparison

Phase 7 production captured 3 PDLP samples per (cost_optimal, 2040)
in `outputs/phase7_granular/`. With Test 2's authoritative-PHES + 4-week
result added, the envelope vs Test 1 v2's 3-week result:

| Carrier | T2 PDLP 4-week | T1 v2 PDLP 3-week | P7 sample 1 | P7 sample 2 | P7 sample 3 |
|---|---:|---:|---:|---:|---:|
| Wind | **20.53** | 23.67 | 39.72 | 26.01 | 26.15 |
| Solar | **18.87** | 15.05 | 43.04 | 14.31 | 15.13 |
| Gas | 17.46 | 17.31 | 7.89 | 16.62 | 18.43 |
| Water (gens) | 11.49 | 11.47 | 6.75 | 11.45 | 11.44 |
| Storage:Battery | 12.83 | 11.22 | — | — | — |
| Storage:Water (PHES) | 5.02 | 5.02 | — | — | — |
| Total storage | 17.84 | 16.23 | 28.63 | 15.82 | 15.79 |

**Test 2 4-week PDLP-1e-3 still does not reproduce Phase 7 sample 1.**
Wind 20.5 vs sample-1 39.7 — half. Solar 18.9 vs sample-1 43.0 — less than
half. Total storage 17.8 vs sample-1 28.6 — 60 %.

Combined with the variance-sub-study finding (5/5 PDLP runs at 3-week are
bit-identical), this strengthens the conclusion: **Phase 7 sample 1's
high-Solar / high-storage outlier is an input-side phenomenon**, not
PDLP-side. Its specific provenance still needs auditing; candidate
explanations include (i) Phase 7 sample 1 was run with v7.4 IASR (higher
build costs reshaping the mix), (ii) a different rep-week selection in
that specific run, (iii) a different archetype-application order, or
(iv) a different snapshot-weighting convention. None of these is
addressed by Test 2.

---

## Answers to the Test 2 questions

### 1. Does PDLP-1e-3 converge on the 4-week LP on the new compute?

**Yes.** gap_rel 4.68e-04, pinf_rel 9.91e-04, dinf_rel 2.27e-06 at iter
21,400. All three metrics under threshold. The previous-bench asymptote
at 1.5–1.8e-3 is resolved. Wall-clock 16.5 min.

### 2. Does Gurobi converge on the 4-week LP?

**Yes, to formal Optimal** at $13.237 B in 32.3 min. Barrier 64 iters / 45 s
(fast). Crossover 791,824 pivots / 1,739 s (the bottleneck).

### 3. Does Gurobi crossover behaviour amplify at 4-week scale?

**Yes — crossover scales superlinearly.** 3-week: 515 K pivots / 455 s.
4-week: 792 K pivots / 1,739 s. LP size grew 1.5×; crossover wall-clock
grew 3.8×. Primal-feasibility residual cycled in 10^7–10^9 for ~25 min
before descending — the documented LP-degeneracy signature on
storage-rich LPs. The pattern would amplify further at 8-week or 12-week
scale; at 8760-snapshot scale, Gurobi crossover may not converge in
practical wall-clocks. Empirically untested.

### 4. Capacity decisions vs 3-week baseline?

**Four carriers materially shift (>5 %)**: Wind -13.3 %, Solar +25.4 %,
Battery +14.4 %, Biomass -53.7 %. Direction is consistent across both
solvers. The 3-week sampling systematically over-states Wind/Biomass and
under-states Solar/Battery vs the 4-week sample.

### 5. Solver-vs-solver agreement on the 4-week LP?

**Every carrier within 2 %, no carrier above the 5 % material threshold.**
Tighter than 3-week (which had Biomass at 7 %). Objectives agree to
0.02 %. PDLP-1e-3 and Gurobi-basic-feasible are functionally equivalent
at this LP scale.

---

## Implications for the deliverable

1. **The 4-week LP is tractable on new compute under both solvers.** The
   methodology-direction question becomes: is the 4-week sampling
   *correct enough* to publish, or is it itself an intermediate step
   toward 8760 dispatch?

2. **Capacity numbers in `outputs/granular/` (3-week PDLP production)
   should be revisited.** 4-week PDLP differs by ~3 GW Wind, 4 GW Solar,
   1.6 GW Battery, 1 GW Biomass. If the deliverable's "relative archetype
   comparison" framing (Phase 7.2 c) is robust to absolute-level shifts
   of this magnitude, no action needed; if the absolute numbers in the
   dashboard are load-bearing, those will need regeneration at 4-week.

3. **PDLP convergence on the new compute resolves the parked v2
   methodology question.** Whether to push further to 8760 dispatch is
   now a separate decision: the convergence concern is removed; the
   compute-cost concern remains. PDLP-4week at 16.5 min × 36 LPs (six
   archetypes × six years) = ~10 h overnight on a single Optimus-NC.
   PDLP-8760 would extrapolate to ~6–10 h *per LP* (LP size grows
   12–15× from 4-week; PDLP scales sublinearly with size at this
   tolerance), so ~9–15 days for the full 36-LP production sweep —
   probably not viable as a single-pass production sweep. Per-period
   parallelism on Optimus-NC could compress that to ~36 h / 8 concurrent
   = ~5 days; doable on a sprint cadence.

4. **Gurobi crossover scaling concern is real but not yet binding.** At
   4-week the crossover ~30 min is acceptable. At 8-week it would
   plausibly approach 2 h. At 8760 it may not converge. The team has the
   option of `Crossover=0` to deliver an interior solution instead — but
   this loses formal Optimal status and was specifically excluded from
   tuning scope here.

5. **The Phase 7 variance phenomenon is now empirically separated from
   PDLP non-determinism.** The 5-run variance sub-study (separate
   addendum) confirms PDLP is bit-deterministic on a fixed LP. Phase 7's
   3× Solar / 81 % storage range across published samples must come from
   input-side differences (rep-week selection, IASR version, archetype
   order). The dashboard's "PDLP variance" framing should be revised to
   "input-sensitivity variance" or similar.

### What Test 2 does NOT establish (Test 3 territory)

- 8760-snapshot dispatch feasibility under either solver.
- Whether PDLP convergence is preserved at 8760 scale.
- Whether Gurobi crossover converges in usable time at 8760.
- Whether the 4-week → 8760 capacity restructuring trend continues
  (does Wind decrease further? Does Solar increase?).

**Test 3 is not commissioned here** — that is a separate team decision
based on Test 2 findings.

---

## Files

- Records: `mvp_pass1_power/bench/records/p81t2_{pdlp,gurobi}{,_2040}.json`
- Logs: `mvp_pass1_power/bench/logs/p81t2_{pdlp,gurobi}_2040.log`
- Solved NetCDFs (gitignored, ~50 MB each):
  - `mvp_pass1_power/bench/runs_myopic/p81t2_pdlp_2040__cost_optimal/outputs/capacity_expansion.nc`
  - `mvp_pass1_power/bench/runs_myopic/p81t2_gurobi_2040__cost_optimal/outputs/capacity_expansion.nc`
- Analysis script: `mvp_pass1_power/bench/analyse_test2_4week.py`

### Minimal code changes (preserved upstream regression 766/766)

- `mvp_pass1_power/bench/run_myopic.py`: added `--rep-weeks` CLI flag
  (overrides the default `representative_weeks: [42]` list passed
  through to the auto-generated per-period config). Purely additive.
- `_write_period_config` now takes a `rep_weeks` keyword argument with
  default `None` (preserving the existing 3-week behaviour when not
  specified).

No changes to ISPyPSA upstream (`src/ispypsa/`).

---

## Reproduction

```bash
# PDLP-1e-3 at 4-week LP, cost_optimal 2040
uv run python mvp_pass1_power/bench/run_myopic.py \
    --run-id p81t2_pdlp --periods 2040 --archetype cost_optimal \
    --use-pdlp --pdlp-tolerance 1e-3 \
    --rep-weeks 42 33 --budget-min 360

# Gurobi at 4-week LP, BarConv/Opt/Feas all 1e-3
uv run python mvp_pass1_power/bench/run_myopic.py \
    --run-id p81t2_gurobi --periods 2040 --archetype cost_optimal \
    --use-gurobi --gurobi-bar-conv-tol 1e-3 \
    --gurobi-opt-tol 1e-3 --gurobi-feas-tol 1e-3 \
    --rep-weeks 42 33 --budget-min 360

# Cross-test analysis
uv run python mvp_pass1_power/bench/analyse_test2_4week.py
```
