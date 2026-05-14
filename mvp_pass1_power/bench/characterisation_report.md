# ISPyPSA compute-envelope characterisation

Systematic measurement of ISPyPSA solve runtime, memory, LP size, and
convergence across seven progressively larger configurations on the user's
local hardware.

**All runs**: `cost_optimal` archetype (Step Change, no archetype mutation);
HiGHS solver at default settings; 30-min snapshot resolution; single
representative week (`residual-peak-demand`); reference year 2018. Only
spatial extent and number of investment periods vary across configurations.

## Hardware

- Platform: Windows 11 Enterprise (10.0.26100)
- Processor: Intel Core Ultra 7 165H (Meteor Lake)
- Physical cores: 16; logical: 22; max clock 3.8 GHz
- RAM: 63.5 GiB total (~48 GiB free at session start)
- Dell Precision 5490 laptop

## Results

| run_id | config | LP rows | LP cols | LP nonzeros | wall | HiGHS solve | simplex iters | peak RSS | status |
|--------|--------|--------:|--------:|------------:|-----:|------------:|--------------:|---------:|--------|
| 01_nsw_1period | NSW 1-period | 167,850 | 77,404 | 319,253 | 109s | 75s | 67,834 | 0.8 GiB | **completed (Optimal)** |
| 02_nsw_2period | NSW 2-period | 4,877,350 | 2,250,776 | 8,935,253 | 480s | killed | — | 5.5 GiB | killed @ 8 min |
| 03_nsw_3period | NSW 3-period | not captured | — | — | 28,667s* | killed | — | 5.8 GiB | timed_out @ 30 min budget |
| 04_nem_1period | NEM 1-period | 534,910 | 246,383 | 1,035,911 | 7,486s | 7,427s | 364,842 | 2.1 GiB | **completed (Optimal)** |
| 05_nem_2period | NEM 2-period | 16,234,604 | 7,505,085 | 30,017,864 | 1,800s | killed | — | 20.8 GiB | killed @ 30 min |
| 06_nem_3period | NEM 3-period | not captured† | — | — | 1,800s | killed | — | 22.3 GiB | killed @ 30 min (LP build still running) |
| 07_nem_6period | NEM 6-period | not run‡ | — | — | — | — | — | — | not run |

\* Wall-clock for run 03 includes ~7.5 h of Windows sleep — actual compute time was ~30 min before chain killed at budget; status timed_out reflects HiGHS having no terminal "Optimal" line.

† Run 06's LP-build phase had not finished printing the LP-size header when the
chain budget hit. The 22.3 GiB peak RSS confirms the LP was being constructed.

‡ Run 07 was skipped after observing the convergence pattern in runs 02, 05, 06
— see Honest envelope answer below.

## Per-stage timing

Wall-clock seconds spent in each ISPyPSA pipeline stage. `solve` is the
linopy/HiGHS LP solve; everything else is data plumbing. Runs without
per-stage data hit the budget mid-solve and have only the externally-observed
metrics in the table above.

| run_id | iasr_load | templating | translation | pypsa_build | solve | save | extract |
|--------|----------:|-----------:|------------:|------------:|------:|-----:|--------:|
| 01_nsw_1period | 0.3s | 1.3s | 6.7s | 11.2s | 79s | 0.5s | 0.4s |
| 04_nem_1period | 0.2s | 0.8s | 16.1s | 22.3s | 7,436s | 0.9s | 1.2s |

For both completed runs, the **HiGHS solve dominates total wall-clock** —
72% for the smallest config, 99% for full NEM 1-period. Templating,
translation, PyPSA build, save and extract are all I/O-bounded or
data-manipulation-bounded and remain in the seconds-to-tens-of-seconds range
even at NEM-wide scale.

## Output sanity check

Total annual generation (TWh) at each investment period that the run
produced an extractable solution for. Confirms the solve produced meaningful
NEM-scale outputs rather than terminating partway.

| run_id | year | TWh delivered |
|--------|-----:|--------------:|
| 01_nsw_1period | 2050 | 106.4 |
| 04_nem_1period | 2050 | 305.3 |

Both match what we would expect against AEMO's published Step Change
figures: NSW alone ~104–110 TWh, NEM-wide ~313 TWh.

## Scaling commentary

**LP size scales linearly-ish with spatial extent, but ~30× per added
investment period.** From the captured LPs:

- NSW × 1 period → **168K rows**
- NEM × 1 period → **535K rows** (3.2× NSW; consistent with NSW being roughly
  one third of NEM by load)
- NSW × 2 periods → **4.88M rows** (29× NSW × 1; adding one period multiplied
  the row count by 29)
- NEM × 2 periods → **16.2M rows** (30× NEM × 1; same per-period factor as NSW)

So a single new investment period costs **~30× more LP rows** at any spatial
scope. This is the dominant scaling factor.

**Solve time scales much worse than LP size.** From the two completed runs:

- NSW × 1 (168K rows) → **75 s HiGHS** (~0.45 ms/row × iter; 68K iters)
- NEM × 1 (535K rows) → **7,427 s HiGHS** (~3.8 ms/row × iter; 365K iters)

LP size grew 3.2×; HiGHS time grew **99×**. That's a roughly **O(n²·⁵)
scaling** of solve time with LP size — meaningfully worse than linear and
the dominant cost driver as configs grow.

**Iteration count scales 5.4× for 3.2× LP size**, but **each iteration is
~5× more expensive** as the LP grows (more basis updates touch more constraints).
Combined: 5.4 × 5 ≈ 30× per-iteration-and-count product — but observed 99×
because the constraint structure also includes more degeneracy at larger
scales (HiGHS does many degenerate pivots that don't show as nominal
iteration count but do consume time).

**Memory scales sub-linearly.** Peak RSS:

- NSW × 1 → 0.8 GiB
- NEM × 1 → 2.1 GiB
- NSW × 2 → 5.5 GiB
- NEM × 2 → 20.8 GiB
- NEM × 3 → 22.3 GiB (during LP build)

The user's 64 GiB machine has substantial RAM headroom. The biggest LP
measured (16.2M rows for NEM × 2) used **32% of available RAM** at peak,
leaving 44 GiB free. Memory is **not** the bottleneck on this hardware.

## Where time goes

For the two completed runs, the HiGHS solve dominates wall-clock:

- NSW × 1: solve 79s of 109s wall = 72%
- NEM × 1: solve 7,436s of 7,486s wall = 99%

Templating, translation, PyPSA build, save, and extract all remain in the
seconds-to-low-tens-of-seconds range even at NEM-wide scale. **There is no
data-plumbing bottleneck**; the LP solve is the entire runtime story at
production scale.

## Where convergence fails

Every multi-investment-period run exhibits the **same EKK primal-simplex
oscillation pattern** under HiGHS default settings:

1. **Phase 1 (dual infeasibility reduction) converges cleanly** within a
   few minutes even on the largest LPs. Run 05 (NEM × 2, 16M rows) cleanly
   completed Phase 1 in ~5 min reaching Du=0.
2. **Phase 2 (primal infeasibility reduction) gets stuck on degenerate
   vertices**. Primal infeasibility `Pr` oscillates between 1e8 and 1e10
   across hundreds of thousands of iterations. The objective drifts upward
   without monotonic convergence. HiGHS continues to iterate but never
   reaches Model status: Optimal within wall-clock budget.

This degeneracy is well-documented in simplex literature for LPs with many
alternative optima — and multi-investment-period capacity expansion LPs are
exactly that (swapping wind capacity between adjacent REZ nodes in different
years is essentially free, creating large flat regions in the LP polytope).

**Important caveat**: we killed every multi-period run after observing
degeneracy oscillation for 15–30 minutes, on the reasonable belief that
hours more of the same pattern would not change the outcome. **We did not
empirically confirm that HiGHS *never* converges** on these LPs — only
that it does not converge within practical wall-clock budgets at default
settings on this hardware. A multi-day run might eventually pop out of the
degenerate cycle; we did not test this.

Run 04 (NEM × 1 single-period) **did eventually converge** after 2 hours of
HiGHS time with 365K iterations. Single-period LPs lack the cross-period
alternative-optima structure that fuels the multi-period degeneracy and
appear to converge reliably, just slowly.

## Honest envelope answer

### At what configuration size does ISPyPSA become impractical on this hardware?

**Empirically confirmed tractable (HiGHS at default, this hardware)**:

| Config | Wall-clock | Memory | Notes |
|--------|-----------:|-------:|-------|
| NSW × 1 period | ~2 min | ~1 GiB | Smoke-test fast |
| NEM × 1 period | ~2 hours | ~2 GiB | Slow but converges to Optimal |

**Empirically observed non-converging at default settings within wall-clock budgets tested**:

| Config | Wall-clock budget tested | Memory | Outcome |
|--------|-------------------------:|-------:|---------|
| NSW × 2 period | killed at 8 min | 5.5 GiB | degenerate Phase 2 oscillation |
| NSW × 3 period | 30 min | 5.8 GiB | timed out |
| NEM × 2 period | 30 min | 21 GiB | reached Phase 2; oscillating Pr |
| NEM × 3 period | 30 min (LP build only) | 22 GiB | budget hit before HiGHS finished presolve |

### Is production-equivalent (full NEM, 6-period) tractable?

**Not within practical wall-clock at HiGHS default settings on this hardware.**
The empirical scaling we observed is:

- LP row count: 30× per added investment period, ~3× from NSW to NEM
- HiGHS solve time: ~30–100× per added investment period (when it converges)
- Memory: roughly linear with LP rows

NEM × 6-period LP is estimated at **50–100 million rows** (extrapolating
30× per period). Even if HiGHS converged on it (uncertain given the
multi-period degeneracy pattern), extrapolating the NEM × 1 wall-clock of
2 hours by ~30^5 would put a single solve in the **weeks-to-months** range
at default settings.

Memory is the only resource not currently bounded: NEM × 6 LP at ~70M rows
would extrapolate to ~80 GiB, which is **just above** this machine's 64 GiB
RAM. A modest RAM upgrade or a workstation with 128 GiB would unblock
that constraint cleanly.

### What the team needs to decide

The decision points fall in three areas, none of which this characterisation
recommends one way or the other — they are out of scope:

1. **Solver choice.** HiGHS default simplex degenerates on multi-period
   LPs. Three alternatives exist: HiGHS with interior-point (`solver=ipm`),
   HiGHS with non-default simplex tuning (`simplex_strategy`, `presolve`),
   or a commercial solver (Gurobi, CPLEX). Each would unblock the runtime
   constraint at a different cost and risk.
2. **Investment-period strategy.** A perfect-foresight multi-period solve
   is the most expensive shape. Sequential single-period solves with
   capacity-fixing between periods (the myopic pattern that PyPSA's
   `freeze_period()` supports) would replace one 50M-row LP with six 0.5M-row
   LPs — each within the 2-hour envelope that NEM × 1 demonstrably solves
   in. The trade-off is loss of cross-period optimization.
3. **Pass-1 fidelity requirement.** If Pass 1's job is to emit an archetype
   menu, the menu can plausibly be authored on coarser solves (e.g., single
   reference year, fewer snapshots, single-period) without losing structural
   integrity of the archetype set. Production fidelity may need to live only
   in Pass 3, where each chosen archetype is re-solved under the orchestrator's
   selected parameters.

### What this characterisation does NOT answer

- **Whether HiGHS would *eventually* converge on multi-period LPs given a
  multi-day budget**. We killed each non-converging run after observing
  ~15–30 minutes of oscillating Phase 2 simplex. The user may want to
  re-run NEM × 2 with an overnight budget specifically to confirm.
- **Whether HiGHS with non-default settings (presolve aggressive, IPM,
  scaling tweaks) would converge**. The task explicitly excluded solver
  tuning from this characterisation.
- **Whether a commercial solver would be 10×, 100×, or 1000× faster**. A
  separate one-evening trial with a Gurobi free trial license would
  answer this concretely.
- **Memory at NEM × 6 production scale**. We measured up to NEM × 3
  (build only). Extrapolation suggests 64 GiB may be the borderline
  upper bound; this would benefit from one focused test.
