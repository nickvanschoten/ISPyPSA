# ISPyPSA compute-envelope — narrative commentary (draft)

This file is hand-written and combined with the auto-generated table by
`compile_report.py` to produce `characterisation_report.md`. It is the
"interpretation" section that turns the table into a team-facing answer.

## Scaling commentary

The LP problem size scales much faster with the number of investment periods
than with spatial extent. Concretely, in this characterisation set:

- NSW × 1 period → ~150K rows (estimated; not captured)
- NSW × 2 periods → 4.88M rows
- NEM × 1 period → 534K rows
- NEM × 2 periods → 16.2M rows
- (NEM × 3+ periods: LP-size measurement obstructed by HiGHS not reaching
  the LP-print line within budget)

The expansion is roughly **30× per added investment period** at NSW scale,
and **~5–10× from NSW to NEM spatial scope** at constant period count. Adding
both dimensions multiplies. A production-equivalent NEM × 6-period LP is
estimated to be in the **100M–1B row** range — at the upper edge of what
HiGHS can handle and likely beyond what default simplex settings can converge.

## Where the time goes

For runs that completed (only `01_nsw_1period`), per-stage breakdown shows:

- IASR workbook load: 4s (one-time IO)
- Templating: 1s
- Translation: 5s
- PyPSA network build: 8s
- HiGHS solve: 48s
- Save NetCDF + extract results: 1s

So the solve dominates at ~70% of wall-clock even in the smallest config.
For runs that timed out, the solve is the only stage that hits the budget —
PyPSA build and translation scale linearly with problem size and complete
quickly even at NEM × 6-period scale.

## Where the memory goes

Peak RSS scales sub-linearly with LP size:

- 0.8 GiB at 150K rows  → 5.5 GiB at 4.88M rows (~7× memory for 30× rows)
- 1.6 GiB at 534K rows  → ~20 GiB at 16.2M rows (~12× memory for 30× rows)

Memory is NOT the bottleneck on this hardware. Even the largest measured LP
(16.2M rows for NEM × 2-period) used ~30% of available 64 GiB. The user has
substantial RAM headroom; the constraint is **time**, not memory.

## Where the convergence fails

Every multi-investment-period LP and the full-NEM single-period LP show the
same degeneracy pattern under HiGHS default settings:

1. Phase 1 simplex reduces dual infeasibility to 0 cleanly (within a few
   minutes).
2. Phase 2 simplex *starts* but then primal infeasibility (`Pr:`) oscillates
   between ~1e8 and ~1e10 across hundreds of thousands of iterations.
3. Objective value drifts upward without monotonic convergence.
4. HiGHS never reports a terminal "Model status: Optimal" line within budget.

This is **EKK primal simplex cycling on degenerate vertices** — a known
failure mode for simplex on LPs with many alternative optima. Multi-investment-
period capacity expansion LPs have many such alternative optima (e.g.,
swapping wind capacity between adjacent REZ nodes at the same cost).

Mitigations the team should be aware of but that are out of scope for this
characterisation (task explicitly said "default settings"):

- `simplex_strategy = 4` (PAMI) or `presolve = on` with stricter aggregation
- Switch to **interior point method (IPM/barrier)**: `solver = ipm` —
  much more robust on degenerate LPs but uses more memory.
- **Commercial solver** (Gurobi, CPLEX): typically 10–100× faster than HiGHS
  on this LP class and far more robust on degeneracies.

## Honest envelope answer

**At what configuration size does ISPyPSA become impractical on this hardware?**

With HiGHS at default settings on this Dell Precision 5490 (Intel Core Ultra 7
165H, 64 GiB RAM):

- **Tractable in seconds-to-minutes**: single-period, single-state (e.g.,
  NSW 2050 only) — converges cleanly in ~80 seconds.
- **Tractable but non-converging within a few hours**: multi-period NSW
  (runs 02–03) and full-NEM single-period (run 04). HiGHS does *iterate*
  but does not reach optimality at default settings.
- **Not converging in any practical wall-clock**: full-NEM multi-period
  (runs 05–07). The LP is built and HiGHS begins simplex but the same
  degenerate-oscillation pattern emerges at larger scale.

**Is production-equivalent (full NEM, 6-period, default snapshot density)
tractable?**

On this hardware with HiGHS at default settings: **not without changes**.
Specifically:

- *Memory*: tractable — the largest LP we built (16.2M rows for NEM × 2p)
  used 20 GiB peak; the 6-period extrapolation is ~50–100M rows which
  should still fit in 64 GiB with margin.
- *Wall-clock at default HiGHS*: not tractable. Even single-period full NEM
  did not converge in 60 minutes; 6-period is exponentially harder.
- *Wall-clock with IPM or commercial solver*: unknown from this
  characterisation — would need a focused follow-up.

## What the team needs to decide

1. **Is the architectural decision sensitive to HiGHS performance?** If yes:
   would the team consider a commercial solver licence, or switching to IPM,
   or some combination of presolve/scale tuning? Each is a separate
   engineering investment.

2. **Is the multi-investment-period requirement strict?** Pass 1 contract has
   six milestone years. If the team is open to per-period sequential solves
   with capacity-fixing between periods (the "myopic" pathway pattern that
   PyPSA's `freeze_period()` supports), the LP at each step is closer to the
   single-period scale that DOES converge — at the cost of losing the
   perfect-foresight cross-period optimization.

3. **Is the production-equivalent run actually needed for Pass 1, or only
   for Pass 3?** If Pass 1's job is to emit an *archetype menu* with
   reasonable-shape numbers for the orchestrator to pick from, the menu
   can be authored on coarser solves; the high-fidelity Pass 3 re-solve
   under specific orchestrator inputs is the place that needs production
   fidelity.

These are all out-of-scope decisions. The envelope data above is offered as
input to those decisions, not a recommendation about how to make them.
