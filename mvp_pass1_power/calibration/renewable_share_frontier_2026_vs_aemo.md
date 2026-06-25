# 2026 renewable-share frontier vs AEMO ISP Step Change

**Date:** 2026-06-25. **Menu:** Pass-1 power-sector, Draft-2026 ISP inputs
(v7.5 workbook + 2026 traces), full-NEM, recursive-dynamic myopic chain
(2030-2050), Gurobi barrier (Crossover=0, BarConvTol=1e-4), `cost_optimal`
archetype, 7 carbon-price trajectories (0/40/80/150/250/350/550 AUD/tCO2e).
Frontier data: `mvp_pass1_power/outputs/frontier_2026/`.

## Renewable share (% bulk-grid generation), by year × carbon price

| year | $0 | $40 | $80 | $150 | $250 | $350 | $550 |
|------|----|-----|-----|------|------|------|------|
| 2030 | 70.9 | 84.4 | 89.5 | 91.1 | 91.3 | 91.4 | 91.7 |
| 2035 | 80.8 | 89.0 | 92.1 | 93.1 | 93.3 | 93.4 | 93.6 |
| 2040 | 82.9 | 89.7 | 92.4 | 93.3 | 93.4 | 93.6 | 93.8 |
| 2045 | 87.0 | 91.9 | 93.8 | 94.8 | 94.8 | 94.9 | 95.0 |
| 2050 | 91.3 | 93.1 | 94.3 | 95.2 | 95.2 | 95.3 | 95.4 |

Monotonic in carbon price and in time; saturates above ~$150 (the $150-$550
columns cluster within ~1 pp). All 35 (trajectory × year) points solved Optimal.

## Comparison to AEMO 2024 ISP Step Change

Reference: AEMO's *published* 2024 ISP Step Change (the latest full ISP with a
published outcome; the Draft 2026 ISP — which this menu uses for *inputs* —
has not published an outcome trajectory). Figures from
`aemo_2024_isp_step_change.md`.

### 1. Demand — the dominant divergence, and a vintage difference (not a bug)

| | 2030 | 2050 |
|---|---|---|
| MVP modeled demand (Draft-2026 Step Change traces) | 189 TWh | **252 TWh** |
| AEMO 2024 ISP Step Change grid consumption | 202 TWh | **313 TWh** |

The MVP correctly uses AEMO's parsed **Draft-2026 Step Change** demand traces,
which run ~19% below the 2024 ISP Step Change in 2050. AEMO revised both its
scenario *set* (2024: Progressive Change / Step Change / Green Energy Exports →
2026: Step Change / Accelerated Transition / Slower Growth) and the demand
projections between vintages. **This is the dominant driver of the wind+solar
capacity undershoot below** — less demand needs less build. It also means the
capacity comparison is partly apples-to-oranges; the renewable *share* is the
more vintage-robust metric.

### 2. Grid-scale wind + solar capacity (GW)

| cp | 2030 | 2050 |
|----|------|------|
| $0 | 46.5 | 66.8 |
| $40 | 55.7 | 70.3 |
| $150 | 62.3 | 75.2 |
| $550 | 63.0 | 77.2 |
| **AEMO** | **55** | **127** |

2030 brackets AEMO (~$40/t matches AEMO's 55 GW). 2050 undershoots badly
(66-77 vs 127 GW, ~half) — largely explained by #1 (the ~19% lower demand),
not an over-/under-build error per se.

### 3. Renewable share and firming retention

MVP saturates at ~95% (2050) vs AEMO's published ~98%. The ~3 pp gap is the
firming the MVP retains: 2050 gas ~17-19 GW (vs AEMO 15) and **1.7 GW coal
capacity persists at *every* carbon price, including $550** (vs AEMO's 0 —
all coal retired). Mechanism: the **recursive-dynamic chain is additive — it
carries built capacity forward but never retires it**, so committed/early-built
fossil persists as (largely idle, low-CF) capacity even when a high carbon price
would retire it. AEMO's perfect-foresight ISP retires all coal. This is a method
limitation of the myopic chain, not a pricing error (the carry-forward prices
carried fossil at true marginal cost — see validation below).

## Carry-forward validation (the methodological gate)

The carry-forward fix was validated end-to-end: at $150, 16.3 GW of carried
fossil dispatches at mean 9% CF (economic peaking, not free baseload); at $0
(most thermal), carried gas prices at $153.7/MWh vs fresh $158.5 — priced
identically, not zeroed. No free-baseload distortion across the carbon range.

## Honest caveats

- **Vintage mismatch:** Draft-2026-input menu vs 2024-ISP-outcome reference.
  The ideal comparison is vs the Draft 2026 ISP outcome (not yet published).
- **Single reference year (2018 weather)** vs AEMO's 30+ stochastic years —
  capacity diverges where weather variability is load-bearing.
- **Myopic recursive-dynamic, additive-only** (no retirement of carried
  capacity) vs AEMO perfect foresight — see #3.
- **Distributed PV (rooftop) is exogenous** (demand-side) in ISPyPSA, so the
  MVP produces no rooftop build row (AEMO: 36→86 GW).

## Modelling-fix provenance (this menu vs earlier attempts)

Reaching a feasible, validated 2050 frontier required three fixes, all diagnosed
to root cause: (1) AEMO "Non REZ" placeholder REZs (V0/N0) dropped — they crashed
post-solve extraction; (2) min-stable-level (`p_min_pu`) zeroed in the
capacity-expansion LP — as a hard floor in a no-unit-commitment LP it forced
must-run overgeneration at high-solar hours in the VRE-rich out-years (min-load
is a unit-commitment concept, enforced in operational dispatch, not investment);
(3) solver switched PDLP→Gurobi — zeroing `p_min_pu` increased LP degeneracy and
PDLP's gap asymptoted above tolerance, while Gurobi barrier solves each full-year
period in ~1-1.8 h to a clean optimum.
