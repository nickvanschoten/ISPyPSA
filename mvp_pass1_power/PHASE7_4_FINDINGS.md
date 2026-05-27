# Phase 7.4 — rep-week capacity-signal distortion diagnostic

**Premise (team).** Rep-week-extrapolated CFs send distorted signals during LP
optimisation; the LP under-deploys VRE/storage and over-deploys thermal. Phase
7.4 asks whether the signal can be *corrected within rep-week sampling* (Path B)
rather than escalating to 8760-hour dispatch.

**Diagnostic-first result: the distortion is narrower and more wind-specific
than the premise assumed.** Diagnosis on the committed 3-week production runs
(`nem_3week_v1_cost_optimal`), comparing LP-perceived availability
(`generators_t.p_max_pu`, capacity- and snapshot-weight-weighted) against
trace-derived annual CFs (Wind 0.364, Solar 0.284). **Diagnostic only — nothing
implemented.**

## 7.4.1 — Mechanism

| metric | 2030 | 2040 | 2050 | reference |
|---|---|---|---|---|
| Wind perceived CF | 0.275 (−24 %) | 0.293 (−19 %) | 0.299 (−18 %) | trace 0.364 |
| Solar perceived CF | 0.214 (−25 %) | 0.241 (−15 %) | 0.281 (~0 %) | trace 0.284 |
| Demand mean vs true | +10 % | +8 % | +8 % | true OPSO |

**Root cause — week *selection*, not intra-week weighting.** The two
heavily-weighted sampled weeks (≈mid-April, ≈mid-June at NEM 2050) both sit at
wind availability ~0.29–0.31, below the annual 0.364. Wind's high-yield periods
are not in the demand-stress-selected weeks. Each sampled week is contiguous
half-hourly, so **intra-week time-of-day structure is preserved** — reweighting
*within* weeks cannot close the gap.

Decomposition (answering the 7.4.1 questions):

- *Do selected weeks under-sample VRE-favourable hours?* — **Yes for wind**
  (systematic, every year). **Partially for solar** (early years; converges to
  accurate by 2050).
- *Is uniform intra-week weighting the cause?* — **No.** The gap is which weeks
  are chosen, not the weighting inside them.
- *Carrier-specific or systematic?* — **Wind-specific and systematic
  (−18 to −24 % every year).** Solar milder and year-dependent.
- *Why firm-capacity 80 % vs annual 60–70 %?* — **Not evidenced.** The sampled
  weeks' *mean* demand is within ±7 % of the perceived annual mean (they capture
  peaks for adequacy but are not energy-heavy). Thermal over-deployment is more
  plausibly a *downstream consequence* of wind under-valuation (the LP builds
  firm capacity because it under-counts wind) than a direct CF-signal inflation.

**The problem reduces to one number: wind is perceived ~18–24 % low.** Solar
(2050) and demand are already close; firm-capacity inflation is a symptom, not a
separate cause.

## 7.4.2 — Correction candidates

| | What it does | Preserves | Risks / breaks | Cleanness vs complexity |
|---|---|---|---|---|
| **A — per-carrier wind `p_max_pu` scale** | Multiply wind availability in sampled weeks by ≈ annual/sampled (~1.22) so the LP perceives annual wind yield | Peak-demand weeks & weights unchanged → peak-adequacy intact; LP size unchanged | **Distorts wind's temporal shape** — scaled wind may appear to cover evening/peak hours it doesn't, risking *under-sized firm capacity*; cross-carrier balance shifts | Cleanest to implement; methodologically dirtiest |
| **B — add a wind-favourable representative week** | Select one high-wind week alongside the demand-stress weeks | Real temporal profiles (no shape distortion); peak weeks retained | Dilutes peak-week weight; +50 % LP size → PDLP convergence risk; reopens week-count/dedup inconsistency | Most honest; highest compute + convergence risk |
| **C — VRE-aware week selection** | Change the selector to balance peak-stress *and* annual-VRE coverage | One mechanism, no post-hoc scaling | Hard to satisfy both objectives; may weaken peak signal; selector lives in `isp-trace-parser` (upstream) | Conceptually clean; implementation reaches upstream |
| **D — display-only VRE realignment (no LP change)** | Leave the LP as-is; correct only *displayed* wind annual generation using trace CF (the legitimate-VRE half of Phase 7.3.1) | LP capacity decisions untouched; zero convergence risk; honest for weather-driven VRE | Does **not** correct the LP capacity decision (the stated 7.4 goal) — wind stays under-built; only the displayed number moves | Trivial; does not meet 7.4's premise |

## Recommendation

No in-rep-week correction cleanly fixes the LP *capacity decision* without risk —
the upfront caveat is borne out.

1. For **corrected capacity decisions**: **Candidate B** is the only option that
   fixes the signal without faking a temporal shape — but it must pass a PDLP
   convergence smoke first (the 4-week sampling previously failed to converge at
   1e-3, so adding a week is a real risk).
2. **Candidate A** is cheap and preserves peak weeks, but the temporal-shape
   distortion could *under-size firm capacity* — trading a wind-under-build
   problem for a reliability-under-build problem. High methodological risk.
3. **Candidate D** is the safe fallback matching Phase 7.3.1's principled split
   (realign VRE *display*, leave dispatchables/LP alone) — but explicitly does
   not meet Phase 7.4's capacity-correction premise.

## Risk assessment

- **Probability a clean in-rep-week correction fixes wind capacity decisions
  without breaking peak-adequacy: low-to-moderate.** The distortion is a
  week-selection gap; the cheap fix (A) distorts shape, the honest fix (B) adds
  compute/convergence risk and dilutes the peak signal.
- **If low-risk capacity-decision correction is required, the evidence points
  away from Path B (in-rep-week) toward 8760 dispatch** — or accepting Candidate
  D's display-only realignment and documenting the wind under-build as a known,
  bounded rep-week limitation.

**Commissioned next (separate prompt): Candidate B convergence smoke, with
Candidate D as fallback.** No implementation performed in this diagnostic.
