# Phase 7.4 — rep-week capacity-signal distortion diagnostic

> **CORRECTION NOTICE (2026-06-03) — structural-preference conclusion superseded.**
>
> The Phase 7.4 framing — that ISPyPSA exhibits a *structural LP preference*
> against wind, with Path B (correcting the rep-week signal in-place) declared
> non-viable — is **wrong as a general statement about LP behaviour**.
> Phase 8.1 Test 3 (single cost_optimal_2040 at full-year 8760 PDLP-1e-3 on
> Optimus-NC) ran the same LP class at full annual resolution and the result
> reverses:
>
> | Resolution | Wind GW (cost_optimal 2040) | Δ vs 8760 |
> |---|---:|---:|
> | 3-week (Phase 7.2 production) | 23.7 | -3.7 GW (-14 %) |
> | 4-week (Phase 8.1 Test 2)     | 20.5 | -6.9 GW (-25 %) |
> | **8760 (Phase 8.1 Test 3)**   | **27.4** | — |
>
> Wind builds *more* at 8760 than at any rep-week resolution. The "preference"
> Phase 7.4 attributed to the LP formulation was a **rep-week sample-selection
> artefact**: most wind-favourable hours fall outside the sampled weeks, so
> rep-week LPs see a wind resource that is systematically poorer than the true
> annual one. At 8760 the model sees the full distribution and shifts capex
> toward wind. Test 3 also shows the objective drops $2.21 B (-16.7 %) from
> 4-week to 8760 — rep-week sampling was systematically *over-stating* system
> cost as well as under-stating wind.
>
> Evidence: [`bench/phase81_test3_addendum.md`](bench/phase81_test3_addendum.md).
>
> **Generalisation status (FINAL) — falsified at 3/3 converged archetypes.**
> The 5-archetype 2040 8760 verification ran on Optimus-NC; the three
> archetypes that reached PDLP-1e-3 convergence (cost_optimal, rcp, gfm)
> all show the same wind uplift at 8760 vs 3-week production:
>
> | Archetype | Wind 3-week | Wind 8760 | Uplift |
> |---|---:|---:|---:|
> | cost_optimal | 23.67 GW | 27.58 GW | **+16.5 %** |
> | rapid_coal_phaseout | 27.98 GW | 32.74 GW | **+17.0 %** |
> | gas_fleet_maintained | 27.98 GW | 32.74 GW | **+17.0 %** (≡ rcp at 2040, see below) |
>
> The original Phase 7.4 claim was universal ("the LP structurally prefers
> less wind"). Three independent converged archetypes rejecting it — all
> with the same ~16–17 % uplift direction and magnitude — is sufficient to
> falsify the universal. The framing is wrong as a general LP property.
>
> **The two non-completing runs do not resurrect the universal**:
> - storage_led plateaued at gap_rel ~9e-3 (descent ~1.02× per 4000-iter
>   window) — the "no coal, no gas" forcing creates storage-SOC degeneracy
>   PDLP-1e-3 cannot tighten. Extends the existing 3-week "storage_led 1e-3
>   floor" caveat to 8760. **Not a clean test of endogenous wind response
>   anyway** — sl is storage-constrained by archetype design.
> - nuclear_baseload and fossil_incumbent both stopped 1–3 h short of [L]
>   termination with gap and dinf already under 1e-3 and pinf bouncing at
>   1.7–2.0e-3 (adaptive-restart tail). The runs were ended deliberately
>   when the team retired the forced-style archetype structure; the
>   trajectory shapes match the converged archetypes' (rcp/gfm/co) at
>   equivalent iter counts, suggesting they would have landed in the same
>   wind-uplift regime had they been allowed to terminate.
>
> The gfm-and-rcp identity at 2040 (digit-for-digit through PDLP terminal
> iter 35,840) is a separate, equally important structural finding: the
> 2030+2035 gas-floor mandate cannot bind at single-period myopic 2040
> because nothing carries earlier-year build state forward. **gfm IS rcp
> at single-period 2040 by LP construction.** Documented under §1.4 of
> [STATUS_PRE_REDESIGN.md](STATUS_PRE_REDESIGN.md).
>
> Evidence sources for the 3/3 finalisation:
> - [`bench/phase81_test3_addendum.md`](bench/phase81_test3_addendum.md) — cost_optimal 2040
> - [`bench/phase81_clip_fix_and_5archetype_verification.md`](bench/phase81_clip_fix_and_5archetype_verification.md) — rcp/gfm convergence + sl/nb/fi mid-flight states
>
> **What still stands** from the original Phase 7.4 below:
> - The rep-week distortion mechanism analysis (§7.4.1) is correct *as a
>   description of why rep-week LPs see a poorer wind resource*. The numbers
>   in §7.4.1 are valid rep-week diagnostics.
> - The CF-floor / availability-overlay implementations (§7.4.2) failed to
>   close the gap *under rep-week*, which is consistent with the new
>   understanding — they were correcting the symptom (perceived CF) without
>   touching the cause (week selection). They were never going to work.
>
> **What is overturned:**
> - The conclusion "Path B is not viable; the LP has a structural anti-wind
>   preference that requires escalation to 8760." Path B was being asked to
>   recreate full-resolution behaviour from a 3-week sample; that is a
>   sampling-coverage problem, not an LP-formulation problem.
> - Any downstream methodology built on the "structural preference" framing
>   must be revisited. Specifically, anything that treats rep-week wind
>   under-deployment as a property of the model itself rather than the sample.
>
> The original text is retained below the line for audit trail. Use it as a
> rep-week diagnostic, not as a general LP-behaviour statement.

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
