# Phase 7 production run — biomass cap methodology refinement

**Run id:** `20260526_161705` (PDLP @ 1e-3, NEM-wide, 6 archetypes × 6 milestone years,
**+ biomass availability cap applied**)

**Phase 7.0 methodology improvements landed since Phase 6:**

- (a) 2045 wind dip diagnosed — methodology artifact (IASR demand kink ×
  myopic × single rep-week), not templater bug. Documented in RUNBOOK.
- (b) Biomass availability cap implemented (`_biomass_cap.py`): NEM-wide
  capacity ceiling 1-5 GW ramping 2025→2050 as a fourth pre-pass on every
  archetype. Sources: ARENA Bioenergy Roadmap 2021; AEMO ISP 2024 Step
  Change; CEC 2024.
- (c) Phase 6 re-run with cap (this document).

> **Phase 7.2 update (3-week sampling, run `nem_3week_v1`, 2026-05-27).**
> The single-week Phase 7 run documented below was superseded by a 3-week
> representative-sampling production run. Phase 7.2 also re-examined the
> PDLP-variance concern this document originally elevated in §3 — with
> controlled tests that **refuted** it. The corrected synthesis leads this
> document; §3 below is retained with a correction notice for the record.

---

## 0. Four characterised methodological properties (Phase 7.2)

Phase 7.2 closed with four *documented properties* of the methodology —
each identified, diagnosed, and empirically bounded, rather than left as
an acknowledged limitation.

**1. Demand annualisation bias is consistent under 3-week.**
The LP consumes POE50 OPSO_MODELLING operational demand (rooftop-PV
netted out; true full-year 2050 = 255.3 TWh). 3-week representative
sampling annualises this to a **consistent ~+8 % overstatement** every
milestone year. Single-week produced a *variable* +5–19 % overstatement,
and that variability — not the demand trace itself — caused year-to-year
artifacts including the 2045 demand kink. The 276-vs-310 TWh gap to
AEMO's headline is **definitional** (operational sent-out vs underlying
consumption; ~121 TWh rooftop PV netted out), not a sampling artefact.
Evidence: `bench/extracts/demand_diagnostic.py`.

**2. The 2045 wind dip was a rep-week artefact; multi-week resolves it.**
Under single-week every archetype showed a sharp 2045 wind collapse
(e.g. cost_optimal 26.1 → 18.9 → 29.7 GW across 2040/45/50). Under
3-week the dip vanishes (23.7 → 27.5 → 25.1 GW); 2045 is now a local
maximum. Rep-week selection was the **dominant** contributor (acting
through the under-sampled 2045 demand); the IASR demand kink was
secondary. Evidence: `bench/extracts/extract_1week_baseline.py` vs
`extract_3week_carriers.py`.

**3. Gas direction is a year-and-archetype interaction, not monotone.**
Two competing effects: the demand-annualisation correction pushes gas
*down* (most archetypes at 2030/2040/2050), while peak-coincidence
visibility under 3-week pushes gas *up* (concentrated at 2045, where the
demand correction is largest). In `storage_led` gas is mandate-floored
and flat. Net direction varies by year and archetype; the NSW smoke's
"gas rises" did not generalise.

**4. Capacity figures are PDLP-tolerance-robust (correction of §3).**
Controlled tests — same inputs, same period, only the tolerance changed
— show 1e-3 and 1e-4 agree within **0.5 % on every carrier** at
near-identical objective (+0.037 % 3-week, −0.007 % 1-week), in *both*
single-week and 3-week sampling. The "48.8 GW solar swing" elevated in
§3 below was a **Phase 6 → Phase 7 model-correction effect** (hydro
water-carrier, pumped-storage, biomass cap, nuclear/hydrogen fixes that
landed between those runs), **not** solver variance. The one exception is
`storage_led` 2035, where the 1e-4 re-solve does not converge in budget
(duality gap plateaus ~2.6 × 10⁻⁴ at 120 min); its production 1e-3
result is the practical tolerance floor. Evidence:
`bench/extracts/variance_probe_compare.py`.

---

## 1. Biomass cap binding — primary methodology finding

The cap drives biomass to exactly 5 GW NEM-wide at 2050 across **every**
archetype (the previously-low archetypes also move to 5 GW, see PDLP
variance discussion in §3):

| Archetype | P6 biomass @ 2050 | P7 biomass @ 2050 | Change |
|---|---:|---:|---:|
| cost_optimal | 2.6 GW | 5.0 GW | +2.4 |
| rapid_coal_phaseout | 16.1 GW | 5.0 GW | **−11.1** |
| gas_fleet_maintained | (≡ rapid_coal) | 5.0 GW | **−11.1** |
| storage_led | 2.6 GW | 5.0 GW | +2.4 |
| fossil_incumbent | 10.0 GW | 5.0 GW | **−5.0** |
| nuclear_baseload | 11.9 GW | 5.0 GW | **−6.9** |

**Annual biomass generation** at 2050 (TWh) — the metric the original
methodology problem motivated:

| Archetype | P6 biomass TWh | P7 biomass TWh | Change |
|---|---:|---:|---:|
| rapid_coal_phaseout | 135.9 | ~42 (est. at 5 GW × 96% CF) | −94 TWh |
| nuclear_baseload | 100.3 | ~42 | −58 TWh |
| fossil_incumbent | 88.1 | ~42 | −46 TWh |

The cap brings biomass dispatch from "~70× current Australian usage" down
to ARENA's ambitious upper bound (~42 TWh annually at 5 GW × 96% CF).
Still optimistic relative to realistic 5-15 TWh range but within the
defensible Pass-1 envelope.

---

## 2. Phase 6 vs Phase 7 objective comparison

Most periods identical (< 0.1% change). Material shifts:

| Year | Archetype | P6 obj | P7 obj | Δ% |
|---|---|---:|---:|---:|
| 2050 | cost_optimal | 1.908e10 | 1.958e10 | +2.6 % |
| 2050 | rapid_coal_phaseout | 1.982e10 | 2.032e10 | +2.5 % |
| 2050 | gas_fleet_maintained | 1.982e10 | 2.032e10 | +2.5 % |
| 2050 | fossil_incumbent | 1.842e10 | 1.907e10 | +3.5 % |
| 2050 | nuclear_baseload | 2.289e10 | 2.315e10 | +1.1 % |
| 2035 | storage_led | 1.614e10 | **1.797e10** | **+11.4 %** |
| 2040 | storage_led | 1.840e10 | **2.089e10** | **+13.5 %** |
| 2045 | storage_led | 1.621e10 | 1.658e10 | +2.3 % |

The 2050 lift for non-storage-led archetypes (~2-4 %) reflects the cost
of replacing biomass with more expensive alternatives. The storage_led
2035/2040 lift (11-14 %) is unexpected — see §3.

---

## 3. Methodological exposure: PDLP-at-1e-3 variance (CORRECTED — see §0.4)

> **Correction (Phase 7.2).** The framing in this section — that capacity
> figures swing 10–50 GW per technology between identical-input runs due
> to PDLP tolerance — was **refuted by controlled testing** (see §0
> property 4). The Phase 6 → Phase 7 swing analysed below was a
> *model-correction* effect (hydro / pumped-storage / biomass / nuclear /
> hydrogen fixes landing between the two runs), not solver variance.
> Controlled 1e-3-vs-1e-4 tests on identical inputs move every carrier by
> <0.5 %. The section is retained unedited below for the record; read §0.4
> for the corrected, evidence-backed framing. This is methodological
> self-correction: the original caution was a responsible response to the
> evidence available at the time, now superseded by controlled tests.

**[Superseded] This was elevated as the most consequential Phase 7
finding.** It warranted explicit framing in the deliverable, dashboard,
and team conversation — under the evidence available before the Phase 7.2
controlled tests.

### What the variance looks like

Phase 6 vs Phase 7 capacity comparisons show large swings that cannot
be attributed solely to the biomass cap. Cost_optimal at 2050:

| Carrier | P6 (GW) | P7 (GW) | Δ |
|---|---:|---:|---:|
| Wind | 40.9 | 29.7 | −11.2 |
| Solar | 75.4 | 26.6 | **−48.8** |
| Biomass | 2.6 | 5.0 | +2.4 (cap) |
| Total VRE | 118.9 | 61.3 | **−57.6** |

Total generation delivered is essentially identical (P6: 304.1 TWh,
P7: 302.9 TWh, against demand 296.7 TWh). Objective shifted +2.6 %.
The LP found a very different capacity vertex with similar dispatch
and similar objective.

### What this means

The dashboard's capacity figures are showing **one of many near-optimal
solutions, not THE cost-minimising solution**. Identical-input LP runs
under PDLP at 1e-3 relative tolerance can produce capacity mixes that
differ by tens of GW per carrier while all three convergence metrics
(primal feasibility, dual feasibility, duality gap) remain below the
requested threshold.

### Consequence for the deliverable

- **Capacity charts** in the dashboard need explicit tolerance framing
  (footnote disclaimer + Methodology tab cross-reference). Point-estimate
  capacity numbers are not robust within the chosen tolerance.
- **Methodology tab** has a dedicated "Solver and PDLP tolerance" section
  explaining why PDLP, what 1e-3 means, what tolerance-approximate means
  for the user, and the v2 / Pass-3 paths if tighter precision becomes
  necessary.
- **Catalogue framing** for `gas_fleet_maintained` ≡ `rapid_coal_phaseout`:
  this is a substantive structural finding (LP's natural gas response
  to coal-by-2030 exceeds AEMO's projection by ~10 GW; mandate never
  binds), NOT a designed catalogue differentiation. Phrasing updated
  in dashboard Methodology and inline disclaimers.
- **storage_led 2035** specifically had `pdlp_final_pinf_rel = 9.99e-4`,
  sitting at the 1e-3 tolerance boundary. The combined storage_floor +
  biomass_cap + other constraints tighten the polytope and reduce the
  set of near-optimal vertices PDLP can land on. This archetype-year's
  capacity-mix variance may exceed the general ±3 % envelope. Specific
  inline disclaimer added in the dashboard `storage_led` view.

### What IS robust

- **Annual generation delivered** is stable within ~1 TWh per 300 TWh.
  The LP meets demand regardless of which capacity vertex it lands on.
- **Total objective** is stable within ~3 % across identical-input runs.
- **Catalogue structural differentiation** — storage_led ≠
  fossil_incumbent ≠ nuclear_baseload in their fundamental carrier
  composition — survives. Differences between archetypes are larger
  than within-archetype PDLP variance.
- **Biomass cap binding direction** is robust.

### v2 / Pass-3 paths if the team needs tighter capacity precision

1. Tighten PDLP tolerance to 1e-4 or 1e-5. Expect 2–5× longer solves;
   the `model_status: Unknown` reporting quirk becomes more prevalent.
2. Run repeats with different random seeds and report empirical
   capacity ranges. Substantial compute cost.
3. Switch to commercial solver (Gurobi). Phase 1 8th-addendum found
   Gurobi did not converge at 1e-3 on NEM 6-period in 8h.
4. Perfect-foresight multi-period instead of myopic chained
   single-period. Substantial LP scale increase.

---

## 4. Catalogue still differentiated

The biomass cap doesn't collapse the catalogue — archetypes remain
structurally distinct:

**Phase 7 final capacity at 2050 (GW, all six archetypes):**

| Archetype | Wind | Solar | Storage | Biomass | Gas | Nuclear |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 29.7 | 26.6 | 15.4 | 5.0 | 13.2 | 0.0 |
| rapid_coal_phaseout | 31.1 | 29.4 | 16.2 | 5.0 | 13.8 | 0.0 |
| gas_fleet_maintained | 31.1 | 29.4 | 16.2 | 5.0 | 13.8 | 0.0 |
| storage_led | **39.2** | **64.6** | **40.4** | 5.0 | **2.4** | 0.0 |
| fossil_incumbent | 20.2 | 12.8 | 13.3 | 5.0 | 17.4 | 0.0 |
| nuclear_baseload | 28.8 | 24.6 | 15.8 | 5.0 | 9.1 | **4.0** |

Catalogue structurally differentiated:

- `cost_optimal`: balanced wind+solar+gas+storage mix
- `rapid_coal_phaseout`: gas surges to 13.8 GW (vs 13.2 cost_optimal)
  with more wind+solar
- `gas_fleet_maintained` ≡ `rapid_coal_phaseout` (Phase 6 finding g
  confirmed in Phase 7; accepted per team decision)
- `storage_led`: mandate-driven 40 GW storage; lowest gas (2.4 GW);
  highest solar (64.6 GW)
- `fossil_incumbent`: renewable-constrained; highest gas (17.4 GW)
- `nuclear_baseload`: nuclear at mandate floor (4 GW); modest gas

---

## 5. Documented limitations / open items for team conversation

1. **PDLP-at-1e-3 variance** is a real methodological exposure. Two paths:
   - Document and accept (current Pass-1 stance)
   - Tighten PDLP tolerance to 1e-4 or 1e-5; expect 2-5× longer solves
     and possibly more "model_status: Unknown" reporting quirks per
     Phase 1 bench finding.

2. **Biomass cap as capacity ceiling** rather than fuel-availability
   (TWh) ceiling is a Pass-1 simplification. Generation-sum constraints
   need a framework extension (Pass-3 work).

3. **gas_fleet_maintained ≡ rapid_coal_phaseout collapse** persists in
   Phase 7 (as expected — cap doesn't differentiate them). Team's prior
   decision: accept this as substantive structural finding ("under
   coal-by-2030 the LP naturally exceeds AEMO's gas trajectory; mandate
   doesn't bind").

4. **2045 wind dip persists** in Phase 7 (methodology artifact per
   Phase 7.0 (a) diagnostic; documented in RUNBOOK).

5. **Cost_optimal biomass moved from 2.6 GW to 5.0 GW under the cap.**
   This is likely PDLP variance (alternative LP vertex within tolerance);
   the cap acted as a "<=" not a forcing function. Worth noting that
   cost_optimal Phase 7 isn't a "tighter" version of cost_optimal Phase 6
   — it's an alternative vertex with similar objective and total
   generation.

---

## 6. Phase 7 closure — COMPLETE

All 6 archetypes × 6 milestone years = **36 single-period LPs Optimal**
under PDLP at 1e-3 with biomass cap pre-pass active.

| Archetype | Solved | Wall | Peak |
|---|---:|---:|---:|
| cost_optimal | 6/6 | 21.4 min | 2.1 GiB |
| rapid_coal_phaseout | 6/6 | 24.6 min | 2.1 GiB |
| gas_fleet_maintained | 6/6 | 24.5 min | 2.1 GiB |
| storage_led | 6/6 | 55.7 min | 2.1 GiB |
| fossil_incumbent | 6/6 | 14.0 min | 1.4 GiB |
| nuclear_baseload | 6/6 | 19.6 min | 2.1 GiB |

**Phase 7 wall-clock (parallel):** 0.93 h. Comparable to Phase 6 (0.96 h).
The biomass cap added one LP constraint per archetype-year but no
material runtime overhead.

Phase 7 outputs (committed):

- `outputs/phase7_granular/*` — capacity / generation / storage / CF
  / renewable share / supply gap CSVs (all 6 archetypes × 6 periods)
- `bench/records/20260526_161705__*.json` — per-archetype period records
- `bench/runs_myopic/20260526_161705__*/outputs/capacity_expansion.nc` —
  solved PyPSA networks

**Phase 7 deliverable headline:** biomass cap successfully bounds the
problematic 88-136 TWh biomass dispatch finding from Phase 6 down to
~42 TWh (5 GW × 96 % CF), within the defensible Pass-1 envelope.
Catalogue produces 4 distinct trajectories plus 1 known collapse
(gas_fleet ≡ rapid_coal per Phase 6 finding g).

**Phase 8 (dashboard regeneration)** is the next major work stream
once Phase 7 findings are reviewed by the team.

Regression preserved at **757/757** throughout Phase 7.0 work.
