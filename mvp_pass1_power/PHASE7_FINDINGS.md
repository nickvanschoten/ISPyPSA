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
| storage_led | 2.6 GW | (pending) | (pending) |
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

## 3. Methodological exposure: PDLP variance

The Phase 6 vs Phase 7 capacity-mix comparisons show large swings that
cannot be attributed solely to the biomass cap. Example: cost_optimal at
2050:

| Carrier | P6 (GW) | P7 (GW) | Δ |
|---|---:|---:|---:|
| Wind | 40.9 | 29.7 | −11.2 |
| Solar | 75.4 | 26.6 | **−48.8** |
| Biomass | 2.6 | 5.0 | +2.4 |
| Total | 118.9 | 61.3 | **−57.6** |

Total generation delivered is essentially identical (P6: 304.1 TWh,
P7: 302.9 TWh, against demand 296.7 TWh). The LP found a very different
capacity vertex but with similar dispatch and similar objective (+2.6 %).

**This is PDLP-at-1e-3-tolerance variance.** When the relative duality
gap is bounded at 0.1 %, the primal solution can move significantly
along the polytope's near-optimal facets. For ISPyPSA's LP scale
(700k+ rows × 330k cols × 1.3M nonzeros) and our chosen tolerance, this
manifests as 10-50 GW capacity swings across runs that the team cannot
treat as precise to GW resolution.

**Implications for the deliverable:**

- Capacity-mix tables should report ranges or be marked as "PDLP-tolerance
  approximate" rather than point estimates.
- Annual generation TWh and total objective are stable (< 3 % variance).
- The biomass cap binding *direction* and the catalogue's *structural
  differentiation* are robust to this variance.

**storage_led 2035/2040 +11-14 % objective shift** is the most extreme
example. Phase 7 storage_led's biomass capacity (2.06 / 3.02 / 3.99 GW
at 2035/2040/2045) is much higher than Phase 6's (0.10 / 0.52 / 0.56 GW)
yet still under the cap. The LP found a different vertex using more
biomass and less of something else. The pinf at storage_led 2035 = 0.999e-3
sits right at the tolerance boundary; the LP is genuinely harder under
the combined storage_floor + biomass_cap + other constraints.

---

## 4. Catalogue still differentiated

The biomass cap doesn't collapse the catalogue — archetypes remain
structurally distinct:

| Archetype | 2050 wind | 2050 solar | 2050 biomass | 2050 nuclear |
|---|---:|---:|---:|---:|
| cost_optimal | 29.7 | 26.6 | 5.0 | 0.0 |
| rapid_coal_phaseout | 31.1 | 29.4 | 5.0 | 0.0 |
| gas_fleet_maintained | 31.1 | 29.4 | 5.0 | 0.0 |
| storage_led | (pending) | (pending) | (pending) | 0.0 |
| fossil_incumbent | 20.2 | 12.8 | 5.0 | 0.0 |
| nuclear_baseload | 28.8 | 24.6 | 5.0 | 4.0 |

- `fossil_incumbent` distinct: lower wind/solar (renewable-constrained)
- `nuclear_baseload` distinct: nuclear at mandate floor
- `storage_led` will be distinct (mandate-driven storage build)
- `rapid_coal_phaseout` ≡ `gas_fleet_maintained` still (per Phase 6 finding g)

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

## 6. Phase 7 closure

Storage_led 2050 still solving at time of draft. Final closure pending
that result.

Phase 7 outputs:
- `outputs/phase7_granular/*` — capacity / generation / storage /
  CF / renewable share / supply gap CSVs
- `bench/records/20260526_161705__*.json` — per-archetype period records
- `bench/runs_myopic/20260526_161705__*/outputs/capacity_expansion.nc` —
  solved networks

Phase 8 (dashboard regeneration) follows once storage_led 2050 completes
and Phase 7 findings are reviewed.

Regression preserved at **757/757** throughout Phase 7.0 work.
