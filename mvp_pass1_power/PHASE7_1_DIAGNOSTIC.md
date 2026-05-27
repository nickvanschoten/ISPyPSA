# Phase 7.1 — systematic carrier-by-carrier diagnostic

**Scope:** all six archetypes × six milestone years, run id `20260526_161705`.
**Goal:** classify per-carrier anomalies as
**A** (genuine bug) /
**B** (methodology limitation, accept + document) /
**C** (data-driven economic finding, accept + understand) /
**D** (catalogue constraint interaction).

**Stance:** no fixes implemented during this diagnostic — findings inventory
only. The team chooses which to commission.

---

## Headline finding (surface immediately)

**Hydrogen 62 TWh @ 2025 is a v6.0 IASR data gap × translator handling
gap.** The IASR v6.0 `hydrogen_prices` table starts at FY 2025-26
(column `2025_26_$/gj`); FY 2022-23, 2023-24, 2024-25 columns are
empty. The translator's marginal-cost lookup uses `.fillna(0.0)`, so
at investment-period 2025 (FY 2024-25) every hydrogen generator gets
**zero marginal cost**. LP responds rationally to free fuel: builds
6.27 GW of hydrogen reciprocating engines (12 sub-region cohorts) and
dispatches them at ~96 % CF for 62.6 TWh.

The same data gap applies to every archetype (cost_optimal 62.6 TWh,
fossil_incumbent 60.4 TWh, gas_fleet_maintained 62.6 TWh, storage_led
62.6 TWh, nuclear_baseload 62.7 TWh, rapid_coal_phaseout 62.6 TWh —
within PDLP noise of identical). Classification **A — genuine bug
worth fixing**. Recommended response: backfill the FY 2022-24 columns
with the FY 2025-26 value (forward-fill from earliest populated year),
OR fail-fast at templater load when fuel-price columns required by the
configured investment_periods are NaN.

**Scope estimate:** ~15 LOC in `src/ispypsa/translator/generators.py`
or `src/ispypsa/templater/` to backfill earliest-populated-FY into NaN
columns of `hydrogen_prices` (and possibly other fuel-price tables
that share the same v6.0 sparsity). Re-run all 6 archetypes (one
period each: 2025) to verify; full re-run if biomass/storage_floor
constraints redistribute as expected.

---

## Per-carrier findings

### (1) Hydrogen — A (bug)

**Observation.** 62.6 TWh @ 2025 across all archetypes, ≈0 TWh @
2030-2050. The 6.27 GW H₂ reciprocating engines built in 2025 disappear
from later-year LPs (each year solved independently against IASR
baseline + cap-extended ECAA fleet; no carry-forward).

**Evidence trace.**
- `mvp_pass1_power/bench/runs_myopic/20260526_161705__cost_optimal_2025__cost_optimal/ispypsa_inputs/hydrogen_prices.csv`:
  ```
  2022_23_$/gj, 2023_24_$/gj, 2024_25_$/gj, 2025_26_$/gj, 2026_27_$/gj, ...
  ,           , ,           , ,             42.90,        41.46, ...
  ```
  Three empty cells at the start of the single data row.
- Parquet at `marginal_cost_timeseries/hydrogen_reciprocating_engines_nq.parquet`
  shows `marginal_cost = 0.0` for every snapshot in 2025.
- Translator chain: `src/ispypsa/translator/generators.py:_get_dynamic_fuel_prices`
  uses `_CARRIER_TO_FUEL_COST_TABLES["Hydrogen"]` which maps to
  `hydrogen_prices` without a `fuel_cost_mapping_col` → falls to ELSE
  branch (line 778-780), sets `isp_fuel_cost_mapping = "Hydrogen"`
  index. Then `_calculate_dynamic_marginal_costs_single_generator`
  multiplies fuel_price × heat_rate, with `.fillna(0.0)` after the
  pivot — empty cells become zero.

**Classification.** A — bug. The empty cells are valid in the v6.0
workbook (AEMO didn't publish hydrogen prices before 2025-26), but the
templater/translator silently swallows the gap. Two reasonable fixes:

1. **Forward-fill at templater load:** earliest-populated-FY value
   propagates back into prior FY columns. Plausible economic reading
   (hydrogen prices in 2024 ≈ 2025 ≈ early-deployment cost).
2. **Fail-fast:** raise at templater load if any column required by
   `investment_periods` is empty in a fuel-price table. Forces the
   modeller to choose explicit handling rather than silent zero.

**Affects:** Every archetype × 2025 period. The 62 TWh spurious
hydrogen displaces ~60 TWh of what would otherwise be coal/gas/water
dispatch at 2025. Knock-on: 2025 results are not comparable to AEMO
Step Change as quantitative deliverable.

**Scope:** one templater function + a few tests + re-run all 6
archetypes' 2025 period (other periods unaffected — only 2025 LP looks
up FY 2024-25). Estimate ~2 h agent work + ~10 min wall-clock per
2025 re-solve.

---

### (2) Coal — B (methodology)

**Observation.** Black Coal generation:
2025: 67 TWh / 2030: 115 TWh / 2035: 65 TWh / 2050: 12 TWh.
The 2030 PEAK at 115 TWh is the "anti-trend rise" the user flagged.

**Evidence trace.**
- Capacity respects IASR retirement schedule:
  - 2025: 14 active gens / 18.4 GW
  - 2030: 12 active gens / 16.2 GW (Liddell already gone, Eraring retiring 2025)
  - 2035: 8 active gens / 8.3 GW (Vales Point B 2033, Yallourn 2028 retired)
- Marginal costs are economically reasonable: $27 → $29 → $26 /MWh
  (avg) across 2025/2030/2035 — coal is genuinely the cheapest
  dispatchable in the LP's merit order.
- Capacity factors back out at 41 % (2025), **81 % (2030)**, 89 %
  (2035), 32 % (2050). The 2030 dispatch at 81 % CF on 16 GW is what
  drives the 115 TWh.

**Classification.** B — methodology. The IASR-baseline data and
retirement schedule are correct. The high 2030 CF reflects the
LP's response to:

- **Single representative week** ("residual-peak-demand") biases
  toward the highest-firm-capacity-need week. Coal is the cheapest
  firm capacity in 2030 (gas at $120/MWh, biomass-cap not yet
  binding hard, storage not mandated). LP runs coal flat-out.
- **Annual generation extrapolation** from rep-week × snapshot
  weightings: each rep-week snapshot is weighted to represent ~52
  hours of the full year. If the rep week's coal dispatch is high,
  the annualised TWh figure inflates.
- AEMO Step Change uses 8760 × multi-weather-year dispatch which
  captures shoulder/off-peak periods where coal would be undercut
  by VRE → lower annual coal CF.

**Comparison to AEMO Step Change 2030 NEM coal:** AEMO projects
~55 TWh; model produces 115 TWh. Model is ~2× AEMO at 2030.

**Affects:** Every archetype × every milestone year where coal still
exists. The over-dispatch is a function of rep-week methodology, not
archetype-specific. Same root cause as solar/wind suppression (§3, §4).

**Recommended response:** document in Methodology tab as a known
limitation of single-rep-week × myopic-single-period dispatch.
Quantitative coal-TWh figures are not comparable to AEMO Step Change.
Capacity figures (retirement schedule) ARE comparable.

**Scope:** Methodology tab text only. No code changes.

---

### (3) Solar — B (methodology)

> **Phase 7.2 correction.** The "+ PDLP variance" originally in this
> classification was misattributed and has been removed. Controlled
> 1e-3-vs-1e-4 tests show solar capacity is tolerance-robust (<0.5 %);
> the Phase 6 → Phase 7 solar shift cited below was a model-correction
> effect, not solver variance. The B-methodology finding (single-rep-week
> under-values daytime solar) stands and is the dominant cause. See
> PHASE7_FINDINGS §0.4.

**Observation.** Cost_optimal solar capacity at 2050 = 26.6 GW (single-
week Phase 7; 28.7 GW under 3-week) vs AEMO Step Change ~120 GW.
Generation ≈ 53–66 TWh vs AEMO ~230 TWh. **Solar is ~22–24 % of AEMO's
projection in both capacity and generation.**

**Evidence trace.**
- **REZ build limits are not binding.** Sum of solar `*_build_limit`
  RHS = 416 GW NEM-wide; LP builds 13.8 GW new (~3 % utilisation).
  Plenty of headroom for the LP to choose more solar if economic.
- Solar capacity factor 17-23 % across years — within physical range
  (Australian average solar CF ~18-25 %).
- Solar marginal cost = 0 (correct — non-fuel carrier).
- Per-bus distribution shows LP picking Q8 (4.9 GW), N3 (1.9 GW),
  S5 (1.6 GW) — scattered builds, no specific REZ over-concentrated.

**Classification.** B — methodology.

- Single representative week (residual-peak-demand) under-weights
  daytime solar contribution. The peak typically falls on evening
  hours when solar is zero. LP doesn't see the full annual solar
  value. 3-week sampling (adding a summer peak-demand week + a spring
  shoulder week) partially mitigates this — solar rises from 26.6 to
  28.7 GW at 2050 — but the deeper structural undervaluation persists.
- The Phase 6 → Phase 7 solar shift (75.4 → 26.6 GW) was **not** PDLP
  variance, contrary to the original classification. It was the
  cumulative effect of model corrections (hydro / pumped-storage /
  biomass / nuclear / hydrogen) landing between those runs. Controlled
  1e-3-vs-1e-4 tests confirm solar capacity is tolerance-robust
  (<0.5 % shift). See PHASE7_FINDINGS §0.4.

**Affects:** Every archetype × every year — the rep-week solar
valuation effect is uniform across the catalogue.

**Recommended response:** documented in Methodology tab (single-rep-week's
poor solar valuation; 3-week partial mitigation). For quantitative
solar-capacity comparison to AEMO, a v2 mitigation is the team's choice
(add more solar-rich rep weeks, switch to 8760, etc.).

**Scope:** Methodology tab text. No code.

---

### (4) Wind — B (methodology)

**Observation.** Cost_optimal wind 2050 = 29.7 GW / 79 TWh vs AEMO
Step Change ~60 GW / 175 TWh. Wind is ~50 % of AEMO's projection.

**Evidence trace.**
- Wind build limits: sum of `*_Wind_build_limit` RHS = 173 GW NEM-wide;
  LP builds 14.9 GW new (~9 %). Headroom available.
- Wind CF 23-30 % across years — within Australian onshore wind range.
- 2045 dip diagnosed separately in Phase 7.0 (a). **Phase 7.2 resolved
  it:** under 3-week sampling the dip vanishes (2040/45/50 = 23.7 / 27.5
  / 25.1 GW; 2045 is now a local maximum). Rep-week selection was the
  dominant contributor; the IASR demand kink was secondary. See
  PHASE7_FINDINGS §0.2.

**Classification.** B — same as solar. Single rep week × myopic
single-period. (The "PDLP-tolerance variance" originally listed here was
misattributed — see §(3) correction and PHASE7_FINDINGS §0.4.) The 2045
dip is a methodology artefact, not a bug, and is resolved under 3-week.

**Affects:** Same as solar — every archetype × every year.

**Recommended response:** same as solar. Methodology tab text.

**Scope:** Methodology tab text. No code.

---

### (5) Gas — B (methodology) + C (data-driven)

**Observation.** Cost_optimal gas:
2025: 8.8 TWh (CF 8 %) / 2030: 25.5 TWh / 2040: **84.8 TWh** (CF
53 %) / 2050: 76.1 TWh (CF 66 %). AEMO Step Change projects gas
~10-15 TWh @ 2050. Model is ~5-6× AEMO at 2050.

**Evidence trace.**
- Average gas marginal cost: $120-166/MWh across years (gas fuel
  ~$8-12/GJ × heat_rate ~7 GJ/MWh = $56-84/MWh fuel + VOM gets to
  ~$120-160/MWh). Within IASR data range.
- Gas capacity grows from 11.9 GW (2025) to 18.4 GW (2040) then
  declines to 13.2 GW (2050) — LP is BUILDING new gas through the
  2030s and then letting it retire.
- Coal merit-order replacement: 2030 coal retirement opens 100 TWh
  of demand; LP fills 25 of that with gas, 70 with biomass+coal
  remaining, 15 with VRE growth.

**Classification.** Mixed:

- **B (methodology)** — the rep-week × myopic × peaking-demand
  picture rewards firm dispatchable gas for the same reason it
  rewards coal. AEMO's full-year + multi-weather methodology gives
  storage and VRE more value, displacing gas.
- **C (data-driven economic finding)** — gas marginal cost is
  legitimately calculated; storage capacity build is small in
  cost_optimal (15-16 GW) so the LP has limited storage to use for
  firming. AEMO's storage projections (27-33 GW) are higher than the
  LP builds without a mandate.

The `storage_led` archetype demonstrates this clearly: under the
mandate-driven 40 GW storage build, gas drops from 13.2 GW
(cost_optimal 2050) to **2.4 GW** (storage_led 2050). Same fuel cost,
same heat rate — gas vs storage trade-off is genuinely LP-economic.

**Affects:** cost_optimal, rapid_coal_phaseout, gas_fleet_maintained,
nuclear_baseload, fossil_incumbent — all archetypes without a storage
mandate. storage_led is the counterfactual.

**Recommended response:** Methodology tab framing explains the
storage-mandate-displaces-gas finding as a substantive
methodological lever. No fix required — this is the LP making
defensible economic choices under the chosen methodology.

**Scope:** Methodology tab text. No code.

---

### (6) Hyblend — A (likely same root cause as Hydrogen)

**Observation.** Hyblend appears as a tiny carrier:
- Existing capacity: 2 generators / 400 MW (SA Hydrogen Turbine
  and similar).
- Dispatch: ~0 TWh in cost_optimal; trace amounts (≤ 0.22 TWh) in
  some archetypes-years at 2050.

**Evidence trace.**
- `Hyblend` is a fixed-blend H₂ + natural gas carrier.
  `_CARRIER_TO_FUEL_COST_TABLES["Hyblend"]` uses `hydrogen_prices`
  as the blend table. The same FY 2022-24 NaN gap that breaks
  hydrogen also breaks the hyblend blend calculation in those years.
- The 2 existing Hyblend generators have time-varying marginal_cost
  via the blend chain. SA Hydrogen Turbine MC = $40.6/MWh
  (constant in 2025) — that's the dynamic gas+H₂ blend. Not
  obviously broken at 2025 in cost_optimal (LP doesn't dispatch
  because no economic incentive; the higher MC from blend vs pure
  gas keeps it off).

**Classification.** Likely A — same data-gap root cause as hydrogen,
but the LP's response is different (Hyblend has existing generators
with non-zero MC at 2025 from the blend calc; the gap shows up as a
slightly wrong blend percent rather than free fuel).

**Affects:** Hyblend dispatch is so small (< 0.5 TWh per
archetype-year) that this isn't a significant finding for the
deliverable. The fix follows from fixing hydrogen (§1).

**Recommended response:** included in the §1 hydrogen fix. No
separate code change.

---

### (7) Liquid Fuel — C (data-driven, correct)

**Observation.** Liquid Fuel = 0 TWh throughout, in every archetype.

**Evidence trace.**
- 6 existing Liquid Fuel generators, total 0.66 GW.
- Average marginal cost: $474/MWh (very expensive — diesel/oil).
- LP correctly skips liquid-fuel dispatch except as deep-peaking
  reserve which doesn't fire in the rep week.

**Classification.** C — LP making correct economic decisions. Not a
bug or anomaly.

**Affects:** N/A.

**Recommended response:** none. Document as expected behaviour.

---

### (8) Water (hydro) — B (rep-week sensitivity)

**Observation.** Water CF rises 35 % (2025) → 52 % (2030) → 68 %
(2050). Stable capacity ~6.7 GW. p_max_pu sample shows constant
0.4 within the rep week (the Phase 1 seasonal-monthly p_max_pu fix
is applied but the rep week falls inside a single month, so p_max_pu
appears constant inside the week).

**Evidence trace.**
- 32 hydro generators across the NEM, 6.8 GW total, p_max_pu = 0.4
  for sampled generators (per Phase 1's seasonal cap fix).
- CF rising over years reflects LP using hydro flat-out
  as displaceable thermal retires. By 2050 hydro is at 68 % CF —
  the LP is hitting the p_max_pu ceiling in the rep week's
  high-residual-demand hours and the annualised CF reflects that
  ceiling-bound dispatch extrapolated to full year.

**Classification.** B — methodology. The Phase 1 hydro fix prevents
naive p_max_pu=1 dispatch. The remaining CF rise is the LP's
rational response to firm-capacity scarcity. AEMO's full-year
methodology would give a flatter hydro CF closer to ~35-40 %.

**Affects:** All archetypes × all years.

**Recommended response:** Methodology tab text. The Phase 1 hydro
fix is doing its job; the rising CF over years is a separate
rep-week artefact.

**Scope:** Methodology tab text. No code.

---

## Summary table

| # | Carrier | Class | Affects | Recommended response | Re-run? |
|---|---|---|---|---|---|
| 1 | Hydrogen | **A (bug)** | All archetypes × 2025 (62 TWh spurious) | Backfill FY 2022-24 from earliest populated FY, OR fail-fast at templater | **Yes, 2025 period (~10 min × 6 archetypes)** |
| 2 | Coal | B (methodology) | All archetypes × all years | Methodology tab text | No |
| 3 | Solar | B (methodology) | All archetypes × all years | Methodology tab text (rep-week solar valuation; "PDLP variance" retracted — see §0.4 of PHASE7_FINDINGS) | No |
| 4 | Wind | B (methodology) | All archetypes × all years | Methodology tab text (2045 dip resolved under 3-week) | No |
| 5 | Gas | B + C | Non-storage_led archetypes | Methodology tab text — storage-mandate-displaces-gas is the substantive finding | No |
| 6 | Hyblend | A (folded into Hydrogen fix) | Tiny dispatch — low priority | Included in §1 fix | (with §1 re-run) |
| 7 | Liquid Fuel | C (correct) | N/A | None | No |
| 8 | Water | B (methodology) | All archetypes × all years | Methodology tab text | No |

---

## What this means for the deliverable

**Quantitative comparison to AEMO Step Change is methodologically
constrained** by the representative-week × myopic single-period
combination. (PDLP at 1e-3 is *not* a contributor — controlled tests
show capacity is tolerance-robust; see PHASE7_FINDINGS §0.4.) The model:

- **Respects retirement schedules** (coal capacity matches IASR).
- **Respects build limits** (REZ caps have headroom; LP isn't
  constraint-bound on VRE).
- **Computes marginal costs reasonably** (gas $120-166/MWh, coal
  $25-40/MWh — within IASR range).
- **BUT over-dispatches firm capacity** (coal 2030, gas 2040+) and
  **under-builds VRE** (solar 22 %, wind 50 % of AEMO) because the
  rep week selects residual-peak hours where VRE is undervalued.

**Capacity figures and dispatch shares should be reported as
"methodology-conditional"** in the Methodology tab — the LP under
representative-week + myopic single-period produces a defensible but not
directly-comparable-to-AEMO picture. The capacity *point estimates* are
themselves tolerance-robust (PHASE7_FINDINGS §0.4); the gap to AEMO is a
sampling/horizon effect, not solver noise.

**The one genuine bug to fix is hydrogen 2025** (finding 1). The
other anomalies are real but methodology-driven; the team may or may
not want to invest in mitigations (8760 dispatch, multi-week sampling,
tighter PDLP, perfect-foresight) — those are Pass-3 decisions.

---

## Recommended sequencing

1. **Surface this document** to the team for review.
2. **If team commissions hydrogen fix** (finding 1): ~2 h agent work
   to backfill FY 2022-24 in hydrogen_prices at templater load, plus
   ~10 min × 6 archetypes re-run on the 2025 period only. Total
   ~1 hour wall-clock.
3. **Update Methodology tab** with findings 2-8 framing (Phase 8 (2)
   PDLP exposition partially covers this; needs the rep-week
   sensitivity additions). ~1 h agent work.
4. **Update dashboard "AEMO comparison" framing** to flag that
   capacity/dispatch numbers are methodology-conditional. Cross-link
   to PHASE7_1_DIAGNOSTIC.md (this doc).
5. **No further re-runs needed** beyond the hydrogen fix's 2025
   periods.

**Regression preserved at 757/757 throughout this diagnostic
(no code changed).**
