# Phase 7.3.1 — AEMO capacity-factor availability diagnostic

**Purpose.** Before switching annual-generation *display* from
rep-week-extrapolated dispatch to `capacity × AEMO-annual-CF × 8760`, inventory
what AEMO actually publishes per carrier, identify coverage gaps, and size the
expected change. **Diagnostic only — nothing implemented.**

**Scope.** Twelve carriers in the catalogue: Black Coal, Brown Coal, Gas,
Hyblend, Hydrogen, Liquid Fuel, Solar, Wind, Water (hydro), Biomass, Nuclear,
Battery (storage).

---

## Headline finding

**The proposed methodology is cleanly feasible for VRE (wind/solar) only.**
For every *dispatchable* carrier, the "AEMO-published annual CF" does not exist
as a published input — AEMO's per-carrier annual CF is an **emergent output of
the ISP dispatch model**, carried in the detailed scenario-results export
(the `data-MSGG…` workbook the team used for capacity-trajectory anchoring).
**That export is NOT in this repo.** What the repo *does* hold:

- **VRE annual CF** — derivable now, authoritatively, from AEMO's own
  `isp_2024` traces (per-project half-hourly CF). Computed below.
- **Thermal availability** — IASR publishes forced/partial outage and
  maintenance rates. These bound CF from above (≈ max achievable utilisation)
  but are **not** the dispatch CF, which is far lower and economically driven.
- **No published annual CF** for coal/gas/hydro/biomass/H2 dispatch, and AEMO
  does not model nuclear at all.

So the decision the team faces is really: **adopt AEMO-trace CFs for VRE
(strong), and choose a fallback per dispatchable carrier** (obtain the
`data-MSGG` output, use IASR availability as a ceiling, or use industry
benchmarks).

---

## AEMO trace-derived VRE annual CF (computed)

From `mvp_pass1_power/data/traces/isp_2024/project/reference_year=2018/`
(per-project half-hourly CF, simple cross-project mean):

| resource_type | annual CF |
|---|---|
| WIND | **36.4 %** |
| SAT (single-axis-tracking solar) | **28.4 %** |
| FFP (fixed-flat-plate solar) | 25.2 % |

Reproduce: a 3-file groupby-mean over `value` by `resource_type` (see method note
on aggregation below — this is a *simple* mean, not capacity-weighted-by-build).

---

## Per-carrier inventory

Current rep-week CF column is from the 3-week production `cost_optimal` run
(`outputs/granular/capacity_factors.csv`), 2050 unless noted.

| Carrier | AEMO source for annual CF | Granularity | Current rep-week CF (2050) | Proposed approach |
|---|---|---|---|---|
| **Wind** | `isp_2024` traces (in repo) | per-project → REZ → NEM; time-invariant weather (2018) | 0.287 | **AEMO trace CF 0.364** (capacity-weighted by built REZ) |
| **Solar** | `isp_2024` traces (in repo) | per-project → REZ → NEM | 0.263 | **AEMO trace CF ~0.284** (SAT); weight by built REZ |
| **Black Coal** | ISP dispatch output (`data-MSGG`, *not in repo*) | NEM/region, time-varying (declines with retirement) | 0.823 | **Gap** → obtain ISP output; fallback IASR availability ceiling (~0.92) is wrong for CF — prefer ISP output or rep-week |
| **Brown Coal** | ISP dispatch output (*not in repo*) | as coal | 0.868 (2045; retired by 2050) | **Gap** → same as Black Coal |
| **Gas** | ISP dispatch output (*not in repo*) | NEM/region, time-varying; peaker vs CCGT differ | 0.610 | **Gap** → obtain ISP output; peaker CF is low (~5–15 %), CCGT higher — single CF would be wrong |
| **Water (hydro)** | ISP dispatch output (*not in repo*); energy-constrained | NEM/region; **bundles conventional hydro + PHES** | 0.351 | **Gap + disaggregation needed** — see note 1 |
| **Biomass** | ISP dispatch output (*not in repo*) | NEM | 0.934 | **Gap** — see note 2; this is the carrier whose 95 %-CF finding the switch could correct, *if* AEMO publishes a lower CF |
| **Nuclear** | **AEMO does not model nuclear** | n/a | 0.994 (nuclear_baseload) | **Industry benchmark ~85–90 %** (IAEA/IEA advanced-reactor refs) — see note 5 |
| **Hydrogen** | ISP treats as GPG/H2; unusual | NEM | **3.468 (invalid)** | **Gap** — rep-week extrapolation breaks entirely here (CF > 1); see note 6 |
| **Hyblend** | ISP GPG with H2 blend fraction | NEM | ~0.001 | **Gap** — tied to Gas CF + blend table; near-zero dispatch |
| **Liquid Fuel** | ISP dispatch output (*not in repo*) | NEM; emergency peaker | 0.000 | **Gap**; industry peaker CF ~1–3 % | 
| **Battery (storage)** | AEMO publishes storage *energy*, not a generation CF | NEM | n/a | **Not a capacity×CF carrier** — annual throughput (discharge TWh), not capacity×CF×8760; leave as dispatch |

---

## Summary — magnitude of change at 2050 cost_optimal

For the two carriers where AEMO CF is in hand:

| Carrier | Cap GW | Rep-week gen | AEMO-CF gen | Δ |
|---|---|---|---|---|
| Wind | 25.0 | 62.8 TWh | 79.7 TWh | **+27 %** |
| Solar | 28.7 | 66.1 TWh | 71.5 TWh | +8 % |

For dispatchable carriers the magnitude is **gap-dependent** (cannot be sized
without the ISP output). Directional expectations:

- **Biomass** rep-week gen 41.4 TWh at ~93 % CF. If AEMO's published biomass CF
  is realistic (~50–70 %), the switch would **cut biomass ~30–45 %** —
  potentially the largest single change, and the one that would *naturally
  correct* the 95 %-CF finding. **Flagged >30 %.**
- **Wind +27 %** is close to the >30 % flag and rises if capacity-weighting by
  high-CF built REZ lifts the effective CF above the simple-mean 36.4 %.
- **Coal/Gas**: rep-week CFs (0.82 / 0.61) may be *higher* than AEMO dispatch
  CF in a high-VRE system → switch could **reduce** thermal generation, but
  unquantifiable without the ISP output.

---

## Methodological notes / tricky cases

1. **Water (hydro) — conventional vs PHES.** The `Water` carrier bundles
   conventional hydro *and* pumped-storage discharge. AEMO models these
   differently (conventional = energy-limited inflow; PHES = round-trip
   storage). A single annual CF is meaningless for the bundle. **Disaggregate
   before any CF treatment**; conventional-hydro realistic annual energy is
   ~14–16 TWh NEM vs the current 36 TWh `Water` total (which includes PHES
   throughput).

2. **Biomass.** AEMO's *published* biomass CF is the deciding input and is not
   in the repo. Report what AEMO publishes specifically before assuming a
   value — do **not** pick a CF to force the 95 %→realistic correction. If AEMO
   models biomass as baseload-available, the switch may *not* correct the
   finding; if energy-limited, it will.

3. **Coal — time-varying.** Published CF must decline as the fleet ages and
   retires (utilisation falls). A constant CF would misrepresent the trajectory;
   use per-milestone-year values from the ISP output.

4. **Gas — peaker vs baseload.** OCGT peakers (~5–15 % CF) and CCGT/baseload
   gas (higher) have very different CFs. A single "Gas" CF would be wrong; the
   ISP output distinguishes plant types — the dashboard's single Gas carrier
   would need a capacity-weighted blend.

5. **Nuclear — known gap.** AEMO does not model nuclear. Requires an industry
   benchmark (~85–90 % for advanced reactors, IAEA/IEA). The current rep-week CF
   (0.994) is implausibly high (LP runs it flat-out); a benchmark would slightly
   reduce nuclear generation.

6. **Hydrogen / Hyblend.** Hydrogen rep-week CF computes to **3.468** — i.e.
   > 1, physically impossible. This is a rep-week-extrapolation artefact (a tiny
   capacity denominator with annualised dispatch weighting). It is a strong
   independent argument for the methodology switch *for low-capacity carriers*,
   but there is no AEMO hydrogen CF to switch *to* — flag as a gap requiring a
   benchmark or capping at a sane value.

7. **Aggregation choice (flagged).** AEMO traces are per-project/REZ; the
   dashboard displays NEM-wide. The VRE CFs above are **simple cross-project
   means**. The correct NEM-wide value is **capacity-weighted by the REZ the LP
   actually builds** (and arguably generation-weighted for a true fleet CF).
   These differ — a VRE fleet concentrated in high-CF REZ would exceed the
   simple mean. This choice materially affects the displayed number and must be
   decided before implementation.

8. **IASR availability ≠ CF.** IASR forced+partial outage / maintenance tables
   give *availability* (coal full-outage ~6–8 %, so ~92 % available). This is a
   CF **ceiling**, not the dispatch CF. Using availability as CF would massively
   over-state thermal generation (e.g. coal at 92 % vs realistic ~50–80 %).

9. **IASR-assumption vs ISP-output discrepancy.** Where both exist, the
   **ISP-output-derived CF is the more authoritative reference** for *displayed
   generation* (it reflects actual modelled dispatch), while IASR assumptions
   are *inputs*. Flag any case where they diverge.

---

## Coverage gaps and proposed fallbacks (for team decision)

| Gap | Affected carriers | Fallback options (in preference order) |
|---|---|---|
| ISP dispatch CF not in repo | Coal, Gas, Water, Biomass, Liquid Fuel, Hyblend | (a) obtain AEMO `data-MSGG` scenario-results export; (b) keep rep-week for these carriers; (c) IASR availability as a *labelled ceiling*, not CF |
| AEMO does not model nuclear | Nuclear | industry benchmark ~85–90 % (IAEA/IEA) |
| Hydrogen CF undefined / rep-week invalid | Hydrogen, Hyblend | benchmark or cap; near-zero dispatch makes this low-stakes |
| Storage has no generation CF | Battery | leave as dispatch throughput (not capacity×CF) |
| VRE aggregation basis | Wind, Solar | decide capacity-weighted-by-built-REZ vs simple mean before implementing |

---

## Decision points for team review

1. **Proceed with AEMO-CF methodology?** Strong for VRE (authoritative,
   in-repo, corrects the known rep-week VRE under-valuation: wind +27 %).
   Partial for everything else.
2. **How to source dispatchable-carrier CFs?** Obtain the `data-MSGG` ISP
   output (best), or accept a mixed methodology (AEMO-CF for VRE, rep-week or
   benchmark elsewhere) — which the dashboard would need to label honestly.
3. **Is the magnitude worth the framing cost?** Wind +27 % and a likely large
   biomass cut are material, AEMO-direction-correct changes; but a *mixed*
   methodology (some carriers AEMO-CF, some rep-week) is harder to explain than
   either pure approach.

**No implementation performed. Surfacing for team review before commissioning.**

---

# Extension — AEMO 2026 draft ISP (CDP4 ODP) derived CFs

**Data source.** AEMO 2026 draft ISP CDP4/ODP capacity + energy exports, read
from `iasr outputs/NEM-aemo2026draft-step_change-CDP4 (ODP)-{capacity,energy}.csv`
(step_change primary; slower_growth / accelerated_transition available as
sensitivities). CF derived as `energy_TWh / (capacity_GW × 8.76)`. Reproducible
via `bench/extracts/aemo_cf_diagnostic.py`.

> **Rounding caveat.** AEMO publishes capacity (GW) and energy (TWh) rounded to
> integers, so CFs for small/declining carriers (late-life coal, gas peakers)
> carry rounding noise — treat 2-decimal CFs as indicative, not exact.

## AEMO 2026 step_change derived CF (exact, per milestone year)

| AEMO category | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---|---|---|---|---|---|
| Coal | 0.636 | 0.378 | 0.261 | 0.228 | 0.285 | retired |
| Gas | 0.126 | 0.029 | 0.029 | 0.044 | 0.057 | **0.061** |
| Hydro | 0.186 | 0.228 | 0.261 | 0.212 | 0.179 | 0.163 |
| Wind | 0.442 | 0.356 | 0.388 | 0.349 | 0.364 | 0.379 |
| Solar (Utility) | 0.388 | 0.243 | 0.249 | 0.224 | 0.211 | 0.226 |
| Bioenergy | — | — | — | — | — | — (0 capacity all years) |
| Distillate | 0.000 | — | — | — | — | — |

Two corrections to the initial computation worth flagging:

- **Gas CF *rises* 2.9 % → 6.1 %** (2030 → 2050), not flat at 2.9 %. Still a
  deep peaker, but the 2050 value is 6.1 %.
- **Coal retires entirely by 2050 in AEMO** (0 GW). The deliverable still carries
  ~1.7 GW residual black coal at 2050 — there is *no* AEMO 2050 coal CF to apply
  (using the 2045 value 0.285 below as the nearest). This capacity mismatch is
  itself a flag.
- **Bioenergy is 0 GW in *every* year** — AEMO builds none, so there is no CF at
  all (not a low CF — a structural absence).

## ⚠ Critical finding — uniform AEMO-CF substitution breaks energy balance

Applying AEMO CFs **uniformly** (every carrier = its own capacity × AEMO-CF ×
8.76) against the **deliverable's** 2050 capacity mix does **not** meet demand:

| Archetype | Rep-week gen | Uniform-AEMO gen | Shortfall vs demand (~276–288 TWh) |
|---|---|---|---|
| cost_optimal | 283.6 | 167.4 | **−41 %** |
| rapid_coal_phaseout | 284.1 | 174.9 | −38 % |
| gas_fleet_maintained | 284.1 | 174.9 | −38 % |
| storage_led | 287.9 | 242.3 | −16 % |
| fossil_incumbent | 282.5 | 131.1 | **−54 %** |
| nuclear_baseload | 284.1 | 191.8 | −32 % |

**Why.** A *dispatch* CF is an economic outcome of the *whole system mix*, not a
property of the technology. AEMO's gas CF is 2.9 % because AEMO's mix has so
much VRE+storage that gas only peaks; AEMO's bioenergy CF is 0 % because AEMO
builds ~none. The deliverable's LP built gas as **mid-merit (61 %)** and biomass
as **baseload (93 %)** precisely because *its* mix has less VRE/storage. Borrowing
AEMO's dispatch CFs and applying them to a different capacity mix displays
generation the modelled system never produced — and the shortfall scales with
how *un-AEMO-like* the archetype is (fossil_incumbent worst at −53 %; storage_led
best at −12 %, being closest to AEMO's VRE/storage-heavy mix).

**The principled split:**

- **Weather-driven CF (Wind, Solar)** — a *resource* property, independent of
  the capacity mix. AEMO's trace/ISP CF is legitimately substitutable. (Caveat:
  capacity×CF is *potential* generation; actual dispatched VRE is lower by
  curtailment — so even here the display overstates unless curtailment is
  modelled.)
- **Dispatch-driven CF (Coal, Gas, Hydro, Biomass)** — *endogenous* to the mix.
  AEMO's value reflects AEMO's system, not the deliverable's. Substituting it is
  both incoherent (breaks balance) and wrong in principle.

This reframes the methodology question: it is sound for VRE display alignment,
**unsound as a uniform replacement** for dispatchable carriers.

## Magnitude — 2050 cost_optimal, per carrier

| Carrier | Cap GW | Rep-week gen | AEMO-CF | AEMO gen | Δ | >30 %? |
|---|---|---|---|---|---|---|
| Wind | 25.0 | 62.8 | 0.379 | 83.0 | +32 % | ✔ |
| Solar | 28.7 | 66.1 | 0.226 | 56.9 | −14 % | |
| Black Coal | 1.7 | 12.2 | 0.285 (2045; AEMO retires by 2050) | 4.2 | −65 % | ✔ |
| Gas | 12.1 | 64.9 | 0.061 | 6.5 | **−90 %** | ✔ |
| Water | 11.8 | 36.2 | 0.163 | 16.8 | −54 % | ✔ |
| Biomass | 5.1 | 41.4 | 0.0 | 0.0 | **−100 %** | ✔ |
| **Total** | | **283.6** | | **167.4** | **−41 %** | |

**Gas crash** is uniform ~−90 % across all six archetypes (CF 0.061 at 2050).
**Biomass → 0** is −100 % across all six (AEMO builds no bioenergy in any year).

## Per-carrier decisions to surface

1. **Bioenergy = 0 in AEMO.** AEMO-CF substitution cannot "correct" the 95 %-CF
   biomass finding — it would *delete* biomass display entirely. Three options:
   - **(a) Apply AEMO 0 %** — biomass disappears from display. Honest to AEMO
     but discards a real LP build (5 GW) the system depends on for firming.
   - **(b) Industry benchmark** (CSIRO/ARENA realistic, ~60–75 % fuel-
     constrained) — keeps biomass visible at a defensible CF; *recommended* if
     biomass stays in the display at all.
   - **(c) Keep rep-week** for biomass, documented — simplest, but leaves the
     93 % artefact visible.
   This is fundamentally the *capacity-cap-vs-energy-cap* question from
   PHASE7_FINDINGS §5 item 2 resurfacing as a display choice.

2. **Gas CF crash (2.9–6.1 %).** Right *direction* for AEMO alignment but a
   dramatic headline shift (gas 65→6.5 TWh at cost_optimal; ~−90 % every
   archetype). Cannot be applied in isolation without compensating generation or
   the energy balance breaks (see critical finding).

3. **Coal disaggregation.** AEMO publishes a single *Coal* category; the
   deliverable splits Black/Brown. Simplest: apply AEMO's aggregate CF (63.6→
   22.8 %) to both. Surfacing in case a Black/Brown split is wanted (needs a
   separate source).

4. **Carriers AEMO doesn't model** — defined fallbacks:
   - **Nuclear** → industry benchmark ~85–90 %.
   - **Hydrogen / Hyblend** → rep-week (experimental carrier; near-zero dispatch;
     rep-week Hydrogen CF is invalid >1, so cap or benchmark).
   - **Battery, Pumped Hydro** → rep-week (storage dispatch is an LP economic
     decision, not resource-limited; capacity×CF is meaningless for storage).

5. **2025 anomaly.** AEMO's 2025 CFs differ markedly from 2030+ (Solar Utility
   38.8 % vs 22–25 %; Gas 12.6 % vs 2.9 %) — a transition-year composition
   artefact. Decide: use AEMO 2025 directly, or interpolate from the 2030
   baseline. Recommend interpolate — the 2025 values reflect a fleet composition
   that doesn't match the deliverable's 2025.

6. **Hydro disaggregation.** AEMO *Hydro* bundles conventional + PHES; the
   deliverable's *Water* carrier is conventional + PHES discharge, separate from
   *Pumped Hydro* storage. AEMO's 16–26 % applied to Water would overstate
   conventional hydro (energy-limited at lower CF). Either apportion AEMO's
   Hydro, or apply directly to Water with a documented caveat.

## Scenario use

Use **step_change only** for the deliverable (the catalogue's anchoring
scenario). slower_growth / accelerated_transition are sensitivity references —
no combined/blended use is warranted; flag only if the team wants a CF
uncertainty band, in which case the three scenarios bound it.

## Recommendation framing for team review

The diagnostic's core result is that **"capacity × AEMO-CF × 8760" is the right
tool for the wrong half of the problem**:

- **VRE display alignment is legitimate and valuable** — wind +39 %, moving the
  deliverable's known rep-week VRE under-valuation toward AEMO. Worth doing.
- **Uniform dispatchable substitution is incoherent** — it breaks energy balance
  by 30–53 % because dispatch CFs belong to AEMO's mix, not the deliverable's.

A defensible methodology is therefore **hybrid and explicitly labelled**: AEMO
(or trace) CF for weather-driven VRE; rep-week (or the LP's own dispatch) for
dispatch-driven carriers; benchmark fallbacks for unmodelled carriers. The cost
is that a hybrid is harder to explain than either pure approach — but a pure
AEMO-CF display would show a system that fails to meet its own demand by a third.

**No implementation performed. Surfacing for team decision before commissioning.**
