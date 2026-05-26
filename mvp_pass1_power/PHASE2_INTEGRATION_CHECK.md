# Phase 2 pre-solve integration check

State of the codebase before commissioning Phase 6 production runs. Surfaces
the integrated catalogue + overlays for team confirmation.

---

## 1. Six-archetype catalogue (Phase 2.1)

All six production archetypes use AEMO 2024 IASR **Step Change** as the base
scenario. Deployment mandates anchor against AEMO's published Step Change
trajectory (Coalition 2024 policy reference for nuclear, which AEMO does not
model).

| Archetype | Coal closure | New gas | Deployment mandate |
|---|---|---|---|
| `cost_optimal` | IASR schedule | Available | None |
| `rapid_coal_phaseout` | ≤ 2030 | Available | None |
| `gas_fleet_maintained` | ≤ 2030 | Available | Gas ≥ 12,500 MW @ 2030 & 2035 |
| `storage_led` | ≤ 2035 | All gas dropped (incl. CCS) | Storage ≥ 1.25× AEMO trajectory per year |
| `fossil_incumbent` | +10 yrs | Available | Solar dropped + 75 % wind dropped |
| `nuclear_baseload` | IASR schedule | Available | Nuclear ≥ 2,000 MW @ 2045, ≥ 4,000 MW @ 2050 |

### AEMO Step Change anchor values used

**Gas (gas_fleet_maintained binding only 2030 & 2035):**

| Year | AEMO projects | Mandate | Binding? |
|---|---:|---:|---|
| 2030 | 11,884 MW | 12,500 MW | yes (+616 MW) |
| 2035 | 11,852 MW | 12,500 MW | yes (+648 MW) |
| 2040 | 12,640 MW | — | non-binding |
| 2045 | ≥ 14,188 MW | — | non-binding |

**Storage (storage_led, 1.25× AEMO each year):**

| Year | AEMO | Mandate |
|---|---:|---:|
| 2025 |  2,756 MW |  3,445 MW |
| 2030 | 27,141 MW | 33,926 MW |
| 2035 | 30,895 MW | 38,619 MW |
| 2040 | 30,747 MW | 38,433 MW |
| 2045 | 33,427 MW | 41,784 MW |
| 2050 | 32,280 MW | 40,350 MW |

**Nuclear (nuclear_baseload, Coalition 2024 reference):**

| Year | Mandate |
|---|---:|
| 2025-2040 | — |
| 2045 | ≥ 2,000 MW |
| 2050 | ≥ 4,000 MW |

### Constraint translation path

Mandates flow as PyPSA `custom_constraints_lhs` / `custom_constraints_rhs`
rows generated at archetype-mutation time by
[`_capacity_floor.py`](archetypes/_capacity_floor.py).

For each milestone year T:
- LHS sums `p_nom` over all matching extendable new entrants active at T
  (build_year ≤ T < build_year + lifetime — PyPSA's multi-period convention).
- Existing-asset contribution (where `closure_year > T`) is subtracted from
  the RHS so the LHS contains only decision variables, satisfying the
  existing custom_constraints framework.
- Mandate years not in `config.temporal.capacity_expansion.investment_periods`
  are skipped with a warning. Years where existing capacity already meets
  the floor are skipped with an info log.

Storage capacity floors flow via a new `storage_capacity` term type added
to [`translator/mappings.py`](../src/ispypsa/translator/mappings.py) and
wired through to `StorageUnit.p_nom` in
[`pypsa_build/custom_constraints.py`](../src/ispypsa/pypsa_build/custom_constraints.py).

---

## 2. Option B maintenance overlay (Phase 2.2)

Pre-pass applied to **every** archetype before per-archetype mutations.
Captures the refurbishment and life-extension spending that ageing thermal
plants actually require but which PyPSA's zero-existing-capital convention
omits from the LP objective.

```
premium(row, T) = max(0, (eol_window - years_to_closure) / eol_window * max_premium)
fom_$/kw/annum   += premium    (per ECAA thermal row)
```

| Fuel | eol_window | max_premium |
|---|---:|---:|
| Black / Brown Coal | 10 yr | 50 AUD/kW/yr |
| Gas (CCGT / OCGT) | 5 yr | 20 AUD/kW/yr |

**Sources** ([`_maintenance_overlay.py`](archetypes/_maintenance_overlay.py)):
AEMO Bayswater refurbishment (Origin Energy 2022 ASX disclosure, ≈ 76 AUD/kW/yr
annualized — used as upper bound); CSIRO GenCost 2024-25 Final Table 4.1
(coal O&M ranges 50-100 AUD/kW/yr for late-life black coal); CSIRO Coal Plant
Working Paper 2024 (aged-fleet opex multipliers 1.3-1.6×); GenCost 2024-25
§3.3 (gas O&M ranges).

**Expected impact:** fossil_incumbent sees substantially higher costs because
it extends ageing plant operation; cost_optimal sees a modest lift in early
years; gas_fleet_maintained sees a lift in 2030-2035 where the mandate forces
gas retention.

**Limitation:** linear ramp is a fleet-level simplification. Real refurbishment
spend is lumpy and plant-specific. Pass 3 should use per-plant schedules.

---

## 3. EOL renewable repowering (Phase 2.3)

Pre-pass applied to **every** archetype. Addresses the 2045 wind capacity
dip observed in prior production runs.

For each ECAA wind / solar row:

```
closure_year     += 20    (one repowering cycle)
fom_$/kw/annum   += capex / (years_to_close + life_extension)
```

| Fuel | repowering_capex | life_extension |
|---|---:|---:|
| Wind | 1,000 AUD/kW | 20 yr |
| Solar | 800 AUD/kW | 20 yr |

**Sources** ([`_repowering.py`](archetypes/_repowering.py)): CSIRO GenCost
2024-25 Final §3.5 (greenfield CapEx baselines ~2,000 / ~1,400 AUD/kW);
IRENA Renewable Power Generation Costs 2023 (40-60 % repowering range);
CSIRO renewable energy work (typical 20-yr modern-turbine life extension).

**Pass-1 limitations** (documented in module):
1. **No capacity-factor uplift.** Modern wind turbines deliver 2-3× the CF
   of 2015-vintage; solar PV CF rises ~10-15 % with bifacial / tracking.
   ISPyPSA traces are sourced per the IASR vintage and cannot be modulated
   per-asset without trace modification — out of MVP scope.
2. **Not an LP investment decision.** Pass 1 treats repowering as a fleet-wide
   default. Pass 3 should inject "repowered" rows into `new_entrant_generators`
   tied to each ECAA wind / solar site.
3. **Single repowering cycle only.** Real repowering can repeat.

Whether the overlay fully resolves the 2045 wind capacity dip is a Phase 6
production-run finding — the cost-side premium can still make
greenfield-elsewhere preferable for some assets, so a residual dip is
possible. The Phase 6 run will quantify this.

---

## 4. Pre-pass orchestration

The `_with_pre_passes` wrapper in
[`archetypes/__init__.py`](archetypes/__init__.py) runs all three pre-passes
in order on every wrapped archetype:

1. `_pumped_storage_fix.apply` — Wivenhoe / Shoalhaven / Borumba / Snowy 2.0
   re-routed from ecaa_generators to ecaa_batteries.
2. `_maintenance_overlay.apply` — ageing premium added to ECAA thermal FOM.
3. `_repowering.apply` — ECAA VRE closure_year extended + repowering FOM.

Then the per-archetype `apply()` function runs, which may add the mandate
constraint rows via `_capacity_floor.add_capacity_floor` (for the three
mandate archetypes only).

---

## 5. Test suite state

```
755 passed, 0 failed
```

(755 from 735 prior baseline; 20 new tests added across Phase 2 work — covering
the mandate constraint helper, the Option B overlay, the repowering overlay,
the parametrized integration check across all six wrapped archetypes, and
the registry-shape invariant.)

The 6 CLI test failures noted at session start are a pre-existing doit
task-target collision on `coal_and_biomass_price_consultant_scenario_mapping.csv`,
unrelated to Phase 2.

---

## 6. Items NOT in scope (deferred)

- **Tier 2 archetypes** (e.g. demand-flexibility, transmission sensitivity)
- **Trace-coverage limitation** acknowledged but not addressed.

---

## 7. Smoke test diagnostic — verified pre-existing

The Phase 2 closure originally surfaced the MVP smoke (via
`mvp_pass1_power/scripts/run_workflow.py`) as blocked on a column-name
mismatch. Diagnostic work has now verified the issue chain is **wholly
pre-existing to Phase 2** — three distinct Phase 1 follow-up bugs are
involved, not one.

### Verification trace

1. **Phase 2 commits don't touch the affected code paths.** `src/ispypsa/templater/`
   and `src/ispypsa/iasr_table_caching/` were last modified by Phase 1
   commits c14e7e0 / eb655ef. Phase 2 only touched `mvp_pass1_power/`,
   `tests/`, and a controlled extension to `translator/mappings.py` +
   `pypsa_build/custom_constraints.py` for the `storage_capacity` term
   type — decoupled from the templater/cache code paths involved in the
   smoke failure.

2. **The normalisation function works.** Running
   `_normalise_cached_csvs_to_v74` directly on the stale cache
   transforms `'ISP Sub-region'` → `'ISP sub-region'` (and 142 other
   v6.0→v7.4 normalisations) as designed.

3. **Fresh cache rebuild advances past the column issue but reveals
   downstream Phase 1 bugs.** A clean cache delete + rebuild gets the
   workflow further into the templater and surfaces a second error at
   `templater/storage.py:54` (`cleaned_storage_summaries["technology_type"]`
   returns a DataFrame, implying duplicate columns after the ECAA +
   new_entrants concat). Pre-existing.

### Pre-existing Phase 1 issues blocking the MVP workflow smoke

| # | Issue | Where | Fix path |
|---|---|---|---|
| (a) | Stale on-disk cache from before Phase 1; `build_local_cache` only runs when the sentinel file is missing, so the new normalisation pass never ran on it | `mvp_pass1_power/data/workbook_cache/` | Delete the cache and let `build_local_cache` rebuild from the workbook (slow but one-time) |
| (b) | Workflow script's sentinel `existing_generators_summary.csv` is consolidated away by Phase 1's normalisation, breaking the cache-existence check on subsequent runs | `mvp_pass1_power/scripts/run_workflow.py:85` | Switch sentinel to the v7.4-canonical `existing_committed_anticipated_additional_generator_summary.csv` (Phase 1 closure commit acknowledges fixing this in the bench runner; the MVP workflow script was missed) |
| (c) | `_template_battery_properties` fails with a duplicate-column error on the v7.4-normalised consolidated summary + new_entrants_summary concat | `src/ispypsa/templater/storage.py:54` | Phase 1 follow-up: dedupe / disambiguate the snake-cased `technology_type` columns before filtering |

### Implication for Phase 6 commissioning

- **Phase 6 production runs use `mvp_pass1_power/bench/run_production.py`**,
  which spawns `bench/run_myopic.py` subprocesses. Per Phase 1 closure,
  the bench runner uses the corrected v7.4 sentinel and works end-to-end
  (Phase 1's NSW 2-period PDLP smoke succeeded via this path).
- **The MVP demonstration script (`scripts/run_workflow.py`) and the
  MVP-postprocess pipeline (`scripts/run_all_archetypes.sh` which calls
  the same workflow) will fail until the three Phase 1 issues above are
  patched.** This is a documentation / demonstration impact, not a
  production-run blocker.

### Recommendation

The three Phase 1 issues are independent of Phase 2 and small enough to
fix as a Phase 1 follow-up in their own commit before Phase 6
commissions. The minimum patch set:

- Add v7.4 sentinel in `scripts/run_workflow.py` (1-line change).
- Investigate and fix `templater/storage.py:54` duplicate-column.
- Add a cache-invalidation marker (or document the "delete cache to
  recover" workaround).

None of these block Phase 6 production runs via the bench runner path.

---

## 8. Phase gate

This document surfaces the integrated state for team confirmation. Phase 6
production runs (extended IASR v6.0 baseline, 6 milestone years × 6
archetypes, PDLP at 1e-3) follow Phase 2 closure when authorised.

**Phase 2 regression confirmed at 755/735 throughout the diagnostic work;
no cache or code changes from this verification step land in the
committed Phase 2 history.**
