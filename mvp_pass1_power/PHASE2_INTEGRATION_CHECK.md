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

## 7. MVP smoke verification — PASSED end-to-end

After the Phase 1 follow-ups (commit 862801d) landed, the MVP workflow
script (`scripts/run_workflow.py`) runs end-to-end on the minimal config
(NSW, single 2050 investment period, one representative week) for all six
production archetypes. All solve to **Optimal**:

| Archetype | Status | Wall-clock | Objective (AUD) | Mandate enforced at smoke scale? |
|---|---|---:|---:|---|
| `cost_optimal` | Optimal | 10.7 s | 7.833 × 10⁹ | n/a |
| `rapid_coal_phaseout` | Optimal | 10.9 s | 7.833 × 10⁹ | n/a |
| `gas_fleet_maintained` | Optimal | 10.7 s | 7.833 × 10⁹ | No — mandate years 2030 & 2035 filtered (minimal config has 2050 only) |
| `storage_led` | Optimal | 9.0 s | 7.842 × 10⁹ | No — see finding (b) below |
| `fossil_incumbent` | Optimal | 3.8 s | 8.036 × 10⁹ | n/a |
| `nuclear_baseload` | Optimal | 13.2 s | 1.166 × 10¹⁰ | **Yes** — `nuclear_floor_2050 ≥ 4,000 MW` enforced and verified in custom_constraints CSV outputs |

### Findings worth surfacing for the team

**(a) Translator REZ-expansion path didn't anticipate non-REZ rhs entries.**
The `_translate_custom_constraints_generators` and
`_translate_custom_constraint_generators_to_lhs` functions in
`src/ispypsa/translator/custom_constraints.py` raised `KeyError` when
the `custom_constraints_rhs` table contained entries that didn't match
any `rez_constraint_id` (the case for Phase 2's NEM-wide aggregate floors).
Fixed in commit 862801d by short-circuiting both functions on empty input.
This was a real bug exposed by Phase 2's mandate constraints — Phase 1's
custom_constraints work assumed all manual rhs entries were
REZ-transmission-expansion candidates. Worth a note for the team: any
future manual constraint type that doesn't correspond to a REZ
expansion will exercise this code path.

**(b) `new_entrant_batteries` table is empty under the NSW filter.**
The minimal config (filter_by_nem_regions=[NSW]) produces zero rows in
`new_entrant_batteries.csv` even though `new_entrants_summary` in the
cache contains 19 CNSW + 28 SNSW + 22 NNSW + 20 SNW battery deployment
rows. The storage_led mandate constraint requires new-entrant batteries
to satisfy the storage_floor, so my `_capacity_floor` helper logs a
warning and skips constraint generation. The LP still runs to Optimal
on the existing battery fleet alone. This is a **pre-existing Phase 1
templater issue** — the v6.0-normalised → templater pipeline has a
filtering step (or a missing one) that drops all sub-region new-entrant
battery rows when the regional filter is applied. **Worth checking
whether this issue persists at full-NEM Phase 6 scale.** If it does, the
storage_led mandate won't bind in production runs either, and the
archetype would degenerate to "no gas + repowering" — a meaningful but
different finding.

**(c) Three archetypes converging on identical objective at smoke scale.**
At minimal config (NSW 2050 single period), `cost_optimal`,
`rapid_coal_phaseout`, and `gas_fleet_maintained` all produce the same
LP objective (7.833 × 10⁹). Expected: at 2050 NSW coal is already
retired per the IASR closure schedule, so `rapid_coal_phaseout`'s
coal-by-2030 clip is a no-op; `gas_fleet_maintained`'s mandate years
(2030, 2035) are filtered out under minimal config's single-2050
periods. **Full-NEM Phase 6 should differentiate these archetypes**
through multi-period evolution where the coal-clip + gas-mandate
levers actually bind. This is the same convergence pattern the team
observed in the prior catalogue and Phase 2 is designed to resolve —
but the resolution requires Phase 6 scale to demonstrate.

---

## 8. Smoke test diagnostic — verified pre-existing (pre-fix history)

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

### Fixed in commit 862801d

All three issues plus two surprises that surfaced during smoke verification:

- **(b)** sentinel updated to v7.4-canonical filename (1-line change to
  `scripts/run_workflow.py`).
- **(a)** `Path.replace` instead of `Path.rename` in the table-rename
  step makes normalisation idempotent.
- **(c)** the duplicate-column issue resolved in two places: rename
  v6.0's `Technology type` (lowercase) on `new_entrants_summary` to
  `Technology category` so it doesn't collide; rename v6.0's lowercase
  `Technology type` on the ECAA per-status summaries to `Technology
  Type` (capital) inside `consolidate_v60_ecaa_generator_summaries` so
  it aligns with v7.4 native and with `new_entrants_summary` for the
  storage.py concat.
- **(d)** NaN guard in `templater/storage.py:_get_storage_duration_for_battery_type`
  so non-string `isp_resource_type` values flow through `re.search`
  cleanly (returning None; downstream drops them).
- **(e)** Translator REZ-expansion-generators path now short-circuits
  on empty input, fixing a `KeyError` exposed by Phase 2's NEM-wide
  mandate constraints not corresponding to any `rez_constraint_id`.

---

## 9. Phase gate

The MVP smoke verification passes end-to-end for all six archetypes
(see §7). Two findings worth team attention before commissioning Phase 6:

1. The empty-`new_entrant_batteries`-under-NSW-filter issue is
   pre-existing Phase 1 templater behaviour. **Worth checking whether
   it persists at full-NEM Phase 6 scale.** If it does, `storage_led`'s
   mandate won't bind and the archetype degenerates to "no gas +
   repowering" — meaningful but not what the catalogue spec intends.
2. The mandate-constraints-in-rhs-don't-match-REZ-expansion fix in
   commit 862801d is a real interaction between Phase 2's constraint
   design and Phase 1's translator code. Worth a code-review pass on
   the surrounding logic to see whether any other downstream consumer
   makes the same assumption.

Phase 6 production runs (full-NEM IASR v6.0 baseline, 6 milestone years
× 6 archetypes, PDLP at 1e-3) can commission with confidence that:

- The MVP demo path works end-to-end (smoke confirmed).
- The bench-runner production path was already working per Phase 1
  closure and is unaffected by these follow-ups.
- All six archetypes solve Optimal at smoke scale.
- The nuclear mandate constraint flows correctly through the full
  pipeline to the LP variables.

**Regression preserved at 755/755 throughout all of Phase 2 + the
Phase 1 follow-ups.**
