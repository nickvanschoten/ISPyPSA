# STATUS — pre-archetype-redesign baseline

**Purpose.** Snapshot of the ISPyPSA fork's state immediately before the
scenario-modelling space is redesigned from six structural archetypes toward
a carbon-price sweep. This document is the audit trail for what was
established, what was superseded, and what remains a known loose end at the
moment of sign-off.

**Date prepared:** 2026-06-03
**Branch:** `main`
**Baseline commit:** *to be stamped after the sign-off commit lands*
**Proposed tag:** `stable-pre-archetype-redesign`

---

## 1. What is established at this baseline

### 1.1 Pipeline + contract

- Six-archetype Pass 1 pipeline runs end-to-end at full-NEM scale through
  the templater → translator → PyPSA build → HiGHS solve → simple-msm CSV
  emission chain. All six archetypes produce simple-msm-compatible
  `method_years.csv` rows.
- The Pass 1 ↔ Pass 2 contract (cost decoupling, NGER emission cross-walk,
  per-archetype max/min/activity bounds) is implemented and documented.

### 1.2 Phase 8.1 compute findings (Optimus-NC, 4× Xeon Platinum 8280L, 3 TiB RAM)

- **Test 1**: both Gurobi (BarConv/Opt/Feas = 1e-3) and HiGHS PDLP-1e-3
  solve the 3-week production LP cleanly in <12 min wall-clock; capacity
  decisions agree to within 1 % on every carrier above 0.5 GW.
- **Test 2**: 4-week sampling tractable; first observation that capacity
  mix is resolution-sensitive (more Solar / less Wind vs 3-week).
- **Test 3**: **8760-hour single-LP dispatch tractable on the new compute**.
  PDLP-1e-3 converged on the cost_optimal 2040 8760 LP in 6.14 h wall-clock
  (5.77 h solver), 30.4 GiB peak RSS. This resolves the parked v2 question
  — rep-week sampling limits were compute-bound, not algorithmic.
  Evidence: [`bench/phase81_test3_addendum.md`](bench/phase81_test3_addendum.md).
- **Variance sub-study**: PDLP is fully deterministic on a fixed LP across
  five independent re-solves of the same input. The Phase 7 "variance" the
  team observed was input-side (model corrections landing between runs),
  not solver non-determinism. Evidence:
  [`bench/phase81_variance_substudy_addendum.md`](bench/phase81_variance_substudy_addendum.md).

### 1.3 Wind reversal — Phase 7.4 conclusion overturned for cost_optimal

At 8760 resolution, the LP builds **27.4 GW Wind for cost_optimal 2040**
vs 23.7 GW (3-week) and 20.5 GW (4-week) — a +16 % to +34 % step-up at
full resolution. The Phase 7.4 "wind structural-preference" hypothesis is
falsified for cost_optimal: the under-deployment was a rep-week
sample-selection artefact, not an LP-formulation property. Phase 7.4's
correction notice is in place at
[`PHASE7_4_FINDINGS.md`](PHASE7_4_FINDINGS.md).

The wind-vs-AEMO gap narrows from ~10 GW (3-week) to ~5 GW (8760) for
cost_optimal 2040. Whether the residual ~5 GW is data-side (IASR feed),
framing-side (POE50 vs underlying demand), or model-side is unresolved.

### 1.4 gfm-collapse confirmed at 8760

The 5-archetype 2040 verification (in flight as of 2026-06-03 11:30) shows
`gas_fleet_maintained` and `rapid_coal_phaseout` solving to **identical
PDLP iteration trajectories digit-for-digit through iteration 32,000**
(same iterate values, same gap_rel = 1.08e-3, same pinf = 6.61e-04). This
confirms the Phase 6/7 finding that the 12,500 MW gas mandate is
**non-binding** at the LP's natural transition-gas response — and the
finding is robust to the 8760 resolution upgrade. Unlike the wind result
(rep-week artefact), the gfm-collapse is a real structural property of the
model: under coal-by-2030 with IASR demand growth and IASR fuel costs, the
LP builds gas at or above the mandate floor irrespective of resolution.

### 1.5 Rooftop clip fix

- **Code change**: `src/ispypsa/translator/buses.py:127` no longer clips
  `node_trace["value"]` to `lower=0.0`. Negative net demand (rooftop
  exports exceeding local gross demand) now passes through as negative
  `Load.p_set`, which PyPSA + linopy + HiGHS handle correctly (bus
  becomes a net injector; surplus flows via links / charges storage /
  displaces local generation).
- **Why**: the OPSO_MODELLING demand traces are AEMO's "operational
  demand" convention = gross_demand − total_rooftop_PV. Aggregated across
  a demand_node, this can go negative at snapshots when local rooftop
  exceeds local gross. The previous clip silently discarded
  ~1.7 TWh/year (NEM-wide at 2018-baseline; larger at growth-scaled
  years) of physical export flow, opening an energy-balance gap that
  grew with rooftop-PV growth scaling.
- **Provenance**: clip was introduced 2025-11-27 by upstream commit
  `8ec1c4b` ("set minimum node demand to zero") without documented
  rationale, no PR description, no test asserting the clipping behaviour.
- **Smoke validation**: passed (PDLP converges, objective deltas match
  the predicted direction, no infeasibility, no bus-mismatch warnings).
- **Test coverage**: `tests/test_translator/test_buses.py` updated to
  assert that the negative values in the fixture pass through to the
  returned trace (regression guard against re-introducing the clip).
- **Documentation**:
  [`bench/rooftop_export_accounting.md`](bench/rooftop_export_accounting.md)
  (diagnostic) and
  [`bench/rooftop_clip_fix_scoping.md`](bench/rooftop_clip_fix_scoping.md)
  (scoping + smoke validation).

### 1.6 Regression status

```
767 passed, 1 skipped, 6 failed in 190 s
```

The two `tests/test_translator/test_buses.py` cases now assert
negative-load pass-through and contribute to the +1 (was 766/766 prior to
the rooftop fix; 767 is the new baseline after retitling the same two
tests to enforce the new semantics).

The 6 failures are pre-existing CLI infrastructure issues in
`tests/test_cli/`:

```
tests/test_cli/test_create_and_run_capacity_expansion_model.py::test_core_functionality_and_triggers
tests/test_cli/test_create_and_run_operational_model.py::test_core_functionality_and_triggers
tests/test_cli/test_create_ispypsa_inputs.py::test_create_ispypsa_inputs_task
tests/test_cli/test_create_ispypsa_inputs.py::test_cli_flags_and_dependency_chain
tests/test_cli/test_create_operational_timeseries.py::test_core_functionality_and_triggers
tests/test_cli/test_create_pypsa_friendly_inputs.py::test_core_functionality_and_triggers
```

All six fail with the same root cause: doit refuses to register the
task pipeline because `cache_required_iasr_workbook_tables` is declared
as both a task (at [src/ispypsa/cli/dodo.py:743](src/ispypsa/cli/dodo.py#L743))
*and* as a target of another task (at
[src/ispypsa/cli/dodo.py:52](src/ispypsa/cli/dodo.py#L52)). Error:

> `iasr_scenario_mapping.csv' is a target for cache_required_iasr_workbook_tables and cache_required_iasr_workbook_tables.`

**Confirmed pre-existing**: `tests/test_cli/` does not import
`src/ispypsa/translator/buses.py` and has no dependency on the rooftop
clip. The duplicate-target bug lives in `dodo.py` whose last edit
predates Phase 8.1 work (commit `c67154d`, "testing feedback and fixes").
These failures are tracked separately and are NOT a regression caused
by this branch.

---

## 2. What has been superseded (correction notices applied)

### 2.1 Phase 7.4 structural-preference framing

- **What it said**: rep-week capacity-signal distortion is an LP
  formulation property; Path B (correcting the signal in-place) is not
  viable; escalation to 8760 is required.
- **What overturns it**: Phase 8.1 Test 3. At 8760 resolution the same
  LP builds *more* Wind, not less. The "preference" was a sample-coverage
  problem, not a formulation problem. Path B was unrecoverable because
  no in-place fix to a 3-week sample can synthesise the wind hours that
  fall outside the sample.
- **Correction location**:
  [`PHASE7_4_FINDINGS.md`](PHASE7_4_FINDINGS.md) — top-of-document
  CORRECTION NOTICE. Original text retained below the notice for audit
  trail.

### 2.2 Rep-week production absolute numbers (README, PHASE7_FINDINGS, dashboard)

- **What they showed**: per-archetype capacity / cost / emissions at
  Phase 7.2 3-week sampling, presented as the deliverable's headline.
- **What changes**: capacity mix and absolute cost level shift
  materially at 8760 (cost_optimal 2040: +33 % Wind, -28 % Solar,
  -17 % objective). Cross-archetype *relative* rankings and the
  cost–emission trade-off direction survive; absolute levels do not.
- **Correction location**: top-of-document notice in
  [`README.md`](README.md), [`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md),
  and a `st.warning(...)` banner at the head of every dashboard tab in
  [`dashboard/dashboard.py`](dashboard/dashboard.py).

### 2.3 PDLP "variance" framing

- **What it said** (Phase 6/7 mid-history): PDLP at 1e-3 introduces
  solver-driven variance into the capacity mix; "48.8 GW solar swing"
  attributed to PDLP non-determinism.
- **What overturned it**: the §0 correction in
  [`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md) (already in place); the
  Phase 8.1 variance sub-study (5 independent PDLP re-solves of the
  same LP returned bit-identical iteration trajectories). The "swing"
  was Phase 6 → Phase 7 model corrections (hydro water-carrier,
  pumped-storage routing, biomass cap, nuclear/hydrogen) landing
  between runs, *not* solver variance.
- **Correction location**: already reframed in
  [`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md) §0.4; confirmed by
  [`bench/phase81_variance_substudy_addendum.md`](bench/phase81_variance_substudy_addendum.md).

---

## 3. Known loose ends (explicit, not hidden)

### 3.1 5-archetype 8760 verification — in flight at sign-off

The verification asking "does the wind reversal generalise beyond
cost_optimal?" is **mid-flight as of 2026-06-03 ~11:30**. Five PDLP-1e-3
runs started ~02:40 the same day on Optimus-NC. Iteration trajectories
at ~9 h wall-clock:

| Archetype | Iter | gap_rel | pinf_rel | dinf_rel | Status |
|---|---:|---:|---:|---:|---|
| cost_optimal | 25,200 | 9.93e-04 ✓ | 8.79e-04 ✓ | 1.73e-06 ✓ | Test 3 — converged |
| rapid_coal_phaseout | 32,000 | 1.08e-03 | 6.61e-04 | 1.31e-06 | very close |
| fossil_incumbent | 40,000 | 7.11e-04 ✓ | 7.50e-03 ✗ | 7.05e-08 | gap converged, pinf needs more iters |
| gas_fleet_maintained | 32,000 | 1.08e-03 | 6.61e-04 | 1.31e-06 | digit-identical to rcp ≤ 32k |
| nuclear_baseload | 32,000 | 1.06e-03 | 1.12e-03 | 2.04e-07 | very close |
| storage_led | 36,000 | 1.20e-02 ✗ | 3.81e-04 ✓ | 8.99e-06 | hard tail; gap 12× above target |

**Implication for sign-off:**

- The four "easier" archetypes (rcp, fi, gfm, nb) are tracking toward
  convergence. Final capacity numbers are not yet written out, so the
  wind-reversal generalisation claim remains *inferred from trajectory*,
  not *confirmed from capacities*.
- `storage_led` 2040 at 8760 is in the same "hard archetype-year"
  category as `storage_led` 2035 at 3-week was at Phase 7.2. The 1e-3
  tolerance floor caveat documented in [`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md)
  §0.4 extends to 8760. **No autonomous tolerance relaxation has been
  applied** — the run is left to its budget.
- The team may choose to either (a) wait for the 4 easy archetypes to
  finish before tagging, or (b) tag now with this state documented and
  re-tag (or amend the tag) once the verification capacities are
  available. The redesign does not depend on this verification
  completing first.

### 3.2 The 6 pre-existing CLI test failures

Tracked separately (see §1.6). Fix is a one-line dodo.py edit (remove
the duplicate target declaration); deferred so the redesign baseline
matches the working state at sign-off and the fix is not entangled with
the redesign branch.

### 3.3 Full 36-LP 8760 production NOT yet run

Phase 8.1 Test 3 demonstrated *single-LP* 8760 feasibility. Phase 8.1
verification demonstrates *single-year multi-archetype* 8760 in flight.
**The full 36-LP (6 archetypes × 6 milestone years) 8760 production
sweep has not yet been executed.** Test 3's wall-clock estimate puts
this at ~1.5 days on Optimus-NC at 6-way parallel; the team has the
compute but the redesign may make this moot (carbon-price sweep emits
a different deliverable shape than 36 archetype-year cells).

### 3.4 Phase 7 sample 1 outputs provenance

The high-variance outlier in `outputs/phase7_granular/sample1/` was
identified in Phase 7.2 as coming from an untracked input configuration
(the corresponding solve script was not committed to a reproducible run
config). The published rep-week numbers are being superseded by 8760
anyway, but the provenance gap is noted here so it isn't lost.

### 3.5 Rooftop renewable-share framing

The model is **bulk-grid only**. With operational-demand inputs (rooftop
already netted out), the renewable share computation is on bulk-grid
generation only and lands around 40 %. AEMO's all-sector ~80–90 %
renewable share at 2050 includes rooftop PV. To produce an
AEMO-comparable headline the post-processor would need to add rooftop
back as a Tier 2 step — this is a STABLE decision left to the
deliverable-packaging phase. The relative cross-archetype renewable
ranking is correct regardless.

### 3.6 POE50 hard-coded

`src/ispypsa/translator/buses.py` consumes the POE50 (50 % probability
of exceedance) AEMO demand trace. POE10 is more conservative and used
by AEMO for capacity adequacy. Exposing POE10 as an alternative would
be a small code change (trace selection parameter) but is not currently
wired into the ModelConfig. Out of scope for the redesign.

### 3.7 EOL line-ending normalisation noise

Six pre-existing tracked files appear as "modified" in `git status` due
to CRLF/LF re-detection on the Windows working copy (no content
differences):

- `mvp_pass1_power/archetypes/__init__.py`
- `mvp_pass1_power/archetypes/cost_optimal.py`
- `mvp_pass1_power/archetypes/fossil_incumbent.py`
- `mvp_pass1_power/archetypes/nuclear_baseload.py`
- `mvp_pass1_power/archetypes/rapid_coal_phaseout.py`
- `tests/test_model/test_pypsa_friendly_inputs/test_custom_constraints/snapshots.csv`

`git diff --ignore-cr-at-eol` confirms zero content delta. These will
either be (a) re-normalised by the sign-off commit (folded in as a
side-effect of the working-tree being touched), or (b) left as
unchanged in working tree and surfaced for a deliberate `.gitattributes`
fix as a separate task. Decision deferred to the sign-off commit step.

---

## 4. Pre-redesign artefact inventory

### 4.1 Tracked at sign-off (force-added or pre-existing)

| Category | Files | Why tracked |
|---|---|---|
| Phase 8.1 finding addenda | `bench/phase81_test1_addendum.md`, `bench/phase81_test2_addendum.md`, `bench/phase81_test3_addendum.md`, `bench/phase81_variance_substudy_addendum.md` | Audit trail for Tests 1-3 + variance sub-study |
| Phase 8.1 compute survey | `bench/compute_survey_new_machine.md` | Optimus-NC characterisation |
| Rooftop fix documentation | `bench/rooftop_export_accounting.md`, `bench/rooftop_clip_fix_scoping.md` | Diagnostic + scoping for the fix |
| Phase 8.1 analysis scripts | `bench/analyse_test2_4week.py`, `bench/analyse_test3_8760.py`, `bench/analyse_variance_substudy.py`, `bench/compare_test1_gurobi_pdlp.py`, `bench/phase81_progress.py`, `bench/run_variance_2_to_5.sh` | Reproducibility of the Phase 8.1 analyses |
| Phase 8.1 solver logs (completed runs) | `bench/logs/p81fix_*`, `bench/logs/p81t1_*`, `bench/logs/p81t2_*`, `bench/logs/p81t3_*`, `bench/logs/p81vs_*` | Audit trail (small, KB-scale; matches existing bench/logs convention) |
| Phase 8.1 bench records (completed runs) | `bench/records/p81fix_*`, `bench/records/p81t1_*`, `bench/records/p81t2_*`, `bench/records/p81t3_*`, `bench/records/p81vs_*` | Audit trail |
| Missing essential modules | `archetypes/_pumped_storage_fix.py` (imported by `archetypes/__init__.py`), `postprocess/extract_dispatch_timeseries.py` (imported by `dashboard.py`) | These were on disk but ungitted; force-added to make the repo runnable from a fresh clone |
| Rooftop fix | `src/ispypsa/translator/buses.py` + `tests/test_translator/test_buses.py` | The fix itself + regression coverage |
| Bench tooling | `bench/instrumented_runner.py` + `bench/run_myopic.py` | Gurobi flag plumbing + full-year / rep-week mode for Phase 8.1 tests |
| Correction notices | `PHASE7_4_FINDINGS.md`, `README.md`, `PHASE7_FINDINGS.md`, `dashboard/dashboard.py` | Documented above |
| This STATUS doc | `STATUS_PRE_REDESIGN.md` | Pre-redesign snapshot |

### 4.2 Deliberately left local (gitignored)

| Category | Files | Why local |
|---|---|---|
| Solved PyPSA networks | `bench/runs_myopic/`, `runs/`, `bench/runs/`, all `*.nc` | ~GB-scale NetCDFs; regenerable from configs + data |
| Auto-generated configs | `bench/configs_myopic/` | Rebuilt by `run_myopic.py` |
| Input data | `data/` (IASR workbook, trace data, NGA PDF) | Downloadable; ~1.6 GB |
| Python bytecode | `__pycache__/` | Standard |
| 5-archetype verification logs | `bench/logs/p81v_*.log` | **Still being written**; not committed at sign-off because the runs are mid-flight. Force-add after they complete if the team wants the trajectory snapshot. |
| 5-archetype verification records | `bench/records/p81v_*.json` | Will only exist after the runs complete. |

---

## 5. Proposed sign-off commits (for team review before staging)

| # | Scope | Files | Message |
|---|---|---|---|
| 1 | Missing essential modules | `archetypes/_pumped_storage_fix.py`, `postprocess/extract_dispatch_timeseries.py` | "force-add essential modules that were on disk but untracked" |
| 2 | Rooftop clip fix | `src/ispypsa/translator/buses.py`, `tests/test_translator/test_buses.py` | "fix: pass through negative net demand (rooftop exports) from OPSO_MODELLING traces — revert upstream commit 8ec1c4b's undocumented clip" |
| 3 | Bench tooling | `bench/instrumented_runner.py`, `bench/run_myopic.py` | "bench: Gurobi tolerance flags + full-year / rep-weeks selection plumbing for Phase 8.1" |
| 4 | Phase 8.1 findings + scripts | `bench/phase81_*.md`, `bench/rooftop_*.md`, `bench/compute_survey_new_machine.md`, `bench/analyse_*.py`, `bench/compare_test1_gurobi_pdlp.py`, `bench/phase81_progress.py`, `bench/run_variance_2_to_5.sh` | "Phase 8.1: force-add Tests 1-3 findings, variance sub-study, rooftop scoping, analysis scripts" |
| 5 | Phase 8.1 logs + records (completed runs only) | `bench/logs/p81{fix,t1,t2,t3,vs}_*.log`, `bench/records/p81{fix,t1,t2,t3,vs}_*.json` | "Phase 8.1: force-add solver logs + bench records for completed Phase 8.1 runs" |
| 6 | Correction notices | `PHASE7_4_FINDINGS.md`, `README.md`, `PHASE7_FINDINGS.md`, `dashboard/dashboard.py`, `STATUS_PRE_REDESIGN.md` | "docs: pre-redesign correction notices + STATUS_PRE_REDESIGN" |

After all six commits land, propose tag `stable-pre-archetype-redesign`
pointing at the final commit (the STATUS doc) with annotated message
summarising the state.

---

## 6. What this baseline is NOT

- **NOT a completed 8760 production sweep.** Single-LP feasibility is
  proven; 5-archetype verification is in flight; the 36-LP all-archetype
  / all-year sweep has not been run.
- **NOT a fix for the 6 CLI test failures.** They are documented as
  pre-existing and deferred.
- **NOT an authoritative final calibration against AEMO Step Change.**
  The wind-vs-AEMO gap is closing under 8760 but a residual ~5 GW gap
  for cost_optimal 2040 is unresolved (and the all-archetype 8760
  picture isn't yet available).
- **NOT a commitment to keep the six-archetype catalogue.** The next
  step is the carbon-price-sweep redesign; this tag exists so that
  redesign can branch from a known-good state and the team can return
  here if needed.
