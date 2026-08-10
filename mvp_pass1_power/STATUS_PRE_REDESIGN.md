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

### 1.4 gfm ≡ rcp confirmed structural — single-period myopic has no inter-period inheritance

The 5-archetype 2040 verification ran on Optimus-NC and confirmed that
`gas_fleet_maintained` and `rapid_coal_phaseout` solve to **bit-identical
LPs at 2040 single-period myopic**:

| | rcp 2040 8760 | gfm 2040 8760 |
|---|---:|---:|
| LP rows | 39,194,880 | 39,194,880 (identical) |
| LP cols | 18,077,904 | 18,077,904 (identical) |
| LP nonzeros | 72,494,603 | 72,494,603 (identical) |
| PDLP iter trajectory | every printed iter (0, 4000, …, 32000) | digit-identical Pobj/Dobj/gap/pinf/dinf |
| PDLP terminal iter | 35,840 [L] | 35,840 [L] (identical) |
| Final convergence | gap 9.96e-04 / pinf 8.56e-04 / dinf 5.98e-07 | identical to every sig fig |
| Objective | $13,477,681,276 | $13,477,681,276 (identical to the dollar) |

**This is structural, not behavioural.** The gas-floor mandate is for
years 2030 and 2035 only; neither falls inside the single-period 2040
investment-periods set. Without cross-period build inheritance the
2030/2035 mandate cannot bind at 2040, so gfm's LP IS rcp's LP at
single-period 2040 by construction. The Phase 6/7 "gfm collapses onto
rcp" finding is correct, but the mechanism is sharper than originally
characterised: **it's an architectural property of single-period myopic
mode (no inter-period build inheritance), not a "naturally-builds-enough-
gas-anyway" behavioural outcome.**

This finding is direct evidence that **the current production structure
is independent-static-per-year** — see §5.1 for how this carries into the
redesign reconnaissance.

Evidence: [`bench/phase81_clip_fix_and_5archetype_verification.md`](bench/phase81_clip_fix_and_5archetype_verification.md).

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

The "passed" total moved from the prior 766/766 invariant to 767 because
the two `tests/test_translator/test_buses.py` cases now assert
negative-load pass-through (positive `(... < 0.0).any()` assertions on
both expected and got traces) in addition to the legacy unchanged
content. **The invariant is not silently degrading from 766**: 767 is
the new floor after the deliberate addition of regression coverage for
the clip-fix semantics.

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

**Confirmed pre-existing** (verified at finalisation): re-ran
`tests/test_cli/` at parent commit `544325d` (one before the clip fix
`3f3439e`) — same 6 failures, same error pattern, 1 passed, 1 skipped.
`tests/test_cli/` does not import `src/ispypsa/translator/buses.py`; the
duplicate-target bug in `dodo.py` predates Phase 8.1 work. **These
failures are tracked separately, unrelated to grid-electricity modelling,
and are NOT a regression caused by this branch.**

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

### 3.1 5-archetype 8760 verification — completed (rcp, gfm) + stopped (sl, nb, fi)

The verification asking "does the wind reversal generalise beyond
cost_optimal?" ran on Optimus-NC and was **deliberately stopped by the
team after ~14 h** when the redesign direction (carbon-price sweep
replacing the forced-style archetype catalogue) was settled. Two
archetypes converged with saved NetCDFs; three were stopped before [L]
termination and are recorded here as superseded characteristics, not
deliverables.

Final state at stop:

| Archetype | Status | Wall (h) | PDLP iters | gap_rel | pinf_rel | dinf_rel |
|---|---|---:|---:|---:|---:|---:|
| **rcp** | ✓ converged [L] | 8.89 | 35,840 | 9.96e-04 ✓ | 8.56e-04 ✓ | 5.98e-07 ✓ |
| **gfm** | ✓ converged [L] (≡ rcp bit-identically) | ~9.0 | 35,840 | 9.96e-04 ✓ | 8.56e-04 ✓ | 5.98e-07 ✓ |
| nb | ✗ stopped near-converged | ~14 | 48,000 | 2.94e-04 ✓ | 1.78e-03 | 2.01e-07 ✓ |
| fi | ✗ stopped near-converged | ~14 | 60,000 | 4.99e-04 ✓ | 1.94e-03 | 1.37e-08 ✓ |
| sl | ✗ stopped on slow-tail | ~14 | 56,000 | 9.02e-03 | 2.80e-04 ✓ | 2.18e-06 ✓ |

**Wind-reversal 3-week → 8760, all converged archetypes:**

| Archetype | 3-week Wind | 8760 Wind | Uplift |
|---|---:|---:|---:|
| cost_optimal | 23.67 GW | 27.58 GW | **+16.5 %** |
| rapid_coal_phaseout | 27.98 GW | 32.74 GW | **+17.0 %** |
| gas_fleet_maintained | 27.98 GW | 32.74 GW | **+17.0 %** |

**The wind reversal generalises 3/3 converged archetypes.** Phase 7.4's
universal "structural anti-wind preference" is decisively falsified.
PHASE7_4_FINDINGS.md correction finalised at 3/3.

**Why the three stopped runs are recorded as superseded — not re-opened:**

- **storage_led plateau at gap 9.02e-03 with descent ~1.02×/window**:
  the "no coal, no gas" archetype forces massive storage-SOC chains that
  create PDLP-degenerate LP behaviour. Extends the existing 3-week
  "storage_led 1e-3 floor" caveat to 8760. **No autonomous tolerance
  relaxation applied — superseded.** storage_led is a forced-style
  archetype the redesign abandons; it's also the least-clean test of
  endogenous wind response (storage-constrained by archetype design).
- **nuclear_baseload and fossil_incumbent near-converged-stopped**:
  both have gap ✓ and dinf ✓; only pinf is bouncing in the
  adaptive-restart tail (1.78e-3 and 1.94e-3 respectively). They
  would have completed within another 1-3 h had they been allowed to
  run, but the team retired the forced-style archetype structure
  before that point. **NetCDFs not recovered — superseded.**
- **Full 8760 production wall (~3.8 days at 6-way parallel)** is also
  superseded: the redesign emits a carbon-price-sweep deliverable, not
  6 archetype × 6-year cells.

The trajectories at stop are preserved in `bench/logs/p81v_*.log` and
the convergence-state in `bench/records/p81v_*.json`; see
[`bench/phase81_clip_fix_and_5archetype_verification.md`](bench/phase81_clip_fix_and_5archetype_verification.md)
for the full narrative.

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
| Phase 8.1 solver logs (Tests 1-3 + variance + smoke) | `bench/logs/p81fix_*`, `bench/logs/p81t1_*`, `bench/logs/p81t2_*`, `bench/logs/p81t3_*`, `bench/logs/p81vs_*` | Audit trail (small, KB-scale; matches existing bench/logs convention) |
| Phase 8.1 bench records (Tests 1-3 + variance + smoke) | `bench/records/p81fix_*`, `bench/records/p81t1_*`, `bench/records/p81t2_*`, `bench/records/p81t3_*`, `bench/records/p81vs_*` | Audit trail |
| 5-archetype verification logs + records | `bench/logs/p81v_*.log`, `bench/records/p81v_*.json` | rcp/gfm converged with [L] terminal; sl/nb/fi stopped before [L]. All five log trajectories preserved as audit trail for the falsification evidence on Phase 7.4. |
| Clip-fix + 5-archetype verification report | `bench/phase81_clip_fix_and_5archetype_verification.md` | Narrative + per-archetype convergence detail + supersedes-resolution for the stopped runs |
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
| Solved NetCDFs for the 5-archetype verification | `bench/runs_myopic/p81v_*__*/outputs/capacity_expansion.nc` | ~250 MB each; only exist for rcp and gfm (the two that reached [L]); regenerable from configs + data |

---

## 5. Carried forward into the redesign

The redesign is a separate scoping commission. The findings below are
inputs to that scoping; they are recorded here so the redesign agent does
not have to re-derive them from the addenda.

### 5.1 The current production structure is independent-static-per-year

Direct evidence: gfm ≡ rcp at single-period 2040 (§1.4). The gas-floor
mandate at 2030 and 2035 cannot bind at single-period 2040 because nothing
carries 2030/2035 build state forward. **The current path is static — not
sequential, not perfect-foresight.** This pre-answers the third prong of
the redesign reconnaissance's "what temporal structure is the production
currently using?" question: the path is static.

The persistence probe in the redesign scoping should therefore ask
*whether persistence matters*, not whether it is currently absent — it is
absent. If a recursive-dynamic middle is desired (sequential with
inter-period build carry-forward), it would have to be **built**, not
merely confirmed.

This also means the assembly-fallback's hidden-inconsistency concern
(per-year greenfield frontiers don't share a capacity history) is **live**
for any sweep deliverable assembled from per-year cells without explicit
state passing. The redesign should treat this as a recon item, not an
assumption.

### 5.2 Points-vs-trajectories — routed to STABLE gap analysis

The carbon-price sweep produces per-snapshot-of-the-sweep capacity points.
Whether STABLE consumes these as independent points or as a trajectory
(with carry-forward state between sweep points) is a STABLE-side
question, not an ISPyPSA-side one. ISPyPSA's job is to emit each sweep
point at the correct temporal-structure setting (decided by §5.1's
persistence probe).

### 5.3 Candidate-set prerequisite

The current archetype catalogue exposes some technologies only via forcing
(nuclear, hydrogen) and others only via constraint relaxation (CCS — not
present). The carbon-price sweep needs a clear answer to: for each
candidate technology, is it (a) freely buildable subject to economics
under the sweep, or (b) only-via-force? This is a recon item for the
redesign — it determines what the sweep can actually trade between.

### 5.4 8760 production wall-clock characterisation

Single-LP 8760 at PDLP-1e-3 on Optimus-NC: ~6 h with clip-clipped LP, ~15 h
with the clip fix. Per-year per-sweep-point wall ~15 h is the relevant
unit cost for sweep planning. 6-way parallel reduces per-batch wall to ~15
h; 12-way parallel halves that. Memory budget ~30 GiB per concurrent job
(comfortable within Optimus-NC's available headroom).

---

## 6. Proposed sign-off commits

These commits are already in place at the baseline (verified at
finalisation):

| Commit | Scope | Message |
|---|---|---|
| `544325d` | Missing essential modules | `mvp_pass1_power: force-add two essential modules that were untracked` |
| `3f3439e` | Rooftop clip fix | `fix: pass through negative net demand from OPSO_MODELLING traces` |
| `0cc2ad5` | Bench tooling | `bench: Gurobi tolerance flags + full-year/rep-weeks plumbing` |
| `74887cd` | Phase 8.1 findings + scripts | `Phase 8.1: findings, variance substudy, rooftop scoping + analysis scripts` |
| `fed5b09` | Phase 8.1 logs + records (Tests 1-3 + variance + smoke) | `Phase 8.1: force-add solver logs + bench records for completed runs` |
| `2085ccf` | Correction notices + initial STATUS | `docs: pre-redesign correction notices + STATUS_PRE_REDESIGN` |
| *pending* | This finalisation pass (5-archetype results + Phase 7.4 → 3/3) | `Phase 8.1 finalisation: 5-archetype verification stopped, Phase 7.4 falsified 3/3, STATUS updated` |

After the finalisation commit lands, propose tag
`stable-pre-archetype-redesign` on it with annotated message summarising
the state.

---

## 7. What this baseline is NOT

- **NOT a completed 8760 production sweep.** Single-LP feasibility is
  proven; 5-archetype verification ran for 2040 with rcp/gfm converged
  and sl/nb/fi stopped (per §3.1); the 36-LP all-archetype / all-year
  sweep has not been run and **is not on the redesign critical path**
  (the sweep emits a different deliverable shape).
- **NOT a fix for the 6 CLI test failures.** Verified pre-existing
  (§1.6); deferred so the redesign baseline matches the working state.
- **NOT an authoritative final calibration against AEMO Step Change.**
  The wind-vs-AEMO gap is closing under 8760 (cost_optimal residual
  ~5 GW); the redesign re-frames the question (cross-archetype
  comparison is replaced by carbon-price sweep), so authoritative-
  calibration in the archetype sense is no longer the target.
- **NOT a commitment to keep the six-archetype catalogue.** The next
  step is the carbon-price-sweep redesign; this tag exists so that
  redesign can branch from a known-good state and the team can return
  here if needed.
