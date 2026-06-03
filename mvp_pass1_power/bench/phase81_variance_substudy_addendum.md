# Phase 8.1 — PDLP variance sub-study (5× PDLP-1e-3 on identical 3-week LP)

Companion to
[phase81_test1_addendum.md](phase81_test1_addendum.md) and
[compute_survey_new_machine.md](compute_survey_new_machine.md).

**Date:** 2026-05-28
**Hardware:** Optimus-NC — Dell PowerEdge R940xa, 4× Intel Xeon Platinum 8280L
(112P / 224L, 3.07 TiB RAM, Cascade Lake-SP, AVX-512, Windows Server 2022).
**Configuration:** identical to Test 1 v2 (cost_optimal_2040, 3-week sampling,
authoritative PHES via `_pumped_storage_fix.py`, v6.0 IASR cache). Run with
five different `run_id` values (`p81vs_pdlp_r1` through `p81vs_pdlp_r5`); LP
content is bit-identical across the five runs.

**Cache version**: **v6.0** (confirmed from md5 of `build_costs.csv` against
`_v60_smoke_cache/build_costs.csv` — identical; "GenCost 2022-23 Final report"
marker present; no `affine_heat_rates_*` files. Same cache as Test 1 v2.)

---

## TL;DR

**5/5 runs produced bit-identical results.** Zero variance across every
solver metric and every carrier-level capacity decision.

| Metric | All 5 runs |
|---|---|
| Objective | **$13,457,503,631** (every run, to the dollar) |
| PDLP iterations | **13,240** (every run) |
| Final gap_rel | **9.76e-04** (every run) |
| Final pinf_rel | 4.98e-04 (every run) |
| Final dinf_rel | 6.04e-06 (every run) |
| Per-carrier stdev | **0.000 % on every carrier** |
| Storage:Battery | 11.215 GW (every run) |
| Storage:Water (PHES) | 5.015 GW (every run) |
| Wind | 23.673 GW (every run) |
| Solar | 15.048 GW (every run) |
| Gas | 17.311 GW (every run) |

Wall-clock varied 439–492 s across the 5 runs (range 53 s, 12 % of mean), but
this is **system noise from concurrent execution with Track A (Test 2)** —
runs 1–4 averaged 444 s while Track A was PDLP-running; run 5 took 492 s
while Track A was in Gurobi crossover (more CPU contention). Zero algorithmic
variance.

**Conclusion:** PDLP is fully deterministic on this LP family at this
tolerance setting. **Phase 7 production's documented "PDLP variance"
(3× Solar range, 81 % storage range across 3 PDLP samples) cannot be
PDLP non-determinism.** It must come from input-side differences between
Phase 7 production runs — e.g., different representative-week selection,
different IASR version (v6.0 vs v7.4), different archetype application
order, or different snapshot weightings. A targeted Phase 7 input-version
audit would identify which.

---

## Per-run table

| Run | Wall (s) | Solve stage (s) | HiGHS time (s) | PDLP iters | gap_rel | Objective ($) |
|---|---:|---:|---:|---:|---:|---:|
| p81vs_pdlp_r1 | 439.0 | 328.0 | 300.4 | 13,240 | 9.76e-04 | 13,457,503,631 |
| p81vs_pdlp_r2 | 445.3 | 353.1 | 325.4 | 13,240 | 9.76e-04 | 13,457,503,631 |
| p81vs_pdlp_r3 | 441.2 | 339.6 | 311.2 | 13,240 | 9.76e-04 | 13,457,503,631 |
| p81vs_pdlp_r4 | 451.9 | 353.9 | 326.2 | 13,240 | 9.76e-04 | 13,457,503,631 |
| p81vs_pdlp_r5 | 492.1 | 386.7 | 357.5 | 13,240 | 9.76e-04 | 13,457,503,631 |

`HiGHS time` is the time inside HiGHS's `optimize()` call (excludes Python
overhead). `Solve stage` is the linopy ↔ HiGHS round-trip plus the optimize
call. The 30–57 s variation in HiGHS time is purely OS-scheduling noise from
sharing CPUs with Track A; the algorithm itself ran for an identical 13,240
iterations every time.

---

## Per-carrier variance table

| Component | Min | Max | Range | Mean | Stdev | Stdev % | Phase 7 sample 1 outlier |
|---|---:|---:|---:|---:|---:|---:|---:|
| Wind | 23.6732 | 23.6732 | 0 | 23.6732 | 0 | 0.000 % | 39.72 |
| Gas | 17.3115 | 17.3115 | 0 | 17.3115 | 0 | 0.000 % | 7.89 |
| Solar | 15.0477 | 15.0477 | 0 | 15.0477 | 0 | 0.000 % | **43.04** |
| Water (gens) | 11.4688 | 11.4688 | 0 | 11.4688 | 0 | 0.000 % | 6.75 |
| Storage:Battery | 11.2148 | 11.2148 | 0 | 11.2148 | 0 | 0.000 % | — |
| Storage:Water | 5.0150 | 5.0150 | 0 | 5.0150 | 0 | 0.000 % | — |
| Black Coal | 3.9000 | 3.9000 | 0 | 3.9000 | 0 | 0.000 % | 3.90 |
| Biomass | 1.9089 | 1.9089 | 0 | 1.9089 | 0 | 0.000 % | 0.52 |
| Brown Coal | 1.1600 | 1.1600 | 0 | 1.1600 | 0 | 0.000 % | 1.16 |
| Hyblend | 0.4000 | 0.4000 | 0 | 0.4000 | 0 | 0.000 % | 0.40 |
| Liquid Fuel | 0.1035 | 0.1035 | 0 | 0.1035 | 0 | 0.000 % | 0.10 |

**Total storage across all 5 runs: 16.230 GW** (Battery 11.215 + PHES 5.015).
Phase 7 sample 1's 28.6 GW total storage is **none of these 5 runs**.

---

## Phase 7 outlier check

Phase 7 production captured 3 PDLP-1e-3 samples per (cost_optimal, year) in
`outputs/phase7_granular/`. Sample 1 (rows 31–40 of `capacity_gw.csv`) is
the high-variance outlier: Solar 43.04 GW, Wind 39.72 GW, storage 28.6 GW —
much higher than the other two Phase 7 samples and much higher than Test
1 v2's PDLP solution.

**No run in this variance sub-study reproduces Phase 7 sample 1.** All 5
runs landed at Solar 15.05 GW (3× below the sample-1 value), Wind 23.67 GW
(40 % below), and total storage 16.23 GW (43 % below).

If Phase 7 sample 1 came from PDLP non-determinism on identical inputs,
**we would expect at least one of these 5 runs to approximate it** under
the law of large numbers — even if rare, a 5-run sample should occasionally
land near the high-variance arm. It did not. This is consistent with sample
1 being an **input-side phenomenon**, not a PDLP-side phenomenon.

---

## What this means for Phase 7's tolerance-robustness framing

**Old interpretation** (from `Phase 7 findings draft: biomass cap binding +
PDLP variance exposure` commit + `Phase 8 (1-4): elevate PDLP variance
framing in dashboard + findings`): the dashboard surfaces "PDLP variance" as
an inherent property of PDLP at 1e-3 tolerance, suggesting downstream
consumers of `outputs/granular/` need to treat capacity numbers as having
3× variance bands.

**Revised interpretation given this sub-study**: PDLP-1e-3 on a fixed LP
produces a deterministic single solution. The 3 Phase 7 "samples" must
have been produced under varying inputs, not varying solver runs on the
same input. **The downstream-variance band is therefore an input-sensitivity
band**, not a tolerance-robustness band. The deliverable's "relative
archetype comparison" framing (Phase 7.2 (c)) is still sound — relative
differences should be input-invariant — but the *absolute* numbers in
`outputs/granular/` are tied to whichever specific input set Phase 7 chose
to publish.

**What changed**: the "variance" the user-facing dashboard exposes is real,
but its *mechanism* is different than originally framed. Resolving it
requires identifying which Phase 7 run corresponds to the published
absolute numbers, and ensuring downstream Pass-2 consumers (`simple-msm`)
either use a specific reference run or are robust to within-input-band
variation.

---

## What the sub-study does *not* establish

- Whether PDLP-1e-3 is deterministic across **different LPs** at this scale.
  All 5 runs here were on identical inputs. A multi-archetype, multi-year
  rerun on Optimus-NC would tell the team whether Phase 7's per-archetype
  numbers are reproducible at the run-by-run level given Phase 7's input
  set.
- Whether PDLP-1e-3 is deterministic at **different tolerance settings**.
  Loosening to 1e-2 or tightening to 1e-4 may introduce variance via the
  PDLP algorithm's adaptive-restart logic.
- Whether PDLP is deterministic on **other HiGHS versions**. This study
  used HiGHS 1.12.0 with cuPDLP-C. Future HiGHS releases may have different
  numerical behaviour.

**Not commissioning these as follow-ups** — the sub-study answers the
specific question asked (does PDLP non-determinism explain Phase 7
variance?). The answer is no; the team can decide whether further sub-
studies are warranted.

---

## Files

- Records: `mvp_pass1_power/bench/records/p81vs_pdlp_r{1..5}{,_2040}.json`
- Logs: `mvp_pass1_power/bench/logs/p81vs_pdlp_r{1..5}_2040.log` (5 logs)
- Solved NetCDFs (gitignored, ~10 MB each):
  `mvp_pass1_power/bench/runs_myopic/p81vs_pdlp_r{1..5}_2040__cost_optimal/outputs/capacity_expansion.nc`
- Analysis script: `mvp_pass1_power/bench/analyse_variance_substudy.py`
- Driver script (runs 2-5): `mvp_pass1_power/bench/run_variance_2_to_5.sh`

### Reproduction

```bash
# Single variance run (replace r1 with r2..r5 for additional runs)
uv run python mvp_pass1_power/bench/run_myopic.py \
    --run-id p81vs_pdlp_r1 --periods 2040 --archetype cost_optimal \
    --use-pdlp --pdlp-tolerance 1e-3 --budget-min 30

# Drive all 5 runs in sequence (run 1 launched manually; 2-5 in script)
bash mvp_pass1_power/bench/run_variance_2_to_5.sh

# Cross-run analysis
uv run python mvp_pass1_power/bench/analyse_variance_substudy.py
```
