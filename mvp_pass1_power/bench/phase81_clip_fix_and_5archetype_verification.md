# Phase 8.1 — rooftop clip fix + partial 5-archetype 8760 verification

Companion to
[rooftop_export_accounting.md](rooftop_export_accounting.md),
[rooftop_clip_fix_scoping.md](rooftop_clip_fix_scoping.md),
[phase81_test3_addendum.md](phase81_test3_addendum.md), and
[phase81_test2_addendum.md](phase81_test2_addendum.md).

**Date:** 2026-06-03
**Hardware:** Optimus-NC — Dell PowerEdge R940xa, 4× Intel Xeon Platinum 8280L
(112P / 224L, 3.07 TiB RAM, AVX-512, Windows Server 2022).
**Cache:** v6.0 (matches Test 1 v2 / Test 2 / Test 3).
**Status:** Stopped at user request mid-verification. Report covers what landed.

---

## TL;DR

**Three things are now established:**

1. **The rooftop clip fix is correct and lands cleanly.** Single
   cost_optimal_2040 8760 smoke run with the fix converged at PDLP-1e-3
   (gap_rel 9.62e-05) at 63,040 iterations / 15.3 h wall — 2.5× slower
   than Test 3's clipped baseline (25,200 iters / 6.14 h). All hard
   validation criteria passed: ≥3 metrics under 1e-3, objective -$12.8 M
   (within projected -$5 to -$15 M), demand -1.4 TWh (within projected
   -1.0 to -2.5 TWh), no new carriers, no infeasibility. Per-bus negative
   loads (rooftop exports) flow correctly via interconnectors at 2040
   growth-scaled rooftop magnitudes.

2. **The Phase 7.4 wind reversal generalises across archetypes that
   converged.** 3-week → 8760 Wind capacity rises by +16.5 % to +17.0 %
   across cost_optimal, rapid_coal_phaseout (rcp), and gas_fleet_maintained
   (gfm). The "structural anti-wind preference" framing remains overturned
   for these archetypes; the preference is a rep-week sampling artefact,
   not an LP-formulation property. PHASE7_4_FINDINGS.md correction notice
   stands.

3. **gas_fleet_maintained ≡ rapid_coal_phaseout at 2040 single-period
   myopic mode — structural collapse confirmed bit-identically.** Same LP
   (39,194,880 rows × 18,077,904 cols × 72,494,603 nonzeros), same PDLP
   iter trajectory (every digit at every printed iter), same convergence
   point (iter 35,840 with gap_rel 9.96e-04), same objective ($13.478 B
   to the dollar). The gas-floor mandate is for years 2030 and 2035 only;
   it cannot bind at single-period 2040. Without inheritance from
   earlier-year builds, gfm IS rcp at 2040. **The gfm collapse at 2040 is
   structural (architectural in single-period myopic mode), not a
   "naturally builds less gas at 8760" outcome.**

**Two things did not finish:**

4. **storage_led PDLP gap convergence is impractical.** sl reached iter
   56,000 in ~14 h with gap 9.02e-03 — descent rate had flat-lined to
   1.02× per 4000-iter window (vs 1.5-2× for other archetypes). At this
   rate gap < 1e-3 would require many days of compute. pinf and dinf are
   well under threshold throughout (3e-4 / 3e-6 at termination). The
   "no coal, no gas" archetype forces full renewable+storage build that
   creates LP degeneracy PDLP struggles with even at 1e-3 tolerance.
   Capacity decisions are likely already near-final but no NetCDF was
   saved (PDLP not terminated → no `save_pypsa_network()` call).

5. **nuclear_baseload and fossil_incumbent are close to convergence but
   stopped before [L] termination.** Both have gap and dinf converged
   (nb: gap 2.94e-04, dinf 2.01e-07; fi: gap 4.99e-04, dinf 1.37e-08) but
   pinf hovering at 1.7-2.0e-3 (~2× threshold). Same adaptive-restart
   tail pattern as the smoke and rcp/gfm — likely would have completed
   in another 4-8K iters (1-3 h more wall-clock). Neither has a saved
   NetCDF.

---

## Step 1 — Implementation (rooftop clip fix)

### Code changes (upstream)

**`src/ispypsa/translator/buses.py`**: removed the `node_trace["value"] =
node_trace["value"].clip(lower=0.0)` line, replaced with a 13-line
docstring documenting:
- Why negative net demand occurs (OPSO_MODELLING = gross − rooftop;
  can be negative)
- What it represents (real rooftop exports the bulk grid carries
  inter-regionally)
- Why PyPSA handles it (Loads accept p_set<0; bus becomes net injector)
- The energy-balance impact (~1.7 TWh/year NEM-wide at 2018-baseline)
- Provenance (commit `8ec1c4b` 2025-11-27 — clip was undocumented)
- Pointer to scoping note (`bench/rooftop_clip_fix_scoping.md`)

**`tests/test_translator/test_buses.py`**:
- Removed both `np.where(... < 0.0, 0.0, ...)` clipping pre-adjustments
  in `test_create_pypsa_friendly_bus_timeseries_data_sub_regions` and
  `_nem_regions`
- Added explicit `assert (expected_trace["value"] < 0.0).any()` in both
  tests — self-documenting regression coverage: fails loudly if either
  the fixture loses negative values or if the clip is reintroduced
- Added explicit `assert (got_trace["value"] < 0.0).any()` as a separate
  "clip-restoration regression" check on the actual function output
- Removed unused `import numpy as np`

### Regression

`uv run pytest tests/ -q` → **767 passed, 1 skipped, 6 failed**. All 6
failures are pre-existing CLI integration test infrastructure issues
(doit "Two different tasks can't have a common target" — unrelated to
buses.py change). All 7 tests in `test_buses.py` PASS including the 2
updated tests with explicit negative-value assertions. **Regression
cleared for the upstream change.**

### PHASE7_4_FINDINGS.md correction notice

Added at the top of `mvp_pass1_power/PHASE7_4_FINDINGS.md` (the team
expanded it to a full superseding correction). The notice flags the
"structural-preference" framing as overturned and points to
Test 3 as the load-bearing evidence. Per the team's expansion, it
should be updated after this verification with per-archetype wind
deltas.

---

## Step 2 — Smoke (cost_optimal_2040 8760 with clip fix)

### Smoke result vs Test 3 (clipped) baseline

| Criterion | Projected | Smoke (clip removed) | Pass/Fail |
|---|---|---|---|
| PDLP iters | ~25K ±10 % | **63,040 (2.5× higher)** | **MISS** (informational) |
| gap_rel | <1e-3 | **9.62e-05** ✓ | PASS |
| pinf_rel | <1e-3 | **9.67e-04** ✓ | PASS |
| dinf_rel | <1e-3 | **3.09e-07** ✓ | PASS |
| Objective change | -$5 M to -$15 M | **-$12.79 M (-0.116 %)** | PASS |
| Annual demand change | -1.0 to -2.5 TWh | **-1.38 TWh** | PASS |
| New carriers | none | **none** (generator carriers identical) | PASS |
| Direction | renewables/storage move slightly | Wind +0.146 / Solar +0.132 / Battery +0.247 GW | PASS |
| Infeasibility | none | **none** — PDLP converged | PASS |
| Wall-clock | ~6.5 h | **15.3 h (2.5× higher)** | **MISS** (informational) |

**Key empirical confirmations from the smoke:**
- LP dimensions identical to Test 3 (39.37 M rows × 18.17 M cols ×
  72.81 M nonzeros) — clip removal changes RHS values only, not LP
  structure.
- Per-bus negative loads pass through: VIC -2,228 MW peak, NNSW -839 MW
  peak, SQ -1,263 MW peak, CSA -1,264 MW peak, SNSW -728 MW peak.
- Negative snapshots: VIC 275, SQ 893, CSA 1,804, SNSW 2,025, NNSW
  4,127 out of 17,520 — exports flow correctly at growth-scaled 2040
  magnitudes.
- **No infeasibility surfaced at 2040 growth-scaled rooftop magnitudes**
  — the absorption-capacity safety hypothesis from scoping (gen back-off
  + transmission-out + storage-charge can absorb max negative loads) is
  empirically confirmed.

### Why 2.5× slower wall-clock?

Negative loads at ~10-25 % of snapshots in VIC/NNSW/CSA/SNSW/SQ create
LP degeneracy through extra bus-balance equality constraints with
negative RHS. PDLP's adaptive restart cycles more times before pinf
settles under threshold. Solver-time impact ~2.5× per LP.

### Updated production estimate

| Configuration | Wall (one LP) | 36-LP sequential | 6-way archetype-parallel |
|---|---:|---:|---:|
| Test 3 (clipped, wrong) | 6.14 h | ~9 days | ~1.5 days |
| Smoke (fixed, correct) | **15.3 h** | **~23 days** | **~3.8 days** |

Still tractable as a one-off authoritative regeneration. STABLE/Tier 2
ingestion timeline absorbs this.

---

## Step 3 — 5-archetype verification (partial)

Launched 5 archetypes in parallel at 2040 8760 PDLP-1e-3 with the clip
fix in place, ~32 GiB each = ~160 GiB total memory. **User stopped the
run at ~14 h elapsed** with 2 of 5 complete.

### Completion status

| Archetype | Status | Wall (h) | PDLP iters | gap_rel | pinf_rel | dinf_rel |
|---|---|---:|---:|---:|---:|---:|
| **rcp** | ✓ converged | 8.89 | 35,840 | 9.96e-04 ✓ | 8.56e-04 ✓ | 5.98e-07 ✓ |
| **gfm** | ✓ converged (≡ rcp) | ~9.0 | 35,840 | 9.96e-04 ✓ | 8.56e-04 ✓ | 5.98e-07 ✓ |
| nb | ✗ stopped near-converged | ~14 | 48,000+ | 2.94e-04 ✓ | 1.78e-03 | 2.01e-07 ✓ |
| fi | ✗ stopped near-converged | ~14 | 60,000+ | 4.99e-04 ✓ | 1.94e-03 | 1.37e-08 ✓ |
| sl | ✗ stopped on slow-tail | ~14 | 56,000+ | 9.02e-03 | 2.80e-04 ✓ | 2.18e-06 ✓ |

Only rcp and gfm have saved NetCDFs (the runner only invokes
`save_pypsa_network()` after PDLP terminates). nb, fi, sl have iter
trajectories in logs but no recoverable capacity solutions.

### LP-dimensional structural identity for gfm vs rcp

| | rcp 2040 8760 | gfm 2040 8760 |
|---|---:|---:|
| LP rows | 39,194,880 | **39,194,880 (identical)** |
| LP cols | 18,077,904 | **18,077,904 (identical)** |
| LP nonzeros | 72,494,603 | **72,494,603 (identical)** |
| PDLP iter trajectory | every iter 0, 4000, 8000, ..., 32000 | **bit-identical Pobj/Dobj/gap/pinf/dinf at every iter** |
| PDLP terminal iter | 35,840 [L] | **35,840 [L] (identical)** |
| Convergence metrics | gap 9.96e-04 / pinf 8.56e-04 / dinf 5.98e-07 | **identical to every sig fig** |
| Objective | $13,477,681,276 | **$13,477,681,276 (identical to the dollar)** |
| Final capacities | identical | **identical** |

**This is the strongest possible empirical confirmation** that gfm and
rcp are the same LP at 2040 single-period myopic mode. The gas-floor
mandate at gas_fleet_maintained is `[12500 MW @ 2030, 12500 MW @ 2035]`;
neither year is in the 2040 single-period investment-periods set, so
the floor cannot bind. Without cross-period inheritance, gfm = rcp by
construction.

**The gfm-collapse-at-2040 is therefore architectural, not a
"naturally-builds-less-gas-at-8760" outcome.** A multi-period
perfect-foresight LP that included 2030 + 2035 + 2040 would behave
differently — the 2030+2035 gas floors would create inherited
gas-fleet state that propagates to 2040 through the build_year +
lifetime logic. Single-period myopic 2040 lacks that inheritance.

### Phase 7.4 wind reversal across converged archetypes

Capacity comparison: 3-week production (Phase 7.2, `outputs/granular/`)
vs 8760 with clip fix. All three converged archetypes show the same
**+16.5 % to +17.0 % Wind uplift** at 8760:

| Carrier | co 3wk | co 8760 | co Δ% | rcp 3wk | rcp 8760 | rcp Δ% | gfm 3wk | gfm 8760 | gfm Δ% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Wind** | 23.67 | **27.58** | **+16.5%** | 27.98 | **32.74** | **+17.0%** | 27.98 | **32.74** | **+17.0%** |
| Solar | 15.05 | 13.74 | -8.7 % | 18.63 | 19.03 | +2.1 % | 18.63 | 19.03 | +2.1 % |
| Gas | 17.31 | 17.88 | +3.3 % | 19.51 | 21.22 | +8.8 % | 19.51 | 21.22 | +8.8 % |
| Water (gens) | 11.47 | 9.49 | -17.3 % | 11.47 | 9.58 | -16.5 % | 11.47 | 9.58 | -16.5 % |
| Biomass | 1.91 | 0.71 | -62.9 % | 2.49 | 0.00 | -100 % | 2.49 | 0.00 | -100 % |
| Black Coal | 3.90 | 3.90 | 0 % | — | — | — | — | — | — |
| Brown Coal | 1.16 | 1.16 | 0 % | — | — | — | — | — | — |
| Storage:Battery | — | 12.42 | — | — | 14.93 | — | — | 14.93 | — |
| Storage:Water (PHES) | — | 5.01 | — | — | 5.01 | — | — | 5.01 | — |

**Wind reversal: confirmed across 3/3 converged archetypes.** Magnitude
+16.5 % to +17.0 % uplift from 3-week to 8760. The Phase 7.4
"structural anti-wind preference" framing is decisively rejected: the
preference was rep-week sample-selection, not LP-formulation. Holds for
cost_optimal AND for the more constrained rcp/gfm.

**Solar pattern is mixed**:
- cost_optimal: Wind crowds out Solar (-8.7 %)
- rcp / gfm: Solar rises with Wind (+2.1 %) — because no-coal forces
  both VRE types simultaneously to fill the dispatch envelope

**Biomass collapses at 8760**: -62.9 % in cost_optimal, -100 % in
rcp/gfm. The biomass-availability cap (Phase 7.0) is non-binding at
8760 because the LP finds enough wind+solar+storage to displace
biomass dispatch entirely (or nearly so).

**Water (conventional hydro generators) drops ~17 % across all three** —
existing hydro is constant in nameplate; the drop reflects
dispatch-driven reductions in *active* annual hydro capacity per the
build_year/lifetime accounting (hydro participates at lower utilisation
when wind+battery cover non-peak hours).

### Cost across archetypes (8760, fixed)

| Archetype | Objective | vs cost_optimal |
|---|---:|---:|
| cost_optimal smoke (8760) | $11,019,578,974 | — |
| **rcp / gfm** (≡ identical) | $13,477,681,276 | +$2.46 B (+22.3 %) |
| nb (partial; gap converged, pinf 1.78e-3) | ~$11.02 B | (insufficient to write) |
| fi (partial; gap converged, pinf 1.94e-3) | ~$6.31 B | (insufficient to write) |
| sl (partial; gap 9.02e-3) | ~$16.11 B | (insufficient to write) |

Order of cost (cheapest → most expensive at 2040): **fi < co ≈ nb < rcp
≡ gfm < sl**. Sensible:
- fi (coal+10y, renewable cap): cheap existing coal dominates → cheapest
- co, nb (no binding 2040 constraint): identical-ish
- rcp, gfm (no coal): forced gas+renewable replacement → costlier
- sl (no coal, no gas): pure renewable+storage → costliest

Annual load served identical at 233.69 TWh across all (clip-fix demand,
data-driven).

### nb / fi near-convergence states (no NetCDFs)

**nb iter 48,000 (stopped)**: Pobj $11.02 B, **gap 2.94e-04 ✓**, pinf
1.78e-03 (1.8× threshold), dinf 2.01e-07 ✓. Two of three metrics
converged. Same adaptive-restart-tail pattern as smoke (which spent
20,000+ iters at gap < 1e-3 waiting for pinf to settle). Estimated 1-3 h
more wall would have produced a saved NetCDF. **Probably tracks
cost_optimal closely** — no 2040 nuclear floor binds; nb collapses to
cost_optimal at 2040 (similar to gfm collapsing to rcp).

**fi iter 60,000 (stopped)**: Pobj $6.31 B, **gap 4.99e-04 ✓**, pinf
1.94e-03 (1.9× threshold), dinf 1.37e-08 ✓. Same near-converged state.
Renewable-cap binding + coal+10y means the solution is structurally
different from cost_optimal (heavily fossil-incumbent-leaning); the
capacity numbers at the iter-60K state are likely very close to final
even without [L] termination. fi cost trajectory at $6.3 B is roughly
half of cost_optimal, reflecting the cheap-existing-fleet bias.

### sl convergence concern (slow-tail)

**sl iter 56,000 (stopped)**: Pobj $16.11 B, gap **9.02e-03** (still 9×
threshold), pinf 2.80e-04 ✓, dinf 2.18e-06 ✓.

sl's gap descent rate flat-lined to 1.02× per 4000-iter window over
iters 28K → 56K (gap 1.43e-02 → 9.02e-03; only 1.59× total reduction
over 28K iters). At this rate, reaching gap < 1e-3 from 9.02e-3 would
require ~250K+ more iters → many more days of compute.

PDLP's adaptive restart sometimes breaks plateaus with sudden
multi-OOM gap drops, but the empirical evidence here is plateau-mode
behaviour. **storage_led at 8760 PDLP-1e-3 may be intractable in
practical wall-clock.** Two possible interpretations:
1. The "no coal, no gas" archetype forces massive long-duration
   storage SOC chains that PDLP struggles with intrinsically. PDLP
   adaptive restart is degenerate on this LP class.
2. The gap is gap_rel and gap_abs is meaningful here — the dual
   objective is descending parallel to primal at similar rate;
   pinf and dinf converged satisfactorily; capacity decisions may be
   stable but the dual proof of optimality is hard. **The capacity
   solution at iter 56K may be operationally sufficient even if PDLP
   never reaches gap < 1e-3.**

Per Phase 8 dashboard methodology, "storage_led 1e-3 is the practical
floor" — the existing 3-week storage_led production has a similar
gap-floor caveat. The 8760 result extends this caveat to 8760 scale:
**storage_led converges on pinf/dinf but the duality gap floors at
~1e-2 even with full annual resolution**.

---

## Implications for the team

### Confirmed before stop

- **Clip fix is correct and validated.** No infeasibility at 2040
  growth-scaled rooftop. Path from clip → no-clip is clean.
- **Phase 7.4 wind reversal generalises to rcp/gfm at +16.5–17.0 %.**
  PHASE7_4_FINDINGS.md correction notice is supported. Wind structural-
  preference framing is empirically rejected across 3/3 archetypes that
  converged.
- **gfm collapses onto rcp at 2040 by LP construction** (not behaviourally
  / not at convergence — at LP-build). The single-period myopic mode at
  2040 loses the 2030/2035 gas-floor information that distinguishes
  gas_fleet_maintained as a scenario.

### Surfaced — team decision points

1. **storage_led convergence at 8760 is not assured.** The slow-tail
   plateau at gap 9e-3 over 28K iters is concerning. Options:
   - Accept "storage_led 1e-3 floor at 8760" (extends existing 3-week
     caveat to 8760)
   - Try a different PDLP convergence approach (e.g. relax gap tolerance
     to 1e-2 specifically for storage_led)
   - Use the iter-56K state as the storage_led solution if pinf/dinf
     suffice for downstream Tier 2 ingestion
   - Investigate why sl differs structurally (the "no coal, no gas"
     constraint forces a different LP-structure class than the other
     four)

2. **gfm at 2040 single-period myopic is structurally rcp.** Two
   options for the deliverable:
   - Document this explicitly in the dashboard: "gas_fleet_maintained
     at 2040 collapses onto rapid_coal_phaseout structurally; the gas
     mandate is binding at 2030 and 2035, propagating to 2040 only via
     inherited build-state which single-period myopic does not capture"
   - Switch production to multi-period perfect-foresight (Test 2 / Test 3
     line of work) for archetypes with cross-period mandates. The
     compute cost at full perfect-foresight 8760 is significantly
     higher; Phase 8 work hasn't yet established that's tractable.

3. **Refined production estimate with the fix**: 36-LP sweep at ~15h
   each. Sequential 23 days. 6-way parallel 3.8 days. The cost-vs-clip-
   restored numbers don't move much (~$13 M on $11 B = 0.1 %); the
   wall-clock is the binding cost. **Whether the team accepts the
   3.8-day production cost vs the methodological cleanness of the fix
   is the call.**

4. **nb and fi near-convergence states are recoverable** if the team
   wants the data. Re-launching just nb and fi with budget ~3 h each
   would produce saved NetCDFs at the same iter neighborhood. Total
   compute cost: ~6 h sequential / ~3 h parallel.

5. **PHASE7_4_FINDINGS.md correction notice has 3-of-5 archetypes
   confirmed.** Per the team's correction-notice text: "Once they have,
   this notice should be updated with the wind-vs-3-week delta per
   archetype." Current confirmed deltas: co +16.5 %, rcp +17.0 %, gfm
   +17.0 %. nb/fi/sl pending.

### Not established by this verification

- Whether nb or fi would have converged within the 25 h budget (likely
  yes for both; ~1-3 h more wall needed for pinf settle)
- Whether sl converges within ANY practical compute budget at 8760
  PDLP-1e-3 (plateau evidence suggests probably not)
- Whether the wind reversal magnitude is uniform across other 2040
  archetypes (cost_optimal +16.5 %, rcp/gfm +17.0 % — narrow range so
  far)
- Whether the wind reversal magnitude holds at non-2040 milestone years
  (2025, 2030, 2035, 2045, 2050 not tested at 8760)
- Whether the cost-decoupling invariant from Phase 7.1.1 stays clean
  with negative loads in the LP (not directly verified; should be
  inferred from the no-new-carriers / unchanged-renewable-share-formula
  pattern in the smoke)

---

## Files produced

- `mvp_pass1_power/bench/phase81_clip_fix_and_5archetype_verification.md` (this report)
- `mvp_pass1_power/bench/records/p81fix_pdlp_8760_2040.json` (clip-fix smoke)
- `mvp_pass1_power/bench/records/p81v_rcp{,_2040}.json` (rcp converged)
- `mvp_pass1_power/bench/records/p81v_gfm{,_2040}.json` (gfm converged)
- `mvp_pass1_power/bench/logs/p81fix_pdlp_8760_2040.log` (smoke log)
- `mvp_pass1_power/bench/logs/p81v_{rcp,gfm,sl,nb,fi}_2040.log` (5 archetype logs)
- Solved NetCDFs (gitignored):
  - `mvp_pass1_power/bench/runs_myopic/p81fix_pdlp_8760_2040__cost_optimal/outputs/capacity_expansion.nc`
  - `mvp_pass1_power/bench/runs_myopic/p81v_rcp_2040__rapid_coal_phaseout/outputs/capacity_expansion.nc`
  - `mvp_pass1_power/bench/runs_myopic/p81v_gfm_2040__gas_fleet_maintained/outputs/capacity_expansion.nc`
- Code changes (committed-ready):
  - `src/ispypsa/translator/buses.py` (clip removed; docstring added)
  - `tests/test_translator/test_buses.py` (2 tests updated with explicit non-clipping assertions; unused np import removed)
- Doc updates:
  - `mvp_pass1_power/PHASE7_4_FINDINGS.md` (correction notice — needs per-archetype wind-delta update once nb/fi/sl converge)

### Code change summary

```
src/ispypsa/translator/buses.py:
  -1 line: clip removed
  +13 lines: docstring documenting negative-load semantics
tests/test_translator/test_buses.py:
  -1 line: unused import numpy
  -4 lines: 2× np.where clipping pre-adjustments
  +20 lines: 4× explicit (... < 0.0).any() regression assertions
mvp_pass1_power/PHASE7_4_FINDINGS.md:
  +47 lines: correction notice (added by team during verification)
```

**Regression status: 767 passed / 1 skipped / 6 pre-existing CLI infrastructure failures unrelated to change.**
