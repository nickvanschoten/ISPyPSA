# Rooftop negative-net-demand clip fix — scoping note

Scoping only. **No code changes in this note.**

**Date:** 2026-06-02
**Trigger:** Rooftop export accounting diagnostic
([rooftop_export_accounting.md](rooftop_export_accounting.md)) identified
~1.71 TWh/year of rooftop exports lost at `src/ispypsa/translator/buses.py:127`
via `node_trace["value"].clip(lower=0.0)`.
**Goal:** Answer the three scoping questions before implementing the fix.

---

## TL;DR

**The clip is NOT load-bearing.** Empirical smoke test confirms PyPSA +
linopy + HiGHS handle negative load values cleanly (bus becomes a net
injector, exports flow on links to importing buses, gen at the deficit
bus backs off). The clip was added 2025-11-27 by commit `8ec1c4b` with
message "set minimum node demand to zero" — no documented rationale, no
referenced failure, no PR description. Reads as a convention-driven
change ("demand should be non-negative"), not a defensive fix.

**Recommended fix: remove the clip.** Use PyPSA's native negative-load
handling. No new generator, no new carrier, no formulation complexity.
~3-line change in `buses.py` plus a test update.

**Single residual risk: infeasibility at extreme rooftop scaling.** If a
sub-region's negative net demand exceeds (existing gen back-off
capacity + transmission export capacity + storage charge capacity) at
some snapshot, the LP is infeasible. Smoke-testing on cost_optimal 2040
8760 will surface this if it exists; at 2018-baseline scaling (where
peaks are -7,631 MW in VIC and VIC has ~6 GW interconnector capacity
plus large gen fleet), risk is low. **For ≥2040 growth-scaled rooftop
the negative magnitudes will be larger** — worth confirming smoke
result before extrapolating to multi-archetype production.

**Smoke plan**: single cost_optimal 2040 8760 with the fix, compare to
Test 3 baseline. Validate ~$5-15M lower cost (~$1.7 TWh × ~$5-10/MWh
displaced gen marginal cost), bulk-grid gen drops ~1.7 TWh, PDLP
convergence preserved, no Rooftop carrier appears anywhere.

---

## Q1 — Was the clip load-bearing?

### Git history

```
commit 8ec1c4bc45d1e4c1917bbe1ce1296a06171bdd20
Author: nick-gorman
Date:   Thu Nov 27 19:08:05 2025 +1100

    set minimum node demand to zero
```

Diff (the entire commit):

```python
# src/ispypsa/translator/buses.py (was buses.py:127 in current HEAD)
  node_trace = node_traces.groupby("Datetime", as_index=False)["Value"].sum()
+ node_trace["Value"] = node_trace["Value"].clip(lower=0.0)
  node_trace["Datetime"] = node_trace["Datetime"].astype("datetime64[ns]")
```

Plus two test updates in `tests/test_translator/test_buses.py` that add
`expected_trace["Value"] = np.where(expected_trace["Value"] < 0.0, 0.0,
expected_trace["Value"])` to align the expected output with the new
clip — i.e. tests assert the clip is applied, but do not document why.

**No PR description, no commit body, no issue reference, no test
docstring rationale.** The author's intent has to be inferred from
the commit message: "set minimum node demand to zero" reads as a
domain-convention assertion ("demand should be non-negative") rather
than a fix for an observed failure.

The commit immediately before this (`952b9956 disable new entrant p_nom_max
and p_nom_mod`) is similarly terse and undocumented — same author, same
period. It suggests a working session of small upstream tweaks that
weren't intended to be load-bearing.

### Empirical smoke test of PyPSA's behaviour with negative load

A minimal 2-bus PyPSA model (bus A with `p_set=[-5, 0, 5]`, bus B with
`p_set=[10, 10, 10]`, link A→B with `p_nom=20`, gens at each bus) solves
cleanly under HiGHS:

| t | load_A | gen_A | gen_B | link A→B | bus_A balance |
|---|---:|---:|---:|---:|---:|
| 0 | -5 | 5 | 0 | 10 | gen − load − flow = 5 − (−5) − 10 = 0 ✓ |
| 1 | 0 | 10 | 0 | 10 | 10 − 0 − 10 = 0 ✓ |
| 2 | 5 | 10 | 5 | 5 | 10 − 5 − 5 = 0 ✓ |

At t=0 the model correctly routes bus_A's negative load (an "export"
into the bus) through the existing gen_A + the link to feed bus_B.
Objective: $275 = (5+10+10)·$1 from gen_A + 5·$50 from gen_B. Energy
balance exact, no warnings, no infeasibility.

**Conclusion**: PyPSA / linopy / HiGHS have no problem with negative
load. Removing the clip does not expose a solver issue.

### Reasoned analysis of removal risks

| Risk | Severity | Why |
|---|---|---|
| Solver-level failure | None | Negative RHS in linear constraint; standard LP. |
| Unboundedness | None | Load values bounded by data; bus must balance. |
| Infeasibility (general) | **Possible at extreme scaling** | If `\|negative_load\|` > absorbable (gen back-off + transmission out + storage charge), LP is infeasible. |
| PyPSA validation error | None | PyPSA accepts negative `p_set` on Loads — no class-level validator. |
| Bus-balance corruption | None | Same constraint, different sign on RHS. |
| Dual-solve quirks | None | Both PDLP and barrier handle negative RHS identically. |

### Infeasibility risk — quantification

Maximum single-subregion negative load observed in 2018-baseline trace:
**-7,631 MW (VIC, midday peak)**.

VIC's absorption capacity at that snapshot:
- Existing utility gen at VIC: ~7,500 MW thermal (LYA + LYB + Yallourn +
  Loy Yang A + Loy Yang B etc.) — can back off most of it
- Interconnector capacity out of VIC: ~3 GW (Heywood + Murraylink to SA)
  + ~2 GW (VIC-NSW) + ~600 MW (BassLink to TAS) = ~5.6 GW total
- Storage at VIC: variable per scenario; cost_optimal_2040 has ~10 GW
  Battery NEM-wide of which VIC has substantial share

At 2018-baseline rooftop scaling, VIC's -7,631 MW peak is well within
absorbable capacity. **No infeasibility expected at 2040 scaling either**
because: (a) interconnector + storage capacity grows with the build, (b)
VIC's existing thermal can back off to zero.

The growth-scaled rooftop magnitude for 2040 likely peaks at ~12-15 GW
in VIC (rough 2× scaling). That's still well below the absorption capacity
in a cost_optimal_2040 fleet. **But this should be empirically confirmed
in the smoke test rather than assumed.**

### Q1 verdict: not load-bearing

The clip exists as undocumented author convention, not as a guard
against a real solver problem. Empirical test confirms PyPSA handles
negative load cleanly. Removing the clip is safe at the formulation
level. The residual infeasibility risk is data-magnitude-dependent and
addressable through smoke testing rather than formulation safeguards.

---

## Q2 — How should negative net demand enter the formulation?

### Candidate A: Negative load (`p_set < 0`)

PyPSA's Load component accepts any real `p_set` value. The bus balance
becomes:

```
sum(gen.p) + sum(link.p_in − link.p_out) + sum(storage.p_discharge − p_charge) = load.p_set
```

With `load.p_set < 0`, the bus must net-inject. The LP figures out how:
back off local gen, push out via link, charge storage, or combination.
Empirically confirmed cleanly in the smoke above.

### Candidate B: Zero-cost generator with `p_max_pu` = export profile

Add a `Generator` per bus, carrier `"Rooftop Export"`, `marginal_cost=0`,
`p_nom_max = max(|negative_load|)`, `p_max_pu = max(0, -load) / p_nom_max`.

Both load and rooftop-export generator would coexist; the load value
would stay at `max(0, OPSO_MODELLING)` (i.e. the current clipped value)
and the export generator would inject up to its `p_max_pu` per snapshot.

### Comparison

| Aspect | Negative load (A) | Zero-cost gen (B) |
|---|---|---|
| Lines of code | ~3 (delete clip + small docstring) | ~50 (new generator type + carrier mapping + capacity setting) |
| Semantic match | Direct (rooftop export = negative net demand) | Indirect (rooftop modelled as supply) |
| Renewable-share impact | Unchanged — rooftop stays out of renewable_share denominator | Would inflate denominator unless explicitly excluded |
| Carrier appearance | None — rooftop invisible to capacity_gw.csv | New "Rooftop Export" carrier in capacity_gw.csv |
| Solar capacity confusion | None — Solar means utility-scale only | Risk — "Solar" + "Rooftop Export" both renewable |
| Cost-decoupling clean | Yes — no marginal cost added to dispatch | Yes — but new gen needs explicit `fuel_cost=0` |
| Test changes | Update 2 test cases to remove `np.where` clipping | Add new tests for the rooftop generator infrastructure |
| Production data prep | Unchanged (existing trace) | Need to compute per-bus export profile separately |
| Risk of misclassification downstream | None | Real (dashboard, post-processors need to know about new carrier) |

### Q2 verdict: negative load (A)

**Negative load is clearly the right approach.** It's the direct
semantic match, requires no new infrastructure, and preserves the
"rooftop is invisible to the supply representation" property that the
dashboard's renewable-share methodology relies on.

The zero-cost generator approach would require explicit dashboard /
extract_granular_outputs.py / cost_decoupling-logic changes to keep
rooftop out of renewable_share denominators and out of operator-controllable
cost intensity. That's substantially more change for no semantic gain.

---

## Q3 — Interactions

### Transmission

PyPSA `Link` has `p_min_pu` and `p_max_pu` per-snapshot (defaults -1 and +1
for typical interconnectors). Flow can go in either direction up to
`p_nom`. In the smoke test above, the link carried 10 MW A→B at t=0,
including 5 MW originating from bus_A's negative load (i.e. the export).

**Behaviour at transmission saturation**: if the link is at `p_nom` and
the bus still has net injection (= negative load not yet absorbed), then
the LP needs additional absorption locally: gen back-off (if any gen
above zero) or storage charge. If neither is available, **LP is
infeasible**. For ISPyPSA's NEM model with multiple interconnectors
per region and substantial existing thermal that can back off, this is
unlikely to bind at 2018-baseline scaling.

**Curtailment as a feature**: there is no "negative-load curtailment"
mechanism in ISPyPSA today (no symmetric counterpart to Unserved Energy).
If the smoke surfaces infeasibility, the cleanest mitigation would be a
per-bus zero-cost "export curtailment" generator (paired with negative
load) — but that's only needed if the data demands it.

### Storage

PyPSA `StorageUnit` and `Store` accept p_charge from the bus regardless
of the bus's net sign. The storage charging variable is just an
additional consumer at the bus, and the LP fills it up to capacity if
the marginal value of stored energy exceeds the marginal value of
present consumption.

At a bus with negative net demand (rooftop surplus), storage charging
is "free" energy — the LP will charge to capacity if storage has room
and a forward dispatch opportunity exists. This is physically realistic
(batteries soak up midday rooftop overgeneration is a standard
behaviour in real systems).

**Confirmed**: PyPSA's storage_unit balance does not care about the
sign of bus net demand; it just balances. No special handling needed.

### Cost-decoupling invariant

The Pass-1 deliverable's cost-decoupling pattern
(`postprocess/extract_method_years.py`) computes `output_cost_per_unit`
as bundled cost minus fuel cost. Rooftop has:
- Zero marginal cost
- Zero fuel use
- No fuel-price decoupling needed

When the clip is removed, the LP objective drops slightly (~$5-15M for
1.7 TWh of bulk-grid gen displacement at ~$5-10/MWh average displaced
marginal cost). This drop appears in:
- `network.objective` (bundled cost)
- `extract_method_years.py` output_cost_per_unit (decoupled cost — drops
  proportionally because fuel use also drops in the importing regions
  whose dispatch was displaced)

**Critically**: the dispatch reduction in importing regions removes
both:
- The marginal-cost component of the displaced gen (already in bundled
  cost)
- The fuel-cost component of the displaced gen (subtracted by
  decoupling logic)

So the output_cost_per_unit drops by the *non-fuel* component of the
displaced gen's marginal cost (VOM + emission cost if any). For typical
NEM thermal back-off, that's ~$1-3/MWh VOM. The total drop in
output_cost_per_unit across 1.7 TWh is therefore small (~$2-5M
annualised) — barely visible at archetype-level reporting.

**Invariant**: rooftop export does NOT enter as supply, does NOT
contribute to operator-controllable cost intensity (because it's a
load reduction, not a generator). The Phase 7.1.1 verification pattern
(operator-controllable cost intensity = bulk-grid cost / bulk-grid
generation) is unchanged.

### Q3 verdict: clean

Transmission, storage, and cost-decoupling all behave correctly under
the negative-load fix. The single residual concern is infeasibility at
extreme rooftop scaling (transmission saturation case) — addressable
through smoke testing rather than formulation safeguard.

---

## Recommended implementation

### Code change (estimated ~5 minutes)

**File: `src/ispypsa/translator/buses.py`**

Remove line 127:

```python
# CURRENT (HEAD)
node_trace = node_traces.groupby("datetime", as_index=False)["value"].sum()
node_trace["value"] = node_trace["value"].clip(lower=0.0)         # ← DELETE
# datetime in nanoseconds required by PyPSA
node_trace["datetime"] = node_trace["datetime"].astype("datetime64[ns]")
```

Replace with brief docstring inline:

```python
# PROPOSED
node_trace = node_traces.groupby("datetime", as_index=False)["value"].sum()
# Negative values are valid: AEMO's OPSO_MODELLING is gross_demand − rooftop_PV
# and can be negative when local rooftop generation exceeds local gross demand.
# PyPSA Loads accept p_set < 0; the bus becomes a net injector and the surplus
# flows out via links / charges storage / displaces local gen back-off. See
# mvp_pass1_power/bench/rooftop_clip_fix_scoping.md for the analysis behind
# this. Previously clipped to 0 by commit 8ec1c4b (2025-11-27); the clip was
# undocumented and load-bearing-checked to be non-essential.
node_trace["datetime"] = node_trace["datetime"].astype("datetime64[ns]")
```

### Test changes (estimated ~10 minutes)

**File: `tests/test_translator/test_buses.py`** — remove the two
`np.where(... < 0.0, 0.0, ...)` expected-trace adjustments. The tests
should now assert the raw aggregated trace passes through unchanged.

### Regression discipline

Per the user's commission: "If the fix touches upstream src/ispypsa/buses.py
(it will — that's where the clip is), re-run the regression suite after
implementing, since this is an upstream change rather than bench-only."

Plan: `uv run pytest tests/ -q` after the edit. Confirm 766/766 still
passes (with the two updated test cases now expecting unclipped output).
If any other tests fail unexpectedly, surface those.

---

## Smoke plan

**Single cost_optimal 2040 8760 LP**, fix applied. Identical to Test 3
(p81t3_pdlp_8760) configuration except `buses.py:127` clip removed.
Same machine, same v6.0 cache, same authoritative PHES, same PDLP-1e-3
tolerance.

### Run

```bash
# After applying the buses.py fix and updating the two test cases
uv run python mvp_pass1_power/bench/run_myopic.py \
    --run-id p81fix_pdlp_8760 --periods 2040 --archetype cost_optimal \
    --use-pdlp --pdlp-tolerance 1e-3 --full-year --budget-min 960
```

Wall: ~6.5 h per Test 3 timing.

### Validation criteria

1. **Convergence preserved**: PDLP reaches gap_rel < 1e-3, all three
   metrics. Iteration count similar to Test 3's 25,200.
2. **Demand drops by ~1.71 TWh** vs Test 3's 241.36 TWh → expect ~239.65
   TWh (or close, depending on how much of the 1.71 TWh clip-loss applies
   at the 2040 growth-scaled volumes — could be larger if rooftop scales
   up).
3. **Bulk-grid generation drops by ~1.71 TWh** vs Test 3's 240-something
   TWh. The drop should appear in the importing regions' utility-scale
   dispatch (less Gas / less Wind dispatched to serve loads now served
   by negative-load rooftop exports).
4. **Objective drops by ~$5-15M** (small, but proportional to the displaced
   gen's marginal cost). Test 3 was $11.03B; fix expects ~$11.02B or
   similar.
5. **No new Rooftop carrier** in `capacity_gw.csv`, `generation_twh.csv`,
   `renewable_share.csv` — the dashboard outputs should be unchanged in
   structure.
6. **No infeasibility** in any sub-region at any snapshot. If
   infeasibility surfaces, surface it — that's the formulation question
   the user warned about ("surface it as a more substantial decision
   rather than forcing a fix").
7. **Cost-decoupling invariant intact**: `extract_method_years.py` output
   `output_cost_per_unit` should drop by a small amount (~$1-3/MWh
   reduction in displaced regions' fuel-decoupled gen). The Phase 7.1.1
   operator-controllable cost intensity check should pass.
8. **Direction sanity**: VIC, SQ, CSA, SNSW (the four subregions with
   significant negative-clip incidence per the diagnostic) should show
   slightly less bulk-grid gen vs Test 3. Other subregions can show
   slightly more imports (consuming the exported energy).

### Comparison artefacts

| Quantity | Test 3 (clipped) | Test 3 fix (smoke) | Expected Δ |
|---|---:|---:|---:|
| PDLP iters | 25,200 | TBD | ~25K ±10% |
| gap_rel final | 9.93e-04 | TBD | similar |
| Objective | $11,032,365,128 | TBD | -$5M to -$15M |
| Annual demand | 241.36 TWh | TBD | -1.0 to -2.5 TWh |
| Total bulk-grid generation | 247.21 TWh (approx) | TBD | -1.0 to -2.5 TWh |
| Wind | 27.43 GW | TBD | similar |
| Solar | 13.61 GW | TBD | similar |
| Battery | 12.17 GW | TBD | similar |
| Wall-clock | 6.14 h | TBD | similar (no LP-size change) |

### If smoke surfaces infeasibility

Per the user's instruction: don't force the fix. Surface the infeasibility
empirically with subregion / snapshot identification, and present:
- The infeasibility detail (which bus, which snapshot, magnitude of
  unabsorbed negative load)
- The data conditions that produced it (rooftop magnitude, transmission
  state, etc.)
- Candidate mitigations (per-bus zero-cost export curtailment generator
  paired with negative load; or proportional scaling-down of negative
  loads to fit absorbable capacity)

The team then decides whether to mitigate or roll back. **Don't choose
the mitigation autonomously** — the clip-restoration would be a
substantive methodology change.

---

## Sequencing

Per the user's note: "the 5-archetype 8760 verification has NOT started
yet, so there's no in-flight work to coordinate around. The clean order
is: scope → fix + smoke → then verification and full production both
run on the fixed model from the outset."

Recommended sequence:
1. Surface this scoping note for team review.
2. On approval: implement the fix (~15 min including test update).
3. Run regression suite (`uv run pytest tests/ -q`, ~5-10 min).
4. Launch single-LP smoke (~6.5 h wall-clock).
5. Validate against expected direction; if clean, proceed to 5-archetype
   verification on the fixed model. If anomalous (infeasibility, wrong-
   direction movement, regression breakage), surface findings and pause.

---

## What this scoping does NOT establish

- Whether negative-load magnitudes at 2040 growth-scaling exceed
  absorbable capacity in any sub-region. The smoke test will surface
  this empirically.
- Whether the rep-week sampled production runs (Tests 1-2) would have
  surfaced the same negative-load magnitudes if the clip were absent.
  Probably yes for the peak-summer week which has high rooftop generation.
- The behaviour at v7.4 IASR cache (where build costs and capacity
  parameters differ). The smoke is on v6.0 to match Test 3.
- Whether other clip-style defensive code exists elsewhere in the
  ISPyPSA codebase that should be reviewed in tandem. Not searched.

---

## Files referenced

- `src/ispypsa/translator/buses.py:127` — the clip
- `tests/test_translator/test_buses.py` — two test cases asserting the clip
- Commit `8ec1c4bc45d1e4c1917bbe1ce1296a06171bdd20` — commit that introduced the clip
- `mvp_pass1_power/bench/rooftop_export_accounting.md` — diagnostic that surfaced this
- `mvp_pass1_power/postprocess/extract_method_years.py` — cost-decoupling logic
- `mvp_pass1_power/postprocess/extract_granular_outputs.py` — renewable-share logic
- `.venv/Lib/site-packages/pypsa/components/_types/loads.py` — PyPSA Load class
