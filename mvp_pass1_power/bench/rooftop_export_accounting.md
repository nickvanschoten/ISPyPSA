# Rooftop PV export accounting in the demand and supply representation

Focused diagnostic — read-only investigation. No code changes.

**Date:** 2026-06-02
**Scope:** ISPyPSA + isp-trace-parser + AEMO trace conventions, as used by
the Phase 7.2/7.3 / Phase 8.1 deliverable runs. Cache version v6.0 (matches
Test 1 v2 / Test 2 / Test 3).

---

## TL;DR

| Layer | Question | Answer |
|---|---|---|
| 1 | What does the demand trace net out? | **Total rooftop generation** (self-consumption AND exports) — confirmed empirically via the identity `OPSO_MODELLING_PVLITE = OPSO_MODELLING + PV_TOT` (exact, every snapshot). |
| 2 | Do rooftop exports appear as supply? | **No.** Zero rooftop entries in any IASR cache table (v6.0); zero `rooftop` references in `src/ispypsa/*.py`; "Solar" carrier = "Large scale Solar PV" only. Rooftop is invisible to the supply side. |
| 3 | What do dashboard `total_supply` and `renewable_share` mean? | **Bulk-grid only.** Both numerator (renewable carriers: Wind + utility Solar + Biomass) and denominator (total generation) exclude rooftop. The published 40.7 % renewable_share_pct for cost_optimal_2040 is **not directly comparable to AEMO's renewable-share** which includes rooftop. |
| 4 | Does rooftop netting interact with snapshot weighting? | **Indirectly yes.** Netting itself happens at trace level (resolution-invariant). But the rep-week snapshot weighting `8760 / snapshot_count` amplifies whatever the netted demand happens to be in the sampled weeks. Peak-winter/peak-summer rep-weeks happen to have low rooftop generation (winter peak is evening; summer peak is afternoon when rooftop is high — net effect varies). This is part of the 7% (3-week) / 5% (4-week) over-statement Test 3 identified. |

**Additional finding — rooftop export clipping**: ISPyPSA's `buses.py:127`
clips negative net-demand to zero. In **4.34 % of trace snapshots** the
local OPSO_MODELLING goes negative (local rooftop exceeds local gross
demand — mostly VIC, SQ, CSA midday peaks; max -7,631 MW in VIC). The
clipped MW would be rooftop exports available as supply to other
subregions. **At 2018 reference year, the clipped exports sum to
~1.71 TWh/year NEM-wide** (~0.77 % of total NEM demand; ~2.1 % of total
rooftop generation). Small but quantifiable energy-balance gap.

---

## Layer 1 — what does OPSO_MODELLING net out?

### Trace types supported by isp-trace-parser

From `isp_trace_parser/demand_traces.py:55-57`:

```python
demand_type: Optional[
    list[Literal["OPSO_MODELLING", "OPSO_MODELLING_PVLITE", "PV_TOT"]]
] = None
```

Three demand-trace variants exist per (subregion, POE, scenario, reference_year).

### Identity confirmed empirically

Direct comparison from the project's parquet
(`mvp_pass1_power/data/traces/isp_2024/demand/scenario=Step Change/reference_year=2018/data_0.parquet`),
CNSW POE50 noon snapshots:

| datetime | OPSO_MODELLING | OPSO_MODELLING_PVLITE | PV_TOT | Sum check |
|---|---:|---:|---:|---|
| 2023-09-10 12:00 | 583.768 | 983.940 | 400.172 | 583.768 + 400.172 = **983.940** ✓ |
| 2023-09-10 12:30 | 575.423 | 953.820 | 378.397 | 575.423 + 378.397 = **953.820** ✓ |
| 2023-09-11 12:00 | 620.546 | 1020.205 | 399.659 | 620.546 + 399.659 = **1020.205** ✓ |

**Exact identity**: `OPSO_MODELLING_PVLITE = OPSO_MODELLING + PV_TOT`
holds every snapshot.

NEM-wide annual aggregates (POE50, all replicates averaged):

| | Annual TWh (per replicate year) |
|---|---:|
| OPSO_MODELLING | **223.38** (rooftop-netted demand) |
| PV_TOT | **80.90** (total rooftop PV generation) |
| OPSO_MODELLING_PVLITE | **304.28** (= 223.38 + 80.90; underlying load) |

**Note**: AEMO's quoted "~310 TWh underlying consumption" maps cleanly to
OPSO_MODELLING_PVLITE 304 TWh (the trace is for 2018-vintage volumes,
growth-scaled to 2040 elsewhere; the *ratio* of rooftop to underlying is
the load-bearing quantity).

### Interpretation

- **OPSO_MODELLING** = the demand the bulk grid has to serve, **after**
  rooftop has reduced the local load. AEMO measures this at the bulk-grid
  interface; behind-the-meter rooftop (both self-consumption and exports)
  shows up as reduced bulk-grid demand. There is no measurable distinction
  at the bulk-grid level between rooftop self-consumed and rooftop exported
  to a neighbour on the same LV feeder — both look like "less demand on
  the bulk grid". AEMO encodes this in OPSO_MODELLING.
- **OPSO_MODELLING_PVLITE** = the same demand WITHOUT rooftop's netting
  effect — i.e. customer-side gross load (= operational + rooftop's full
  contribution).
- **PV_TOT** = the total rooftop PV generation that produced the
  OPSO ↔ PVLITE delta.

### Layer 1 verdict: choice (A)

ISPyPSA loads `OPSO_MODELLING` (hard-coded at
`src/ispypsa/translator/buses.py:112`). This trace **already nets total
rooftop generation — both self-consumption AND exports — before ISPyPSA
sees the data**. There is no separation between "rooftop served local
load" and "rooftop served export" in OPSO_MODELLING: both are subtracted
uniformly at the bulk-grid-measurement level.

```python
trace_data = get_data.get_demand_multiple_reference_years(
    reference_year_mapping=reference_year_mapping,
    subregion=list(isp_sub_regions["isp_sub_region_id"].unique()),
    scenario=scenario,
    poe="POE50",
    demand_type="OPSO_MODELLING",       # ← here
    directory=trace_data_path / Path("demand"),
    year_type=year_type,
    select_columns=["subregion", "datetime", "value"],
)
```

POE50 hard-coded. demand_type hard-coded. No CLI override is exposed
through the templater configuration.

---

## Layer 2 — does rooftop appear as supply?

### Code grep results

`grep -rn "rooftop\|Rooftop\|ROOFTOP" src/ispypsa --include="*.py"` → **0 matches**.

Only file in the codebase mentioning rooftop at all:
`src/ispypsa/iasr_table_caching/parser_configs/7.4/rooftop_pv.yaml`. That
file defines IASR-workbook sheet locations for `Rooftop PV` data (consultant
scenario mapping, power capacity, energy capacity) — but no Python loader
reads those tables and no carrier/generator/load infrastructure consumes
them.

**Notably**: there is no `rooftop_pv.yaml` in `parser_configs/6.0/` at all
(only v7.4 has it). Tests 1-3 ran on v6.0, so rooftop data wouldn't even
be parsed from the workbook regardless of whether downstream code consumed
it.

### IASR cache inspection

`mvp_pass1_power/data/workbook_cache/` (v6.0):

- No `*rooftop*` files
- No `*roof*` files
- New-entrant solar technologies: only "Large scale Solar PV" and "Solar
  Thermal (15hrs storage)"
- Build-cost solar technologies: only "Large scale Solar PV" and "Solar
  Thermal (15hrs Storage)"
- ECAA generator-summary "Solar" entries: all "Large scale Solar PV"
  (Avonlie, Beryl, Bomen, Broken Hill, Coleambally, ... — utility-scale
  PV farms)

### Layer 2 verdict: zero

**Rooftop is entirely absent from ISPyPSA's supply representation.** No
generator, no load-component, no negative-demand entry, no carrier. The
LP solves a bulk-grid-only system; rooftop is invisible to it on the
supply side. This is **internally consistent** with Layer 1 (rooftop
already removed from the demand the model sees) — neither side of the
energy balance includes rooftop, so no double-counting or missing
accounting occurs *within the model's frame of reference*.

---

## Layer 3 — what do `total_supply` and `renewable_share` represent?

### Computation in `extract_granular_outputs.py`

```python
_RENEWABLE_CARRIERS = {"Wind", "Solar", "Biomass"}
# ...
renewable_mwh = float(gen_mwh[gen_mwh.index.isin(_RENEWABLE_CARRIERS)].sum())
renewable_share = (renewable_mwh / total_gen_mwh * 100.0) if total_gen_mwh > 0 else 0.0
```

Where `total_gen_mwh = float(gen_mwh.sum())` over all "real" carriers
(exclusion of `bus_for_custom_constraint_gens` and `Unserved Energy`).

The "Solar" carrier here is identical to the IASR "Large scale Solar PV"
carrier per Layer 2 — utility-scale only.

The docstring at the top of `extract_granular_outputs.py` lines 7-16
explicitly documents exclusions:
- Water (hydro): excluded for trace-availability reasons (uniform 100 %
  p_max_pu inflates ~20 pp)
- Nuclear, Hydrogen: excluded for context reasons (debate, upstream
  embodied)
- **Rooftop is not mentioned — it is implicitly excluded because it does
  not appear as a carrier at all** (Layer 2).

### Dashboard `total_generation_twh` / `demand_twh`

From `extract_granular_period`:

```python
"demand_twh": demand_mwh / 1e6,                     # OPSO_MODELLING (rooftop-netted)
"total_generation_twh": total_gen_mwh / 1e6,        # bulk-grid supply only
"supply_gap_pct": ((total_gen_mwh - demand_mwh) / demand_mwh * 100.0),
```

Cross-checked against `outputs/granular/demand_generation.csv`:

| Year | demand_twh | total_generation_twh | supply_gap_pct |
|---|---:|---:|---:|
| 2025 | 182.23 | 183.38 | 0.63 |
| 2030 | 212.09 | 215.94 | 1.82 |
| 2035 | 228.85 | 234.18 | 2.33 |
| 2040 | 253.05 | 258.90 | 2.31 |

`supply_gap_pct` is the LP's internal energy-balance residual (curtailment
or over-dispatch within solver tolerance), not a rooftop accounting
artefact.

### Layer 3 verdict: bulk-grid only

The deliverable's dashboard numbers represent **the bulk-grid power
system** — what generates and what serves the bulk-grid-visible demand.
Rooftop is excluded from both sides of every reported quantity:
- `demand_twh`: rooftop-netted
- `total_generation_twh`: bulk-grid generators only
- `renewable_share_pct`: bulk-grid renewables ÷ bulk-grid total
- `supply_gap_pct`: bulk-grid supply ÷ bulk-grid demand

### Implications for AEMO comparison

AEMO publishes renewable-share figures that **include rooftop PV**. For
2040 Step Change AEMO's all-sector renewable share is in the 80–90 %
range (high VRE + rooftop scenario). ISPyPSA's 40.7 % bulk-grid renewable
share for cost_optimal_2040 is **not directly comparable** without
adjustment.

To produce an AEMO-comparable renewable-share, the following would need
to be added back:

| Adjustment | Numerator (renewable TWh) | Denominator (total TWh) |
|---|---:|---:|
| Add NEM-wide rooftop PV generation | +80.9 TWh (2018-baseline; grow-scaled to ~120-160 TWh for 2040) | +80.9 TWh (same) |

Conceptually: `renewable_share_AEMO = (bulk_renewables + rooftop) / (bulk_total + rooftop)`.

This adjustment would require post-processing — neither `outputs/granular/`
nor the dashboard performs it currently.

---

## Layer 4 — consistency across resolutions

### Where rooftop netting happens

**At trace level, before ISPyPSA sees the data.** AEMO publishes
OPSO_MODELLING as a CSV with one row per half-hour per subregion. The
netting `OPSO_MODELLING = OPSO_MODELLING_PVLITE - PV_TOT` is computed by
AEMO upstream of the trace file. ISPyPSA reads the already-netted value
and uses it as the load at each LP snapshot.

isp-trace-parser does no further netting — it only restructures CSVs into
parquets with metadata columns.

### Snapshot weighting formula

From `src/ispypsa/translator/snapshots.py:222-231`:

```python
snapshots["snapshot_count"] = snapshots.groupby("investment_periods").transform("count")
snapshots["objective"]   = 8760 / snapshots["snapshot_count"]
snapshots["generators"]  = 8760 / snapshots["snapshot_count"]
```

Each investment period gets a uniform per-snapshot weighting of
`8760 hours / number of snapshots in that period`. So:

| Configuration | Snapshots/year | Weight per snap (h) |
|---|---:|---:|
| 3-week (Test 1 v2) | 1,008 | 8.69 |
| 4-week (Test 2) | 1,344 | 6.52 |
| **8760 (Test 3)** | **17,520** | **0.50** |

### Interaction with rooftop netting

The netting itself is **resolution-invariant** — the OPSO_MODELLING value
at a given timestamp is the same MW value whether that snapshot ends up
in a 3-week, 4-week, or 8760 LP.

**But** the snapshot-weighting `8760/snapshot_count` scales the net-demand
value to "represent" multiple weeks at rep-week resolutions. If the
sampled rep-weeks happen to have above-average net demand (low rooftop
relative to gross), the annual integral is over-stated. If they have
below-average net demand (high rooftop relative to gross), the integral
is under-stated.

The selected rep-weeks for Tests 1-2 are:
- `residual-peak-demand` (peak winter — mid-June): low rooftop (winter, evening peak)
- `peak-demand` (peak summer — Australian afternoon): high rooftop (midday)
- `week 42` (mid-October spring): moderate rooftop
- `week 33` (added in Test 2 wind-favourable): moderate rooftop

Empirical annual demand totals per Tests 1-3:

| | demand_twh (annual) | vs 8760 baseline |
|---|---:|---:|
| 3-week (Test 1 v2) | 258.90 | +7.3 % |
| 4-week (Test 2) | 252.11 | +4.4 % |
| **8760 (Test 3)** | **241.36** | — (truth) |

The 7.3 % / 4.4 % over-statement at rep-week sampling is a known
sample-selection bias documented in Test 3's addendum. **Rooftop netting
contributes to this bias** because peak-winter weeks have lower rooftop
generation (less netting) and thus higher net demand; weighting those
weeks at 8.69 h/snap amplifies the high net-demand value. The
peak-summer week partially offsets this (high rooftop netting → lower
net demand → smaller amplification). The 3-week mix net-tips toward
over-statement; 4-week with the added wind-week tips slightly less.

**Layer 4 verdict**: rooftop netting itself is resolution-invariant
(trace-level), but it interacts with snapshot weighting through
sample-selection bias on the already-netted demand value. The
contribution of this rooftop-related component to the 7.3 % / 4.4 %
total over-statement cannot be isolated without simulating each rep-week
in PVLITE mode and reconstructing the rep-week-implied annual rooftop
total — a follow-up if the team wants the decomposition.

---

## Supplementary — the negative-clipping rooftop-export gap

### Behaviour

`src/ispypsa/translator/buses.py:120-127` aggregates subregion demand
traces per demand_node, then applies `node_trace["value"].clip(lower=0.0)`.

This means: if a sub-region's aggregated OPSO_MODELLING goes negative at
a given timestamp (local rooftop exceeds local gross demand), the LP load
is set to zero for that timestamp instead of being negative.

A negative OPSO_MODELLING value physically represents **rooftop exports**
that the bulk grid would carry to other subregions (where they would
serve other loads). By clipping to zero:
- The exporting subregion's demand is set to 0 (not negative)
- The exported MW is NOT made available as supply to any other subregion
- The model effectively **loses** that rooftop-export energy

### Quantification

| Statistic | Value |
|---|---|
| Snapshot fraction with OPSO_MODELLING < 0 | **4.34 %** (282,732 of 6,522,048 trace snapshots) |
| Maximum single-subregion negative value | **-7,631 MW** (VIC) |
| Subregions with negative entries | VIC, SQ, CSA, SNW, SNSW, NQ, NNSW, SESA, CNSW (9 of 12) |
| Subregions never negative | TAS, CQ, GG |
| NEM-wide clipped (lost) energy per year | **1.71 TWh** (~0.77 % of OPSO-modelling annual demand; ~2.1 % of total rooftop generation) |

This is a **modelling gap**, not a bug — it's an explicit choice in
`buses.py:127`. The gap is small in absolute terms (~1.7 TWh/year), but
worth flagging because:
1. It contradicts the energy-balance assumption that "rooftop netting is
   neutral at the bulk-grid level" — for the 4 % of clipped snapshots,
   rooftop exports that would physically traverse the bulk grid are
   removed from the LP entirely.
2. The clipping concentrates in subregions where rooftop penetration is
   highest (VIC, SQ, CSA), so the geographic pattern of the gap is
   non-uniform.
3. At future years with higher rooftop penetration (growth-scaled to
   2040+), the fraction of clipped snapshots will be larger than the
   4.34 % seen at 2018-baseline reference. Whether the project's
   growth-scaling logic preserves the OPSO_MODELLING-can-go-negative
   property is not investigated here.

### Why the clip exists (best guess)

PyPSA's load representation expects non-negative values. The clip
prevents PyPSA from raising a validation error or treating negative
demand as artificial supply. **No code comment in `buses.py` documents
the choice or its implications** — this should be on the team's list of
"undocumented modelling assumptions to surface in methodology notes".

---

## Things to flag to the team

1. **Renewable-share comparison to AEMO**: the dashboard's 40.7 %
   bulk-grid renewable share for cost_optimal_2040 is not directly
   comparable to AEMO's ~80-90 % all-sector renewable share. Either
   adjust ISPyPSA's number to include rooftop or label both numbers as
   "bulk-grid only" / "all-sector" explicitly in the dashboard.

2. **Rooftop is absent from the supply representation**. This is internally
   consistent with the demand netting — no double-counting. But:
   - AEMO-facing reports that include rooftop in generation totals need
     post-processing to add it back.
   - The Tier 2 / STABLE handoff should clarify whether STABLE wants
     bulk-grid quantities or all-sector quantities. They are different
     numbers.

3. **OPSO_MODELLING clipping at -∞ → 0**: ~1.7 TWh/year of rooftop
   exports are lost in the model. Quantitatively small at 2018-baseline;
   probably larger at future-year scaling. Undocumented in code. Worth
   surfacing in the methodology notes regardless of size.

4. **Rep-week-vs-8760 demand over-statement is partially rooftop-driven**.
   The 7.3 % (3-week) and 4.4 % (4-week) over-statements Test 3 quantified
   include a rooftop-netting amplification component because rep-week
   selection happens to favour low-rooftop weeks. This is structural to
   the rep-week methodology, not a fixable artefact unless the rep-week
   selection is rebalanced. **Conclusion: the Test 3 demand-correction
   finding stands; the rooftop-netting interaction is a contributing
   mechanism but not the root cause** (the root cause is rep-week
   selection bias on already-netted demand).

5. **POE50 hard-coding** at `buses.py:111`: the deliverable always uses
   POE50, not POE10. AEMO and many users report based on POE10 for
   capacity planning. This is a separate framing choice the team may
   want to expose explicitly in dashboard methodology.

---

## What this diagnostic does NOT establish

- Whether v7.4 (with the `rooftop_pv.yaml` parser config) changes any of
  the above. The v7.4 parser config exists but no Python code consumes
  it; if a future ISPyPSA release adds a rooftop loader, the Layer 2
  finding could change.
- Whether AEMO's published 2040 demand projection is rooftop-netted at
  the value the project uses for growth-scaling. The "growth-scaling
  preserves OPSO_MODELLING semantics" assumption is unverified here.
- What `outputs/granular/`'s "Solar" capacity column would mean if a
  future hook adds rooftop. Currently it's exclusively utility-scale; if
  rooftop were added it would inflate dramatically.
- The PVLITE 304 TWh vs AEMO's 310 TWh underlying-consumption headline:
  the 6 TWh gap is small enough to plausibly be a year-vintage difference
  (2018 reference vs AEMO's headline year), but not investigated here.

---

## Files referenced

- `src/ispypsa/translator/buses.py:107-132` — demand-trace loading, OPSO_MODELLING / POE50 hard-codes, clip-to-zero
- `src/ispypsa/translator/snapshots.py:197-239` — snapshot weighting formula
- `src/ispypsa/iasr_table_caching/parser_configs/7.4/rooftop_pv.yaml` — defined but unused
- `mvp_pass1_power/postprocess/extract_granular_outputs.py:1-165` — renewable_share + supply totals computation
- `mvp_pass1_power/data/traces/isp_2024/demand/scenario=Step Change/reference_year=2018/data_0.parquet` — empirical identity check source
- `.venv/Lib/site-packages/isp_trace_parser/demand_traces.py:55-57` — demand_type enum
- `mvp_pass1_power/outputs/granular/demand_generation.csv`, `renewable_share.csv` — what the dashboard publishes
