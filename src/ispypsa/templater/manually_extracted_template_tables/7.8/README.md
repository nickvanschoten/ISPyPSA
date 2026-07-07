# Manually-extracted template tables — IASR 2026 ISP FINAL (v7.8)

`load_manually_extracted_tables("7.8")` reads every `*.csv` in this directory.
These tables hold workbook data that `isp-workbook-parser` can't structure, so
they are extracted by hand per workbook vintage.

| Table | v7.8 status |
|---|---|
| `policy_generator_types.csv` | Reused from v7.5 (vintage-invariant tech-label map; byte-identical across 6.0/7.3/7.4/7.5). |
| `custom_constraints_lhs.csv` | **Transcribed from the 2026 FINAL workbook** (see below). |
| `custom_constraints_rhs.csv` | **Transcribed from the 2026 FINAL workbook** (see below). |

## Custom-constraint source

The custom-constraint LHS/RHS encode AEMO's REZ **group / transmission-limit
constraints**. In the FINAL (v7.8) workbook these are published in the sheet
**"Build limits - REZs"**, in three stacked tables:

- **REZ Group Constraints** — rows 136–265
- **REZ transmission limit constraints** — rows 267–317
- **REZ secondary transmission limit constraints** — rows 319–335

Each constraint occupies a block: the first row carries the *Group constraint ID*
(column D), the import / summer-typical limit (columns E–H), and the first LHS
*Term* (column B); subsequent rows in the block list the remaining terms, with
per-term coefficients embedded in the term string (e.g. `-0.5 * SQ-CQ`,
`0.85 * BPH`). A term with no explicit coefficient is `1.0`.

The ISPyPSA encoding is unchanged from v7.5: one LHS row per term
(`constraint_id, term_type ∈ {generator_output, storage_output, link_flow},
term_id, coefficient`) and one RHS row per constraint
(`constraint_id, constraint_type, rhs`). `term_id` must match a PyPSA component
`isp_name`: `link_flow` → a flow-path / REZ-connection Link; `generator_output`
→ a Generator; `storage_output` → a StorageUnit (battery).

The RHS value used is the **summer-typical transmission limit** (column F) for
group/transmission constraints, and the **import limit** (column I) for the
Hydrogen import constraints (whose transmission columns read `N/A`) — matching
the v7.5 convention.

## What changed vs the v7.5 set (the FINAL deltas)

| Change | Evidence in "Build limits - REZs" |
|---|---|
| **N9 split → N9a / N9b** | `initial_resource_limits` now lists N9a (r-data) and N9b in place of N9. |
| **`N9_Hydrogen` → `N9a_Hydrogen`** | Row 276: group ID `N9a_Hydrogen`, terms `N9a` + `NNSW-SQ`, import limit **1020**. The v7.5 link ref `N9-CNSW` is stale; renamed to `N9a-CNSW`. |
| **New `HCC1`** (tied to N9b) | Row 258: group ID `HCC1`, limit **750**, terms incl. `N9b`, `Hunter Power Station`. **FLAGGED — not transcribed** (see below). |
| **New `MN2`** | Row 179: limit **1120**. **FLAGGED — not transcribed.** |
| **New `NW1`** | Row 307: limit **310**. **FLAGGED — not transcribed.** |
| **New `SNW1`** | Row 284: limit **2790**. **FLAGGED — not transcribed.** |
| **`WV1` → `WV1 (pre WRL)` / `WV1 (after WRL)` + new `WV2 (after WRL)`** | Rows 194 / 208 / 217: limits 490 / 550 / 2240. **FLAGGED — not transcribed** (term set changed to a detailed generator-coefficient form). |
| **`SWNSW2` → `SWNSW2 (after VNI West)`** | Row 226: limit **2700** (was 4400). Transcribed under the stable id `SWNSW2` with the v7.5-structured membership and the FINAL limit. |
| **`MN1` limit 1630 → 1460** | Row 172. |
| **`CNSW1` limit 815 → 1800** | Row 238. |
| **`SQ1 (After Borumba PHES)`** new variant (3800) | Row 147. Not transcribed (Borumba-PHES scenario variant, not needed for the menu run). |

## Transcribed constraints (→ FINAL workbook source row)

RHS limits (`custom_constraints_rhs.csv`):

| constraint_id | rhs | type | FINAL source row | notes |
|---|---|---|---|---|
| NQ1 | 2420 | <= | 140 | unchanged from v7.5 |
| CQ1 | 1700 | <= | 143 | unchanged |
| SQ1 | 1400 | <= | 145 | unchanged |
| SQ1 (after CQ-SQ Upgrades) | 2500 | <= | 154 | unchanged |
| SEVIC1 | 4200 | <= | 161 | unchanged |
| SEVIC1 Post V8 Option 2 | 6200 | <= | 165 | unchanged |
| SWV1 | 2495 | <= | 169 | unchanged |
| MN1 | 1460 | <= | 172 | **limit updated** (was 1630) |
| NSA1 | 585 | <= | 187 | unchanged |
| NET1 | 1600 | <= | 192 | unchanged |
| CNSW1 | 1800 | <= | 238 | **limit updated** (was 815) |
| SWQLD1 | 3000 | <= | 271 | unchanged |
| N9a_Hydrogen | 1020 | <= | 276 | **renamed** from N9_Hydrogen (N9 split) |
| Q7_Hydrogen | 1050 | <= | 278 | unchanged |
| Q1_Hydrogen | 490 | <= | 280 | unchanged |
| Q4_Hydrogen | 960 | <= | 282 | unchanged |
| WNV1 | 2040 | <= | 299 | unchanged |
| SWNSW1 | 1200 | <= | 323 | unchanged |
| SWNSW2 | 2700 | <= | 226 | **limit updated** (was 4400); see flag below |

LHS membership (`custom_constraints_lhs.csv`): each constraint's term rows mirror
the v7.5-validated PyPSA component encoding for the constraints whose membership
is unchanged in the FINAL workbook (NQ1, CQ1, SQ1, SQ1-after-CQ-SQ, SEVIC1,
SEVIC1 Post V8, SWV1, MN1, NSA1, NET1, CNSW1, SWQLD1, Q7/Q1/Q4_Hydrogen, WNV1,
SWNSW1). The two edited constraints:

- **N9a_Hydrogen** (rows 276–277): `N9a-CNSW` (link, 1.0) + `NNSW-SQ` (link, 1.0).
  The `N9a-CNSW` term is the FINAL-faithful split of v7.5's `N9-CNSW`.
- **SWNSW2** (rows 226–237): v7.5 link/generator/storage membership retained,
  RHS updated to the FINAL `SWNSW2 (after VNI West)` limit 2700.

## Verification

Templater + translator consume the tables end-to-end:

- Unfiltered `create_ispypsa_inputs_template` keeps all 19 transcribed constraints
  (none rejected by `_filter_custom_constraints`).
- `_process_manual_custom_constraints` (translator) emits PyPSA-format
  `custom_constraints_lhs` / `custom_constraints_rhs` for 16 of the 19; the
  three not emitted (NQ1, NET1, Q1_Hydrogen) reference only REZ-connection links
  (Q1/Q2/Q3-NQ, T1/T4-TAS) that exist as *inferred* link names in the templater
  but are not present in the translator's flow-path Link set — identical
  behaviour to the v7.5 encoding (not a v7.8 regression).
- A NSW-only `filter_by_nem_regions=[NSW]` run completes
  template + translate with no error; under NSW filtering the manual constraints
  are dropped by the upstream `_filter_custom_constraints` (see "Known upstream
  filter behaviour" below), so the NSW `custom_constraints_*` output contains the
  endogenous REZ build/resource/expansion constraints only.

## FLAGGED for human review (NOT transcribed — do not guess)

The following FINAL group constraints are **new in v7.8** with LHS term sets that
do not map cleanly onto confirmed PyPSA component `isp_name`s, so they were left
out rather than guessed (per the "flag rather than guess" rule):

- **HCC1** (row 258, limit 750) — terms `N9b`, `Hunter Power Station`, and
  load / PV / V2G / battery-charge terms (`-0.03 * SNW Load`, `0.06 * PV SNW
  Area1`, `0.06 * SNW V2G Area1`, …). `Hunter Power Station` and the SNW
  load/V2G/PV-area terms are not resolvable to model component names.
- **NW1** (row 307, limit 310) — Murray-region VIC solar/BESS farms
  (`Bannerton SF`, `Gannawarra SF`, `Wemens SF`, … `V1_SAT_North West VIC`,
  `0.83 * V1_WH_North West VIC`) not present as confirmed component names.
- **SNW1** (row 284, limit 2790) — coal/GPG/hydro coefficient terms
  (`0.2 * Bayswater`, `0.33 * Mt Piper`, `0.856 * SNWGPGSO`, `0.77 * Kangaroo
  Valley Hydro`, …) plus aggregate `CNSW Generators` / `CNSW Demand` pseudo-terms.
- **MN2** (row 179, limit 1120) — `0.6 * S3-existing` / `0.9 * S3-new entrant`
  splits and an `Electrolyser load at B…` term that have no single-component
  equivalent.
- **WV1 (pre/after WRL)** and **WV2 (after WRL)** (rows 194 / 208 / 217) — the
  FINAL form replaced v7.5's simple `V3-WNV` / `V4-WNV` link constraint with a
  detailed per-generator coefficient list (`0.33 * Kiamal SF`, `0.58 * Horsham
  SF`, `0.79 * Ararat WF`, …). The generator `isp_name`s and the pre/after-WRL
  variant selection need a human decision.
- **SQ1 (After Borumba PHES)** (row 147, limit 3800) — Borumba-PHES scenario
  variant; not required for the archetype-menu run.

Other known data-topology gaps surfaced while transcribing:

- **N9a / N9b carry no `isp_sub_region_id`** in the templated
  `renewable_energy_zones` (the "Renewable energy zones" sheet still lists the
  pre-split `N9`→CNSW, while `initial_resource_limits` lists N9a/N9b). No
  `N9a-…` / `N9b-…` REZ-connection Link is therefore inferred, so the
  `N9a-CNSW` term in `N9a_Hydrogen` (and any future N9b term) is filtered out
  downstream. Wiring N9a/N9b to their sub-region is the same open REZ-topology
  item tracked for the 2026 fleet.

## Known upstream filter behaviour (not specific to v7.8)

`_filter_custom_constraints` (templater) drops a constraint unless **every** term
references a selected component, and it checks `storage_output` terms against the
selected *generators* set — batteries are not passed in. Consequently any manual
constraint that contains a `storage_output` term, or a link/generator outside the
filtered region, is dropped under regional filtering (e.g. a NSW-only run). This
is upstream ISPyPSA behaviour and applies identically to the v6.0 / v7.5 sets.
