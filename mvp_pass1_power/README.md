# MVP: ISPyPSA-based Pass 1 power-sector integration

A working prototype that takes a small set of structurally distinct power-sector
archetypes, runs each through ISPyPSA, and emits simple-msm-contract-compatible
CSVs for Pass 2 consumption.

**Status:** working end-to-end at full-NEM production scale — six archetypes ×
six milestone years (2025–2050), sub-regional NEM with REZ discrete nodes,
3-week representative sampling, solved with HiGHS PDLP at 1e-3 tolerance via
myopic single-period decomposition (~3 h wall-clock for all 36 solves in
parallel on the bench server). The deliverable supports **relative**
archetype-to-archetype comparison under a documented methodology, not
AEMO-absolute reproduction. See [Honest assessment](#honest-assessment) for
what it does and does not claim.

---

## What this MVP demonstrates

### 1. End-to-end ISPyPSA → simple-msm pipeline

From the AEMO 2024 IASR workbook (v6.0), through the ISPyPSA
templater/translator/PyPSA build/HiGHS solve, to per-archetype-per-milestone-year
rows in the simple-msm `method_years.csv` schema. All six archetypes solve at
full-NEM scale across all six milestone years (36 single-period LPs); all
produce simple-msm CSVs.

### 2. Archetype-driven structural pathways

Six production archetypes are defined as mutations to the ISPyPSA input CSVs
between templater and translator, with three of them additionally appending
PyPSA `custom_constraints` rows to enforce AEMO-anchored deployment mandates
per milestone year. See `RUNBOOK.md` §1 for the full catalogue spec.

  - `cost_optimal` — unmodified IASR Step Change baseline.
  - `rapid_coal_phaseout` — coal retired by 2030; gas remains available.
  - `gas_fleet_maintained` — coal retired by 2030; gas ≥ 12,500 MW @ 2030 & 2035.
  - `storage_led` — coal by 2035; no new gas; storage ≥ 1.25× AEMO trajectory per year.
  - `fossil_incumbent` — coal life +10y; constrained renewable build (MGA upper bound).
  - `nuclear_baseload` — Coalition 2024 phased nuclear: ≥ 2,000 MW @ 2045, ≥ 4,000 MW @ 2050.

Deployment mandates anchor against AEMO's published 2024 ISP Step Change
projections (Coalition 2024 policy reference for nuclear, which AEMO does not
model), so the archetypes read as alternative policy pathways relative to a
public authoritative source. The numerical results sections below reference the
prior four-archetype catalogue and will be refreshed once Phase 6 production
runs complete.

### 3. Cost decoupling done honestly

The simple-msm contract requires `output_cost_per_unit` to exclude commodity
costs the orchestrator prices separately. Our post-processor reads ISPyPSA's
bundled marginal cost, subtracts dispatch-weighted fuel cost computed from
`heat_rate × fuel_price`, and exposes both the decoupled number AND the bundled
number as diagnostics. Real numbers from the full-NEM cost_optimal run, 2050:

| metric                                    | value          |
|-------------------------------------------|---------------:|
| output_cost_per_unit (excl fuel)          |  40.0 AUD/MWh  |
| diagnostic_bundled_cost_per_unit          |  65.4 AUD/MWh  |
| diagnostic_fuel_cost_per_unit             |  25.4 AUD/MWh  |
| fuel_share_of_total_cost                  |         38.9 % |
| annual_mwh_delivered                      |    276.3 TWh   |
| total_CO2e_t_per_MWh                      |          0.130 |

The methodological consequence we accept and document: ISPyPSA's LP minimises
the bundled cost. Pass 2 sees decoupled cost and prices fuels independently.
If Pass 2's fuel price differs from IASR's, the capacity mix ISPyPSA chose is
not necessarily the mix Pass 2 would pick. **For Pass 1 (archetype menu) this
is acceptable. For Pass 3 (high-fidelity re-solve under Pass-2 fuel prices),
the ISPyPSA solve must be re-run with overrides** — this is the architectural
contract the Pass-1 layer establishes.

### 4. Emissions sourced from NGER Determination

All Scope 1 combustion emission factors come from the National Greenhouse
Accounts Factors 2024 (DCCEEW, July 2024 edition; underlying legal basis:
National Greenhouse and Energy Reporting (Measurement) Determination 2008,
Schedule 1). CO2, CH4, and N2O are reported separately. Cross-walk is in
[`postprocess/nger_factors.py`](postprocess/nger_factors.py) and emitted as
[`outputs/simple_msm/nger_factor_table.csv`](outputs/simple_msm/nger_factor_table.csv).

For fuels relevant to electricity generation:

| ISPyPSA carrier | NGA Table | CO2 | CH4 (CO2e) | N2O (CO2e) | Total kg CO2e/GJ |
|---|---|---:|---:|---:|---:|
| Black Coal | Table 4 | 90.0 | 0.04 | 0.20 | **90.24** |
| Brown Coal | Table 4 | 93.5 | 0.02 | 0.30 | **93.82** |
| Gas (pipeline)         | Table 5 | 51.4 | 0.10 | 0.03 | **51.53** |
| Liquid Fuel (diesel)   | Table 8 | 69.9 | 0.10 | 0.20 | **70.20** |
| Biomass                | Table 4 | 0    | 0.80 | 1.00 | **1.80**  |
| Biomethane             | Table 5 | 0    | 0.10 | 0.03 | **0.13**  |
| Hydrogen / Wind / Solar / Water / Storage | — | 0 | 0 | 0 | 0 |

Hyblend (gas+H2 mix) is handled with linear blending against ISPyPSA's
per-year H2 fraction table.

### 5. Calibration against AEMO 2024 ISP Step Change

The dashboard Calibration tab compares cost_optimal total generation against
AEMO Overview headline figures. Full-NEM `cost_optimal` at 2050 vs AEMO NEM:

| Quantity        | ISPyPSA NEM 2050 | AEMO NEM 2050 | ratio |
|---|---:|---:|---:|
| wind+solar (GW) | 53.7  | ~127  | 42 % |
| gas (GW)        | 12.1  | ~15   | 81 % |
| coal (GW)       |  1.7  |   0   | n/a  |
| grid TWh        | 284   | 313   | 91 % |

VRE capacity sits well below AEMO's projection — a known methodology effect:
representative-week sampling under-values daytime solar, and the demand the LP
serves (POE50 OPSO_MODELLING operational, ~255 TWh true full-year 2050) is below
AEMO's *underlying* consumption headline (313 TWh) by the ~121 TWh of
behind-the-meter rooftop PV that the operational trace nets out (3-week
annualisation adds ~8 %, narrowing the served-demand gap to ~9 %). **The
deliverable supports relative archetype comparison, not AEMO-absolute
reproduction** — see Honest assessment. Diagnostic scripts:
[`bench/extracts/`](bench/extracts/).

---

## Six archetypes — concrete results

Full NEM, 2050 milestone year, 3-week sampling, PDLP 1e-3. Cost intensity is
**operator-controllable only** (capex + non-fuel opex; fuel excluded for the
Pass 1 ↔ Pass 2 contract — Pass 2 prices fuel endogenously). Bundled cost and
carbon intensity shown alongside:

| Archetype | Cost excl-fuel (AUD/MWh) | Cost bundled | tCO2e/MWh | Renewable share |
|---|---:|---:|---:|---:|
| `cost_optimal`        | 40.0 | 65.4 | 0.130 | 60 % |
| `rapid_coal_phaseout` | 42.6 | 68.3 | 0.098 | 63 % |
| `gas_fleet_maintained`| 42.6 | 68.3 | 0.098 | 63 % |
| `storage_led`         | **66.8** | 76.1 | **0.022** | **84 %** |
| `fossil_incumbent`    | **26.1** | 63.5 | **0.265** | 40 % |
| `nuclear_baseload`    | 63.9 | 78.8 | 0.089 | 58 % |

The catalogue produces five structurally distinct trajectories plus one
substantive collapse:

- **`gas_fleet_maintained` ≡ `rapid_coal_phaseout`** (identical to 3 d.p.).
  Not a design failure: under coal-by-2030 the LP's natural gas response
  already exceeds AEMO's Step Change gas trajectory, so the 12,500 MW gas
  floor never binds. A real finding about transition gas demand.
- **`storage_led`** — mandate-driven storage (~35 GW battery at 2050) displaces
  gas to 2.4 GW; highest renewable share (84 %), lowest emissions, highest
  operator-controllable cost (capital-heavy).
- **`fossil_incumbent`** — leans on existing thermal, builds little new
  capacity; *lowest* operator-controllable cost (26 AUD/MWh) but *highest*
  emissions (0.265 tCO2e/MWh) — the cost-vs-carbon trade-off in sharp relief.
- **`nuclear_baseload`** — nuclear at the mandate floor (4 GW at 2050); the
  nuclear cost penalty shows as the highest bundled cost (78.8 AUD/MWh).

On an operator-controllable basis the ranking inverts the bundled one:
capital-heavy decarbonised archetypes (storage_led, nuclear) cost most to
build, fossil_incumbent least — exactly the signal Pass 2 needs when it layers
its own fuel and carbon prices on top. The **relative cost–emission trade-offs
across archetypes are the robust deliverable**; absolute capacity vs AEMO is
methodology-conditional (see Honest assessment).

---

## Honest assessment of MVP scope

Early scaling attempts stalled — full-NEM multi-period perfect-foresight LPs
(~16M nonzeros) degenerated under HiGHS primal simplex and stalled under IPM.
That envelope study is documented in
[`bench/characterisation_report.md`](bench/characterisation_report.md) and its
addenda. The production configuration that works combines **HiGHS PDLP at 1e-3
tolerance** with **myopic single-period decomposition** (each milestone year
solved independently from the IASR baseline): full NEM × 6 archetypes × 6 years
solves in **~3 h wall-clock in parallel** on the bench server (dual Xeon
Platinum 8260, 1024 GiB; per-solve peak RSS ~2.4 GiB, longest single solve
~100 min).

### What the deliverable claims

- **Relative cost–emission trade-offs across the six archetypes are robust** —
  the ranking and the structural story (storage displaces gas; nuclear carries
  a cost penalty; fossil_incumbent trades cost for carbon; gas_fleet collapses
  into rapid_coal) survive the methodology's known biases.
- **Implied abatement costs across archetypes are methodologically defensible.**
- **Capacity point estimates are tolerance-robust** — 1e-3 vs 1e-4 agree within
  0.5 % per carrier (controlled test; see dashboard Methodology tab).

### What it does NOT claim

- **Absolute capacity / dispatch comparable to AEMO.** VRE builds ~40–50 % of
  AEMO's projection because representative-week sampling under-values daytime
  solar and the operational demand trace (POE50 OPSO, rooftop-netted) sits
  below AEMO's underlying-consumption headline.
- **Specific milestone-year point estimates to high precision.**
- **An alternative ISP modelling exercise** — this is a Pass-1 archetype menu,
  not a replacement for AEMO's ISP.

### Documented methodology properties (not unexamined limitations)

These were identified, diagnosed, and bounded (see
[`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md) §0 and
[`bench/extracts/`](bench/extracts/)):

1. **Demand annualisation** — 3-week sampling overstates true operational
   demand by a *consistent* ~8 %; single-week's *variable* 5–19 % overstatement
   was what produced the spurious 2045 demand kink.
2. **2045 wind dip** — a single-rep-week artefact; resolved under 3-week.
3. **Gas direction** — a year/archetype interaction (annualisation-down vs
   peak-coincidence-up), not a monotone effect.
4. **PDLP tolerance-robustness** — the historical "48.8 GW solar swing" was a
   Phase 6→7 *model-correction* effect, not solver variance.

### Out of scope for this Pass-1 MVP

- Multi-reference-year stochastic dispatch (example trace dataset is 2018 only).
- True cross-period new-entrant chaining (myopic solves are independent
  IASR-baseline-per-year, not state-passed).
- Policy-target enforcement inside the LP beyond the archetype mandates.
- Pass 3 re-parameterisation, MGA, and orchestrator integration.

---

## Three risks — how the MVP resolves each

### Risk 1: ISPyPSA has no published AEMO benchmark

**What we did.** Ran full-NEM cost_optimal across all six milestone years and
compared headline generation/capacity to AEMO's 2024 ISP Step Change. At 2050,
modelled grid generation is 284 TWh (~91 % of AEMO's 313 TWh) and wind+solar
capacity is 53.7 GW (~42 % of AEMO's ~127 GW). The VRE gap is a *characterised*
methodology effect (representative-week solar under-valuation + operational-vs-
underlying demand definition), not an unexplained divergence.

**What we did NOT do.** Reproduce AEMO's absolute capacity mix. The deliverable
is reframed around **relative** archetype comparison rather than AEMO-absolute
matching — see "What the deliverable claims" above.

**Status of risk:** *resolved by reframing*. The pipeline produces internally
consistent, structurally differentiated trajectories; the absolute-vs-AEMO gap
is documented and attributed (see [`PHASE7_1_DIAGNOSTIC.md`](PHASE7_1_DIAGNOSTIC.md)
and [`bench/extracts/`](bench/extracts/)). Closing the absolute gap (more
solar-rich representative weeks, 8760-hour dispatch, underlying-demand basis)
is a v2 / Pass-3 task.

### Risk 2: Fuel costs aren't separable from VOM in ISPyPSA's LP output

**What we did.** Implemented post-hoc fuel-cost subtraction in
[`postprocess/extract_method_years.py`](postprocess/extract_method_years.py).
For each generator × snapshot, we compute `heat_rate × fuel_price` from the
pypsa-friendly `generators.csv` (`isp_heat_rate_gj/mwh`) and the IASR fuel-price
tables, then subtract from the bundled `marginal_cost × dispatch` to get
non-fuel opex. Add to capex × p_nom_opt, divide by annual MWh delivered → the
decoupled `output_cost_per_unit`.

**Real numbers (full NEM, 2050).** Fuel is **39 % of bundled cost** for
`cost_optimal` (25.4 / 65.4 AUD/MWh) and **59 %** for `fossil_incumbent`
(37.4 / 63.5 AUD/MWh) — the operator-controllable `output_cost_per_unit` is the
remainder (40.0 and 26.1 AUD/MWh respectively). The decoupling is mechanically
correct and Pass 2-consumable.

> **Note — fuel-cost lookup regression (Phase 1 follow-up l, fixed 2026-05-27).**
> The v6.0→v7.4 cache schema rename silently broke the coal/gas/biomass
> fuel-price lookup, collapsing computed fuel cost to ~0 (so `excl_fuel` ≈
> bundled). Fixed by resolving the v7.4-canonical table names; the numbers
> above are post-fix. Strict tests now assert the dominant fuels resolve to a
> material price. The earlier "fuel > 0" verification check was too weak to
> catch it — it passed on trace hydrogen while coal/gas were zero.

**The methodological consequence.** ISPyPSA's LP minimises bundled cost. The
simple-msm orchestrator sees decoupled cost. If Pass 2's endogenous fuel price
differs from ISPyPSA's IASR price, the mix ISPyPSA chose is not the mix Pass 2
would pick. For Pass 1 (selecting archetype menu), this is OK because archetype
shape is robust to fuel-price perturbations of ~10–30 %. For Pass 3, this is
not OK — capacity decisions must be re-solved under Pass 2's prices.

**Status of risk:** *resolved at the contract level*; the architectural
implication for Pass 3 is documented above. The team must decide whether
Pass 3 (a) re-solves ISPyPSA with fuel-price overrides (correct, takes time)
or (b) treats Pass 1 capacity as fixed and re-dispatches at finer temporal
resolution (cheaper, less self-consistent).

### Risk 3: Emission factors are entirely external to ISPyPSA's data model

**What we did.** Implemented an NGA Factors 2024 cross-walk in
[`postprocess/nger_factors.py`](postprocess/nger_factors.py) — 11 ISPyPSA
carriers mapped to NGA scope-1 combustion factors. Each carrier's CO2 / CH4 /
N2O components are exposed separately in the simple-msm output. The cross-walk
table is emitted as a CSV alongside the simple-msm rows so the provenance is
visible to Pass 2.

**Real numbers.** For full-NEM cost_optimal 2050:
```
energy_emissions_by_pollutant:
  CO2:       0.1252 tCO2e/MWh
  CH4_CO2e:  0.0023 tCO2e/MWh
  N2O_CO2e:  0.0027 tCO2e/MWh
  total:     0.1302 tCO2e/MWh
```
The total is dominated by combustion CO2; CH4/N2O are small but non-zero
because biomass contributes meaningful CH4/N2O even with biogenic-zero CO2.

**Status of risk:** *resolved at the contract level*. Source attribution is
authoritative (Australia's legal emission-factor regime). Multi-pollutant
support extends naturally (add columns to the cross-walk; NGA tracks SOx, NOx,
PM where relevant for stationary energy). Upstream emissions (Scope 3, fugitive
coal-mining methane etc.) are excluded by design — they sit on the fuel-supply
sector in the multi-sector model.

---

## Reproducing the MVP

Prerequisites: Python 3.11+, `uv` package manager (`pip install uv`), ~10 GB
disk for traces + IASR + intermediates, and an internet connection to download
inputs on first run.

```bash
# 0. From the ISPyPSA repo root, install dependencies
uv sync

# 1. Download the 2024 IASR workbook (v6.0) — ~11 MB
curl -L -o mvp_pass1_power/data/iasr_2024_v6.0.xlsx \
     https://data.openisp.au/archive/workbooks/6.0.xlsx

# 2. Download example trace data (reference year 2018) — ~1.6 GB
mkdir -p mvp_pass1_power/data/traces
uv run python -c "
from isp_trace_parser.remote import fetch_trace_data
from pathlib import Path
fetch_trace_data(dataset_type='example', dataset_src='isp_2024',
                 save_directory=Path('mvp_pass1_power/data/traces'))
"

# 3. (optional) Download NGA Factors 2024 PDF for provenance inspection
curl -L -o mvp_pass1_power/data/NGA_Factors_2024.pdf \
     https://www.dcceew.gov.au/sites/default/files/documents/national-greenhouse-account-factors-2024.pdf

# 4. Quick smoke: run each archetype at minimal (NSW single-period) scale
for arch in cost_optimal rapid_coal_phaseout gas_fleet_maintained \
            storage_led fossil_incumbent nuclear_baseload; do
    uv run python mvp_pass1_power/scripts/run_workflow.py \
        --config mvp_pass1_power/configs/minimal.yaml --archetype $arch
done
# For the full-NEM 6-year production runs, use the myopic driver — see
# CLAUDE.md "Reproduction" (mvp_pass1_power/bench/run_myopic.py).

# 5. Emit simple-msm CSVs
uv run python -m mvp_pass1_power.postprocess.emit_simple_msm \
    --runs-dir mvp_pass1_power/runs \
    --workbook-cache mvp_pass1_power/data/workbook_cache \
    --out mvp_pass1_power/outputs/simple_msm

# 6. Calibration report for cost-optimal
uv run python mvp_pass1_power/calibration/compare_to_aemo.py \
    --run mvp_pass1_power/runs/minimal_step_change__cost_optimal \
    --out mvp_pass1_power/calibration
```

Total reproduction time: roughly **15 minutes on a developer laptop** after
the one-time IASR workbook + trace data download.

---

## Viewing the dashboard

A Streamlit dashboard renders the contract outputs and a per-archetype
operational view. The simplest way to launch it:

- **Windows**: double-click `run-dashboard.bat` at the project root.
- **Linux / macOS**: `./run-dashboard.sh` from the project root.

Both scripts `cd` to the project root, check that `.venv/` exists, and prefer
`uv run` if available (falling back to the venv's Streamlit otherwise). The
dashboard opens in your default browser at `http://localhost:8501`. Prerequisite
is a one-time `uv sync` to install dependencies.

To launch manually:

```bash
uv run streamlit run mvp_pass1_power/dashboard/dashboard.py
```

---

## Repository layout

```
mvp_pass1_power/
├── README.md                         — this file
├── configs/
│   ├── minimal.yaml                  — NSW, 2050 single period (quick smoke)
│   ├── fast.yaml                     — NSW multi-period
│   └── baseline.yaml                 — full NEM
├── bench/
│   ├── run_myopic.py                 — full-NEM myopic 6-year production driver
│   └── extracts/                     — reproducible diagnostic scripts (demand,
│                                       carrier, variance-probe comparisons)
├── archetypes/
│   ├── cost_optimal.py               — no-op (Step Change reference)
│   ├── rapid_coal_phaseout.py        — coal retired by 2030
│   ├── gas_fleet_maintained.py       — coal by 2030 + 12.5 GW gas floor
│   ├── storage_led.py                — no new gas + 1.25× AEMO storage
│   ├── fossil_incumbent.py           — extend coal life, cap new VRE
│   └── nuclear_baseload.py           — phased nuclear (2/4 GW @ 2045/2050)
├── scripts/
│   ├── run_workflow.py               — runs one archetype end-to-end
│   └── run_all_archetypes.sh         — sequences six archetypes + post-process
├── postprocess/
│   ├── nger_factors.py               — NGER cross-walk to ISPyPSA carriers
│   ├── extract_method_years.py       — per-period aggregation, cost decoupling
│   └── emit_simple_msm.py            — produces methods.csv, method_years.csv
├── calibration/
│   ├── aemo_2024_isp_step_change.md  — sourced AEMO benchmark figures
│   ├── compare_to_aemo.py            — side-by-side comparison + report
│   └── calibration_report.md         — generated report
├── data/
│   ├── iasr_2024_v6.0.xlsx           — AEMO 2024 IASR workbook
│   ├── traces/isp_2024/              — wind/solar/demand traces (1.6 GB)
│   ├── workbook_cache/               — parsed IASR tables
│   └── NGA_Factors_2024.pdf          — DCCEEW emission factors document
├── runs/                             — per-archetype ISPyPSA outputs
└── outputs/
    └── simple_msm/
        ├── methods.csv               — one row per archetype
        ├── method_years.csv          — one row per (archetype, year)
        ├── diagnostics.csv           — cost decoupling diagnostics
        └── nger_factor_table.csv     — emission-factor provenance
```

---

## Answers to the demonstration questions

> Is the ISPyPSA Pass 1 integration architecturally sound?

**Yes.** Every required simple-msm field maps cleanly to ISPyPSA outputs (or
combinations thereof), the post-processing module is small (~500 lines) and
lives entirely outside ISPyPSA core, and the archetype-mutation mechanism is
straightforward at the CSV layer. Two small upstream ISPyPSA patches would
improve ergonomics but aren't blocking: (a) stop dropping `isp_*` columns at
`pypsa_build/generators.py:91-95`, (b) add `co2_emissions` to PyPSA carriers
driven by an emission-factor input table.

> Calibrated against AEMO well enough to be credible?

**Credible as a relative-comparison tool, with the AEMO-absolute gap
characterised.** Full-NEM cost_optimal 2050 generation is ~91 % of AEMO's
headline; VRE capacity is ~42 %. The gap is diagnosed (representative-week
solar valuation + operational-vs-underlying demand definition), not unexplained
— see "What the deliverable claims" and [`PHASE7_1_DIAGNOSTIC.md`](PHASE7_1_DIAGNOSTIC.md).
The deliverable is framed for relative archetype comparison, not AEMO
reproduction.

> Bounded enough in remaining work to be feasible within project constraints?

**Yes — and the compute-envelope unknown that dominated the early MVP is now
resolved.** Full NEM × 6 archetypes × 6 years solves in ~3 h wall-clock in
parallel via HiGHS PDLP (1e-3) + myopic decomposition; per-solve peak RSS is
~2.4 GiB, well within the bench server. Perfect-foresight multi-period remains
intractable under HiGHS, which is why myopic decomposition is the production
pattern (see [`bench/characterisation_report.md`](bench/characterisation_report.md)).

What's been delivered since the early MVP:

1. **Compute envelope confirmed** — PDLP 1e-3 + myopic is production-viable at
   full NEM scale.
2. **Six-archetype catalogue** with AEMO-anchored mandates (gas floor, storage
   floor, nuclear floor) enforced as PyPSA custom constraints.
3. **Six milestone years** `[2025…2050]` via myopic single-period decomposition.
4. **3-week representative sampling** with the demand-annualisation,
   wind-dip-resolution, gas-direction, and PDLP-robustness properties
   characterised (see [`PHASE7_FINDINGS.md`](PHASE7_FINDINGS.md) §0).
5. **NGER emission cross-walk** and **cost decoupling** (fuel separated for the
   Pass 2 contract; fixed in follow-up (l)).

Remaining v2 / Pass-3 work (out of scope for this Pass-1 deliverable):

- **Close the AEMO-absolute VRE gap** — more solar-rich representative weeks or
  8760-hour dispatch; underlying-demand basis.
- **True cross-period new-entrant chaining** (myopic solves are currently
  independent IASR-baseline-per-year).
- **Multi-reference-year cycling** (full trace dataset; 2018 only today).
- **Systematic v6.0 → v7.4 schema audit** — one focused pass validating every
  IASR-table-name reference against v7.4-canonical, since renames have surfaced
  silent downstream breakages (follow-ups a–l).
- **Source-code patches in ISPyPSA core**: (a) preserve `isp_*` columns into
  PyPSA generators; (b) add per-carrier `co2_emissions`.

---

## Sources

- ISPyPSA: [github.com/Open-ISP/ISPyPSA](https://github.com/Open-ISP/ISPyPSA) (v0.1.3 — pre-1.0 beta)
- PyPSA: [github.com/PyPSA/PyPSA](https://github.com/PyPSA/PyPSA) (v1.0.7)
- AEMO 2024 ISP and IASR v6.0: [aemo.com.au — 2024 Integrated System Plan](https://www.aemo.com.au/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp)
- National Greenhouse Accounts Factors 2024: [DCCEEW](https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors-2024)
- NGER (Measurement) Determination 2008: [legislation.gov.au](https://www.legislation.gov.au/Series/F2008L02309)
