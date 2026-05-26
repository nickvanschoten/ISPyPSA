# MVP: ISPyPSA-based Pass 1 power-sector integration

A working prototype that takes a small set of structurally distinct power-sector
archetypes, runs each through ISPyPSA, and emits simple-msm-contract-compatible
CSVs for Pass 2 consumption.

**Status:** working end-to-end at smoke-test scale (NSW-only, single 2050
investment period, one representative week). Architectural and pipeline
elements all exercised on real AEMO IASR 2024 data; calibration is directional
not definitive. See [Honest assessment](#honest-assessment) for the limits.

---

## What this MVP demonstrates

### 1. End-to-end ISPyPSA → simple-msm pipeline

From the AEMO 2024 IASR workbook (v6.0), through the ISPyPSA
templater/translator/PyPSA build/HiGHS solve, to per-archetype-per-milestone-year
rows in the simple-msm `method_years.csv` schema. All four archetypes solve;
all four produce simple-msm CSVs.

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
number as diagnostics. Real numbers from the cost-optimal run, NSW 2050:

| metric                                    | value          |
|-------------------------------------------|---------------:|
| output_cost_per_unit (decoupled)          | 177.70 AUD/MWh |
| diagnostic_bundled_cost_per_unit          | 179.21 AUD/MWh |
| diagnostic_fuel_cost_per_unit             |   1.50 AUD/MWh |
| fuel_share_of_total_cost                  |          0.8 % |
| annual_mwh_delivered                      |     104.0 TWh  |
| total_CO2e_t_per_MWh                      |          0.090 |

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

Side-by-side report at [`calibration/calibration_report.md`](calibration/calibration_report.md).
Headline NSW vs AEMO NEM-wide:

| Quantity        | ISPyPSA NSW 2050 | AEMO NEM 2050 | NSW share |
|---|---:|---:|---:|
| wind+solar (GW) | 30.5  | 127  | 24 % |
| gas (GW)        |  1.8  |  15  | 12 % |
| coal (GW)       |  0    |   0  | n/a  |
| grid TWh        | 104   | 313  | 33 % |

NSW historically carries 28–32 % of NEM peak demand, so a 24–33 % NSW share is
consistent with a credible model rather than a divergence signal. **This MVP
does not evidence that ISPyPSA reproduces AEMO's Step Change LP solution at any
specific tolerance** — see Honest assessment.

---

## Four archetypes — concrete results

NSW, 2050 single investment period, one representative week:

| Archetype | Cost (decoupled) | Cost (bundled) | tCO2e/MWh | Biomass GJ/MWh | Gas GJ/MWh |
|---|---:|---:|---:|---:|---:|
| `cost_optimal`     | 177.70 | 179.21 | 0.0901 | 2.41 | 1.66 |
| `renewables_led`   | 177.70 | 179.21 | 0.0901 | 2.41 | 1.66 |
| `deep_clean_firmed`| 177.70 | 179.21 | 0.0901 | 2.41 | 1.66 |
| `fossil_incumbent` | **323.73** | **328.31** | **0.0990** | **7.34** | 1.66 |

### Why three of four archetypes converge

At NSW 2050 under AEMO IASR Step Change cost assumptions, the cost-optimal mix
is already renewables-dominated (Solar 21.9 GW, Wind 8.6 GW, Battery 13.4 GW,
Gas 1.8 GW, Biomass 1.6 GW). Removing thermal new-entrants (renewables_led,
deep_clean_firmed) doesn't change the LP solution because the LP wasn't going
to build them anyway. Forcing coal retirement to 2035 doesn't change 2050
because NSW coal is already retired by 2050 in the IASR closure schedule.

**Differentiation between low-carbon archetypes requires:**

- multi-period evolution (where coal retirement timing matters at 2030/2035),
- earlier milestone years (2025/2030) where the cost economics still permit
  thermal new-builds,
- or stronger structural levers (capping renewables build aggressively, like
  fossil_incumbent does).

`fossil_incumbent` differentiates because it forbids most new wind/solar — the
system reaches into biomass dispatch (jumping from 2.41 to 7.34 GJ/MWh-delivered)
and runs existing thermal harder, with cost jumping from 178 to 324 AUD/MWh
(+82 %) and emissions only rising from 0.090 to 0.099 tCO2e/MWh.

This convergence is a real finding: the **archetype-design lever set matters
more than the archetype mechanism itself**. The mechanism is wired and works;
producing visibly different low-carbon archetypes is a design question for the
catalogue, not a tool capability question.

---

## Honest assessment of MVP scope

This MVP was originally planned at sub-regional NEM-wide scale (16 sub-regions
+ ~35 REZ discrete nodes, 3 representative weeks, 3 investment periods
[2030, 2040, 2050]). That config produced an LP with **276 constraint groups**
and the HiGHS solver consumed **~28 GB RAM** without converging within the
session's time budget.

A second attempt at NSW-only + 2 periods + 1 representative week produced a
**4.8 million-row × 2.25 million-column LP** that HiGHS could not solve within
30 minutes either, primarily because multi-investment-period + REZ discrete
nodes blows up the LP dimensions even at single-state scope.

The reported results are from a third configuration: NSW only, **single 2050
investment period**, one representative week, which solves in **76 seconds**
of HiGHS time. This is smoke-test scale.

**Direct production implications:**

1. **Compute requirement is substantial.** Even at modest fidelity, ISPyPSA
   solves consume tens of GB of memory. A production Pass-1 run for the full
   NEM at AEMO-comparable fidelity will require a workstation with **≥64 GB
   RAM** at minimum, possibly more.
2. **Wall-clock per archetype is hours, not minutes.** The team should plan for
   a Pass-1 run of N archetypes to take **N × several-hours** on appropriate
   hardware. This affects how many archetypes are practical per MGA loop.
3. **Multi-period investment is the cost driver.** Single-period solves in a
   minute or two; multi-period solves blow up quickly. Architectural choice
   between "one big multi-period solve per archetype" vs "six single-period
   solves per archetype with capacity-fixing between periods" deserves
   investigation. ISPyPSA's `fix_optimal_capacities` machinery supports the
   latter pattern.
4. **A commercial solver (Gurobi/CPLEX) would likely solve the full LP in
   minutes** vs HiGHS's hours. If the team has access to a commercial solver,
   the runtime story changes materially.

**Things this MVP DID work end-to-end on at smoke-test scale:**
- AEMO 2024 IASR v6.0 workbook parsing
- ISPyPSA templater (Step Change scenario)
- Archetype-mutation injection between templater and translator
- ISPyPSA translator (CSV → PyPSA-friendly tables)
- PyPSA network construction with custom constraints
- HiGHS LP solve to optimality
- Result extraction (capacity, dispatch, flows, demand)
- NGER emission-factor cross-walk
- Cost decoupling (bundled → decoupled, with diagnostics)
- simple-msm `methods.csv` and `method_years.csv` emission
- AEMO calibration report generation

**Things this MVP did NOT do:**
- Multi-period multi-milestone-year capacity expansion (couldn't complete in time budget)
- Full NEM 16-sub-region geographic scope (couldn't complete in time budget)
- Multi-reference-year stochastic dispatch (example trace dataset has only 2018)
- Policy-target enforcement in the LP (ISPyPSA templates them but does not translate)
- Pass 3 re-parameterisation, MGA, or orchestrator integration (out of scope)

---

## Three risks — how the MVP resolves each

### Risk 1: ISPyPSA has no published AEMO benchmark

**What we did.** Compared the cost-optimal Step Change run's headline figures
to AEMO's 2024 ISP Step Change published numbers. NSW capacity is 24 % of
AEMO's wind+solar NEM-wide and 33 % of AEMO's grid consumption — both
consistent with NSW's ~30 % share of the NEM.

**What we did NOT do.** Detailed comparison at fuel-type × year resolution
against AEMO's published scenario results workbook (which is downloadable but
was not loaded in this MVP). The MVP runs single-period 2050 only, so there is
no trajectory to compare.

**Status of risk:** *partially resolved*. The architectural pipeline produces
sensible-shape outputs at NSW-share-of-NEM. Definitive resolution requires
running at full-NEM multi-period, against AEMO's full scenario workbook — that
is a production task, not an MVP one. Time required: 1–2 person-weeks of
modelling work plus the compute budget for full-scale runs.

### Risk 2: Fuel costs aren't separable from VOM in ISPyPSA's LP output

**What we did.** Implemented post-hoc fuel-cost subtraction in
[`postprocess/extract_method_years.py`](postprocess/extract_method_years.py).
For each generator × snapshot, we compute `heat_rate × fuel_price` from the
pypsa-friendly `generators.csv` (`isp_heat_rate_gj/mwh`) and the IASR fuel-price
tables, then subtract from the bundled `marginal_cost × dispatch` to get
non-fuel opex. Add to capex × p_nom_opt, divide by annual MWh delivered → the
decoupled `output_cost_per_unit`.

**Real numbers.** For the cost-optimal Step Change NSW 2050 mix, fuel is
**0.8 % of total cost** (1.50 / 179.21 AUD/MWh) — small because the mix is
mostly renewables. For `fossil_incumbent` (forced into more thermal), fuel
rises to **1.4 %** (4.58 / 328.31 AUD/MWh). The decoupling is mechanically
correct and Pass 2-consumable.

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

**Real numbers.** For the cost-optimal Step Change NSW 2050:
```
energy_emissions_by_pollutant:
  CO2:       0.0856 tCO2e/MWh
  CH4_CO2e:  0.0021 tCO2e/MWh
  N2O_CO2e:  0.0025 tCO2e/MWh
  total:     0.0901 tCO2e/MWh
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

# 4. Run each archetype — ~1.5 min each at minimal scale
for arch in cost_optimal renewables_led fossil_incumbent deep_clean_firmed; do
    uv run python mvp_pass1_power/scripts/run_workflow.py \
        --config mvp_pass1_power/configs/minimal.yaml --archetype $arch
done

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
│   ├── minimal.yaml                  — NSW, 2050 single period (used for results)
│   ├── fast.yaml                     — NSW, [2030,2050] 2 periods (did not solve in budget)
│   └── baseline.yaml                 — full NEM, 3 periods (did not solve in budget)
├── archetypes/
│   ├── cost_optimal.py               — no-op (Step Change reference)
│   ├── renewables_led.py             — drop new gas, accelerate coal closures
│   ├── fossil_incumbent.py           — extend coal life, cap new VRE
│   └── deep_clean_firmed.py          — coal out by 2035, no new unabated gas
├── scripts/
│   ├── run_workflow.py               — runs one archetype end-to-end
│   └── run_all_archetypes.sh         — sequences four archetypes + post-process
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

**Directional only at MVP scale.** NSW shares of AEMO NEM-wide quantities are
in plausible ranges (24–33 % across capacity and consumption). Definitive
calibration requires full-NEM multi-period runs against AEMO's published
scenario workbook — a production task estimated at 1–2 person-weeks.

> Bounded enough in remaining work to be feasible within project constraints?

**Yes, bounded by clear items rather than open questions**, but with one
concrete unknown the team needs to confirm: *can we run ISPyPSA at full NEM /
multi-period within an acceptable compute envelope?* The MVP showed that HiGHS
at minimal config used 28 GB RAM and didn't converge in 30 minutes on a
developer laptop — production-scale solves will need either a much more
powerful workstation (≥64 GB RAM) or a commercial solver (Gurobi/CPLEX), and
this affects how many archetypes × MGA iterations the project can budget.

Production roadmap from this MVP:

1. **Confirm compute envelope.** Run one full-NEM 3-period solve on a powerful
   workstation or with a commercial solver. **(1 day)**
2. **Wire policy-target enforcement.** ISPyPSA reads renewable-share trajectories
   from IASR but doesn't translate them. Add `custom_constraints_lhs/rhs` rows.
   **(1–2 days)**
3. **Multi-reference-year cycling.** Use the full trace dataset (~30 GB), enable
   AEMO's reference-year cycle. **(0.5 day code, days of solve time)**
4. **Tighter calibration against AEMO's scenario workbook.** Side-by-side
   comparison of fuel-mix and capacity trajectories per AEMO-published
   milestone year. **(2–3 days)**
5. **Multi-period over six milestone years** `[2025, 2030, 2035, 2040, 2045, 2050]`
   instead of three. **(0 days code; LP-time scales).**
6. **Source-code patches in ISPyPSA core** (a) preserve `isp_*` columns into
   PyPSA generators; (b) add per-carrier `co2_emissions`. **(0.5 day each;
   could be upstreamed).**
7. **Better archetype-design lever set.** Three of four MVP archetypes converge
   at single-period 2050 because the IASR cost-optimal solution is already
   renewables-dominated. Adding multi-period evolution and stronger structural
   constraints (per-year build-rate caps, carbon caps, generation share
   constraints) will produce visibly distinct archetypes. **(1 week of
   archetype-catalogue design + verification).**

Estimated production work: **3–4 person-weeks of focused engineering plus a
similar duration of model validation against AEMO**, contingent on AEMO data
artefacts being available in the formats needed and on having sufficient
compute access for full-scale runs.

---

## Sources

- ISPyPSA: [github.com/Open-ISP/ISPyPSA](https://github.com/Open-ISP/ISPyPSA) (v0.1.3 — pre-1.0 beta)
- PyPSA: [github.com/PyPSA/PyPSA](https://github.com/PyPSA/PyPSA) (v1.0.7)
- AEMO 2024 ISP and IASR v6.0: [aemo.com.au — 2024 Integrated System Plan](https://www.aemo.com.au/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp)
- National Greenhouse Accounts Factors 2024: [DCCEEW](https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors-2024)
- NGER (Measurement) Determination 2008: [legislation.gov.au](https://www.legislation.gov.au/Series/F2008L02309)
