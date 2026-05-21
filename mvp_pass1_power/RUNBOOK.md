# Pass 1 Power-Sector MVP — Production Runbook

This document covers the archetype catalogue design, production run configuration,
solver outcomes, calibration status, key methodological choices, and regeneration
instructions for the Pass 1 power-sector integration MVP.

---

## 1. Archetype catalogue

All six archetypes use AEMO 2024 IASR **Step Change** as the base scenario.
Differentiation is imposed entirely through ISPyPSA-input mutations (closure years,
new-entrant option set). No custom_constraints rows are used in the production catalogue.

| Archetype ID | Short name | Differentiation levers |
|---|---|---|
| `cost_optimal` | Cost-optimal Step Change | Unmodified IASR baseline; LP picks unconstrained least-cost mix |
| `fast_fossil_exit` | Fast fossil exit | Coal closure ≤ 2030; drops OCGT (small GT), OCGT (large GT), CCGT from new entrants; CCGT-CCS + H2 + biomass remain as firm options |
| `gas_bridge` | Gas bridge | Coal closure ≤ 2030; all gas new-entrant technologies retained; LP freely picks gas vs storage |
| `storage_led` | Storage-led | Coal closure ≤ 2035; drops all gas new entrants including CCS variants; H2 and biomass remain |
| `fossil_incumbent` | Fossil-incumbent | Coal closure pushed +10 years (AEMO schedule + 10); solar new entrants dropped, 75% of wind new entrants dropped (random_state=0 for reproducibility) |
| `nuclear_included` | Nuclear included | IASR coal schedule unchanged; Advanced Nuclear injected into new_entrant_generators (one row per sub-region) at CSIRO GenCost 2023-24 cost |

### Design rationale

The five non-baseline archetypes each test a qualitatively different hypothesis:

- **fast_fossil_exit / gas_bridge / storage_led** span the coal-exit-with-different-firming
  spectrum. Isolating gas availability as the single lever makes the marginal cost of clean
  firming visible across the three pathways.
- **fossil_incumbent** provides a credible upper bound on carbon intensity for downstream
  MGA near-optimal exploration without requiring a prescriptive fossil-share floor constraint.
  (A floor constraint would require custom_constraints_rhs, which cannot constrain
  storage_units directly in the current ISPyPSA schema.)
- **nuclear_included** tests whether nuclear's high capital cost (≥ $9M/MW) is ever
  cost-competitive against renewables + storage under IASR techno-economics. It answers
  "at what carbon price would nuclear appear?" as an implicit output of the Pass 2 MGA sweep.

### What archetypes do NOT capture

- Year-by-year build-rate caps (e.g., "≤ 3 GW/year of wind"). These require
  `custom_constraints_rhs` rows; not implemented in the production catalogue.
- Interconnector reinforcement sensitivity. Transmission topology is IASR-fixed in all six.
- Demand-side flexibility. Load is treated as inelastic in all six.

---

## 2. Implementation files

| File | Role |
|---|---|
| `archetypes/cost_optimal.py` | Identity mutation |
| `archetypes/fast_fossil_exit.py` | Coal clip + gas new-entrant drops |
| `archetypes/gas_bridge.py` | Coal clip only |
| `archetypes/storage_led.py` | Coal clip (2035) + all gas new-entrant drops |
| `archetypes/fossil_incumbent.py` | Coal lifetime extension + renewable new-entrant thinning |
| `archetypes/nuclear_included.py` | Advanced Nuclear injection from CCGT template |
| `archetypes/__init__.py` | `PRODUCTION_ARCHETYPES`, `APPLY_ARCHETYPE` registry |
| `bench/run_myopic.py` | Sequential single-period solver with `--archetype` param |
| `bench/run_production.py` | Parallel launcher (one subprocess per archetype) |
| `postprocess/extract_granular_outputs.py` | Capacity, generation, storage, CF, renewable share CSVs |
| `postprocess/emit_simple_msm.py` | simple-msm contract CSVs (methods, method_years, diagnostics) |
| `postprocess/nger_factors.py` | NGER 2024 Scope 1 emission factor cross-walk |
| `postprocess/extract_method_years.py` | Cost decoupling + per-pollutant emission intensities |
| `dashboard/dashboard.py` | Streamlit visualisation app |

---

## 3. Production run configuration

### Command

```powershell
uv run python mvp_pass1_power/bench/run_production.py `
    --periods 2025 2030 2035 2040 2045 2050 `
    --budget-min 720 `
    --gurobi-fallback
```

Defaults to all six `PRODUCTION_ARCHETYPES`. `--budget-min 720` = 12 h per period per
archetype (generous; typical NEM 1-period HiGHS simplex completes in 22 min – 2 h).

### Parallelism model

Six `run_myopic.py` subprocesses run concurrently, one per archetype. Each runs six
sequential single-period solves (2025 → 2030 → … → 2050). Periods within an archetype
are sequential because each period's new-entrant capacity is passed forward as
committed assets to the next period.

Peak concurrent load: 6 × 1 LP at any given time. On the dual Xeon Platinum 8260 /
1024 GiB server, HiGHS threads are not constrained by the runner; each instance will
use all available cores unless `--highs-threads` is added.

**Recommendation:** pin to `--highs-threads 8` or similar to keep all six archetypes
progressing rather than one instance monopolising all cores.

### Per-period solve budget

| Config | Expected wall-clock (HiGHS primal simplex) |
|---|---|
| NEM 1p 2025 | ~22 min |
| NEM 1p 2030 | ~22–30 min |
| NEM 1p 2035 | ~22–30 min |
| NEM 1p 2040–2050 | ~45–120 min |
| NEM 6p sequential (all years) | ~4 h total |

All NEM 1-period solves complete Optimal under HiGHS primal simplex. The 4 h overnight
figure for a full 6-period myopic sequence is confirmed for `cost_optimal` on this server.

### Gurobi fallback

If any period exceeds `--budget-min` without converging, `run_production.py` flags it and
(with `--gurobi-fallback`) prints `instrumented_runner.py --use-gurobi` commands.
Gurobi is licensed at CSIRO at `gurobipy>=11,<12` only — use `gurobipy==11.*` explicitly.
Gurobi barrier (BarConvTol=1e-3) is ~10× faster than HiGHS simplex for the NEM LPs
but timed out on NEM 6-period at BarConvTol=0.0092 after 8 h (eighth addendum).
For single-period fallback, Gurobi barrier with default BarConvTol is recommended.

---

## 4. Per-archetype solve outcomes

*Fill in from `bench/records/production_run_{timestamp}.json` after the production run.*

| Archetype | Periods solved (HiGHS) | Periods fallback (Gurobi) | Total wall-clock | Peak RSS (GiB) |
|---|---|---|---|---|
| cost_optimal | — | — | — | — |
| fast_fossil_exit | — | — | — | — |
| gas_bridge | — | — | — | — |
| storage_led | — | — | — | — |
| fossil_incumbent | — | — | — | — |
| nuclear_included | — | — | — | — |

To populate this table after runs complete:

```python
import json
from pathlib import Path

rec = json.loads(Path("mvp_pass1_power/bench/records/production_run_{timestamp}.json").read_text())
for arch, s in rec["per_archetype"].items():
    print(arch, s["periods_solved"], s["periods_failed"], s["total_wall_s"], s["peak_rss_gib"])
```

---

## 5. Post-processing

### simple-msm contract CSVs

```powershell
uv run python -m mvp_pass1_power.postprocess.emit_simple_msm `
    --runs-dir mvp_pass1_power/bench/runs_myopic `
    --workbook-cache mvp_pass1_power/data/workbook_cache `
    --out mvp_pass1_power/outputs/simple_msm
```

Outputs: `methods.csv`, `method_years.csv`, `diagnostics.csv`, `nger_factor_table.csv`.

The `method_years.csv` `output_cost_per_unit` column contains AUD/MWh **excluding**
coal and gas commodity costs (fuel costs subtracted via heat-rate × IASR fuel price).
`diagnostic_bundled_cost_per_unit` retains the full bundled marginal cost for reference.

### Granular CSVs

```python
from pathlib import Path
from mvp_pass1_power.postprocess.extract_granular_outputs import emit_granular_outputs
from mvp_pass1_power.archetypes import PRODUCTION_ARCHETYPES

emit_granular_outputs(
    runs_dir=Path("mvp_pass1_power/bench/runs_myopic"),
    workbook_cache=Path("mvp_pass1_power/data/workbook_cache"),
    out_dir=Path("mvp_pass1_power/outputs/granular"),
    archetype_catalogue=PRODUCTION_ARCHETYPES,
)
```

Outputs (one row per archetype × year):

| File | Content |
|---|---|
| `capacity_gw.csv` | Installed capacity by technology |
| `generation_twh.csv` | Annual generation by technology |
| `storage_capacity.csv` | Total storage power (GW) and energy (GWh) |
| `demand_generation.csv` | Total demand, generation, supply gap % |
| `capacity_factors.csv` | CF per carrier |
| `renewable_share.csv` | Wind + Solar + Water + Biomass share of total generation |

---

## 6. Calibration status

### 2030 demand overshoot (known)

The `cost_optimal` archetype at NEM 6-period shows **+36% total generation vs AEMO
Overview 2024** for the 2030 milestone year. This discrepancy is:

- Consistent across three solver backends (HiGHS simplex, PDLP 1e-3, Gurobi)
- Presumed to be an IASR workbook data issue, not a solver or model artefact
- Documented in the dashboard calibration panel with the flag text:
  "Known data-side discrepancy (36% vs AEMO Overview; established across three solvers)"

The 2050 milestone year aligns with AEMO Overview within ~5%. Downstream Pass 2 users
should be aware that 2030 cost and emission intensities are calibrated against an
over-served demand and may not represent the physical Australian grid in that year.

### AEMO reference values

| Year | AEMO Overview 2024 total generation (TWh) | Source |
|---|---|---|
| 2030 | ~202 | AEMO ISP 2024 Overview |
| 2050 | ~313 | AEMO ISP 2024 Overview |

---

## 7. Renewable classification

**Renewable carriers** = `{"Wind", "Solar", "Water", "Biomass"}`.

These are the zero- or near-zero direct combustion carriers that AEMO's ISP tracks as
"variable renewable" or "clean dispatchable". The classification is used for:
- `renewable_share_pct` in `extract_granular_outputs.py`
- The dashboard renewable share panel

**Excluded from renewable count:**

- **Nuclear**: operationally zero-carbon but politically contested in Australia; excluded
  per AEMO ISP convention. Its carbon intensity is zero in the emission stack regardless.
- **Hydrogen**: upstream embodied emissions (electrolysis or SMR+CCS) are out of scope;
  combustion is zero Scope 1. Excluded from the renewable share count to avoid conflating
  storage/conversion with primary renewable generation.

---

## 8. Emission intensity and GWP basis

### Emission factors

All Scope 1 combustion emission factors from **National Greenhouse Accounts Factors 2024**
(DCCEEW, July 2024 edition; NGER Measurement Determination 2008 Schedule 1).
Cross-walk in `postprocess/nger_factors.py`.

Per-carrier factors (kg CO2-e/GJ, GCV basis):

| Carrier | CO2 | CH4 (CO2e) | N2O (CO2e) | Total CO2e |
|---|---|---|---|---|
| Black Coal | 90.0 | 0.04 | 0.2 | 90.24 |
| Brown Coal | 93.5 | 0.02 | 0.3 | 93.82 |
| Gas | 51.4 | 0.1 | 0.03 | 51.53 |
| Liquid Fuel | 69.9 | 0.1 | 0.2 | 70.2 |
| Biomass | 0.0 | 0.8 | 1.0 | 1.8 |
| Nuclear | 0.0 | 0.0 | 0.0 | 0.0 |
| Hydrogen | 0.0 | 0.0 | 0.0 | 0.0 |

### GWP basis

**Default: AR5 NGER** (CH4=28, N2O=265). This is the basis used in NGA Factors 2024 and
the current NGER Measurement Determination. The `total_CO2e` column in `method_years.csv`
uses AR5 by default.

Physical mass columns `diagnostic_ch4_physical_kg_per_mwh` and
`diagnostic_n2o_physical_kg_per_mwh` are stored in `diagnostics.csv` to support
**GWP-basis switching** without re-running the pipeline. The dashboard GWP toggle uses:

```python
from mvp_pass1_power.postprocess.nger_factors import co2e_per_mwh, GWP_AR5_NGER, GWP_AR6_IPCC

# AR6 recalculation in the dashboard
co2e = co2e_per_mwh(
    physical_ch4_kg_per_mwh=row["ch4_physical_kg_per_mwh"],
    physical_n2o_kg_per_mwh=row["n2o_physical_kg_per_mwh"],
    co2_kg_per_mwh=row["co2_kg_per_mwh"],
    gwp=GWP_AR6_IPCC,  # {"CH4": 27, "N2O": 273}
)
```

**AR6 IPCC GWP**: CH4=27 (fossil, 100-year including climate-carbon feedback),
N2O=273 (IPCC WGI Table 7.SM.7).

### Multi-pollutant note

`energy_emissions_by_pollutant` in `method_years.csv` contains:
- `CO2`: direct CO2 combustion in tCO2/MWh
- `CH4_CO2e`: CH4 as CO2e (AR5)
- `N2O_CO2e`: N2O as CO2e (AR5)
- `total_CO2e`: sum of the three (AR5 basis)

`process_emissions_by_pollutant` is always zero (no NGER process emissions for
electricity generation; calcination etc. are in other NGER facility categories).

---

## 9. Dashboard

```powershell
# Install dashboard deps (not in uv pyproject.toml — install separately)
pip install streamlit plotly

# Launch
uv run streamlit run mvp_pass1_power/dashboard/dashboard.py -- `
    --data-dir mvp_pass1_power/outputs
```

Four panels:
1. **Intensity Curves** — cost and carbon intensity over time, per archetype; GWP toggle
2. **Granular Results** — capacity mix, generation mix, storage, CF, renewable share
3. **Calibration** — demand vs AEMO Overview benchmark with 2030 overshoot flag
4. **Configuration Notes** — archetype design rationale and methodological choices

CSV download buttons are on the Intensity Curves panel (filtered to visible archetypes).

---

## 10. Regeneration instructions

### If IASR workbook is updated

1. Replace `mvp_pass1_power/data/iasr_2024_v6.0.xlsx` with the new workbook.
2. Clear `mvp_pass1_power/data/workbook_cache/` to force re-parsing.
3. Re-run the production suite (Section 3).
4. Re-run the post-processing (Section 5).
5. Re-launch the dashboard to reload CSVs.

The calibration table in Section 6 should be re-verified after step 4.

### If only post-processing needs to change

Skip the solver runs. The solved NetCDFs in `bench/runs_myopic/` persist across runs.
Re-run the two post-processing commands in Section 5 only.

### Downloading data from scratch

```powershell
# IASR workbook
curl -L -o mvp_pass1_power/data/iasr_2024_v6.0.xlsx `
    https://data.openisp.au/archive/workbooks/6.0.xlsx

# Wind/solar/demand traces
uv run python -c "
from isp_trace_parser.remote import fetch_trace_data
from pathlib import Path
fetch_trace_data(
    dataset_type='example', dataset_year=2024,
    save_directory=Path('mvp_pass1_power/data/traces')
)
"

# NGA Factors PDF (provenance only — factors are hardcoded in nger_factors.py)
curl -L -o mvp_pass1_power/data/NGA_Factors_2024.pdf `
    https://www.dcceew.gov.au/sites/default/files/documents/national-greenhouse-account-factors-2024.pdf
```

### Checking what solved networks are available

```powershell
Get-ChildItem mvp_pass1_power/bench/runs_myopic -Recurse -Filter capacity_expansion.nc |
    Select-Object FullName, LastWriteTime
```

---

## 11. Known limitations (Pass 1 scope)

- **Cost decoupling is approximate.** The ISPyPSA LP minimises *bundled* cost; simple-msm
  sees *decoupled* cost. If Pass 2 fuel prices differ from IASR, the capacity mix is not
  what Pass 2 would pick. Acceptable for Pass 1 archetype menu; must be addressed in Pass 3.
- **Myopic state-passing is simplified.** New-entrant capacities from period T are passed
  forward as ECAA assets in period T+1, but IASR baseline retirements and augmentation
  projects are re-read fresh from the workbook each period. True cross-period new-entrant
  cost amortisation is not modelled.
- **fossil_incumbent wind thinning uses random sampling.** `random_state=0` makes it
  reproducible but the 75% figure is an approximation of "constrained renewable build".
  A production deployment would use year-by-year build-rate caps via
  `custom_constraints_rhs`.
- **Nuclear connection costs use CCGT proxy.** The `connection_cost_technology` column
  is set to `"CCGT"` for nuclear rows; this affects connection cost lookup in the
  translator. It is the best available approximation without a nuclear-specific cost
  schedule in IASR.
