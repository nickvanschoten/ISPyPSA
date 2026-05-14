---
name: simple-msm-emission
description: Take a solved ISPyPSA archetype run and emit simple-msm-contract-compatible CSVs (methods.csv, method_years.csv, diagnostics.csv, nger_factor_table.csv). Use when running new archetypes, adding new milestone years, troubleshooting unexpected outputs, or extending the cost-decoupling logic. Covers the NGER emission factor cross-walk and the cost-decoupling methodology.
---

Run the post-processor that turns a solved ISPyPSA Network into
simple-msm-contract-compatible CSVs.

# When to use

- Re-emitting `method_years.csv` after re-running an archetype.
- Adding a new archetype to the catalogue.
- Adding a new milestone year (the contract supports any years passed
  via the `investment_periods` config field).
- Debugging unexpected `output_cost_per_unit` or emission values.
- Extending the NGER cross-walk for a new fuel type.
- Understanding how the cost-decoupling math relates to ISPyPSA's
  bundled `marginal_cost`.

# The simple-msm contract

Per `(method, year)` row in `method_years.csv`, the orchestrator
expects:

- `output_cost_per_unit` — annualised AUD/MWh **excluding** commodity
  costs priced separately by Pass 2 (coal, gas — those live on other
  sector roles in the orchestrator).
- `input_commodities` + `input_coefficients` + `input_units` —
  per-MWh-delivered fuel consumption. Currently emits `coal`,
  `natural_gas`, `diesel`, `biomass`, `hydrogen`, `biomethane` as
  applicable, all in GJ/MWh.
- `energy_emissions_by_pollutant` — Scope 1 combustion emissions per
  MWh delivered. Currently `CO2`, `CH4_CO2e`, `N2O_CO2e`, `total_CO2e`,
  all in tCO2e/MWh.
- `process_emissions_by_pollutant` — currently `total_CO2e = 0` for
  electricity (NGA has no process emissions for electricity
  generation; CCS-process would need extension).
- `max_share`, `min_share`, `max_activity` — author-supplied bounds
  in `ARCHETYPE_CATALOGUE` in `emit_simple_msm.py`. NOT derived from
  the ISPyPSA solve.

# Run the post-processor

```bash
uv run python -m mvp_pass1_power.postprocess.emit_simple_msm \
    --runs-dir mvp_pass1_power/runs \
    --workbook-cache mvp_pass1_power/data/workbook_cache \
    --out mvp_pass1_power/outputs/simple_msm
```

Outputs:

- `methods.csv` — one row per archetype with method_id, description,
  role, representation.
- `method_years.csv` — one row per `(method, year)` with the contract
  schema above.
- `diagnostics.csv` — bundled-vs-decoupled cost, fuel share of total,
  CO2e intensity. **Inspect this when output_cost_per_unit values look
  off** — the fuel share tells you whether the decoupling subtracted
  the right amount.
- `nger_factor_table.csv` — provenance of the emission cross-walk.

The orchestrator (`_find_archetype_runs`) auto-detects which archetype
runs are present by scanning for solved `capacity_expansion.nc` files
under `runs/`. Missing runs are silently skipped, not errored.

# Cost decoupling

ISPyPSA inherits PyPSA-Eur's convention: per-snapshot
`marginal_cost = fuel_price × heat_rate + VOM` bundled into one value.
The simple-msm contract wants `output_cost_per_unit` to exclude the
fuel component (Pass 2 prices fuels itself via cross-sector commodity
flows).

The decoupling, in
[`extract_method_years.py`](../../../mvp_pass1_power/postprocess/extract_method_years.py):

1. Read pypsa-friendly `generators.csv` — preserves
   `isp_heat_rate_gj/mwh`, `isp_vom_$/mwh_sent_out`,
   `isp_fuel_cost_mapping` for every generator.
2. Read IASR fuel-price tables from the workbook cache
   (`coal_prices.csv`, `gas_prices.csv`, `liquid_fuel_prices.csv`,
   etc.). Pick the financial-year column matching the milestone year.
3. For each generator × snapshot, compute
   `fuel_cost_per_mwh = fuel_price_$/GJ × heat_rate_gj/mwh`.
4. Subtract dispatch-weighted fuel cost from the bundled total
   marginal cost: `non_fuel_opex = bundled_opex - fuel_cost`.
5. Combine with CAPEX (`capital_cost × p_nom_opt`, summed over
   active components in the period) and transmission/storage costs.
6. Divide by `annual_mwh_delivered` (sum of load × snapshot weighting
   per period, NOT generator dispatch — see why below).

**Why load-weighted not dispatch-weighted**: simple-msm methods are
denominated per MWh delivered to end-uses (the commodity Pass 2 buys).
Generator dispatch includes storage round-trip losses and transmission
losses that don't reach load; using dispatch as the denominator would
underweight those losses.

**Methodological consequence**: ISPyPSA's LP minimises bundled cost,
not decoupled cost. If Pass 2 prices fuels differently from IASR, the
capacity mix ISPyPSA chose is not necessarily what Pass 2 would have
chosen. For Pass 1 (archetype menu) this is acceptable. For Pass 3
the ISPyPSA solve must be re-run with Pass-2's fuel-price overrides.

The `diagnostics.csv` exposes both decoupled and bundled numbers per
row so this trade-off is inspectable downstream.

# NGER emission cross-walk

All Scope 1 combustion emission factors come from the **National
Greenhouse Accounts Factors 2024** (DCCEEW, July 2024 edition).
Underlying legal basis: National Greenhouse and Energy Reporting
(Measurement) Determination 2008, Schedule 1.

The cross-walk lives in
[`postprocess/nger_factors.py`](../../../mvp_pass1_power/postprocess/nger_factors.py):

| ISPyPSA fuel_type | NGA Table | NGA fuel name | Total kg CO2e/GJ |
|---|---|---|---:|
| Black Coal | 4 | Bituminous coal | 90.24 |
| Brown Coal | 4 | Brown coal (lignite) | 93.82 |
| Gas | 5 | Natural gas (pipeline) | 51.53 |
| Liquid Fuel | 8 | Diesel oil | 70.20 |
| Biomass | 4 | Primary solid biomass fuels | 1.80 (CO2 biogenic = 0) |
| Biomethane | 5 | Biomethane | 0.13 |
| Hydrogen | — | Pure H2 combustion (zero Scope 1) | 0 |
| Wind / Solar / Water / Storage | — | non-combustion | 0 |
| Hyblend (gas + H2 mix) | — | linear blend by H2 fraction | computed per year |

CO2, CH4 (as CO2e), and N2O (as CO2e) reported separately per row;
combined as `total_CO2e`. Multi-pollutant beyond CO2e (NOx, SOx, etc.)
is **not implemented**; extending requires (a) adding NGA factors for
the new pollutant to `_NGER_FACTORS_KG_CO2E_PER_GJ`, (b) extending
`_aggregate_emissions` to fold them in.

For **Hyblend** (gas + H2 mix): ISPyPSA's `gpg_emissions_reduction_h2`
table provides per-year H2 share by energy content for relevant
generators. The cross-walk applies a linear blend: combined EF =
`(1 - h2_frac) × natural_gas_EF`.

The cross-walk table itself is emitted as
`outputs/simple_msm/nger_factor_table.csv` as a provenance artefact —
downstream consumers can verify which factors were used without
inspecting code.

# Adding a new archetype

1. Add a function to `mvp_pass1_power/archetypes/<archetype_id>.py`:

```python
def apply(ispypsa_tables, config):
    # Mutate ispypsa_tables in place or return a new dict
    return ispypsa_tables
```

2. Register it in `mvp_pass1_power/archetypes/__init__.py`'s
   `APPLY_ARCHETYPE` dict.

3. Add an entry to `ARCHETYPE_CATALOGUE` in
   [`emit_simple_msm.py`](../../../mvp_pass1_power/postprocess/emit_simple_msm.py)
   with `method_id`, `short_name`, `description`, and any bounds.

4. Run the archetype:

```bash
uv run python mvp_pass1_power/scripts/run_workflow.py \
    --config mvp_pass1_power/configs/fast.yaml \
    --archetype <archetype_id>
```

5. Re-emit the simple-msm CSVs (above).

# Sanity-checking the output

The MVP's `cost_optimal` archetype at NSW 2050 single-period produces:

- `output_cost_per_unit` (decoupled): 177.70 AUD/MWh
- `diagnostic_bundled_cost_per_unit`: 179.21 AUD/MWh
- `diagnostic_fuel_cost_per_unit`: 1.50 AUD/MWh (0.8% of total)
- `annual_mwh_delivered`: 104.0 TWh
- `total_CO2e`: 0.0901 tCO2e/MWh

If your run produces values >2× different from these for the same
config, something's off — usually either:

1. Wrong investment_periods config (the cost-per-MWh-delivered is
   normalised by load; load varies dramatically by year).
2. Heat-rate / fuel-price lookup failed for some generators
   (check `diagnostics.csv` for `fuel_share_of_total_cost`; should
   be 0–10% for a renewables-dominated mix, 30–60% for a thermal-
   heavy mix).
3. Workbook cache stale — re-run
   `build_local_cache` if you changed scenarios.

For multi-period or full-NEM runs, expected values per the
characterisation:

| Config | Year | Annual TWh | Cost AUD/MWh |
|---|---|---|---|
| NEM 1p | 2035 | 256 | (not yet computed) |
| NEM 1p | 2050 | 305 | (not yet computed) |
| NEM 2p PDLP@1e-3 | 2030 | 274 | (not yet computed) |
| NEM 2p PDLP@1e-3 | 2050 | 304 | (not yet computed) |

The 2030 NEM value (274 TWh) sits +36% above AEMO's published 202 TWh —
this is a real divergence worth investigating against AEMO's full
scenario-results workbook (not solver-related, more likely IASR demand
inputs vs Overview rounded numbers).
