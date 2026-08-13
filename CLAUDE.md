# Claude Coding Preferences for ISPyPSA

## Code Style

### Guiding principle

**Readability first.** Favour clarity over DRY or efficiency. A little repetition is fine if it
makes the code easier to follow. Optimise for a reader who hasn't seen the codebase before.

### Orchestrator + helper pattern

High-level functions should read like a narrative — a sequence of descriptive verb-phrase calls
that tell the story of what the function does. Push all data manipulation into private (`_`)
helper functions.

```python
def template_network_transmission_paths(iasr_tables, scenario):
    paths = _extract_flow_paths(iasr_tables["flow_path_transfer_capability"])
    paths = _add_transfer_limits(paths, iasr_tables["interconnector_transfer_capability"])
    paths = _filter_to_scenario(paths, scenario)
    return paths
```

### Helper function guidelines

- **≤ 10 lines** of actual logic (excluding docstrings/blank lines) for any function that
  touches or transforms data.
- **Private by default** — prefix with `_` unless the function is part of the public API.
- **Descriptive names** — the name should make the orchestrator readable without needing to
  look at the helper's body. Prefer verb phrases: `_extract_*`, `_add_*`, `_filter_*`,
  `_map_*`, `_merge_*`.
- **Single responsibility** — each helper does one thing. If a helper needs an internal
  comment explaining a second step, it should probably be two helpers.

### Clarity over cleverness

- **Avoid positional access** like `iloc[:, 0]` — use named column access (e.g.
  `df["Flow Paths"]`) so the code states what it means.
- **Comment non-obvious regex** — add a concrete example of the input being matched and
  annotate each capture group.
- **Prefer explicit data over clever detection.** If the set of special cases is small and
  stable, declare them as data rather than building logic to infer them from surrounding
  context.

### Control flow

- **Keep it flat.** Prefer simple, linear control flow even if it means some repetition.
- **One level of nesting max** for `if` and `for` statements. If you find yourself writing
  nested loops or nested conditionals, extract the inner block into a helper function.
  The exception is when nesting genuinely is the simplest way to express the logic — but
  that should be rare.

### Non-defensive code

Write non-defensive code by default. Trust design decisions and caller contracts. Don't add
`None` checks, fallback logic, or `try/except` blocks unless explicitly needed or revealed
through testing. Let the code fail clearly when preconditions aren't met.

No backwards compatibility unless explicitly requested — update all call sites directly.

## Logging

Logging surfaces things a user or operator wants to know during a template/translation
run that aren't visible from the returned DataFrames. Errors that should halt the run
are `raise`d, not logged.

### Levels

- **INFO** — used for:
  - The top of a public template/translator orchestrator
    (`logging.info("Creating a template for X")`). Gives a progress trace for long runs.
  - Start and completion of long-running CLI operations (downloads, deletions, file
    generation).
  - Silently dropped or filtered data — rows that appear in the input but not in the
    output (e.g. unmatched options dropped by an inner merge).

- **WARNING** — for data integrity issues the run will tolerate but the caller might
  want to act on:
  - Per-row computations that fail and produce NaN in the output, including paths/REZs
    that were missing from the IASR tables and will receive a default downstream.
  - Empty templated tables that mean a class of components won't appear in the model.
  - User-supplied filter inputs that match nothing in the data.
  - Missing entire input IASR tables.
    *(Note: this category will be deprecated once table-schema-based validation lands —
    that layer should surface missing tables instead.)*

- **DEBUG and ERROR are not used.** Errors are raised as exceptions.

### What not to log

- The successful happy path inside a helper.
- Individual row contents — aggregate into a `sorted(...)` list and log once. The
  fuzzy-match log in `helpers.py` is an exception: it logs each non-exact match
  individually so the user can audit name-matching decisions one by one.
- Anything readily inspected from the returned DataFrame.
- The same condition at multiple call sites — log once at the source where the cause
  is visible.

### Style

- Use f-strings. Wrap collections with `sorted(...)` so messages are stable across runs
  and tests can rely on them.
- Name the specific input/table/region:
  `f"Missing augmentation tables: {missing}"` beats `"some tables are missing"`.
- One summary line over many per-row lines (except the fuzzy-match exception above).

### Tests

Log lines that surface non-obvious data behaviour should be covered with `caplog`. Assert the
**full emitted log message** in one substring check, not a marker plus per-name positive/negative
checks. Because logging style wraps collections in `sorted(...)`, the message is deterministic, so
asserting it whole pins the marker, the listed names, and the absence of any others all at once —
and it doesn't break when an unrelated log line later mentions one of the names you were
negatively checking for.

```python
def test_logs_paths_with_no_capacity(caplog):
    with caplog.at_level("WARNING"):
        my_function(inputs_with_missing_data)
    assert (
        "Flow paths with no capacity data in IASR table "
        "(default will be applied downstream): ['MN-SA']"
    ) in caplog.text
```

Cover both the firing case and a negative case (no log when data is complete).

## Testing

### Test structure

Tests follow a strict ordering: **inputs → function call → expected → assertion.**

Use the `csv_str_to_df` fixture to create readable DataFrame inputs and expected outputs. Place
each `assert_frame_equal` immediately after its expected DataFrame definition. Only include
columns in test inputs that the code actually accesses.

**Always assert with `pd.testing.assert_frame_equal` against a full expected DataFrame.** Don't
substitute row-count checks, set membership on a column, per-cell `iloc[...]` lookups, or
`pd.isna(...)` probes — they hide off-by-one, ordering, and stray-column bugs and read worse than
a single side-by-side comparison. The same applies to empty results: build an empty expected
DataFrame with a header-only `csv_str_to_df("path_id,  geo_from, ...")` call and compare, rather
than asserting `result.empty` plus `list(result.columns) == [...]` separately.

```python
def test_my_function(csv_str_to_df):
    input_data = csv_str_to_df("""
        name,   value
        item1,  100
        item2,  200
    """)

    result = my_function(input_data)

    expected = csv_str_to_df("""
        name,   processed_value
        item1,  150
        item2,  250
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("name").reset_index(drop=True),
        expected.sort_values("name").reset_index(drop=True),
    )
```

### Empty DataFrame handling

Missing data is represented as a DataFrame with all expected columns but no rows ("all columns,
no rows"). This is enforced by schema validation at module boundaries, so internal functions can
rely on receiving complete DataFrames and never need to check for `None` or missing tables.

Functions must handle empty DataFrames gracefully — pandas operations like filtering, groupby,
and concat naturally handle this without special-case code.

### Combinatorial edge cases

Functions with multiple input DataFrames must be tested with:

- Table A empty, Table B populated
- Table A populated, Table B empty
- Both tables empty

```python
def test_both_empty(csv_str_to_df):
    table_a = pd.DataFrame(columns=["id", "value"])
    table_b = pd.DataFrame(columns=["id", "name"])

    result = my_function(table_a, table_b)

    expected = csv_str_to_df("""
        id,  value,  name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
```

Use `pd.DataFrame(columns=[...])` for empty *inputs* (where shared module-level column
constants like `_FLOW_PATH_COLUMNS` make the construction tighter), and header-only
`csv_str_to_df` for empty *expected outputs* (so the assertion's expected shape lives in the
same readable form as every other expected DataFrame in the test).

### Comparing DataFrames

- Sort both sides before comparison when row order doesn't matter
- Use `check_exact=False` or `rtol=1e-5` for floating point comparisons
- Represent NaN cells in `csv_str_to_df` as a blank field after the comma — e.g.
  `A-B,    ,   ,` produces a row with `path_id="A-B"` and NaN for the remaining columns.
  Use this for collapsed/missing-data rows instead of `iloc` + `pd.isna` probes.
- Use `check_dtype=False` when type precision isn't critical (e.g. NaN columns)

## Development Environment

Use `uv` for package management. Key commands:

```bash
uv sync                  # Install dependencies from lock file
uv run pytest tests/     # Run tests
uv run pytest tests/test_foo.py::test_bar -v  # Run a specific test
uv run pre-commit run --all-files             # Run linters
```

## Version Control

- Only commit when explicitly requested
- Commit messages should focus on the "why" rather than the "what"

---

# Project context: Pass 1 power-sector integration MVP

The sections above are upstream ISPyPSA coding style preferences. This
section is project-specific context for the **Pass 1 power-sector
integration MVP** in this fork. Read both before doing any new work.

## The architectural project

A three-pass multi-sector modelling pipeline:

- **Pass 1** — sector-specific mini-models emit a *menu of archetypes*
  per milestone year. For power, that's ISPyPSA running AEMO's 2024 IASR
  data through PyPSA + HiGHS. This fork builds and characterises that
  Pass 1.
- **Pass 2** — a multi-sector orchestrator LP (called **simple-msm**)
  consumes the per-sector archetype menus and picks a cost-optimal
  cross-sector composition under scenario inputs, with MGA layered on
  for near-optimal exploration.
- **Pass 3** — for each Pass 2 selection, re-parameterise the underlying
  sector mini-models at full operational fidelity for the AEMO-facing
  deliverable.

The Pass 1 ↔ Pass 2 contract is a CSV schema. Each power-sector method
emits, per milestone year:

- `output_cost_per_unit` — annualised AUD/MWh **excluding** commodity
  costs the orchestrator prices itself (coal, gas — priced by other
  sector roles in Pass 2)
- `input_commodities` + `input_coefficients` — per-MWh-delivered fuel
  consumption (GJ/MWh per fuel)
- `energy_emissions_by_pollutant` and `process_emissions_by_pollutant` —
  Scope 1 emission intensities per MWh delivered
- `max_share`, `min_share`, `max_activity` — bounds on this method's
  contribution

## What's in this fork

Everything new lives under `analysis/`. Upstream ISPyPSA
(`src/ispypsa/`, `tests/`, etc.) is unchanged.

### Read in this order before starting

1. **[`analysis/README.md`](analysis/README.md)** — what
   the MVP demonstrates, scope honesty, four archetypes, calibration vs
   AEMO.
2. **[`analysis/benchmarks/characterisation_report.md`](analysis/benchmarks/characterisation_report.md)**
   — the first compute-envelope study. Seven configs from NSW 1-period
   (75 s, Optimal) to full-NEM 6-period (intractable at default HiGHS).
3. **[`analysis/benchmarks/ipm_addendum.md`](analysis/benchmarks/ipm_addendum.md)**
   — HiGHS IPM vs primal simplex. IPM stalls at
   `Start factorization 7` during IPX basis identification.
4. **[`analysis/benchmarks/ipm_nocrossover_addendum.md`](analysis/benchmarks/ipm_nocrossover_addendum.md)**
   — Disproof of the "crossover-stall" hypothesis. The stall is in IPX
   barrier, not in the final crossover.
5. **[`analysis/benchmarks/phase1_2_addendum.md`](analysis/benchmarks/phase1_2_addendum.md)**
   — PDLP at default tolerance + myopic period-decomposition.
6. **[`analysis/benchmarks/test1_test2_addendum.md`](analysis/benchmarks/test1_test2_addendum.md)**
   — Final state. **PDLP at 1e-3 tolerance solves NEM 2p in 31 min.**
   **NEM 2035 1-period extended budget converges Optimal in 22 min.**

### Code layout

- [`analysis/archetypes/`](analysis/archetypes/) — four
  archetype-mutation functions that edit ISPyPSA input CSVs between
  templater and translator.
- [`analysis/postprocess/`](analysis/postprocess/) — turns
  a solved PyPSA Network into simple-msm CSVs. NGER emission cross-walk
  in `nger_factors.py`; cost decoupling in `extract_method_years.py`;
  CSV emission in `emit_simple_msm.py`.
- [`analysis/benchmarks/`](analysis/benchmarks/) — the seven-config
  benchmark suite + IPM/PDLP variants + myopic driver. Wrapped by
  [`instrumented_runner.py`](analysis/benchmarks/instrumented_runner.py)
  with per-stage timing and HiGHS log parsing.
- [`analysis/calibration/`](analysis/calibration/) —
  side-by-side comparison vs AEMO's published 2024 ISP Step Change.

### What's NOT committed

`analysis/.gitignore` excludes:

- `data/` — IASR workbook, wind/solar/demand traces, NGA PDF.
  Downloadable.
- `runs/`, `bench/runs/`, `bench/runs_myopic/` — solved PyPSA Network
  NetCDFs (~1 GB). Regenerable from configs.
- `bench/configs_myopic/` — auto-generated by `run_myopic.py`.

The remaining ~420 KB is what's needed to pick up the project.

## Facts you can rely on (don't rediscover)

### Compute envelope on the workstation used

Dell Precision 5490, Intel Core Ultra 7 165H (16C/22T), 64 GiB RAM,
Windows 11.

| Config | Primal simplex | IPM | PDLP default | **PDLP 1e-3** | **Myopic 6×1p** |
|---|---|---|---|---|---|
| NSW 1p | ✓ 75 s | n/a | n/a | n/a | implicit |
| NEM 1p | ✓ 22 min – 2 h | n/a | n/a | n/a | per-period |
| NSW 2p (4.88M rows) | ✗ degenerate | ✗ factorization 7 | ◐ near-Optimal | n/a | n/a |
| NEM 2p (16.2M rows) | ✗ | ✗ presolve | n/a | **✓ 31 min** | n/a |
| NSW 6p | n/a | n/a | n/a | n/a | ✓ ~9 min |
| NEM 6p | ✗ (extrapolated) | ✗ | n/a | extrapolated ~90–120 min | **✓ ~4 h overnight** |

### HiGHS-specific facts

- **Primal simplex Phase 2 degenerates** on every multi-period and
  full-NEM LP. `Pr` oscillates 1e7 – 1e11 across hundreds of thousands
  of iterations. It *does eventually converge on single-period NEM*
  LPs (NEM 1p 2050 in 2 h, 2035 in 22 min) — just slowly.
- **IPM's "Constructing starting basis" is NOT the crossover** that
  `run_crossover` controls — it's IPX's internal basis-id during
  barrier. Both crossover-on and crossover-off stall at the same point.
- **PDLP at 1e-3 requires THREE options set together**:
  `pdlp_optimality_tolerance`, `primal_feasibility_tolerance`,
  `dual_feasibility_tolerance`. With only one, HiGHS reports
  `kUnknown` rather than `kOptimal`.
- **PDLP at relaxed tolerance returns `model_status: Unknown`**
  on the ISPyPSA NEM 2p LP even when all three convergence metrics are
  below the requested 1e-3. The solution is mathematically converged
  (values in `network.generators_t.p` are populated correctly); the
  status field is misleading.

### Cost decoupling

ISPyPSA bundles `fuel_price × heat_rate + VOM` into `marginal_cost`
(PyPSA-Eur convention). The post-processor reads pypsa-friendly
`generators.csv` columns (`isp_heat_rate_gj/mwh`, fuel-cost mapping) plus
IASR fuel-price tables, computes per-generator-per-snapshot fuel cost,
and subtracts from the bundled total. Both decoupled and bundled
numbers are emitted as diagnostics.

**Methodological consequence**: ISPyPSA's LP minimises bundled cost;
simple-msm sees decoupled cost. If Pass 2's endogenous fuel price
differs from IASR's, the capacity mix isn't necessarily what Pass 2
would pick. **For Pass 1 (archetype menu) this is acceptable. For
Pass 3 (high-fidelity re-solve) the ISPyPSA solve must be re-run
with overrides.**

### Emissions

All Scope 1 combustion factors from National Greenhouse Accounts
Factors 2024 (DCCEEW, NGER (Measurement) Determination 2008 Schedule 1).
Cross-walk in [`postprocess/nger_factors.py`](analysis/postprocess/nger_factors.py).
CO2, CH4 (as CO2e), N2O (as CO2e) reported separately. Multi-pollutant
beyond CO2e is not implemented.

## Architectural options on the table (data, not recommendations)

For the team conversation, the empirically-demonstrated paths are:

1. **PDLP at 1e-3 tolerance** — perfect-foresight viable at production
   scale. NEM 2p in 31 min, NEM 6p extrapolates to ~90–120 min.
   Caveats: `model_status: Unknown` reporting quirk, 2030 NEM
   consumption shows +36% vs AEMO Overview (likely IASR data, not
   solver).
2. **Myopic period-decomposition** — sequential single-period default
   simplex. NEM 6p ~4 h overnight, every period formal-Optimal.
   Caveat: state-passing tested as 6 independent IASR-baseline-per-year
   solves, not true cross-period new-entrant chaining.
3. **Commercial solver** — untested. Published benchmarks suggest
   10–100× speedup over HiGHS for this LP class.
4. **Don't do perfect-foresight at all** — accept myopic-with-overlap
   as the production pattern.

## Reproduction

```bash
# 0. Install deps
uv sync

# 1. Download IASR workbook + traces + NGA PDF (~1.6 GB one-time)
curl -L -o analysis/data/iasr_2024_v6.0.xlsx \
    https://data.openisp.au/archive/workbooks/6.0.xlsx
uv run python -c "
from isp_trace_parser.remote import fetch_trace_data
from pathlib import Path
fetch_trace_data(dataset_type='example', dataset_src='isp_2024',
                 save_directory=Path('analysis/data/traces'))
"
curl -L -o analysis/data/NGA_Factors_2024.pdf \
    https://www.dcceew.gov.au/sites/default/files/documents/national-greenhouse-account-factors-2024.pdf

# 2. Run the four MVP archetypes at NSW-only fast config
bash analysis/scripts/run_all_archetypes.sh

# 3. Inspect outputs
cat analysis/outputs/simple_msm/method_years.csv
cat analysis/outputs/simple_msm/diagnostics.csv
cat analysis/calibration/calibration_report.md
```

To re-run a specific bench config:

```bash
# NEM 2p PDLP at 1e-3 (the production-scale solve)
uv run python analysis/benchmarks/run_one_pdlp.py \
    --run-id 05_pdlp_tol_3_nem_2period \
    --config analysis/benchmarks/configs/05_nem_2period.yaml \
    --pdlp-tolerance 1e-3 --budget-min 60

# Or the myopic NEM 6p sequence
uv run python analysis/benchmarks/run_myopic.py \
    --run-id nem_6p_myopic_v2 \
    --periods 2025 2030 2035 2040 2045 2050 \
    --budget-min 360
```

Bench records (JSON) under
[`analysis/benchmarks/records/`](analysis/benchmarks/records/),
solver-stdout transcripts under
[`analysis/benchmarks/logs/`](analysis/benchmarks/logs/).

## Skills

See [`.claude/skills/`](.claude/skills/) for operational guidance on
running bench configs and interpreting solver outputs.
