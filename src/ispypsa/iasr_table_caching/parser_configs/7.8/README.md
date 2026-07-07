# IASR 2026 ISP FINAL v7.8 parser configs (repo-tracked override)

## What this is

`isp-workbook-parser` 2.8.0 (the installed version) ships table configs for
IASR versions **6.0, 7.0, 7.3, and 7.5**. It does NOT ship configs for the
2026 ISP **FINAL** workbook, which self-reports as **workbook version 7.8**
in its Change Log sheet (last numeric value in column B).

When `build_local_cache` encounters a workbook whose version isn't shipped by
the installed `isp-workbook-parser`, it passes this directory as
`Parser(user_config_directory_path=...)` so the underlying parsing logic can
locate the right schema definitions. See `local_cache.py`
`_PARSER_CONFIG_OVERRIDES`.

## Origin

YAMLs in this directory were **cloned verbatim from
`isp_table_configs/7.5/`** at `isp-workbook-parser` 2.8.0 — the same source
the repo-tracked v7.4 override used.

Rationale: the FINAL (v7.8) workbook is a **data refresh** of the DRAFT
(v7.5) workbook. Same ~84 sheets (one added: `Hybrid site limits`), same
table column schemas, but generator-keyed tables carry ~84 more rows (new
committed/anticipated projects) and `Build limits - REZs` grew (N9 split into
N9a/N9b plus new group constraints). The schema is identical to v7.5, so
v7.5 is the closest clone base; only per-table `end_row` ranges needed
patching to capture the added rows.

## Adjustments from the v7.5 clone

All edits carry an inline `# v7.8-adjusted: ...` comment recording the
original v7.5 value and the reason.

1. **Flow-path augmentation cost table names** (`flow_path_costs_forecasts.yaml`):
   v7.5 ships three non-canonical names (CNSW-NNSW with singular "cost";
   MEL_WNV / SEV_MEL with underscores). Renamed to ISPyPSA-canonical form
   (plural "costs", dash separators) directly in the YAML — exactly as the
   v7.4 override did. This means the `if iasr_workbook_version == "7.5"`
   quirk-rename block in `local_cache.py` does **not** need to fire for v7.8;
   the cache lands with canonical filenames natively.

2. **Extended `end_row` for generator-keyed tables that grew** between DRAFT
   and FINAL. The FINAL workbook's actual table extents were measured and the
   `end_row` (and where relevant `column_range`) bumped so every added row is
   read with no truncation and no over-read into the following table or
   footnote block. Affected: Fixed OPEX, Variable OPEX, Heat rates,
   Emissions intensity, Maximum capacity, Retirement (expected_closure_years
   + retirement_costs), Marginal Loss Factors, Seasonal ratings,
   Affine Heat rates.

3. **Build limits - REZs**: `initial_resource_limits` end_row 58->59 (N9 split
   into N9a/N9b adds one REZ row); `initial_transmission_limits` shifted +2
   rows (header_rows [64,65]->[66,67], end_row 112->115).

4. **Flow path augmentation options** (`flow_path_augmentation_options.yaml`):
   every per-flow-path table's two-row header position shifted in FINAL. All
   14 tables' `header_rows`/`end_row` were re-measured. Without this the
   header landed on data rows and the parser raised "duplicate column names".

5. **Gas, Liquid fuel, H2 price sheet** (`gas_liquid_fuel_h2_price.yaml`): the
   lower tables shifted down ~60 rows as the sheet grew. Re-positioned
   `industrial_fuel_costs`, `residential_fuel_costs`, `liquid_fuel_prices`,
   `gpg_secondary_fuel_prices`, `hydrogen_prices`, `biomethane_prices`.

6. **NSW roadmap policy tables** (`energy_policy_targets.yaml`): the NSW EIR
   block shifted up a few rows. Re-positioned
   `nsw_roadmap_min_vre_generation_target`,
   `nsw_roadmap_storage_power_capacity_trajectory`,
   `nsw_roadmap_storage_energy_capacity_trajectory`, and
   `nsw_roadmap_rez_max_connection_limit`.

## Table removed from the FINAL workbook

`gas_and_liquid_fuel_prices_consultant_scenario_mapping` (the small G:N
scenario-label matrix at the top of the "Gas, Liquid fuel, H2 price" sheet)
no longer exists in the FINAL workbook. It is also unused by the v7.x
templater path. `build_local_cache` drops it from the request for v7.8
(see the `if iasr_workbook_version == "7.8"` block in `local_cache.py`)
rather than letting `save_tables` halt on the resulting out-of-bounds error.

## Maintenance

Patch the affected YAML in this directory and add a short
`# v7.8-adjusted: ...` comment. Do **not** modify the installed
`isp-workbook-parser` configs in `.venv/Lib/site-packages/isp_table_configs/`
— those are dependency-controlled and lost on the next `uv sync`.

## Upgrade path

If a future `isp-workbook-parser` release ships a 7.8 config dir, delete this
override directory and remove the `"7.8"` entry from `_PARSER_CONFIG_OVERRIDES`
in `local_cache.py`, so the dependency-controlled configs become the source
of truth again.
