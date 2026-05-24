# IASR 2025-26 v7.4 parser configs (repo-tracked override)

## What this is

`isp-workbook-parser` ≤ 2.8.0 (the latest release at time of writing,
2026-01-28) ships table configs for IASR versions **6.0, 7.0, 7.3, and 7.5**.
It does NOT ship configs for **7.4**.

AEMO's IASR 2025-26 publication on the *2025-26 Inputs, Assumptions and
Scenarios* webpage (the workbook published in August 2025, at
`https://www.aemo.com.au/energy-systems/major-publications/integrated-system-plan-isp/2026-integrated-system-plan-isp/2025-26-inputs-assumptions-and-scenarios`)
self-reports as **workbook version 7.4** in its Change Log sheet (last
value in column B). The filename is `2025-inputs-and-assumptions-workbook.xlsm`
without a version suffix; the "7.3" label sometimes used informally for
that file refers to the section numbering of the accompanying IASR report,
not the workbook version.

When the team's `build_local_cache` encounters a workbook whose version
isn't shipped by the installed `isp-workbook-parser`, it passes this
directory as `Parser(user_config_directory_path=...)` so the
underlying parsing logic can locate the right schema definitions.

## Origin

YAMLs in this directory were **cloned verbatim from
`isp_table_configs/7.5/`** at `isp-workbook-parser` 2.8.0.

Rationale: AEMO's v7.4 (August 2025) sits between the parser's shipped
v7.3 (July 2025) and v7.5 (December 2025 addendum-merged). Cloning from
v7.5 minimises row-range patching, because v7.5 evolved from v7.4 and
v7.5's row ranges are generally supersets of v7.4's row ranges. v7.3's
configs are further from v7.4 in time and structure.

## Maintenance

Where v7.4 differs from the v7.5 clone (e.g., extended row ranges,
renamed columns), patch the affected YAML in this directory and add a
short `# v7.4-adjusted: ...` comment recording the original v7.5 value
and the reason. Do **not** modify the installed `isp-workbook-parser`
configs in `.venv/Lib/site-packages/isp_table_configs/` — those are
dependency-controlled and will be lost on the next `uv sync`.

## Upgrade path

If a future `isp-workbook-parser` release ships a 7.4 config dir, this
override directory should be deleted and `build_local_cache` reverted
to its pre-override path, so the dependency-controlled configs become
the source of truth again.
