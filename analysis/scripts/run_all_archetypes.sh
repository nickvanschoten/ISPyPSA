#!/usr/bin/env bash
# Run all six production archetypes sequentially with the fast config, then
# emit simple-msm CSVs and the calibration report.
#
# This is the demonstration entry point — what the team should run to
# reproduce the MVP's outputs.

set -euo pipefail

CONFIG="${CONFIG:-analysis/configs/fast.yaml}"
RUNS_DIR="analysis/runs"
WORKBOOK_CACHE="analysis/data/workbook_cache"
OUT_DIR="analysis/outputs/simple_msm"

for arch in cost_optimal rapid_coal_phaseout gas_fleet_maintained storage_led fossil_incumbent nuclear_baseload; do
    echo "=== Running archetype: $arch ==="
    uv run python analysis/scripts/run_workflow.py \
        --config "$CONFIG" --archetype "$arch"
done

echo "=== Emitting simple-msm CSVs ==="
uv run python -m analysis.postprocess.emit_simple_msm \
    --runs-dir "$RUNS_DIR" --workbook-cache "$WORKBOOK_CACHE" --out "$OUT_DIR"

echo "=== Building calibration report ==="
# Run name in fast.yaml is "fast_step_change", so the cost_optimal run dir is
# fast_step_change__cost_optimal.
RUN_NAME=$(grep '^  ispypsa_run_name:' "$CONFIG" | awk '{print $2}')
uv run python analysis/calibration/compare_to_aemo.py \
    --run "$RUNS_DIR/${RUN_NAME}__cost_optimal" \
    --out analysis/calibration

echo "Done. See:"
echo "  - $OUT_DIR/methods.csv"
echo "  - $OUT_DIR/method_years.csv"
echo "  - $OUT_DIR/diagnostics.csv"
echo "  - $OUT_DIR/nger_factor_table.csv"
echo "  - analysis/calibration/calibration_report.md"
