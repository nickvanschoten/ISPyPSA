#!/usr/bin/env bash
# Run all four archetypes sequentially with the fast config, then emit
# simple-msm CSVs and the calibration report.
#
# This is the demonstration entry point — what the team should run to
# reproduce the MVP's outputs.

set -euo pipefail

CONFIG="${CONFIG:-mvp_pass1_power/configs/fast.yaml}"
RUNS_DIR="mvp_pass1_power/runs"
WORKBOOK_CACHE="mvp_pass1_power/data/workbook_cache"
OUT_DIR="mvp_pass1_power/outputs/simple_msm"

for arch in cost_optimal renewables_led fossil_incumbent deep_clean_firmed; do
    echo "=== Running archetype: $arch ==="
    uv run python mvp_pass1_power/scripts/run_workflow.py \
        --config "$CONFIG" --archetype "$arch"
done

echo "=== Emitting simple-msm CSVs ==="
uv run python -m mvp_pass1_power.postprocess.emit_simple_msm \
    --runs-dir "$RUNS_DIR" --workbook-cache "$WORKBOOK_CACHE" --out "$OUT_DIR"

echo "=== Building calibration report ==="
# Run name in fast.yaml is "fast_step_change", so the cost_optimal run dir is
# fast_step_change__cost_optimal.
RUN_NAME=$(grep '^  ispypsa_run_name:' "$CONFIG" | awk '{print $2}')
uv run python mvp_pass1_power/calibration/compare_to_aemo.py \
    --run "$RUNS_DIR/${RUN_NAME}__cost_optimal" \
    --out mvp_pass1_power/calibration

echo "Done. See:"
echo "  - $OUT_DIR/methods.csv"
echo "  - $OUT_DIR/method_years.csv"
echo "  - $OUT_DIR/diagnostics.csv"
echo "  - $OUT_DIR/nger_factor_table.csv"
echo "  - mvp_pass1_power/calibration/calibration_report.md"
