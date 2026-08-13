#!/bin/bash
# Phase 8.1 variance sub-study: launch runs 2-5 sequentially.
# Run 1 is launched separately; this script runs 2, 3, 4, 5 back-to-back.

set -e
cd "$(dirname "$0")/../.."

for i in 2 3 4 5; do
  echo "=== Variance sub-study run $i / 5 ==="
  uv run --no-sync python analysis/benchmarks/run_myopic.py \
    --run-id "p81vs_pdlp_r${i}" \
    --periods 2040 \
    --archetype cost_optimal \
    --use-pdlp \
    --pdlp-tolerance 1e-3 \
    --budget-min 30
  echo "=== Run $i complete ==="
done

echo "All 5 variance runs complete."
