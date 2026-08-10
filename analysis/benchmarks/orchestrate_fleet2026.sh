#!/bin/bash
# Fleet of 2026 carbon-price trajectories on the fixed (p_min_pu=0) Gurobi setup.
# $150 already done as pilot_nem_150_v75_r4. Max 2 concurrent (each Gurobi
# full-NEM solve peaks ~32 GiB; 64 GiB total).
cd /c/Users/van538/GitHub/ISPyPSA
TRAJ=(0 40 80 250 350 550)
MAXJ=2
for cp in "${TRAJ[@]}"; do
  while [ "$(jobs -rp | wc -l)" -ge "$MAXJ" ]; do sleep 120; done
  echo "$(date '+%H:%M:%S') launching c${cp}"
  (
    PYTHONPATH=. uv run python mvp_pass1_power/bench/run_myopic.py \
      --run-id prod2026_c${cp} --periods 2030 2035 2040 2045 2050 \
      --recursive-dynamic --full-year \
      --use-gurobi --gurobi-method 2 --gurobi-crossover 0 --gurobi-bar-conv-tol 1e-4 \
      --carbon-price ${cp} --dataset-year 2026 \
      --parsed-traces-directory data/trace_data --budget-min 720 \
      > mvp_pass1_power/bench/logs/prod2026_c${cp}.chain.log 2>&1
  ) &
  sleep 60   # stagger so the two solves' peak factorization phases don't align
done
wait
echo "$(date '+%H:%M:%S') ALL 6 TRAJECTORIES DONE"
