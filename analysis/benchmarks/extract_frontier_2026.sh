#!/bin/bash
# Assemble the 2026 renewable-share frontier across all 7 carbon-price trajectories
# (fixed p_min_pu=0, Gurobi). $150 = pilot_nem_150_v75_r4; rest = prod2026_c{cp}.
cd /c/Users/van538/GitHub/ISPyPSA
OUT=analysis/outputs/frontier_2026
declare -A RUNID=( [0]=prod2026_c0 [40]=prod2026_c40 [80]=prod2026_c80 [150]=pilot_nem_150_v75_r4 [250]=prod2026_c250 [350]=prod2026_c350 [550]=prod2026_c550 )
for cp in 0 40 80 150 250 350 550; do
  echo "=== extracting c${cp} (${RUNID[$cp]}) ==="
  PYTHONPATH=. uv run python analysis/postprocess/extract_frontier_points.py \
    --sweep-id c${cp} --run-id "${RUNID[$cp]}" --years 2030 2035 2040 2045 2050 \
    --carbon-price ${cp} --tns-price 0 \
    --workbook-cache analysis/data/workbook_cache_v75 --out-dir "${OUT}" \
    2>&1 | tail -3 || echo "  c${cp} EXTRACTION FAILED"
done
echo "=== assembling combined frontier ==="
PYTHONPATH=. uv run python - <<'PY'
import pandas as pd, glob, os
out="analysis/outputs/frontier_2026"
fs=sorted(glob.glob(f"{out}/frontier_points_c*.csv"))
if not fs:
    print("NO frontier CSVs produced"); raise SystemExit
df=pd.concat([pd.read_csv(f) for f in fs], ignore_index=True)
df.to_csv(f"{out}/frontier_points_all.csv", index=False)
rcol="renewable_share_pct_bulk_grid" if "renewable_share_pct_bulk_grid" in df.columns else "renewable_share_pct"
ccol="carbon_price" if "carbon_price" in df.columns else ("sweep_id" if "sweep_id" in df.columns else df.columns[0])
piv=df.pivot_table(index="year", columns=ccol, values=rcol)
print("=== RENEWABLE SHARE (% bulk grid) by year x carbon_price ===")
print(piv.round(1).to_string())
PY
echo "DONE_FRONTIER"
