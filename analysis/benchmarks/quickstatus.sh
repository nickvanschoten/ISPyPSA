#!/usr/bin/env bash
# Fast status check — bypasses uv to avoid per-poll startup cost.
cd "$(dirname "$0")"
echo "=== bench status @ $(date +%H:%M:%S) ==="
for cfg in configs/*.yaml; do
  rid=$(basename "$cfg" .yaml)
  rec="records/${rid}.json"
  log="logs/${rid}.log"
  if [ -f "$rec" ]; then
    wall=$(grep -o '"wall_clock_s": [0-9.]*' "$rec" | head -1 | awk '{print $2}')
    peak=$(grep -o '"peak_rss_gib": [0-9.]*' "$rec" | head -1 | awk '{print $2}')
    status=$(grep -o '"status": "[^"]*"' "$rec" | head -1 | sed 's/.*: "//;s/"//')
    iters=$(grep -o '"simplex_iterations": [0-9]*' "$rec" | head -1 | awk '{print $2}')
    rows=$(grep -o '"lp_rows": [0-9]*' "$rec" | head -1 | awk '{print $2}')
    printf "  [DONE]  %-18s wall=%-6s peak=%-5sGiB rows=%-9s iters=%-9s status=%s\n" \
      "$rid" "${wall:-?}s" "${peak:-?}" "${rows:-?}" "${iters:-?}" "${status:-?}"
  elif [ -f "$log" ]; then
    sz=$(stat -c%s "$log")
    last=$(tail -c 5000 "$log" 2>/dev/null | tr '\r' '\n' | grep -E "Iteration|Objective|Pr:|HiGHS run|Optimal|Model status|LP linopy" | tail -1 | cut -c1-100)
    printf "  [LIVE]  %-18s log=%-8s last: %s\n" "$rid" "${sz}B" "$last"
  else
    printf "  [WAIT]  %-18s not started\n" "$rid"
  fi
done
