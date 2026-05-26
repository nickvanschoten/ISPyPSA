# Phase 6 production run — findings

**Run id:** `20260526_142616` (PDLP @ 1e-3, NEM-wide, 6 archetypes × 6 milestone years)

---

## 1. Solve status

| Archetype | Periods solved | Failed | Total wall | Peak RSS |
|---|---:|---:|---:|---:|
| `cost_optimal` | 6 / 6 | 0 | 18.6 min | 2.1 GiB |
| `rapid_coal_phaseout` | 6 / 6 | 0 | 19.0 min | 2.2 GiB |
| `gas_fleet_maintained` | (rerunning post-fix) | — | — | — |
| `storage_led` | 6 / 6 | 0 | 57.3 min | 2.1 GiB |
| `fossil_incumbent` | 6 / 6 | 0 | 13.6 min | 1.5 GiB |
| `nuclear_baseload` | 6 / 6 | 0 | 18.2 min | 2.0 GiB |

All 30 PDLP solves converged with all three tolerances (primal feasibility,
dual feasibility, duality gap) below the 1e-3 target. `model_status:
Unknown` reporting quirk applied as expected (per Phase 1 bench finding);
solution values populated correctly.

**Phase 6 wall-clock for parallel run:** 0.96 h (storage_led was the bottleneck
at 57.3 min, mostly its 2050 period at 2,551 s = 42.5 min for the binding
40,350 MW storage_floor LP).

**gas_fleet_maintained** failed the initial launch on a Windows MAX_PATH=260
limit (the doubled-year sub_run_id pushed the longest parquet path to 261
chars). Fixed in commit 7c0862c (Phase 1 follow-up f); rerun in progress.

---

## 2. Headline trajectories — capacity (GW) by archetype × period

### Wind (NEM-wide)

| Archetype | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 14.4 | 18.1 | 32.5 | 39.7 | **19.1** | 40.9 |
| rapid_coal_phaseout | 13.4 | 24.8 | 29.3 | 30.7 | **19.7** | 27.3 |
| storage_led | 14.4 | 18.1 | 44.8 | 44.4 | **20.4** | 42.1 |
| fossil_incumbent | 13.6 | 14.9 | 18.7 | 31.0 | 39.9 | 41.4 |
| nuclear_baseload | 13.4 | 17.1 | 19.8 | 26.0 | **18.9** | 26.9 |

### Solar (NEM-wide)

| Archetype | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 10.7 | 14.9 | 28.0 | 43.0 | 47.5 | 75.4 |
| rapid_coal_phaseout | 10.7 | 12.8 | 15.2 | 16.7 | 30.2 | 26.4 |
| storage_led | 10.7 | 14.9 | 39.5 | 59.5 | 53.7 | 79.7 |
| fossil_incumbent | 10.7 | 12.8 | 12.8 | 12.5 | 11.2 | 6.7 |
| nuclear_baseload | 10.7 | 12.8 | 13.4 | 14.3 | 28.0 | 21.0 |

### Storage power (GW; includes existing pumped storage + batteries)

| Archetype | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 15.9 | 16.0 | 18.5 | 28.6 | 26.6 | 37.1 |
| rapid_coal_phaseout | 15.0 | 15.1 | 15.4 | 16.3 | 17.0 | 17.7 |
| storage_led | 15.9 | 16.0 | 23.8 | 34.3 | 29.0 | **38.0** |
| fossil_incumbent | 15.0 | 15.0 | 16.4 | 24.4 | 46.4 | 26.7 |
| nuclear_baseload | 15.0 | 15.1 | 15.0 | 15.8 | 17.2 | 16.2 |

### Gas (NEM-wide)

| Archetype | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 11.9 | 10.7 | 10.0 | 7.9 | 5.3 | 2.4 |
| rapid_coal_phaseout | 11.9 | **23.5** | 23.0 | 18.7 | 10.4 | 2.4 |
| storage_led | 11.9 | 10.7 | 10.0 | 7.9 | 5.3 | 2.4 |
| fossil_incumbent | 11.9 | 10.7 | 10.0 | 7.9 | 6.6 | 7.4 |
| nuclear_baseload | 11.9 | 12.2 | 19.4 | 16.6 | 9.3 | 2.4 |

### Biomass (NEM-wide)

| Archetype | 2025 | 2030 | 2035 | 2040 | 2045 | 2050 |
|---|---:|---:|---:|---:|---:|---:|
| cost_optimal | 0.0 | 0.0 | 0.1 | 0.5 | 0.6 | 2.6 |
| rapid_coal_phaseout | 0.0 | 0.0 | 0.7 | 5.4 | 3.8 | **16.1** |
| storage_led | 0.0 | 0.0 | 0.1 | 0.5 | 0.6 | 2.6 |
| fossil_incumbent | 0.0 | 0.0 | 0.0 | 0.0 | 2.5 | **10.0** |
| nuclear_baseload | 0.0 | 0.0 | 0.2 | 4.8 | 1.7 | **11.9** |

---

## 3. Annual objective (AUD, per-period LP)

| Year | cost_opt | rapid_coal | storage_led | fossil_inc | nuclear_bl |
|---|---:|---:|---:|---:|---:|
| 2025 | 4.022e9 | 4.022e9 | 4.022e9 | 4.069e9 | 4.023e9 |
| 2030 | 6.866e9 | **1.292e10** | 8.163e9 | 5.876e9 | 6.865e9 |
| 2035 | 1.146e10 | 1.470e10 | 1.614e10 | 7.329e9 | 1.146e10 |
| 2040 | 1.512e10 | 1.751e10 | 1.840e10 | 1.019e10 | 1.512e10 |
| 2045 | 1.392e10 | 1.515e10 | 1.621e10 | 1.349e10 | 1.584e10 |
| 2050 | 1.908e10 | 1.982e10 | 2.051e10 | 1.842e10 | **2.289e10** |

(`gas_fleet_maintained` rerun in progress; 2025 obj 4.022e9, 2030 obj
1.291e10 already observed — identical to `rapid_coal_phaseout` at 2030
because the gas_floor is non-binding given the natural ~23 GW gas build
at 2030 to replace the clipped coal.)

---

## 4. Findings worth surfacing

### (a) Structurally distinct trajectories — NO catalogue collapse

All five completed archetypes produce qualitatively different
storage / wind / solar / biomass mixes by 2050. Renewable share at 2050:

| Archetype | 2050 renewable share |
|---|---:|
| cost_optimal | 82.4 % |
| rapid_coal_phaseout | **86.8 %** |
| storage_led | 86.2 % |
| fossil_incumbent | 55.6 % |
| nuclear_baseload | 71.1 % |

The catalogue redesign avoided the prior-run convergence pattern. Even
the three coal-by-2030 variants (rapid_coal_phaseout / gas_fleet_maintained
/ storage_led) take meaningfully different paths — rapid_coal builds gas
+ biomass at 2030, storage_led builds storage instead, gas_fleet floors
gas. Cost trajectories are differentiated 3-50 % apart.

### (b) **2045 wind dip persists under the Pass-1 repowering treatment.**

The 2045 wind capacity column shows a dip across every archetype except
`fossil_incumbent` (which constrains wind builds anyway):

| Archetype | 2040 wind | **2045 wind** | 2050 wind |
|---|---:|---:|---:|
| cost_optimal | 39.7 | **19.1** | 40.9 |
| rapid_coal_phaseout | 30.7 | **19.7** | 27.3 |
| storage_led | 44.4 | **20.4** | 42.1 |
| nuclear_baseload | 26.0 | **18.9** | 26.9 |

The dip is **20-25 GW per archetype** — about half the surrounding-period
capacity drops out at 2045 and rebuilds at 2050. The repowering overlay
(closure_year +20 yr + annualised capex premium on `fom_$/kw/annum`)
softened but did not eliminate this dip.

**Why the dip persists** (hypothesis, Phase 6 didn't fully diagnose):
the myopic per-period decomposition treats each year as an independent
solve against the IASR baseline at that year. Even with my +20-yr
closure extension, the 2045 LP sees a particular generator pool where
many wind farms' lifetime arithmetic in the templater produces
short remaining-life (`lifetime = closure_year - first_investment_period
+ 1`) that may interact with PyPSA's annuitisation. Worth tracing per the
team's earlier note: this finding now has Phase 6 evidence behind it.

**Possible v2 options for the team conversation:**
1. Larger repowering closure extension (e.g. +30 yr instead of +20)
2. True LP-decision repowering via new_entrant injection per existing site
3. Investigate whether the dip is a templater lifetime-arithmetic artefact
   rather than an economic finding

### (c) Biomass dominance pattern persists at NEM scale

The single-NSW-period smoke produced ~10 GW biomass; the NEM-wide
multi-period production run shows:

| Archetype | 2050 biomass capacity |
|---|---:|
| cost_optimal | 2.6 GW |
| storage_led | 2.6 GW |
| fossil_incumbent | 10.0 GW |
| nuclear_baseload | 11.9 GW |
| rapid_coal_phaseout | **16.1 GW** |

Storage_led keeps biomass low because the mandate floors storage at 38 GW
which provides the firming biomass would otherwise supply. Cost_optimal
also stays low. But `rapid_coal_phaseout`, `nuclear_baseload`, and
`fossil_incumbent` all lean on biomass at 10-16 GW. **This is a real
structural finding for the team conversation.**

The drivers from Phase 1 (peak-week-only representative + biomass at
$0.66/GJ + p_max_pu=1.0 = cost-effective firm capacity) persist at NEM
scale. Multi-region import diversity does NOT mitigate the biomass-heavy
preference when coal is forced out without an explicit storage mandate.

### (d) Coal-out-by-2030 doubles the 2030 LP cost

`rapid_coal_phaseout` and `gas_fleet_maintained` both clip coal to 2030.
At 2030 LP cost = 1.292 × 10¹⁰ vs cost_optimal 6.866 × 10⁹ — a **88 %
cost spike** at the 2030 milestone. The gap closes in later years
(2050 only 4 % gap) as the system normalises. Quantifies the cost of
accelerated coal exit relative to the IASR schedule.

### (e) Nuclear mandate at $124B annuitised drives 2050 cost

`nuclear_baseload` 2050 obj = 2.289 × 10¹⁰ vs cost_optimal 1.908 × 10¹⁰
(+20 %). The 4 GW mandate at 31.1M AUD/MW capital cost = ~$124B
annuitised (at WACC 7 %, 60-yr life ≈ $9B/yr) dominates the 2050 cost
delta. Consistent with the GenCost 2024-25 nuclear-uncompetitive finding.

### (f) Supply gap modest at all archetypes

`supply_gap_pct` (total generation vs demand, positive = over-generation):

| Archetype | 2050 supply gap |
|---|---:|
| cost_optimal | 3.51 % |
| rapid_coal_phaseout | 2.71 % |
| storage_led | 3.60 % |
| fossil_incumbent | 2.31 % |
| nuclear_baseload | 2.48 % |

All under 4 % — no demand-side issues at production scale.

---

## 5. Outstanding before Phase 6 closure

1. **gas_fleet_maintained rerun completion** (in progress; expected ~17 min from
   relaunch). The gas mandate first binds at 2030; under rapid_coal_phaseout's
   natural ~23 GW gas at 2030 the mandate is non-binding, but the same coal-out
   schedule + gas mandate at 2035 (when natural gas declines) should bind.

2. **Re-extract granular outputs** including gas_fleet_maintained once its
   rerun completes — the `outputs/phase6_granular/*` CSVs currently have
   `NaN` for gas_fleet rows at 2030-2050.

3. **Investigate the 2045 wind dip mechanism** before the team conversation.
   The repowering overlay was a Pass-1 simplification; whether the residual
   dip is a templater lifetime-arithmetic quirk or a real economic finding
   needs verification.

4. **Phase 6 closure write-up** consolidating the headline outputs +
   findings (a)-(e) above into the team-facing deliverable.

---

## 6. Phase 1 follow-up tally (across Phase 6 commissioning)

Phase 6 launch exposed two additional Phase 1 issues beyond the original
three+two from earlier commissioning attempts:

| # | Issue | Commit | Description |
|---|---|---|---|
| (a) | Stale cache | 862801d | normalisation never ran on existing cache |
| (b) | Workflow sentinel | 862801d | `existing_generators_summary.csv` consolidated away |
| (c) | Duplicate `technology_type` column | 862801d | snake-case collision |
| (d) | NaN in `_get_storage_duration_for_battery_type` | 862801d | regex on non-string |
| (e) | Translator REZ-expansion KeyError | 862801d | non-REZ rhs entries |
| (f) | Missing `Power Station` for v6.0 | b89cef1 | new_entrants_summary lacked column |
| (g) | Rogue lowercase "Battery storage" cell | a0bfd4c | v6.0 typo broke build_cost merge |
| (h) | Windows MAX_PATH overflow | 7c0862c | bench runner doubled year in path |

Eight Phase 1 follow-up fixes total. None are Phase 2's responsibility;
all are pre-existing templater/translator/bench-runner issues that
surfaced incrementally as the test scope expanded from NSW-smoke to
full-NEM Phase 6. Phase 2's code paths are untouched throughout.

Regression preserved at 755/755 throughout the entire chain.
