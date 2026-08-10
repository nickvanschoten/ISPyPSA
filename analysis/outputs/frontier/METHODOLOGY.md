# Production trajectory-sweep menu — methodology & reading notes

This directory holds the Pass-1 grid-electricity menu: a set of recursive-dynamic
capacity-expansion trajectories under a span of carbon prices, each emitting one
cost-emissions coordinate per milestone year. It is the input the Pass-2
multi-sector orchestrator (simple-msm) reads when it selects a cross-sector
composition.

## What a row is

One row per `(sweep_id, year)`. Each `sweep_id` (`c0`, `c40`, `c80`, `c150`,
`c250`, `c350`, `c550` — the trailing number is the carbon price in AUD/tCO2e) is a
single recursive-dynamic chain solved at that fixed price. Within a chain, the
milestone years `2030 → 2035 → 2040 → 2045 → 2050` are solved in sequence: each
year's surviving new-build tranche is carried forward into the next year as
brownfield (`p_nom_extendable=False`, `capital_cost=0.0`), so a year's mix is
conditioned on the trajectory that produced it. A 2050 row carries up to four prior
vintages.

The top two rungs are $350 and $550, not $400/$650: above ~$350 the marginal
abatement curve flattens so hard that adjacent high prices produce near-duplicate
cost coordinates (at 2050, $550 lands within ~0.4% of $350's cost while abating a
little more via a firmer gas-CCS/biomass mix). $350/$550 resolves the CCS-entry
band ($250→$350) better than $400/$650 would while avoiding tail redundancy.

These are **counterfactual price-path points, not a forecast**. Each chain answers
"what does the least-cost grid look like if this carbon price held across the whole
trajectory?" The menu spans the space; Pass-2 picks the point.

## Coverage

**35 rows — the complete 7×5 menu.** All seven chains (`c0`, `c40`, `c80`, `c150`,
`c250`, `c350`, `c550`) solve at all five milestone years (2030 → 2050) to Gurobi
`Optimal`. This supersedes the earlier interim menu, which was missing the `c0`/2050
cell and carried a carry-forward solve defect across the 2035–2050 cells. That
defect is **fixed and validated** — `recursive_dynamic.py` now excludes the
re-templated existing (ECAA) fleet from the carry-forward, so retirement-extendable
existing units are no longer mis-attributed as new builds — and the whole suite has
been re-solved on fully-final 2026 ISP (workbook v7.8) data.

**The 2050 cells are near-optimal-by-continuity, not absent.** The seven largest
LPs (2050, ~40M rows) terminate with a benign dual-non-convergence artifact: Gurobi
prints `Optimal` then a crossover-off `suboptimal` label, and the raw
`objective_value` field is corrupted (~1e16 vs a true ~1e10). The **primal**
solution is sound — capacity is continuous with 2045, unserved energy is ~0, the
assembled frontier is monotone-coherent across all 35 points, and **every reported
cost is recomputed from the primal** (`capital_cost × p_nom_opt` + decoupled opex),
never the objective field. `tolerance_robust` is `False` on the 2050 cells (and on
two other very large LPs — c0/2035 and c80/2040 — whose *primal* feasibility is
marginally loose with healthy duals) to flag this; it does **not** mean the cell is
invalid or absent.

## The cost column — read this before using any cost number

**`cost_per_mwh_excl_fuel_carbon` is the contract column.** It is the full-fleet
annualised intensity, with **fuel and carbon stripped** and **T&S retained**. It
carries the fixed cost of the *whole* delivering fleet as a clean three-way
partition with no overlap and no gap — every generator/store in a year-t network is
in exactly one bucket:

1. **Year-t new build** — capex+FOM, via the year's own `capital_cost`
   (ISPyPSA bundles capex+FOM there for new entrants).
2. **Carried prior vintages** (build_year ∈ prior chain milestones) — capex+FOM
   re-attributed from each vintage's original solved network.
3. **Inherited (ECAA) fleet** (build_year ≤ 2029, plus committed/anticipated
   plant) — FOM re-attributed from the ISPyPSA input roster
   (`fom_$/kw/annum` per unit), billed only on capacity still active in year t
   (retirement-filtered). Its capex is genuinely sunk (zero); only the recurring
   FOM is billed.

The ECAA-roster name is the partition discriminator for bucket 3: carried tranches
and current-year new-build are not in the roster, so the term never double-counts.
This is the only column valid for cross-year or cross-trajectory comparison,
because it carries the operating cost of the whole fleet — not just the year's
incremental new build.

*Pilot-review correction:* an earlier build omitted bucket 3 — ISPyPSA sets ECAA
`capital_cost=0` at translation, which also zeroes the FOM the new-entrant path
bundles into capital_cost, so the inherited fleet's FOM was absent from the LP and
from this column. That understated cost most at 2030 (~$20/MWh, where the existing
fleet dominates) and shrank toward 2050 as the fleet retires into FOM-bearing new
build — a *shape* distortion along the menu's own trajectory axis, not a uniform
offset. It was traced from the anomalously-low c000/2030 figure and fixed by adding
bucket 3 (extraction-only; the solves were unaffected because ECAA capacity is
non-extendable with deterministic closure, so its FOM is dispatch- and
decision-neutral). The diagnostic `cost_per_mwh_year_t_incremental` column is
build-year-attributed and so legitimately excludes the inherited fleet (its FOM
belongs to build_year ≤ 2029, outside every chain year).

- **Fuel stripped** because Pass-2 prices commodity fuels (coal, gas) itself via
  other sector roles. Pass-1 must not double-count them. The fuel intensities are
  emitted separately as `gj_per_mwh_*` for the orchestrator to price.
- **Carbon stripped** because Pass-2 applies its own carbon price. The carbon cost
  the Pass-1 LP saw is reported in `diagnostic_carbon_cost_per_mwh` for audit.
- **T&S retained** — transmission and storage are part of delivering electricity to
  load, so their annualised cost stays in the intensity.

**`diagnostic_cost_per_mwh_year_t_incremental` is a diagnostic, NOT the cost of
electricity.** It is the carbon-and-fuel-stripped year-t LP spend. Because carried
brownfield rows enter the year-t LP at `capital_cost=0.0`, their capex *and* FOM
are attributed to their build-year, not billed in year t. So this column omits the
operating cost of the carried fleet (~86% of capacity by 2050) and will read
implausibly low in late years. It is retained only for build-year-allocation
bookkeeping.

### Why FOM forces the full-fleet column (known refinement)

ISPyPSA bundles fixed O&M into capital cost at the source — `capital_cost =
annuitised_capex + fom_$/kw/annum × 1000` (translator/generators.py) — and the
pypsa-friendly `generators.csv` exposes no separable FOM column. The
recursive-dynamic write-back zeroes `capital_cost` on carried rows by design (they
must not re-bill capex inside the next year's LP), which silently zeroes their FOM
too. A year-t-incremental column therefore cannot carry the carried fleet's FOM,
and disaggregating capex from FOM is impossible with current inputs — the IASR data
does not carry the split. **Disaggregation is a known refinement** for a future
IASR vintage that splits capex and FOM; until then, the full-fleet column (which
re-bundles both from the original capital_cost) is the correct intensity and the
year-t column is bookkeeping only.

Note the re-attribution covers only the chain's **own** vintages (2030..t-1), never
ISPyPSA's native pre-2030 existing fleet (which carries `capital_cost=0` because its
capex is genuinely sunk). The native-existing FOM is therefore absent from every
trajectory equally — a common constant that cancels in any cross-trajectory
comparison.

## Emissions

`co2_t_per_mwh` and `co2e_total_t_per_mwh` are Scope-1 intensities per MWh
delivered to load, computed from NGER 2024 combustion factors applied to physical
fuel burn, with CCS plants at residual `gross × (1 − capture_rate)`. CO2, CH4
(as CO2e) and N2O (as CO2e) are summed into `total_CO2e`.

### Biomass carbon boundary — for the downstream multi-sector optimiser

Biomass **combustion CO2 is reported as zero here, faithfully following the
NGER/NGA convention** (NGA Factors 2024, Table 4): for biomass fuels the
combustion CO2 and the feedstock regrowth removals are **both accounted in the
Land sector (LULUCF), "and are in balance"** — not in the energy sector. ISPyPSA
therefore counts only biomass's **CH4 + N2O combustion residuals (1.8 kg
CO2e/GJ)**, which are non-biogenic and genuinely belong to the energy sector. This
is the NGER's deliberate boundary, not a "carbon-neutral renewable" software
default.

**Consequences for the downstream handoff (must be honoured to avoid
double-counting):**
- Biomass **contributes to renewable share** (it is a renewable energy source) and
  carries **near-zero energy-sector CO2 intensity** — these are not contradictory.
- The downstream model **must account biomass combustion CO2 in its Land/LULUCF
  accounting** (where the regrowth offset also sits, in balance). It must **not**
  (a) add biomass combustion to the energy-sector total, nor (b) credit the
  regrowth sequestration *without* the matching combustion — either would break the
  NGA in-balance convention this menu follows.
- If a future framework instead wants biomass combustion booked in the *energy*
  sector (with LULUCF carrying sequestration only), ISPyPSA would need a **gross**
  combustion factor from a non-NGER source (e.g. IPCC 2006 GL default for primary
  solid biomass, ~100 kg CO2/GJ) — the NGER cannot supply one, as it sets biomass
  combustion CO2 to zero by design.

### Reading the renewable share against the emissions trend

> **Validated (production run): all 2030–2050 cells are valid.** The earlier
> carry-forward defect (carried fossil priced at ~$0 → dispatched as free baseload)
> is fixed in `recursive_dynamic.py` and the suite re-solved; the renewable-share
> and emissions trends below are read off the converged production frontier. The
> renewable share is a **conservative floor** — the three endogenous-retirement
> approximations (flat FOM with no end-of-life overhaul cliff, no brownfield
> repowering, myopic-reactive retirement timing) all push retirement *later* than a
> richer model would, so the true frontier sits at or above these shares.

`renewable_share_pct_bulk_grid` (Wind+Solar+Biomass+Hydro, dispatch basis) rises
modestly across a chain while CO2 intensity falls steeply. **This is physically
coherent, not an artefact.** (Hydro is included: although ISPyPSA models it without
availability traces, the *solved dispatch* runs hydro at ~18 TWh — realistic, not
the ~60 TWh a flat-out run would imply — so excluding it understated the share by
~7 pp.) The decarbonisation is carried by three mechanisms a VRE-share number
does not capture: (1) **biomass growth** — biogenic, NGER-near-zero, counts as
renewable *and* abates; (2) **gas→CCS conversion** — the gas fleet's effective
emission factor falls well below unabated CCGT as capture is built; (3) **coal
exit** — the shrinking coal tail remains the dominant emitter per MWh but on
collapsing volume. The renewable-% and emissions intensities share the same
dispatch denominator, so there is no classification mismatch between them.

## Solver provenance and the `tolerance_robust` flag

Every leg is solved with **Gurobi barrier** (`Method 2`, `Crossover 0`,
`BarConvTol 1e-4` — the pinned production quality choice). Crossover-off returns
the interior solution directly; the per-row diagnostics carry the barrier's
`solve_gap_rel`, `solve_pinf_rel`, `solve_dinf_rel` (absolute primal/dual
infeasibility and complementarity gap), iterations and wall-clock.

- **`solve_model_status` reads `Optimal` then a `suboptimal` crossover-off label.**
  With crossover off Gurobi returns the barrier interior point and stamps it
  `suboptimal` — a label of the crossover state, not a real sub-optimality; the
  dispatch and capacity are valid and post-process correctly. Trust
  `tolerance_robust`, not the raw status. (On the 2050 cells the raw
  `objective_value` field is additionally corrupted — see Coverage — which is why
  every cost column is recomputed from the primal, never the objective.)
- **`tolerance_robust=True`** means the barrier's primal and dual infeasibility are
  both below 1e-2 — the coordinate is well-converged. A `False` flags the 2050 cells
  (dual infeasibility O(1)) and two other very large LPs with marginally-loose
  primal feasibility; in every case the **primal is sound** (the frontier is
  monotone-coherent), so the cost/emissions coordinates remain usable — `False`
  marks "not formally certified", not "invalid".
- The LP has a **flat objective face near the optimum**: many capacity mixes lie
  within ~1–2% cost of each other. The consequence is that **cost coordinates are
  robust but individual-technology compositions are soft** — which is why
  compositions are emitted to a separate non-contract file.

## Compositions are non-contract

`compositions_NONCONTRACT_*.csv` (capacity GW by carrier) is a diagnostic only. At
the certified tolerance the flat objective face makes per-technology capacity soft
(individual carriers can move ~30–70% between near-equal-cost solutions). The
**coordinates** (cost, emissions, fuel intensities) are the certified quantity;
the composition is indicative.

**The `year == 2025` rows are an exception: actual existing-fleet capacity data,
not a modelled (soft) result.** They are the ECAA fleet operating in 2025
(commissioned ≤2025, closure >2025), ~64.7 GW by carrier, sourced from
`ispypsa_inputs/ecaa_generators.csv` (`maximum_capacity_mw`) and recorded
standalone in `existing_capacity_2025.csv`. They give the dashboard's technology-
mix view a real starting point before the modelled 2030→2050 trajectory. 2025 is
**not** added to `frontier_points.tidy.csv` — that file is cost-emissions
coordinates, which 2025 has no modelled value for (we did not model 2025). The
2025 baseline is the existing-fleet starting column for the technology-mix view.

## Scope honesty

- **Bulk-grid framing.** Costs and intensities are for bulk grid electricity
  delivered to load. Distribution-level and behind-the-meter detail is out of scope
  for Pass 1.
- **Nuclear is absent** — not in AEMO's 2024 IASR technology set, so not in the
  menu.
- **No net-negative.** There is no BECCS/DAC credit pathway; emissions intensities
  are floored at the residual combustion of the dispatched fleet, not driven below
  zero.
- **Fuel-price decoupling caveat.** ISPyPSA's LP minimises bundled cost at IASR
  fuel prices; the menu reports fuel-decoupled cost. If Pass-2's endogenous fuel
  price differs materially from IASR's, the *capacity mix* behind a coordinate is
  not necessarily what Pass-2 would choose. Acceptable for the Pass-1 menu; a Pass-3
  high-fidelity re-solve with Pass-2's fuel-price overrides is required before the
  mix behind any selected point is treated as final.
- **Capture-rate placeholder.** CCS residual emissions use a placeholder
  `capture_rate` per technology_type (the NGER combustion factor × (1 − capture)).
  This is a representative central value, not a plant-specific commissioning
  figure; treat CCS intensities as indicative pending plant-level capture data.

## Named gaps

The menu ships with these gaps/obligations, all stated rather than hidden:

- **2050 dual-non-convergence artifact.** The seven 2050 cells are near-optimal-by-
  continuity with a corrupted objective field but a primal-sound solution (see
  Coverage). Costs are recomputed from the primal; the cells are valid, flagged via
  `tolerance_robust=False`.
- **Renewable share is a conservative floor.** The three endogenous-retirement
  approximations (flat FOM / no overhaul cliff, no brownfield repowering, myopic-
  reactive timing) all push retirement later, so the true share sits at or above the
  reported one.
- **T&S strip-before-Tier-2 obligation.** Transmission & storage carbon cost is
  *internalised* in the cost intensity at a placeholder price (20 AUD/tCO2,
  `tns_price_aud_per_tco2`) rather than stripped like fuel and energy carbon. The
  Pass-2 contract is fuel-and-carbon-stripped; T&S carbon is the one carbon-linked
  cost still riding inside the intensity. **Before the menu is consumed at Tier-2,
  the T&S carbon component must be stripped (or the placeholder replaced with the
  orchestrator's own T&S price)** so the orchestrator does not double-count it
  against its endogenous carbon price. Recorded as an obligation, not yet actioned.

## Provenance

AEMO IASR 2026 FINAL (workbook v7.8, Step Change) · NGA Factors 2024 (DCCEEW) ·
ISPyPSA · costs in AUD_2024 · carbon price per chain as labelled · Gurobi barrier
(Method 2, Crossover 0, BarConvTol 1e-4) · full-NEM, full-year 30-minute · endogenous
economic retirement (per-unit FOM, span-weight 5, disestablishment $150k/MW as a
separate transition line) + recursive-dynamic carry-forward · `tns_price_aud_per_tco2`
= 0 (no T&S carbon adder priced in this suite).
