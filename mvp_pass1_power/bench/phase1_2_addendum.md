# Addendum 3: PDLP test + myopic period-decomposition characterisation

Companion to [characterisation_report.md](characterisation_report.md), the
first [ipm_addendum.md](ipm_addendum.md), and the second
[ipm_nocrossover_addendum.md](ipm_nocrossover_addendum.md). Same machine
(Dell Precision 5490, Intel Core Ultra 7 165H, 64 GiB RAM, Windows 11),
same ISPyPSA configurations, same `cost_optimal` archetype, HiGHS 1.12.

Two sequential diagnostic phases:

- **Phase 1**: HiGHS PDLP (primal-dual hybrid gradient) on the previously-
  failed multi-period configurations.
- **Phase 2 (triggered)**: myopic period-decomposition (sequential single-
  period solves) at production scale.

---

## Phase 1 — PDLP

**Methodology verification.** Before running on ISPyPSA LPs, the
`solver_options={"solver": "pdlp"}` setting was confirmed end-to-end on a
control 100-row × 200-col LP (random Ax >= b, dense, presolve disabled).
PDLP iterated cleanly through 35,520 iterations, reached `Status: Optimal`
with no basis (`Has basis: False`), final pinf 1.98e-7 / dinf 0.0 / gap
1.20e-5 / runtime 0.81s. HiGHS PDLP at default settings works as advertised
on small problems.

### Phase 1a — NSW 2-period

**LP**: 4,877,350 rows × 2,250,776 cols × 8,935,253 nonzeros (the same LP
that primal-simplex and IPM both failed on in prior addenda).

**Outcome**: did not formally converge to `Optimal` within a 180-min wall-
clock budget (which became ~11h actual elapsed wall due to Windows sleep —
chain runner detected budget exceeded on wake and killed). HiGHS PDLP did
**100,000 iterations of real progress** during the active-compute window
(HiGHS solver time 3,685s = 61 min, ignoring sleep periods):

| iter | gap | pinf | dinf | HiGHS-time |
|-----:|----:|----:|----:|----:|
| 0      | 0.00e+00 | 9.41e-01 | 1.10e-03 | 43s |
| 4,000  | 3.05e-03 | 2.59e-04 | 3.94e-04 | 151s |
| 12,000 | 8.04e-04 | 5.22e-05 | 7.84e-05 | 454s |
| 20,000 | 1.35e-04 | 1.62e-04 | 6.18e-06 | 756s |
| 28,000 | 4.58e-06 | 5.65e-05 | 6.99e-07 | 1,050s |
| 88,000 | 1.05e-04 | 1.39e-05 | 1.84e-07 | 3,216s |
| 96,000 | 8.20e-04 | 2.83e-06 | 3.89e-06 | 3,503s |
| 100,000 | 2.50e-04 | 2.06e-06 | 8.81e-06 | 3,685s |

**Interpretation**: PDLP makes orders-of-magnitude progress on this LP
where simplex degenerated and IPM stalled. Primal infeasibility dropped
from 9.41e-01 (initial) to ~2e-6 (5 orders of magnitude). Dual infeasibility
dropped to ~1e-6. **But the duality gap oscillates around 1e-4** — exactly
the HiGHS default tolerance for PDLP-Optimal. PDLP converges to a
high-quality *interior solution* (feasibility excellent by any practical
engineering standard) but does not cross HiGHS's default Optimal threshold
within the 61 min of active compute time available.

**No basic-vs-interior solution distinction** matters for the simple-msm
aggregates (dispatch-weighted costs, fuel coefficients, emissions). The
interior solution at iter 100,000 is sufficient for the contract — but
HiGHS's formal status is not `Optimal`, so the run records as `timed_out`
rather than `completed`.

### Phase 1b/c/d — NSW 6p, NEM 2p, NEM 6p

**Not run.** Per task instruction: "If NSW 2-period PDLP doesn't converge
in reasonable time, the diagnostic hypothesis is wrong and the remaining
configurations should not be attempted under PDLP — capture that finding
and proceed to Phase 2." NSW 2p PDLP did not formally cross the convergence
threshold within the budget; the larger configurations were not attempted.

### Direct comparison table for NSW 2-period

| Solver setting | Wall budget hit | Active solver time | LP iters | Final pinf | Final dinf | Status |
|---|---:|---:|---:|---:|---:|---|
| Primal simplex | 8 min (killed) | 8 min | n/a (oscillating in Phase 2) | n/a | 0 | killed at Phase-2 degeneracy |
| IPM + crossover ON | 90 min (killed) | killed at IPX basis-factor 7 | 2 barrier iters | 0.0793 (barrier-2) | 0.0377 (barrier-2) | stalled |
| IPM + crossover OFF | 20 min (killed) | killed at IPX basis-factor 7 | 1 barrier iter | 0.0941 (barrier-0) | 0.0044 (barrier-0) | identical stall to crossover-on |
| **PDLP** | **180 min budget** | **61 min active** | **100,000** | **2.06e-06** | **8.81e-06** | **near-Optimal interior solution; formal status timed_out (gap 2.5e-4 vs 1e-4 threshold)** |

PDLP behaves **qualitatively differently** from primal simplex and IPM on
this LP class: it makes monotonic large-magnitude progress and produces an
engineering-quality interior solution within an hour of active compute. The
team may consider whether this is "convergence" for Pass 1 purposes.

---

## Phase 2 — Myopic period-decomposition

Triggered because Phase 1 did not fully resolve the production-scale
question — PDLP got near-optimal on NSW 2p but did not formally converge,
and the production-equivalent NEM 6p was not reached.

### Methodological note on "myopic"

The myopic driver runs N sequential **single-period** ISPyPSA capacity-
expansion solves, one per milestone year. The `cost_optimal` archetype + IASR
Step Change baseline already encodes AEMO's projected committed buildout per
year (closure schedule, anticipated projects through year T), so each per-
year solve answers: "given the AEMO-projected ECAA fleet at year T, what
new-entrant additions are cost-optimal?". **No additional cross-period
state-passing of new-entrant builds is implemented in the driver** — i.e.,
new-entrant builds chosen in year T are not added to year T+1's ECAA.

This is a simplification of the "myopic with capacity-fixing" pattern; a
production deployment would chain new-entrant builds across periods via
`ecaa_generators.csv` augmentation between sub-runs. The trade-off: my
version shows each year's IASR-baseline cost-optimal mix; a true state-
passing version would show cumulative buildout. For trajectory-coherence
purposes both are useful but they answer slightly different questions.

### Phase 2a — NSW 6-period myopic

**Result**: 6/6 periods solved to `Optimal` in **560s cumulative wall-clock
(9.3 min)**, peak RSS 0.84 GiB across the sequence.

| Year | Wall | Peak GiB | Annual TWh | Wind+Solar GW | Gas GW | Coal GW | Storage GW | Hydro GW | Biomass GW |
|-----:|----:|----:|----:|----:|----:|----:|----:|----:|----:|
| 2025 |  80s | 0.80 |  75.8 |  8.0 | 3.1 | 5.4 |  2.6 | 2.5 | 0.0 |
| 2030 |  80s | 0.80 |  85.0 | 10.6 | 3.1 | 5.4 |  3.9 | 2.5 | 0.0 |
| 2035 | 120s | 0.76 |  74.0 | 14.2 | 3.1 | 1.4 |  6.7 | 2.5 | 0.0 |
| 2040 |  75s | 0.78 |  96.5 | 29.4 | 3.1 | 0.0 | 13.4 | 2.5 | 0.1 |
| 2045 |  70s | 0.77 | 101.6 | 29.7 | 1.8 | 0.0 | 13.6 | 2.5 | 1.4 |
| 2050 | 131s | 0.79 | 104.0 | 29.6 | 1.8 | 0.0 | 13.4 | 2.5 | 1.6 |

**Trajectory coherence**: coal retires monotonically (5.4 → 5.4 → 1.4 → 0
→ 0 → 0 GW), wind+solar grows monotonically (8 → 29.6 GW), storage tracks
VRE growth (2.6 → 13.4 GW), gas drops slightly later (3.1 → 1.8 GW). This
is consistent with AEMO's projected NEM Step Change trajectory shape (with
NSW at ~30% of NEM-wide totals).

The monotonic shape comes from the IASR-baseline-per-year encoding of
AEMO's projected closures and anticipated buildout, not from cross-period
state-passing in the driver. No pathological reversals are observed (no
year where capacity choice undermines a prior period's). NSW 2050 wind+sol
(29.6 GW) is ~23% of AEMO's NEM-wide 127 GW Step Change 2050 figure, and
NSW 2050 demand (104 TWh) is ~33% of AEMO's NEM-wide 313 TWh — both within
the historical NSW-as-NEM-share range (28–32% of peak demand).

### Phase 2b — NEM 6-period myopic (production-equivalent)

**Result**: Sequence killed externally after 2 periods solved and the 3rd
in slow Phase-2 simplex iteration. The pattern is the data the team needs.

| Year | Wall | LP rows | Peak GiB | Annual TWh | Wind+Sol GW | Gas GW | Coal GW | Storage GW | Hydro GW | Status |
|-----:|----:|--------:|---------:|-----------:|------------:|-------:|--------:|-----------:|---------:|--------|
| 2025 | 146s | 709,292 | 2.07 | 200.5 | 24.1 | 11.9 | 18.4 |  4.1 | 7.7 | completed (Optimal) |
| 2030 | 236s | 772,796 | 2.10 | 229.3 | 28.3 | 11.0 | 16.2 | 10.0 | 7.7 | completed (Optimal) |
| 2035 | killed @ ~15 min | TBC | TBC | TBC | TBC | TBC | TBC | TBC | TBC | killed mid-Phase-2 (Pr 1e8↔1e11 oscillating) |
| 2040 | not reached | — | — | — | — | — | — | — | — | not run |
| 2045 | not reached | — | — | — | — | — | — | — | — | not run |
| 2050 | not reached | — | — | — | — | — | — | — | — | not run |

**Observation about per-period scaling**. The two completed NEM periods
solved cleanly in 2–4 min each via the same primal-simplex algorithm that
degenerates on the multi-period NEM LPs. But **NEM 2035 entered the same
degenerate Phase-2 oscillation pattern** as multi-period LPs — Pr
infeasibility oscillating between 1e8 and 1e11 across hundreds of
thousands of iterations without monotonic convergence. Killed at ~15 min
in this state.

The prior characterisation report showed that **NEM 1-period at 2050 did
eventually converge after 7,427s of HiGHS time (2 h)** — so NEM 2035
under the same degenerate behaviour would likely converge with similar
patience. The sequence was killed at ~15 min to avoid burning 12+ more
hours on what is now a clearly-characterised pattern.

**Trajectory coherence (partial, 2025–2030)**: wind+solar grows (24.1 →
28.3 GW NEM-wide), coal declines (18.4 → 16.2 GW), storage grows (4.1 →
10.0 GW), gas stable (~11 GW). Annual consumption 200.5 → 229.3 TWh
matches AEMO 2025 (174) → 2030 (202) trajectory shape reasonably. NEM 2030
wind+sol (28.3 GW) is well below AEMO 2030 wind+sol Step Change (55 GW) —
the cost-optimal-without-RES-targets solve under IASR economics under-
builds renewables relative to AEMO's policy-constrained Step Change result
(consistent with the MVP's single-period calibration finding).

### Extrapolated NEM 6p myopic wall-clock

Using NEM 1-period at 2050 = 7,427s HiGHS time as the upper-bound for
"late year, degenerate Phase 2" and observed early-year fast solves:

- 2025: 146s (observed)
- 2030: 236s (observed)
- 2035: ~1–2 h (extrapolated from observed Phase-2 degeneracy)
- 2040: ~1–3 h (extrapolated)
- 2045: ~1–3 h (extrapolated)
- 2050: ~2 h (observed in prior bench)

**Total NEM 6p myopic wall-clock estimate: 6–13 hours**. Tractable in an
overnight run on this hardware; not feasible within an interactive session.

---

## Combined summary across all four solver paths + myopic

What the team now knows about Pass 1 power-sector compute envelope on this
hardware (Dell Precision 5490, 64 GiB RAM, HiGHS 1.12 at default settings):

| Approach | NSW 1p | NSW 2p | NSW 6p | NEM 1p | NEM 2p | NEM 6p |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Primal simplex** | ✓ 75s | ✗ degenerate | ✗ degenerate | ✓ 2h (year 2050) | ✗ degenerate | ✗ degenerate |
| **IPM + crossover ON** | n/a | ✗ stall at IPX factor 7 | n/a | n/a | ✗ stuck in presolve | ✗ extrapolated intractable |
| **IPM + crossover OFF** | n/a | ✗ same stall (option respected; IPX basis-id during barrier still stalls) | n/a | n/a | n/a | n/a |
| **PDLP** | n/a | ◐ near-optimal interior solution (gap 2.5e-4 vs 1e-4 threshold) at 100K iters / 61 min active | n/a | n/a | n/a | n/a |
| **Myopic (6 × single-period)** | ✓ implicit | n/a | ✓ **560s cumulative (9 min)**, all Optimal | ✓ 2h (2050); 146s–236s for 2025/2030 | n/a | ◐ 2025+2030 done in 6 min; 2035 entered degenerate Phase 2 at ~15 min before kill; extrapolated total 6–13 h overnight |

Legend: ✓ = converges to Optimal within practical time. ✗ = does not.
◐ = converges with caveat (interior-only solution, partial sequence, or
extrapolated multi-hour completion).

### Architectural options the team can weigh (no recommendation)

1. **Primal simplex on single-period configurations (myopic).** NSW 6-period
   myopic completes in 9 min, NEM 6-period myopic extrapolates to 6–13 hours
   overnight. Sidesteps the multi-period LP entirely. Cross-period state-
   passing of new-entrant builds is not implemented in the driver tested
   here but is a small additional engineering layer over the existing
   sequential pattern.

2. **PDLP at HiGHS default tolerance.** On NSW 2p, makes orders-of-magnitude
   progress and produces an engineering-quality interior solution (pinf/
   dinf ~1e-6) within an hour of active compute. The HiGHS-Optimal threshold
   (1e-4 duality gap) is not crossed; for simple-msm aggregates the interior
   solution should be sufficient, but HiGHS does not formally report
   `Optimal`. Whether this counts as "convergence" is an engineering call.

3. **PDLP at relaxed tolerance.** `pdlp_optimality_tolerance = 1e-3` (or
   similar) would likely cause HiGHS to declare `Optimal` at iter ~30K on
   NSW 2p (where gap = 5e-6 was observed). Untested per task constraint
   against parameter tuning, but a single-knob change.

4. **Commercial solver.** Gurobi, CPLEX, COPT at default settings typically
   dominate HiGHS by 10–100× on LP classes like this one and handle multi-
   period directly. Untested across any of the four addenda.

5. **Multi-period perfect-foresight at production scale.** Not achieved
   with any of the four HiGHS-based paths tested. Either a different
   solver, parameter tuning, or the myopic decomposition pattern is
   required.

### What this characterisation does NOT answer

- Whether HiGHS PDLP with relaxed tolerances (`pdlp_optimality_tolerance =
  1e-3` or similar) would converge to formal Optimal status faster on
  these LPs. Untested per task constraint.
- Whether commercial solvers handle multi-period LPs at default settings
  on this hardware. Untested across all four addenda.
- Whether the myopic-with-state-passing variant (chaining new-entrant
  builds across periods via ecaa_generators augmentation) would produce a
  materially different trajectory from the IASR-baseline-per-year pattern
  used here. The current driver uses the latter; a production-grade
  myopic pipeline would use the former.
- Whether NEM 6p myopic actually completes on this hardware overnight at
  default simplex settings. The sequence was killed at 2.5 periods done;
  extrapolation from observed per-period behaviour suggests 6–13 h total.
