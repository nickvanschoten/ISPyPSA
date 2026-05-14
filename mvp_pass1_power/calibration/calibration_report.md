# ISPyPSA cost-optimal vs AEMO 2024 ISP Step Change — calibration report

**Scope mismatch warning.** The MVP was run at NSW-only scale due to session-time constraints (see README, *Honest assessment of MVP scope*). AEMO publishes NEM-wide totals. The comparison below therefore shows ISPyPSA NSW capacity as a *percentage of AEMO's NEM number*, not as a divergence. NSW carries roughly 28–32% of NEM peak demand depending on year — anything in that range is consistent with a credible NSW share, not an indicator of model accuracy.

Source for AEMO numbers: AEMO 2024 ISP overview report. See aemo_2024_isp_step_change.md for citations.

## Capacity by fuel group (GW active in period)

| Year | wind+solar (AEMO vs ISPyPSA) | gas (AEMO vs ISPyPSA) | coal (AEMO vs ISPyPSA) | hydro | storage | biomass+H2 |
|------|-----|-----|-----|-----|-----|-----|
| 2050 | AEMO NEM: 127.0; ISPyPSA NSW: 30.5 (24% of NEM) | AEMO NEM: 15.0; ISPyPSA NSW: 1.8 (12% of NEM) | AEMO: 0; ISPyPSA: 0.0 | 2.5 | 0.0 | 1.6 |

## Annual grid consumption (TWh)

| Year | AEMO NEM | ISPyPSA NSW | share |
|------|---------:|------------:|------:|
| 2050 | 313 | 104.0 | 33% of NEM |

## Honest assessment

**Divergences expected because of MVP simplifications:**

1. **Single reference year (2018), three representative weeks.** AEMO uses 30+ reference years and full 8760. Our coverage of inter-annual variability is one year — capacity decisions sensitive to renewables droughts will diverge.

2. **Distributed PV is not modelled as endogenous capacity.** AEMO's 86 GW 2050 rooftop figure is exogenous load reduction in ISPyPSA's demand traces; it does not appear in our capacity table.

3. **Policy targets templated but not enforced.** ISPyPSA reads renewable-share trajectories from IASR but does not translate them into PyPSA constraints. The cost-optimal LP may therefore under-build renewables relative to AEMO's policy-constrained Step Change result.

**What this calibration evidences vs what it doesn't:**

- *Evidences*: the templater + translator + LP build + solve pipeline produces a defensible-shape capacity trajectory.
- *Does not evidence*: that ISPyPSA at this configuration reproduces AEMO's published Step Change figures within any specific tolerance.
 For an AEMO-facing deliverable, a tighter calibration would need: full 30-reference-year traces, 8760 snapshots per year, policy-share constraints wired in, and side-by-side comparison against AEMO's scenario results workbook (not just the published summary numbers).