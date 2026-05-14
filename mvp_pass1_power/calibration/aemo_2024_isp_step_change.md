# AEMO 2024 ISP — Step Change scenario benchmark figures

These are the published Step Change figures we calibrate the MVP's cost-optimal
ISPyPSA run against. All numbers below are **as published by AEMO** in the
2024 ISP report and overview, sourced via aemo.com.au.

## Capacity outlook (NEM-wide, GW)

| Technology                      | Today (2023-24) | 2030  | 2050  |
|---------------------------------|----------------:|------:|------:|
| Grid-scale wind + solar         |              21 |    55 |   127 |
| Distributed solar PV (rooftop)  |              21 |    36 |    86 |
| Gas-powered generation          |            11.5 |     ~ |    15 |
| Coal-fired generation           |  ~22 (declining)|  retiring | 0 (all retired by 2030–2038) |

## Generation (NEM-wide, TWh)

| Quantity                          | Today | 2030 | 2050 |
|-----------------------------------|------:|-----:|-----:|
| Electricity consumption from grid |   174 |  202 |  313 |

## Sources

- [AEMO 2024 ISP](https://www.aemo.com.au/energy-systems/major-publications/integrated-system-plan-isp/2024-integrated-system-plan-isp)
- [2024 ISP Overview (PDF)](https://www.aemo.com.au/-/media/files/major-publications/isp/2024/2024-integrated-system-plan-overview.pdf)

## Use in this MVP

We compare ISPyPSA's `generation_expansion` and total dispatch outputs to these
numbers at three checkpoints (2030, 2040, 2050) for the `cost_optimal` archetype.

Caveats for honest interpretation:
- AEMO's published figures are at NEM-wide aggregate. ISPyPSA at sub-region
  granularity produces matching aggregates only after summation across sub-regions.
- AEMO uses a 30-reference-year stochastic dispatch model; our MVP runs a single
  reference year (2018) with three representative weeks. Capacity decisions
  will diverge from AEMO whenever weather variability is load-bearing — that
  divergence is expected, not necessarily a bug.
- Distributed PV is exogenous demand-side input in ISPyPSA (treated as
  reduced load), not endogenous build, so the MVP will not produce a
  "distributed solar PV (rooftop)" capacity row directly.
- AEMO does not publish the underlying LP solution; figures are summary numbers
  from the published report. A tighter calibration would need AEMO's detailed
  scenario results workbook (publicly available but separately downloaded).
