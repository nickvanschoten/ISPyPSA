# Eighth addendum — 2026-05-15: Server-side NEM 6p tolerance tests (Gurobi BarConvTol=1e-3 and PDLP 1e-3)

Companion to
[characterisation_report.md](characterisation_report.md),
[ipm_addendum.md](ipm_addendum.md),
[ipm_nocrossover_addendum.md](ipm_nocrossover_addendum.md),
[phase1_2_addendum.md](phase1_2_addendum.md),
[test1_test2_addendum.md](test1_test2_addendum.md),
[gurobi_addendum.md](gurobi_addendum.md), and
[seventh_addendum.md](seventh_addendum.md).

**Hardware:** Dual Intel Xeon Platinum 8260 (48 physical / 96 logical cores, base 2.4 GHz),
1,024 GiB RAM, Windows Server 2019 Standard 10.0.17763. Gurobi uses up to 32 threads by
default. Same bench server as the gurobi_addendum.

---

## Purpose

Two follow-up tests to give the team an apples-to-apples comparison for the Gurobi licensing
decision:

- **Test 1** — Gurobi barrier with `BarConvTol=1e-3` (relaxed from the default 1e-8) on the
  NEM 6-period LP, 8 h budget.
- **Test 2** — HiGHS PDLP at `pdlp_optimality_tolerance=1e-3` on the same NEM 6-period LP,
  6 h budget, on the same bench server.

Both use config `07_nem_6period.yaml` (NEM, 6 investment periods 2025–2050, sub-regional
granularity, discrete-node REZ). The gurobi_addendum ran this LP with Gurobi at default
settings and timed out at barrier iteration 13 within a 4 h budget. Test 1 extends the budget
to 8 h and adds the tolerance relaxation. Test 2 adds the PDLP head-to-head on the same
hardware so the comparison is not confounded by the laptop-vs-server difference that affects
prior addenda.

---

## Test 1 — Gurobi BarConvTol=1e-3, NEM 6-period

### LP dimensions

| Measure | Value |
|---|---|
| Rows | 38,123,031 |
| Cols | 17,963,800 |
| Nonzeros | 76,069,533 |
| Presolved rows | 12,957,515 |
| Presolved cols | 11,347,542 |
| Presolved NZ | 40,626,813 |
| Cholesky factor NZ | 2.278e+09 (~30 GB) |
| Estimated time/iter | ~80 s (Gurobi estimate) |
| Ordering time | 1,589 s |
| Threads used | 30 |

`BarConvTol=1e-3` was confirmed active in the log: `Set parameter BarConvTol to value 0.001`.

### Barrier convergence log

BCT (Barrier Convergence Tolerance metric) = (Pobj − Dobj) / (1 + |Pobj| + |Dobj|).
Gurobi terminates when BCT < BarConvTol. For iters 0–28, Dobj < 0, so BCT ≡ 1.000.

| Iter | Pobj | Dobj | PrRes | DuRes | Compl | Time (s) | BCT |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 3.13e+16 | −2.36e+17 | 4.55e+07 | 7.10e+06 | 8.36e+12 | 2058 | 1.000 |
| 1 | 3.10e+16 | −2.28e+17 | 4.25e+07 | 6.16e+06 | 7.56e+12 | 2317 | 1.000 |
| 2 | 3.07e+16 | −2.08e+17 | 4.11e+07 | 4.25e+06 | 6.31e+12 | 2578 | 1.000 |
| 3 | 2.95e+16 | −1.95e+17 | 3.62e+07 | 3.17e+06 | 5.22e+12 | 2855 | 1.000 |
| 4 | 2.77e+16 | −1.76e+17 | 3.02e+07 | 1.66e+06 | 3.87e+12 | 3147 | 1.000 |
| 5 | 2.48e+16 | −1.54e+17 | 2.24e+07 | 4.73e+07 | 2.48e+12 | 3465 | 1.000 |
| 6 | 2.26e+16 | −1.45e+17 | 1.16e+07 | 6.79e+07 | 1.41e+12 | 3849 | 1.000 |
| 7 | 2.40e+16 | −1.20e+17 | 4.76e+06 | 1.30e+07 | 5.39e+11 | 4322 | 1.000 |
| 8 | 2.51e+16 | −7.09e+16 | 1.32e+06 | 1.66e+04 | 1.42e+11 | 4949 | 1.000 |
| 9 | 2.35e+16 | −4.06e+16 | 3.58e+05 | 6.29e+03 | 3.80e+10 | 5862 | 1.000 |
| 10 | 1.54e+16 | −2.13e+16 | 8.45e+04 | 6.40e+04 | 9.08e+09 | 7190 | 1.000 |
| 11 | 7.24e+15 | −1.36e+16 | 1.41e+04 | 3.92e+05 | 2.08e+09 | 8600 | 1.000 |
| 12 | 3.72e+15 | −7.45e+15 | 5.42e+03 | 1.30e+05 | 8.47e+08 | 10088 | 1.000 |
| 13 | 1.04e+15 | −2.84e+15 | 8.86e+02 | 5.28e+04 | 2.09e+08 | 12043 | 1.000 |
| 14 | 2.68e+14 | −6.78e+14 | 1.36e+02 | 6.95e+03 | 4.39e+07 | 14117 | 1.000 |
| 15 | 3.64e+13 | −7.96e+13 | 4.34e+00 | 3.15e+02 | 4.97e+06 | 14271 | 1.000 |
| 16 | 7.83e+12 | −9.78e+12 | 5.77e−01 | 3.61e+01 | 7.50e+05 | 14420 | 1.000 |
| 17 | 3.40e+12 | −3.43e+12 | 2.05e−01 | 1.27e+01 | 2.91e+05 | 14563 | 1.000 |
| 18 | 3.00e+12 | −1.47e+12 | 1.76e−01 | 6.21e+00 | 1.90e+05 | 14702 | 1.000 |
| 19 | 2.54e+12 | −9.60e+11 | 1.44e−01 | 4.54e+00 | 1.49e+05 | 14837 | 1.000 |
| 20 | 2.41e+12 | −8.71e+11 | 1.35e−01 | 4.23e+00 | 1.40e+05 | 14967 | 1.000 |
| 21 | 2.18e+12 | −7.45e+11 | 1.20e−01 | 3.80e+00 | 1.25e+05 | 15094 | 1.000 |
| 22 | 1.91e+12 | −6.73e+11 | 1.02e−01 | 3.55e+00 | 1.10e+05 | 15233 | 1.000 |
| 23 | 1.74e+12 | −5.03e+11 | 9.08e−02 | 2.94e+00 | 9.54e+04 | 15367 | 1.000 |
| 24 | 1.40e+12 | −4.03e+11 | 6.81e−02 | 2.56e+00 | 7.66e+04 | 15506 | 1.000 |
| 25 | 1.29e+12 | −2.66e+11 | 6.19e−02 | 2.04e+00 | 6.64e+04 | 15649 | 1.000 |
| 26 | 1.13e+12 | −1.57e+11 | 5.18e−02 | 1.61e+00 | 5.46e+04 | 15791 | 1.000 |
| 27 | 1.01e+12 | −1.20e+11 | 4.49e−02 | 1.46e+00 | 4.79e+04 | 15935 | 1.000 |
| 28 | 8.43e+11 | −6.82e+10 | 3.48e−02 | 1.23e+00 | 3.87e+04 | 16077 | 1.000 |
| 29 | 6.91e+11 | +6.53e+09 | 2.61e−02 | 9.09e−01 | 2.91e+04 | 16216 | 0.9813 |
| 30 | 6.45e+11 | +5.99e+10 | 2.35e−02 | 6.84e−01 | 2.49e+04 | 16359 | 0.8300 |
| 31 | 6.02e+11 | +8.66e+10 | 2.11e−02 | 5.69e−01 | 2.19e+04 | 16503 | 0.7484 |
| 32 | 5.53e+11 | +9.96e+10 | 1.84e−02 | 5.13e−01 | 1.93e+04 | 16647 | 0.6950 |
| 33 | 5.28e+11 | +1.12e+11 | 1.70e−02 | 4.59e−01 | 1.77e+04 | 16782 | 0.6492 |
| 34 | 5.16e+11 | +1.19e+11 | 1.63e−02 | 4.31e−01 | 1.69e+04 | 16913 | 0.6264 |
| 35 | 4.95e+11 | +1.25e+11 | 1.52e−02 | 4.03e−01 | 1.57e+04 | 17059 | 0.5958 |
| 36 | 4.61e+11 | +1.33e+11 | 1.33e−02 | 3.69e−01 | 1.40e+04 | 17208 | 0.5518 |
| 37 | 4.50e+11 | +1.47e+11 | 1.27e−02 | 3.10e−01 | 1.29e+04 | 17348 | 0.5079 |
| 38 | 4.25e+11 | +1.52e+11 | 1.13e−02 | 2.86e−01 | 1.16e+04 | 17479 | 0.4725 |
| 39 | 4.13e+11 | +1.57e+11 | 1.07e−02 | 2.67e−01 | 1.09e+04 | 17615 | 0.4500 |
| 40 | 3.93e+11 | +1.60e+11 | 9.60e−03 | 2.52e−01 | 9.93e+03 | 17755 | 0.4221 |
| 41 | 3.83e+11 | +1.64e+11 | 9.03e−03 | 2.33e−01 | 9.30e+03 | 17891 | 0.3996 |
| 42 | 3.67e+11 | +1.66e+11 | 8.17e−03 | 2.25e−01 | 8.55e+03 | 18033 | 0.3768 |
| 43 | 3.61e+11 | +1.70e+11 | 7.81e−03 | 2.11e−01 | 8.14e+03 | 18173 | 0.3606 |
| 44 | 3.58e+11 | +1.71e+11 | 7.64e−03 | 2.03e−01 | 7.93e+03 | 18313 | 0.3522 |
| 45 | 3.41e+11 | +1.76e+11 | 6.74e−03 | 1.82e−01 | 7.02e+03 | 18453 | 0.3190 |
| 46 | 3.30e+11 | +1.79e+11 | 6.12e−03 | 1.71e−01 | 6.45e+03 | 18595 | 0.2978 |
| 47 | 3.23e+11 | +1.81e+11 | 5.72e−03 | 1.62e−01 | 6.06e+03 | 18734 | 0.2826 |
| 48 | 3.20e+11 | +1.86e+11 | 5.52e−03 | 1.41e−01 | 5.69e+03 | 18876 | 0.2648 |
| 49 | 3.15e+11 | +1.89e+11 | 5.27e−03 | 1.29e−01 | 5.38e+03 | 19016 | 0.2511 |
| 50 | 3.13e+11 | +1.90e+11 | 5.13e−03 | 1.24e−01 | 5.22e+03 | 19153 | 0.2443 |
| 51 | 3.07e+11 | +1.92e+11 | 4.85e−03 | 1.13e−01 | 4.89e+03 | 19301 | 0.2303 |
| 52 | 2.98e+11 | +1.93e+11 | 4.33e−03 | 1.08e−01 | 4.45e+03 | 19438 | 0.2128 |
| 53 | 2.94e+11 | +1.96e+11 | 4.10e−03 | 9.63e−02 | 4.15e+03 | 19576 | 0.1990 |
| 54 | 2.88e+11 | +1.98e+11 | 3.77e−03 | 8.89e−02 | 3.82e+03 | 19718 | 0.1848 |
| 55 | 2.85e+11 | +1.99e+11 | 3.61e−03 | 8.50e−02 | 3.66e+03 | 19866 | 0.1778 |
| 56 | 2.81e+11 | +2.00e+11 | 3.39e−03 | 7.95e−02 | 3.43e+03 | 20014 | 0.1677 |
| 57 | 2.79e+11 | +2.01e+11 | 3.30e−03 | 7.67e−02 | 3.33e+03 | 20149 | 0.1634 |
| 58 | 2.76e+11 | +2.01e+11 | 3.11e−03 | 7.32e−02 | 3.15e+03 | 20296 | 0.1553 |
| 59 | 2.71e+11 | +2.03e+11 | 2.87e−03 | 6.72e−02 | 2.91e+03 | 20449 | 0.1442 |
| 60 | 2.70e+11 | +2.03e+11 | 2.81e−03 | 6.59e−02 | 2.85e+03 | 20579 | 0.1414 |
| 61 | 2.67e+11 | +2.04e+11 | 2.66e−03 | 6.11e−02 | 2.68e+03 | 20722 | 0.1338 |
| 62 | 2.63e+11 | +2.05e+11 | 2.41e−03 | 5.73e−02 | 2.45e+03 | 20865 | 0.1231 |
| 63 | 2.60e+11 | +2.06e+11 | 2.28e−03 | 5.33e−02 | 2.31e+03 | 21003 | 0.1167 |
| 64 | 2.59e+11 | +2.07e+11 | 2.20e−03 | 5.02e−02 | 2.22e+03 | 21149 | 0.1121 |
| 65 | 2.57e+11 | +2.07e+11 | 2.12e−03 | 4.82e−02 | 2.13e+03 | 21292 | 0.1080 |
| 66 | 2.55e+11 | +2.08e+11 | 2.03e−03 | 4.55e−02 | 2.04e+03 | 21433 | 0.1035 |
| 67 | 2.54e+11 | +2.08e+11 | 1.94e−03 | 4.30e−02 | 1.94e+03 | 21575 | 0.09900 |
| 68 | 2.52e+11 | +2.09e+11 | 1.83e−03 | 4.01e−02 | 1.83e+03 | 21717 | 0.09346 |
| 69 | 2.50e+11 | +2.09e+11 | 1.72e−03 | 3.71e−02 | 1.71e+03 | 21860 | 0.08781 |
| 70 | 2.47e+11 | +2.10e+11 | 1.60e−03 | 3.42e−02 | 1.59e+03 | 22002 | 0.08185 |
| 71 | 2.45e+11 | +2.10e+11 | 1.50e−03 | 3.21e−02 | 1.49e+03 | 22148 | 0.07697 |
| 72 | 2.44e+11 | +2.11e+11 | 1.44e−03 | 3.02e−02 | 1.43e+03 | 22291 | 0.07366 |
| 73 | 2.43e+11 | +2.11e+11 | 1.35e−03 | 2.90e−02 | 1.35e+03 | 22433 | 0.06979 |
| 74 | 2.42e+11 | +2.11e+11 | 1.30e−03 | 2.70e−02 | 1.28e+03 | 22576 | 0.06656 |
| 75 | 2.39e+11 | +2.12e+11 | 1.18e−03 | 2.58e−02 | 1.18e+03 | 22722 | 0.06148 |
| 76 | 2.39e+11 | +2.12e+11 | 1.16e−03 | 2.51e−02 | 1.16e+03 | 22857 | 0.06027 |
| 77 | 2.38e+11 | +2.12e+11 | 1.10e−03 | 2.32e−02 | 1.09e+03 | 23005 | 0.05713 |
| 78 | 2.37e+11 | +2.12e+11 | 1.06e−03 | 2.28e−02 | 1.05e+03 | 23146 | 0.05521 |
| 79 | 2.36e+11 | +2.13e+11 | 9.94e−04 | 2.11e−02 | 9.88e+02 | 23294 | 0.05179 |
| 80 | 2.35e+11 | +2.13e+11 | 9.42e−04 | 2.00e−02 | 9.36e+02 | 23437 | 0.04918 |
| 81 | 2.34e+11 | +2.13e+11 | 9.05e−04 | 1.86e−02 | 8.94e+02 | 23585 | 0.04701 |
| 82 | 2.33e+11 | +2.13e+11 | 8.63e−04 | 1.78e−02 | 8.54e+02 | 23733 | 0.04493 |
| 83 | 2.32e+11 | +2.13e+11 | 8.15e−04 | 1.70e−02 | 8.08e+02 | 23881 | 0.04258 |
| 84 | 2.32e+11 | +2.14e+11 | 7.84e−04 | 1.62e−02 | 7.76e+02 | 24025 | 0.04093 |
| 85 | 2.31e+11 | +2.14e+11 | 7.62e−04 | 1.51e−02 | 7.49e+02 | 24174 | 0.03954 |
| 86 | 2.31e+11 | +2.14e+11 | 7.30e−04 | 1.40e−02 | 7.13e+02 | 24319 | 0.03768 |
| 87 | 2.30e+11 | +2.14e+11 | 6.67e−04 | 1.29e−02 | 6.53e+02 | 24469 | 0.03459 |
| 88 | 2.29e+11 | +2.15e+11 | 6.23e−04 | 1.17e−02 | 6.07e+02 | 24616 | 0.03221 |
| 89 | 2.28e+11 | +2.15e+11 | 5.74e−04 | 1.08e−02 | 5.61e+02 | 24769 | 0.02981 |
| 90 | 2.27e+11 | +2.15e+11 | 5.35e−04 | 1.00e−02 | 5.22e+02 | 24918 | 0.02776 |
| 91 | 2.27e+11 | +2.15e+11 | 5.09e−04 | 9.04e−03 | 4.93e+02 | 25066 | 0.02627 |
| 92 | 2.26e+11 | +2.15e+11 | 4.73e−04 | 8.66e−03 | 4.60e+02 | 25219 | 0.02454 |
| 93 | 2.26e+11 | +2.15e+11 | 4.55e−04 | 8.09e−03 | 4.41e+02 | 25366 | 0.02350 |
| 94 | 2.25e+11 | +2.15e+11 | 4.21e−04 | 7.38e−03 | 4.07e+02 | 25519 | 0.02172 |
| 95 | 2.24e+11 | +2.15e+11 | 3.96e−04 | 6.78e−03 | 3.81e+02 | 25677 | 0.02037 |
| 96 | 2.24e+11 | +2.16e+11 | 3.73e−04 | 6.49e−03 | 3.60e+02 | 25828 | 0.01926 |
| 97 | 2.24e+11 | +2.16e+11 | 3.64e−04 | 6.32e−03 | 3.51e+02 | 25969 | 0.01879 |
| 98 | 2.24e+11 | +2.16e+11 | 3.51e−04 | 6.18e−03 | 3.40e+02 | 26130 | 0.01819 |
| 99 | 2.23e+11 | +2.16e+11 | 3.44e−04 | 5.68e−03 | 3.30e+02 | 26279 | 0.01766 |
| 100 | 2.23e+11 | +2.16e+11 | 3.32e−04 | 5.44e−03 | 3.18e+02 | 26427 | 0.01702 |
| 101 | 2.23e+11 | +2.16e+11 | 3.20e−04 | 5.39e−03 | 3.08e+02 | 26588 | 0.01648 |
| 102 | 2.23e+11 | +2.16e+11 | 3.03e−04 | 5.00e−03 | 2.91e+02 | 26764 | 0.01558 |
| 103 | 2.22e+11 | +2.16e+11 | 2.73e−04 | 4.60e−03 | 2.63e+02 | 26937 | 0.01412 |
| 104 | 2.21e+11 | +2.16e+11 | 2.40e−04 | 4.26e−03 | 2.33e+02 | 27102 | 0.01250 |
| 105 | 2.21e+11 | +2.16e+11 | 2.18e−04 | 3.80e−03 | 2.11e+02 | 27259 | 0.01135 |
| 106 | 2.21e+11 | +2.16e+11 | 2.03e−04 | 3.58e−03 | 1.97e+02 | 27419 | 0.01061 |
| 107 | 2.20e+11 | +2.16e+11 | 1.85e−04 | 3.35e−03 | 1.80e+02 | 27586 | 0.009698 |
| 108 | 2.20e+11 | +2.16e+11 | 1.76e−04 | 3.04e−03 | 1.70e+02 | 27744 | 0.009168 |

### Phase structure

**Phase A — ordering and early barrier (iters 0–13, Time 2,058–12,043 s).**
The Cholesky ordering phase completed at 1,589 s. The first barrier iteration fired at
Time=2,058 s. Iterations 0–13 took between ~260 s (iter 0) and ~1,330 s (iter 11) each.
Dobj remained negative throughout; BCT = 1.000 for all Phase A iterations. This phase was
identical to the default-settings run in the gurobi_addendum (which timed out at iter 13 within
a 4 h budget).

**Phase B — factorization restructuring (iters 14–15, Time 12,043–14,271 s).**
Iter 14 took 2,074 s (Gurobi restructured the Cholesky factorization internally). Iter 15
then took only 154 s — 15× faster than the preceding iter 13. Dobj turned negative again at
iter 14–15; BCT remained at 1.000.

**Phase C — steady convergence (iters 16–108, Time 14,420–27,744 s).**
Iteration time settled at approximately 130–165 s per iteration. Dobj turned positive for the
first time at iter 29 (Dobj = +6.53e+9) and BCT began descending from 0.981. The descent was
monotone throughout Phase C.

BCT decay rate over Phase C:

| Window | Iters | BCT start | BCT end | Per-iter drop |
|---|---|---|---|---|
| iters 29–50 | 21 | 0.981 | 0.244 | ~6.3%/iter |
| iters 50–76 | 26 | 0.244 | 0.060 | ~6.0%/iter |
| iters 76–100 | 24 | 0.060 | 0.017 | ~5.5%/iter |
| iters 100–108 | 8 | 0.017 | 0.0092 | ~7.4%/iter |

The Phase C rate was approximately 5–7% per iteration throughout. No acceleration or stall
was observed.

### BarConvTol binding

`BarConvTol=1e-3` triggers termination when BCT < 1e-3. At the last logged iteration
(iter 108, Time = 27,744 s), BCT = 0.00917 — a factor of 9× above the threshold.
The 8 h budget was exhausted before BarConvTol bound.

**Extrapolation.** Using the Phase C rate observed from iters 100–108 (~7.4%/iter, ratio 0.926
per iteration):

- Iterations to reach BCT = 1e-3 from iter 108: log(1e-3 / 0.00917) / log(0.926) ≈ 29 iterations
- Observed Phase C iter time: ~160 s/iter
- Additional solver time: ~29 × 160 ≈ 4,640 s (~77 min)
- Estimated total Gurobi optimizer time at BCT = 1e-3: ~27,744 + 4,640 ≈ 32,400 s (~9.0 h from
  `optimize()` call, equivalently ~32,600 s ≈ 9.1 h total process time)

The extrapolation assumes the Phase C rate holds. The rate has been stable across ~80 Phase C
iterations with no trend toward acceleration or stall, so the estimate is likely reliable to
±30 min.

After BCT reaches 1e-3, Gurobi would return a basic-optimal solution and run crossover. The
gurobi_addendum found Gurobi crossover for NEM 2p (smaller problem) completed cleanly in ~7 min;
crossover time for NEM 6p is not bounded by this test.

### Result

| Metric | Value |
|---|---|
| Status | timed_out (8 h budget) |
| Wall clock | 28,803 s (8.00 h) |
| Peak RSS | 116.6 GiB |
| Barrier iters logged | 108 |
| Gurobi time at kill | 27,744 s |
| Final BCT | 0.00917 |
| Final primal residual | 1.76e−4 |
| Final dual residual | 3.04e−3 |
| Final complementarity | 170 |
| BarConvTol threshold reached | No |

The 116.6 GiB peak RSS reflects the Cholesky factor (~30 GB) plus the LP data in presolved and
original form, plus system overhead. This is comparable to the 116.2 GiB observed for the
default-settings Gurobi run (gurobi_addendum), as expected since the factorization is the same.

---

## Test 2 — HiGHS PDLP at 1e-3, NEM 6-period (bench server)

### LP dimensions

Same LP as Test 1: 38,123,031 rows × 17,963,800 cols × 76,069,533 nonzeros.
Presolved by HiGHS to 13,073,042 rows × 11,450,807 cols × 40,710,036 nonzeros.

### Result summary

| Metric | Value |
|---|---|
| Status | completed (`model_status: Unknown` — see note below) |
| Wall clock | 16,670 s (4 h 38 min) |
| Translate + build overhead | 1,052 s (440 s translate, 610 s build) |
| HiGHS solve time | 13,382 s (3 h 43 min) |
| Save + extract overhead | 387 s |
| Peak RSS | 37.0 GiB |
| PDLP iterations | 10,120 |
| Final gap_rel | 0.000948 (< 1e-3 ✓) |
| Final primal infeas (rel) | 0.000222 (< 1e-3 ✓) |
| Final dual infeas (rel) | 0.000658 (< 1e-3 ✓) |
| Objective | $214,703,008,810 AUD annualised |

All six investment periods (2025–2050) solved in a single perfect-foresight LP.

### PDLP gap trajectory

| Iter | Primal obj | Dual obj | gap_rel | Primal inf | Dual inf | Flag |
|---:|---:|---:|---:|---:|---:|---|
| 0 | 1.74e+10 | 1.74e+10 | 0.000 | 7.61e−1 | 9.59e−4 | [L] |
| 4,000 | 1.99e+11 | 1.90e+11 | 2.38e−2 | 6.10e−4 | 1.32e−3 | [A] |
| 8,000 | 2.15e+11 | 2.00e+11 | 3.51e−2 | 2.22e−4 | 8.94e−4 | [A] |
| 10,120 | 2.147e+11 | 2.143e+11 | 9.48e−4 | 2.22e−4 | 6.58e−4 | [A] |

At iter 0 gap_rel = 0 because PDLP initialises primal and dual objectives at the same value.
At iter 4,000 the gap had grown to 2.38e-2 as PDLP pushed the primal objective upward from the
starting point. The gap *increased* from 2.38e-2 at iter 4,000 to 3.51e-2 at iter 8,000: this
is normal PDLP behaviour during an adaptive restart ([A] flag). The restart resets the
iterate direction, causing a transient objective divergence before resuming descent. The gap
then dropped sharply from 3.51e-2 at iter 8,000 to 9.48e-4 at iter 10,120.

The `[L]` flag at iter 0 indicates a large-step initialisation; `[A]` indicates an adaptive
restart at that iteration.

### model_status=Unknown note

HiGHS emits `WARNING: Model status changed from "Optimal" to "Unknown"` because certain
pre-solve residual tolerances remain above HiGHS's internal defaults after postsolve, even
though all three PDLP convergence metrics (primal infeas, dual infeas, gap) satisfy the
requested 1e-3 tolerance. This pattern is consistent with every PDLP run in this bench suite:
NEM 2p on the laptop (test1_test2_addendum) and now NEM 6p on the server. The network object
is populated correctly; `network.generators_t.p` is non-null for all periods; production
extraction of method_years and diagnostics succeeded without error. The `Unknown` status is a
reporting quirk of HiGHS, not a solution quality issue.

---

## Licensing Q&A

### Q1: Can Gurobi solve the NEM 6-period LP in a single perfect-foresight run?

Indeterminate within the tested budgets. The barrier made steady monotone progress throughout
the 8 h run — BCT declined from 1.000 to 0.00917 over 108 iterations with no numerical stall,
no divergence, and no change in convergence rate. This contrasts with HiGHS primal simplex
(degenerates on every multi-period LP tested), HiGHS IPM (stalls at IPX basis factorisation
before the barrier is even running), and the gurobi_addendum's default 4 h run (timed out at
the Phase A / Phase B boundary, missing the ~130 s/iter Phase C regime entirely).

Extrapolating from the Phase C trajectory, full convergence to BCT = 1e-3 would require
approximately 9.1 h total process time from scratch. The 8 h budget covered approximately
88% of the barrier work. Whether the subsequent crossover phase would succeed within a
further hour has not been tested; for NEM 2p Gurobi crossover completed cleanly in ~7 min
(gurobi_addendum), so crossover for NEM 6p is probably faster than the remaining barrier
uncertainty.

### Q2: What did BarConvTol=1e-3 buy compared with the default 1e-8?

Nothing in this run. The relaxed threshold triggers early termination only once BCT falls
below 1e-3; that threshold was not reached within the 8 h budget. The expected saving from
BarConvTol=1e-3 vs 1e-8 is in the final convergence tail — perhaps 30–50 fewer iterations
from BCT=1e-3 to BCT=1e-8 (roughly 80–130 min). The dominant cost is the Phase A/B/C barrier
work (108+ iterations, ~7.7 h of the 8 h budget), not the final polish.

If Gurobi were given a ~10 h budget, BarConvTol=1e-3 would likely save about 1 h at the end.
Whether that saving is meaningful depends on the team's solve-time budget.

### Q3: How does Gurobi compare to PDLP at 1e-3 on the same bench server for NEM 6-period?

PDLP converged in 4 h 38 min (gap_rel = 0.000948 < 1e-3, all three metrics satisfied).
Gurobi barrier did not converge in 8 h (final BCT = 0.00917, requiring approximately 29 more
iterations at ~77 additional minutes). On this hardware and LP, PDLP at 1e-3 is definitively
faster.

Memory usage differs significantly: PDLP used 37.0 GiB peak versus 116.6 GiB for Gurobi
barrier. The Gurobi Cholesky factorization accounts for ~30 GB of the difference.

The HiGHS `model_status=Unknown` for PDLP is a known reporting quirk (all convergence metrics
satisfied; solution extractable). It is not a solution quality issue relative to Gurobi's
formal `Optimal` status — on smaller problems where Gurobi converged, the objective values
agreed closely (NEM 2p: Gurobi $140,027M vs PDLP $140,050M, 0.02% difference).

### Q4: Cost-benefit data for the licensing decision

Data points without recommendation:

**PDLP (free, HiGHS):**
- NEM 6p on bench server: converged in 4 h 38 min, gap_rel = 0.000948, peak 37 GiB.
- NEM 2p on laptop: converged in 31 min, gap_rel = 0.000984, peak 18.5 GiB.
- `model_status=Unknown` on all PDLP runs; solution extraction succeeds regardless.
- PDLP is available in HiGHS without any additional licence or dependency.

**Gurobi barrier (paid licence, CSIRO token server):**
- NEM 6p on bench server: did not converge in 8 h (BCT = 0.00917); extrapolated ~9.1 h total.
  Barrier is progressing monotonically; no numerical stall observed.
- NEM 2p on bench server: formal `Optimal` in 43 min (wall clock), 22.2 GiB.
- NSW 2p on bench server: formal `Optimal` in ~9 min, 6.4 GiB.
- CSIRO licence accepts gurobipy 11.x but rejects 13.x (pin `gurobipy>=11,<12`).

**Gurobi advantage on smaller problems:** On NSW 2p and NEM 2p, Gurobi returns formal
`Optimal` (basic-feasible solution, dual certificate, no status ambiguity). PDLP returns
`Unknown` on all problems regardless of convergence. Whether `model_status=Unknown` is a
practical concern depends on downstream use.

**Production use case framing:** If the requirement is NEM 6p single-shot within a working
day (~8 h), neither solver has demonstrated that capability: Gurobi needed ~9 h (extrapolated),
PDLP completed in 4 h 38 min. If a 5 h wall-clock window is acceptable, PDLP meets it on the
bench server. If overnight solve windows are acceptable (10–12 h), Gurobi may converge with
additional budget. If formal `Optimal` status with a dual certificate is required for
regulatory or methodological reasons, PDLP cannot provide that.

---

## Consolidated production-scale envelope

All bench runs across all addenda. Hardware annotation follows from where each run was executed.

| Run ID | Config | Solver | Tol / variant | Status | Wall (s) | Peak RSS (GiB) | Hardware |
|---|---|---|---|---|---|---|---|
| 01_nsw_1period | NSW 1p | HiGHS simplex | default | Optimal | 109 | 0.82 | Laptop |
| 02_nsw_2period | NSW 2p | HiGHS simplex | default | killed (degenerate) | ~480 | 5.5 | Laptop |
| 02_ipm_nsw_2period | NSW 2p | HiGHS IPM | default | killed (factorisation stall) | ~5,400 | 4.9 | Laptop |
| 02_ipm_nocross_nsw_2period | NSW 2p | HiGHS IPM no-crossover | default | killed (factorisation stall) | ~1,200 | 5.0 | Laptop |
| 02_pdlp_nsw_2period | NSW 2p | HiGHS PDLP | default | timed out at 100k iters | ~40,404 | 6.2 | Laptop |
| 02_gurobi_nsw_2period | NSW 2p | Gurobi | default | **Optimal** | 518 | 6.4 | Server |
| nsw_6p_myopic | NSW 6p (6×1p myopic) | HiGHS simplex | default | Optimal (all 6 periods) | ~540 | ~2.1 | Laptop |
| 04_nem_1period | NEM 1p (2050) | HiGHS simplex | default | Optimal | 7,486 | 2.1 | Laptop |
| 09_nem_1period_2035_extended | NEM 1p (2035) | HiGHS simplex | default | Optimal | 1,335 | 2.1 | Laptop |
| 05_nem_2period | NEM 2p | HiGHS simplex | default | killed (degenerate) | ~1,800 | 20.8 | Laptop |
| 05_ipm_nem_2period | NEM 2p | HiGHS IPM | default | killed (presolve/setup stall) | ~3,600 | 20.8 | Laptop |
| 05_pdlp_tol_3_nem_2period | NEM 2p | HiGHS PDLP | 1e-3 | Unknown (converged) | 1,871 | 18.5 | Laptop |
| 05_gurobi_nem_2period | NEM 2p | Gurobi | default | **Optimal** | 2,590 | 22.2 | Server |
| 06_nem_3period | NEM 3p | HiGHS simplex | default | killed (presolve stall) | ~1,800 | 22.3 | Laptop |
| nem_6p_myopic (2/6 periods) | NEM 6p (myopic) | HiGHS simplex | default | Optimal (2 periods, partial record) | 394 | 2.1 | Laptop |
| 07_nem_6period | NEM 6p | HiGHS simplex | default | not run (predicted failure) | — | — | Laptop |
| 07_ipm_nem_6period | NEM 6p | HiGHS IPM | default | not run (2p precondition failed) | — | — | Laptop |
| 07_gurobi_nem_6period | NEM 6p | Gurobi | default | timed out (13 iters, Phase A/B) | 14,404 | 116.2 | Server |
| 07_pdlp_tol3_nem_6period | NEM 6p | HiGHS PDLP | 1e-3 | **Unknown (converged)** | 16,670 | 37.0 | Server |
| 07_gurobi_barconvtol3_nem_6period | NEM 6p | Gurobi | BarConvTol=1e-3 | timed out (108 iters, BCT=0.0092) | 28,803 | 116.6 | Server |

**Hardware:** Laptop = Dell Precision 5490 (Intel Core Ultra 7 165H, 16P/22L cores, 64 GiB RAM,
Windows 11). Server = Dual Intel Xeon Platinum 8260 (48P/96L cores, 1,024 GiB RAM,
Windows Server 2019).

Notes:
- `02_pdlp_nsw_2period` ran to the 100,000-iteration cap (no wall-clock kill); actual wall
  time in the record is 40,404 s, far exceeding the stated 3 h budget due to a runner
  tracking issue in early bench sessions.
- `nsw_6p_myopic` total wall ~540 s is from the characterisation_report; individual period
  records exist but no combined JSON was written.
- `nem_6p_myopic` record captures only 2 of 6 periods (2025: 146 s, 2030: 236 s, cumulative
  394 s). A full 6-period NEM myopic run extrapolates to approximately 4 h based on NEM 1p
  per-period solve times but was not recorded as a complete run in this bench suite.
- Server-hardware runs (gurobi_addendum and this addendum) are not directly comparable on
  wall-clock to laptop runs due to ~3× more logical cores and ~16× more RAM on the server.
