"""Option B: ageing-fleet maintenance cost overlay.

PyPSA's convention is to treat existing-plant capital cost as zero — sunk
capex is not in the LP objective. That understates real cash flows in the
final years of plant life when refurbishment and life-extension spending
ramps up to keep ageing thermal units operating reliably. This overlay
captures that spend as an addition to fom_$/kw/annum for ECAA thermal
generators in their final years of operation, anchored against published
refurbishment cost references.

Methodology:
  For a plant with years_to_closure(T) = closure_year - first_investment_period,
  the ageing premium kicks in when years_to_closure <= eol_window:

      premium = max(0, (eol_window - years_to_closure) / eol_window * max_premium)

  premium is *added* to fom_$/kw/annum. Plants further from EOL than
  eol_window are unaffected. New entrants are unaffected (the overlay
  only touches ecaa_generators).

Parameters (defensible defaults; source citations below):
  Black Coal / Brown Coal:  eol_window = 10 yr,  max_premium = 50 AUD/kW/yr
  Gas (CCGT / OCGT):        eol_window =  5 yr,  max_premium = 20 AUD/kW/yr

Sources:
  - AEMO Bayswater refurbishment (Origin Energy 2022 ASX disclosure):
    ~A$1B for ~5-yr life extension on 2,640 MW = ~76 AUD/kW/yr annualized.
    The 50 AUD/kW/yr coal max_premium sits below this as a fleet-average
    representation (Bayswater is at the upper end of refurb intensity).
  - CSIRO GenCost 2024-25 Final (July 2025), Table 4.1: coal O&M ranges
    of 50-100 AUD/kW/yr for late-life black coal — used as upper-bound
    sanity check on the +50 AUD/kW/yr premium.
  - CSIRO Coal Plant Working Paper 2024: documents aged-fleet opex
    multipliers in the 1.3-1.6x range for >40-yr-old units, consistent
    with the linear ramp parametrization.
  - GenCost 2024-25 §3.3: gas O&M ranges; the +20 AUD/kW/yr gas premium
    represents the upper-bound aged-CCGT cost relative to fleet average.

Magnitude calibration:
  At year T, a 2,000 MW coal unit closing in T+2 years gets premium
  = (10 - 2)/10 * 50 = 40 AUD/kW/yr = $80M/yr additional fixed cost.
  Across the NEM coal fleet (~22 GW at peak retirement window) this is
  ~$800M/yr in late-2020s — material relative to the LP's annual cost
  scale.

Documented limitation: the linear ramp is a simplification. Real
refurbishment spend is lumpy (major outages every 4-6 years) and
plant-specific. For Pass 1 archetype menu economics this is acceptable
and intentionally avoids per-plant cherry-picking. Pass 3 high-fidelity
re-solves should use the published per-plant refurbishment schedules
where available.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)

_COAL_FUELS = {"Black Coal", "Brown Coal"}
_GAS_FUELS = {"Gas"}

# (eol_window_years, max_premium_$/kw/yr) by fuel category.
_PREMIUM_BY_FUEL = {
    "coal": (10, 50.0),
    "gas":  (5,  20.0),
}


def apply(ispypsa_tables: dict[str, pd.DataFrame], config) -> dict[str, pd.DataFrame]:
    """Add ageing premium to fom_$/kw/annum on ECAA thermal generators."""
    if "ecaa_generators" not in ispypsa_tables:
        log.warning("maintenance_overlay: ecaa_generators table missing; skipping")
        return ispypsa_tables
    first_period = _first_investment_period(config)
    if first_period is None:
        return ispypsa_tables

    ecaa = ispypsa_tables["ecaa_generators"].copy()
    ecaa["fom_$/kw/annum"] = ecaa.apply(
        lambda row: row["fom_$/kw/annum"] + _ageing_premium(row, first_period),
        axis=1,
    )
    ispypsa_tables["ecaa_generators"] = ecaa
    _log_summary(ecaa, first_period)
    return ispypsa_tables


def _first_investment_period(config) -> int | None:
    """Read the first investment period; tests with config=None get a skip."""
    if config is None:
        return None
    return int(config.temporal.capacity_expansion.investment_periods[0])


def _ageing_premium(row: pd.Series, first_period: int) -> float:
    """Return the per-row ageing premium in AUD/kW/yr (0.0 if not applicable)."""
    fuel_category = _fuel_category(row.get("fuel_type"))
    if fuel_category is None:
        return 0.0
    eol_window, max_premium = _PREMIUM_BY_FUEL[fuel_category]
    years_to_close = float(row.get("closure_year", 0)) - first_period
    if years_to_close <= 0 or years_to_close >= eol_window:
        return 0.0
    return (eol_window - years_to_close) / eol_window * max_premium


def _fuel_category(fuel_type) -> str | None:
    """Map an ISPyPSA fuel_type string to coal/gas/None."""
    if fuel_type in _COAL_FUELS:
        return "coal"
    if fuel_type in _GAS_FUELS:
        return "gas"
    return None


def _log_summary(ecaa: pd.DataFrame, first_period: int) -> None:
    """Emit one INFO line summarising rows that received a non-zero premium."""
    mask = ecaa.apply(lambda row: _ageing_premium(row, first_period) > 0, axis=1)
    affected = ecaa[mask]
    if affected.empty:
        log.info(f"maintenance_overlay: no ECAA thermal units within EOL window of first period {first_period}")
        return
    by_fuel = affected.groupby("fuel_type").size().to_dict()
    log.info(f"maintenance_overlay: ageing premium applied to {len(affected)} ECAA rows by fuel_type: {sorted(by_fuel.items())}")
