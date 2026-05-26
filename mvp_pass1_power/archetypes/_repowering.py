"""EOL renewable repowering — Pass 1 simplified treatment.

When wind and solar assets reach end-of-life in the model, repowering is
available as a defensible alternative to forcing greenfield rebuilds:
the asset stays in place (existing grid connection, civil works, access
roads, and consenting are preserved) at materially reduced capital cost
per MW retained.

Pass 1 simplified implementation:
  For each ECAA wind / solar generator:
    1. Extend closure_year by `_LIFE_EXTENSION_YEARS` = 20 (representing one
       repowering cycle on typical 20-25 year original turbine life).
    2. Add a persistent annualised repowering premium to fom_$/kw/annum,
       computed as repowering_capex / (original_life + life_extension).

This keeps the asset in the LP rather than forcing it offline at the
original closure year — the team has observed a 2045 wind capacity dip in
prior production runs as a cohort of mid-2040s-closing wind farms drop
out faster than new entrants build to replace them.

Per-fuel parameters (defensible defaults; sources below):
  Wind:  repowering_capex = 1,000 AUD/kW (≈ 50 % of greenfield ~2,000 AUD/kW)
  Solar:                =   800 AUD/kW (≈ 50 % of greenfield ~1,400 AUD/kW)
  Both: life_extension   =    20 yr

Cited sources:
  - CSIRO GenCost 2024-25 Final (July 2025), §3.5 — greenfield CapEx for
    onshore wind (~2,000 AUD/kW) and large-scale solar PV (~1,400 AUD/kW).
  - IRENA Renewable Power Generation Costs 2023 — global repowering CapEx
    range typically 40-60 % of greenfield (existing site infrastructure
    preserved).
  - CSIRO renewable energy work (project EnergyConnect / RACI 2023) —
    typical wind repowering life-extension ~20 yr at modern turbine
    standards.

Documented Pass-1 limitations (the methodology calls for both cost AND
performance modelling; only the cost side is implemented here):

  1. **No capacity-factor uplift.** Modern (2025+) wind turbines deliver
     2-3× the CF of 2015-vintage units; solar PV CF rises ~10-15 % with
     bifacial / tracking upgrades. ISPyPSA p_max_pu traces are sourced
     per the IASR vintage and cannot be modulated per-asset without
     trace modification — out of scope for the MVP.
  2. **Not an LP investment decision.** The simplified treatment makes
     repowering effectively a fleet-wide default rather than an
     LP-chosen option. Pass 3 high-fidelity re-solves should inject
     "repowered" rows into new_entrant_generators tied to each ECAA
     wind / solar site, so the LP picks repowering vs greenfield as
     a true decision variable.
  3. **Single repowering cycle.** Real repowering can repeat (2025 →
     2045 → 2065 turbines). Phase 1 limits each asset to one cycle.

Applied as a pre-pass on every archetype (consistent baseline VRE
economics across the catalogue).
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)

_VRE_FUELS = {"Wind", "Solar"}
_LIFE_EXTENSION_YEARS = 20

# (greenfield_capex_aud_per_kw, repowering_fraction) by fuel category.
_REPOWERING_CAPEX_AUD_PER_KW = {
    "Wind":  1_000.0,
    "Solar":   800.0,
}


def apply(ispypsa_tables: dict[str, pd.DataFrame], config) -> dict[str, pd.DataFrame]:
    """Extend VRE closure_year and add repowering-capex uplift to fom_$/kw/annum."""
    if "ecaa_generators" not in ispypsa_tables:
        log.warning("repowering: ecaa_generators table missing; skipping")
        return ispypsa_tables
    first_period = _first_investment_period(config)
    if first_period is None:
        return ispypsa_tables

    ecaa = ispypsa_tables["ecaa_generators"].copy()
    vre_mask = ecaa["fuel_type"].isin(_VRE_FUELS)
    ecaa.loc[vre_mask, "fom_$/kw/annum"] = ecaa.loc[vre_mask].apply(
        lambda row: row["fom_$/kw/annum"] + _repowering_premium(row, first_period),
        axis=1,
    )
    ecaa.loc[vre_mask, "closure_year"] = ecaa.loc[vre_mask, "closure_year"] + _LIFE_EXTENSION_YEARS
    ispypsa_tables["ecaa_generators"] = ecaa
    _log_summary(ecaa, vre_mask)
    return ispypsa_tables


def _first_investment_period(config) -> int | None:
    """Read the first investment period; tests with config=None get a skip."""
    if config is None:
        return None
    return int(config.temporal.capacity_expansion.investment_periods[0])


def _repowering_premium(row: pd.Series, first_period: int) -> float:
    """Per-row annualised repowering capex uplift (AUD/kW/yr).

    Premium = repowering_capex / (years_to_original_closure + life_extension).
    """
    fuel = row.get("fuel_type")
    if fuel not in _REPOWERING_CAPEX_AUD_PER_KW:
        return 0.0
    years_to_close = float(row.get("closure_year", 0)) - first_period
    if years_to_close <= 0:
        return 0.0
    total_remaining = years_to_close + _LIFE_EXTENSION_YEARS
    return _REPOWERING_CAPEX_AUD_PER_KW[fuel] / total_remaining


def _log_summary(ecaa: pd.DataFrame, vre_mask: pd.Series) -> None:
    """Emit one INFO line summarising the rows that received the repowering uplift."""
    affected = ecaa[vre_mask]
    if affected.empty:
        log.info("repowering: no VRE ECAA rows present; overlay no-op")
        return
    by_fuel = affected.groupby("fuel_type").size().to_dict()
    log.info(f"repowering: extended closure_year by {_LIFE_EXTENSION_YEARS}y and added annualised premium for {len(affected)} VRE rows: {sorted(by_fuel.items())}")
