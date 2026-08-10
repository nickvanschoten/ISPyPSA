"""Phase 8.x: correct the new-entrant biomass feedstock cost to a
scale-appropriate, sourced delivered cost.

Why this exists
---------------
The cap fix (`_biomass_cap.py`) bounded biomass *capacity* (9.6 -> 5 GW),
but the c550 dispatch diagnosis showed the 5 GW fleet still runs at ~77 %
capacity factor (34 TWh) as cheap firm baseload, beating firmed VRE + LDES
in ~88 % of hours while ~28 % of VRE is curtailed. The investment
decomposition showed the model builds new biomass to the cap in *every*
milestone period (1 GW of fresh new-build in both 2045 and 2050), and the
CAPEX check confirmed biomass is the *most* expensive firm option per kW
($8,825/kW, faithful to GenCost/IASR) — so the over-build is driven by the
*running* economics, not capital cost.

The running-economics defect is the feedstock. The IASR biomass fuel price
is $0.661895/GJ, flat across every year and region. That value is *faithful*
— GenCost's own biomass fuel cost is ~$0.5-2/GJ, so $0.66/GJ is a correct
low-end price. The error is that this cheap price is applied to an
*unlimited* resource: at 5 GW / 34 TWh the fleet burns ~594 PJ/yr of
feedstock, far beyond Australia's cheap residue/byproduct availability
(~5-15 TWh electricity ~ 90-260 PJ per the ARENA/AEMO sources behind the
capacity cap). On IRENA's delivered-feedstock tiers, $0.66/GJ sits in the
waste / processing-residue band ($0-4/GJ) — genuinely cheap but limited.
A multi-GW fleet is entirely past that band, so its marginal feedstock is
locally-collected agricultural / forestry residue (transported), which
IRENA prices at $4-8/GJ, shading toward dedicated energy crops ($8-12/GJ).

The fix
-------
Re-price biomass feedstock to the beyond-residue delivered cost: the
midpoint of IRENA's locally-collected tier, **$6.0/GJ**. This is a
deliberate, sourced deviation from the IASR value — not "correcting
GenCost" (whose $0.66/GJ is right for residue) but "pricing the scaled
fleet at the feedstock it actually consumes once residue is exhausted."
It makes biomass's capacity factor an *economic output* (the LP runs it
only when it is genuinely marginal-cheapest) rather than a clamp.

At c550 (2050) this moves biomass marginal cost from
    0.66 x 17.535 (fuel) + 17.4 (carbon on CH4+N2O residual) = ~$29/MWh
to
    6.0  x 17.535 (fuel) + 17.4 (carbon)                     = ~$122/MWh
i.e. from below CCS gas ($136) to roughly level with it — so biomass falls
from cheap firm baseload to a genuine deep-residual role.

Source
------
IRENA (2012), *Renewable Energy Cost Analysis: Biomass for Power
Generation*, delivered feedstock cost tiers ($/GJ):
    waste / onsite byproduct (bagasse, black liquor)  ~0
    processing residues                               0-4   <- $0.66/GJ tier
    locally collected feedstock (transported)         4-8   <- fix uses 6.0
    dedicated energy crops                            8-12
    internationally traded pellets                    ~8-16

Sanity check (NOT a tuning target): an independent back-out — what marginal
cost lands 5 GW of biomass at the AEMO/industry ~5-15 TWh range on this
run's price-duration curve — gives ~$4-7/GJ implied feedstock. $6.0/GJ sits
mid-range of that back-out, which is the reassurance the sourced value is
plausible; the value is chosen from IRENA's tier, not fitted to the range.

Caveats / Pass-1 limitations
----------------------------
  1. The `biomass_prices` table is a single row shared by all biomass
     generators (no per-generator fuel_cost_mapping_col for the Biomass
     carrier), so this also re-prices the 32 MW existing Tully sugar-mill
     bagasse unit — which genuinely is a ~$0/GJ byproduct. That mis-pricing
     is negligible (32 MW vs ~5 GW new-entrant, ~0.1 % of biomass energy).
     Preserving Tully would require adding a fuel_cost_mapping_col to the
     Biomass carrier (an upstream src/ispypsa change) — not worth it here.
  2. A single flat price loses the "cheap residue runs first, then scaled
     feedstock" shape. The rigorous version is a feedstock supply curve
     (residue tier limited + rising scaled tier), which needs a
     custom-constraints weighted-energy-sum extension + a post-injection
     carried-membership step (~2-4 days, 5-7 files). That is the confirmed
     Pass-3 refinement (Option 2); this flat price is the conservative
     Pass-1 approximation (it slightly over-prices the small genuine-residue
     slice, biasing biomass *down* — the safe direction for a menu).
  3. New-entrant VOM is empty for *all* thermal new entrants (gas and
     biomass alike) — a systematic templater gap, not biomass-specific.
     Adding VOM to biomass only would tilt the biomass-vs-gas merit order
     toward gas (distorting the very competition this fix addresses), and
     among thermal it nets out. It is therefore *not* patched here; a
     consistent all-thermal VOM fix is a separate follow-up. VOM (~$5-12)
     is second-order next to the ~$93/MWh feedstock correction.

Applied as a pre-pass on every archetype via the registry's
`_with_pre_passes` wrapper, alongside the biomass capacity cap, so all
archetypes see consistent biomass availability *and* cost economics.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)


# IRENA (2012) locally-collected-feedstock tier ($4-8/GJ), midpoint. See
# module docstring for full sourcing and the back-out sanity check.
_SCALED_BIOMASS_FEEDSTOCK_COST_GJ = 6.0


def apply(ispypsa_tables: dict[str, pd.DataFrame], config) -> dict[str, pd.DataFrame]:
    """Re-price biomass feedstock to the scaled beyond-residue delivered cost."""
    biomass_prices = ispypsa_tables["biomass_prices"]
    price_columns = _fuel_price_columns(biomass_prices)
    ispypsa_tables["biomass_prices"] = _set_feedstock_cost(
        biomass_prices, price_columns, _SCALED_BIOMASS_FEEDSTOCK_COST_GJ
    )
    log.info(
        f"Biomass feedstock re-priced to {_SCALED_BIOMASS_FEEDSTOCK_COST_GJ} $/GJ "
        f"(IRENA locally-collected tier) across {len(price_columns)} financial years"
    )
    return ispypsa_tables


def _fuel_price_columns(biomass_prices: pd.DataFrame) -> list[str]:
    """Financial-year price columns in the biomass price table (units $/GJ)."""
    return [col for col in biomass_prices.columns if "$/gj" in col]


def _set_feedstock_cost(
    biomass_prices: pd.DataFrame, price_columns: list[str], cost_gj: float
) -> pd.DataFrame:
    """Overwrite every financial-year price with the scaled feedstock cost."""
    repriced = biomass_prices.copy()
    repriced[price_columns] = cost_gj
    return repriced
