import logging

import pandas as pd

_FUEL_SUPPLY_CURVE_COLUMNS = ["tranche", "financial_year", "cap_pj", "adder_$/gj"]


def _translate_fuel_supply_curve(
    curve_csv: str, investment_periods: list[int], fuel: str
) -> pd.DataFrame:
    """Filters a fuel supply curve definition CSV to the modelled investment periods.

    The CSV defines annual price-quantity tranches for one fuel consumed by that
    fuel's generators, with columns 'tranche' (str label), 'financial_year' (int,
    financial year ending, matching investment period labels), 'cap_pj' (float PJ
    per year, blank for an uncapped tranche) and 'adder_$/gj' (float, real AUD/GJ
    premium above the IASR baseline fuel prices already embedded in generator
    marginal costs).

    Args:
        curve_csv: path to the fuel supply curve definition CSV.
        investment_periods: list of years in which investment periods start.
        fuel: the fuel the curve prices, e.g. 'Gas' or 'Biomass', used to name
            the curve in logs and validation errors.

    Returns: `pd.DataFrame` with columns 'investment_period', 'tranche', 'cap_pj'
        and 'adder_$/gj', one row per tranche per investment period.
    """
    logging.info(f"Creating {fuel.lower()} supply curve inputs")
    curve = pd.read_csv(curve_csv)
    _validate_curve_columns(curve, curve_csv, fuel)
    curve = curve[curve["financial_year"].isin(investment_periods)]
    curve = curve.rename(columns={"financial_year": "investment_period"})
    _validate_period_coverage(curve, investment_periods, curve_csv, fuel)
    _validate_uncapped_backstop_tranches(curve, curve_csv, fuel)
    columns = ["investment_period", "tranche", "cap_pj", "adder_$/gj"]
    return curve[columns].reset_index(drop=True)


def _validate_curve_columns(curve: pd.DataFrame, curve_csv: str, fuel: str) -> None:
    missing = [c for c in _FUEL_SUPPLY_CURVE_COLUMNS if c not in curve.columns]
    if missing:
        raise ValueError(
            f"{fuel} supply curve CSV ({curve_csv}) is missing columns: "
            f"{sorted(missing)}"
        )


def _validate_period_coverage(
    curve: pd.DataFrame, investment_periods: list[int], curve_csv: str, fuel: str
) -> None:
    covered = set(curve["investment_period"])
    missing = [year for year in investment_periods if year not in covered]
    if missing:
        raise ValueError(
            f"{fuel} supply curve CSV ({curve_csv}) has no tranche rows for "
            f"investment periods: {sorted(missing)}"
        )


def _validate_uncapped_backstop_tranches(
    curve: pd.DataFrame, curve_csv: str, fuel: str
) -> None:
    # Without an uncapped tranche the curve is a hard national fuel cap, which can
    # make the LP infeasible; scarcity pricing should come from a high-adder
    # uncapped backstop tranche instead.
    capped_only = curve.groupby("investment_period")["cap_pj"].count()
    tranches = curve.groupby("investment_period")["cap_pj"].size()
    fully_capped = sorted(capped_only[capped_only == tranches].index)
    if fully_capped:
        raise ValueError(
            f"{fuel} supply curve CSV ({curve_csv}) needs an uncapped backstop "
            f"tranche (blank cap_pj) in every investment period, missing for: "
            f"{fully_capped}"
        )
