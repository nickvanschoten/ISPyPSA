"""Pumped storage re-modelling fix — applied to ALL archetypes.

Default ISPyPSA / IASR routing:
  - "Pumped Hydro" technology_type rows are split into the storage path by
    src/ispypsa/templater/static_ecaa_generator_properties.py:59-89, but the
    battery-only filter in src/ispypsa/templater/storage.py:36-47 then drops
    them entirely. Borumba is re-injected as a Generator workaround for the
    SWQLD1 REZ group constraint.
  - "Hydro" technology_type pumped storage (Wivenhoe, Shoalhaven) stay in the
    generators path and are dispatched as fully-available Water generators.
  - Snowy 2.0 disappears from the model entirely.

This module mutates ispypsa_tables to re-route the four NEM pumped storage
facilities as PyPSA StorageUnits (the natural fit: power capacity + energy
capacity + round-trip efficiency + state-of-charge dynamics).

Sources for parameters:
  - Wivenhoe (570 MW): CS Energy facility profile; AEMO Generation Information
    NEM. Energy capacity 5.1 GWh ≈ 9 h at full power. RTE ~76% (industry
    average for large reversible-Francis pumped hydro).
  - Shoalhaven (247 MW): Origin Energy facility profile; AEMO Generation
    Information NEM. Energy capacity ~1.5 GWh ≈ 6 h at full power. RTE ~76%.
  - Borumba (1998 MW): Queensland Hydro project briefs; AEMO IASR 2024 v6.0
    anticipated_projects_summary (commissioning 2031-09-01). Energy capacity
    48 GWh ≈ 24 h at full power. RTE ~78%.
  - Snowy 2.0 (2200 MW): Snowy Hydro project documentation; AEMO IASR 2024
    v6.0 committed_generators_summary (commissioning 2028-12-01). Energy
    capacity 350 GWh ≈ 159 h at full power. RTE ~76%.

PyPSA's StorageUnit takes per-direction efficiencies; charge_eff = discharge_eff
= sqrt(RTE) preserves the specified round-trip efficiency under nominal
modelling.
"""

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


_PUMPED_STORAGE_NAMES = ["Wivenhoe", "Shoalhaven", "Borumba", "Snowy 2.0"]


# Per-facility static parameters. Sub-regions and REZ assignments match IASR
# 2024 v6.0 cached summary tables. closure_year values match IASR
# expected_closure_years.csv. commissioning_date matches IASR maximum_capacity
# tables; existing facilities are given a pre-2025 placeholder so the translator
# treats them as available from the first investment period.
_PUMPED_STORAGE_SPECS = [
    {
        "storage_name": "Wivenhoe",
        "sub_region_id": "SQ",
        "region_id": "QLD",
        "rez_id": np.nan,
        "maximum_capacity_mw": 570.0,
        "storage_duration_hours": 9.0,
        "round_trip_efficiency_%": 76.0,
        "status": "Existing",
        "commissioning_date": "1984-01-01",
        "closure_year": 2084,
    },
    {
        "storage_name": "Shoalhaven",
        "sub_region_id": "CNSW",
        "region_id": "NSW",
        "rez_id": np.nan,
        "maximum_capacity_mw": 247.0,
        "storage_duration_hours": 6.0,
        "round_trip_efficiency_%": 76.0,
        "status": "Existing",
        "commissioning_date": "1977-01-01",
        "closure_year": 2069,
    },
    {
        "storage_name": "Borumba",
        "sub_region_id": "SQ",
        "region_id": "QLD",
        "rez_id": np.nan,
        "maximum_capacity_mw": 1998.0,
        "storage_duration_hours": 24.0,
        "round_trip_efficiency_%": 78.0,
        "status": "Anticipated",
        "commissioning_date": "2031-09-01",
        "closure_year": 2130,
    },
    {
        "storage_name": "Snowy 2.0",
        "sub_region_id": "SNSW",
        "region_id": "NSW",
        "rez_id": np.nan,
        "maximum_capacity_mw": 2200.0,
        "storage_duration_hours": 159.0,
        "round_trip_efficiency_%": 76.0,
        "status": "Committed",
        "commissioning_date": "2028-12-01",
        "closure_year": 2129,
    },
]


def apply(ispypsa_tables: dict, config=None) -> dict:
    """Remove pumped storage from generators; add as StorageUnit-bound batteries.

    Also drops Borumba's row from custom_constraints_lhs SWQLD1 (term_type
    'generator_output'). Reason: filter_template._filter_custom_constraints
    drops a constraint entirely if ANY term_id is missing from the selected
    generators set. Borumba is no longer a Generator after this fix, so its
    LHS row would invalidate SWQLD1 wholesale. Dropping just Borumba's row
    keeps SWQLD1 alive with its remaining terms (Tarong, Kogan Creek, etc.);
    the lost 0.5×Borumba_output term slightly relaxes the SWQLD1 transmission
    group limit in 2031+ periods. This is a known methodological caveat for
    the MVP Pass 1 deliverable."""
    available_sub_regions = _existing_sub_regions(ispypsa_tables)
    ispypsa_tables["ecaa_generators"] = _drop_pumped_storage_from_generators(
        ispypsa_tables.get("ecaa_generators")
    )
    ispypsa_tables["ecaa_batteries"] = _append_pumped_storage_to_batteries(
        ispypsa_tables.get("ecaa_batteries"), available_sub_regions
    )
    ispypsa_tables["custom_constraints_lhs"] = _drop_pumped_storage_from_constraints_lhs(
        ispypsa_tables.get("custom_constraints_lhs")
    )
    return ispypsa_tables


def _existing_sub_regions(ispypsa_tables: dict) -> set[str]:
    """Collect the sub_region_id set from whichever input tables are populated.

    Used to skip pumped-storage rows whose sub-region was filtered out upstream
    (e.g. filter_by_nem_regions=['NSW'] drops SQ → Wivenhoe and Borumba should
    not be re-added, otherwise PyPSA warns 'bus not defined')."""
    regions: set[str] = set()
    for table_name in ("ecaa_generators", "ecaa_batteries", "new_entrant_generators"):
        df = ispypsa_tables.get(table_name)
        if df is not None and not df.empty and "sub_region_id" in df.columns:
            regions.update(df["sub_region_id"].dropna().unique())
    return regions


def _drop_pumped_storage_from_generators(generators: pd.DataFrame | None) -> pd.DataFrame:
    """Remove the four pumped-storage facilities from ecaa_generators."""
    if generators is None or generators.empty:
        return generators if generators is not None else pd.DataFrame()
    name_col = _generator_name_column(generators)
    mask = generators[name_col].isin(_PUMPED_STORAGE_NAMES)
    removed = sorted(generators.loc[mask, name_col].unique().tolist())
    if removed:
        log.info(f"pumped_storage_fix: removed from ecaa_generators: {removed}")
    return generators.loc[~mask, :].reset_index(drop=True)


def _append_pumped_storage_to_batteries(
    batteries: pd.DataFrame | None, available_sub_regions: set[str]
) -> pd.DataFrame:
    """Append pumped-storage rows to ecaa_batteries, skipping rows outside the
    sub-regions present in the (already-filtered) input tables."""
    specs_in_scope = [
        spec for spec in _PUMPED_STORAGE_SPECS
        if spec["sub_region_id"] in available_sub_regions
    ]
    added_names = sorted([spec["storage_name"] for spec in specs_in_scope])
    log.info(f"pumped_storage_fix: added to ecaa_batteries: {added_names}")
    if not specs_in_scope:
        return batteries if batteries is not None else pd.DataFrame()
    new_rows = pd.DataFrame([_make_battery_row(spec) for spec in specs_in_scope])
    if batteries is None or batteries.empty:
        return new_rows
    return pd.concat([batteries, new_rows], axis=0, ignore_index=True, sort=False)


def _make_battery_row(spec: dict) -> dict:
    """Build one ecaa_batteries row from a pumped-storage spec."""
    charge_eff = 100.0 * (spec["round_trip_efficiency_%"] / 100.0) ** 0.5
    return {
        "storage_name": spec["storage_name"],
        "isp_resource_type": "Pumped Hydro",
        "technology_type": "Pumped Hydro",
        "status": spec["status"],
        "region_id": spec["region_id"],
        "sub_region_id": spec["sub_region_id"],
        "rez_id": spec["rez_id"],
        "fuel_type": "Water",
        "fom_$/kw/annum": 0.0,
        "maximum_capacity_mw": spec["maximum_capacity_mw"],
        "storage_duration_hours": spec["storage_duration_hours"],
        "commissioning_date": spec["commissioning_date"],
        "closure_year": spec["closure_year"],
        "lifetime": np.nan,
        "round_trip_efficiency_%": spec["round_trip_efficiency_%"],
        "charging_efficiency_%": charge_eff,
        "discharging_efficiency_%": charge_eff,
    }


def _generator_name_column(generators: pd.DataFrame) -> str:
    """Return whichever name column ecaa_generators is using ('generator' or 'name')."""
    for candidate in ("generator", "name"):
        if candidate in generators.columns:
            return candidate
    raise KeyError(
        f"ecaa_generators has no 'generator' or 'name' column; got {list(generators.columns)}"
    )


def _drop_pumped_storage_from_constraints_lhs(
    lhs: pd.DataFrame | None,
) -> pd.DataFrame | None:
    """Drop any custom_constraints_lhs rows that reference pumped-storage names
    via a 'generator_output' term. Without this, filter_template silently drops
    the entire affected constraint."""
    if lhs is None or lhs.empty:
        return lhs
    bad_mask = (lhs["term_type"] == "generator_output") & lhs["term_id"].isin(
        _PUMPED_STORAGE_NAMES
    )
    dropped = lhs.loc[bad_mask, ["constraint_id", "term_id"]].apply(tuple, axis=1)
    if not dropped.empty:
        log.info(
            f"pumped_storage_fix: dropped custom_constraints_lhs rows: "
            f"{sorted(dropped.unique().tolist())}"
        )
    return lhs.loc[~bad_mask, :].reset_index(drop=True)
