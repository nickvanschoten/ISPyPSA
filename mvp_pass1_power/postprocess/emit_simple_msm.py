"""Emit simple-msm-shaped CSVs from one or more solved ISPyPSA archetype runs.

Usage:
    uv run python -m mvp_pass1_power.postprocess.emit_simple_msm \
        --runs-dir mvp_pass1_power/runs \
        --workbook-cache mvp_pass1_power/data/workbook_cache \
        --out mvp_pass1_power/outputs/simple_msm

Produces:
    methods.csv        — one row per archetype
    method_years.csv   — one row per (archetype, milestone year)
    nger_factor_table.csv — provenance of the emission cross-walk
    diagnostics.csv    — bundled vs decoupled cost, fuel cost share, demand
"""

import argparse
import csv
import json
import logging
from pathlib import Path

import pandas as pd
import pypsa

from .extract_method_years import extract_method_year_row
from .nger_factors import nger_factor_table

log = logging.getLogger(__name__)

# Archetype catalogue: archetype_id -> simple-msm method metadata and bounds.
# Bounds are the author-supplied max_share/min_share/max_activity values that
# Pass 2 will see; they are NOT derived from ISPyPSA outputs. The numbers below
# are illustrative MVP values aligned to the structural intent of each archetype.
ARCHETYPE_CATALOGUE = {
    "cost_optimal": {
        "method_id": "electricity__grid_supply__cost_optimal_baseline",
        "short_name": "Cost-optimal Step Change",
        "description": "Unconstrained least-cost expansion under AEMO 2024 IASR Step Change.",
        "default_max_share": 1.0,
        "default_min_share": 0.0,
        "default_max_activity": None,
    },
    "renewables_led": {
        "method_id": "electricity__grid_supply__renewables_led",
        "short_name": "Renewables-led",
        "description": "≥80% renewable share by 2040, ≥95% by 2050; no new coal.",
        "default_max_share": 1.0,
        "default_min_share": 0.0,
        "default_max_activity": None,
    },
    "fossil_incumbent": {
        "method_id": "electricity__grid_supply__fossil_incumbent",
        "short_name": "Fossil-incumbent",
        "description": "Capped wind+solar build; existing thermal lifetime-extended where allowed.",
        "default_max_share": 1.0,
        "default_min_share": 0.0,
        "default_max_activity": None,
    },
    "deep_clean_firmed": {
        "method_id": "electricity__grid_supply__deep_clean_firmed",
        "short_name": "Deep-clean firmed",
        "description": "All coal retired by 2040; ≥90% low-carbon by 2050; no new unabated thermal.",
        "default_max_share": 1.0,
        "default_min_share": 0.0,
        "default_max_activity": None,
    },
}


def _find_archetype_runs(runs_dir: Path) -> dict[str, Path]:
    """Map archetype_id -> run directory by scanning runs/.
    Only includes runs that have a solved capacity_expansion.nc."""
    runs = {}
    for d in runs_dir.iterdir():
        if not d.is_dir():
            continue
        for arch_id in ARCHETYPE_CATALOGUE:
            if d.name.endswith(f"__{arch_id}"):
                if (d / "outputs" / "capacity_expansion.nc").exists():
                    runs[arch_id] = d
    return runs


def _load_solved_network(run_dir: Path) -> pypsa.Network:
    """Read the saved capacity_expansion.nc from a run directory."""
    nc_path = run_dir / "outputs" / "capacity_expansion.nc"
    if not nc_path.exists():
        raise FileNotFoundError(f"No solved network at {nc_path}")
    return pypsa.Network(nc_path)


def _emit_methods_csv(out_dir: Path) -> Path:
    """Emit one row per archetype to methods.csv."""
    rows = [
        {
            "method_id": v["method_id"],
            "short_name": v["short_name"],
            "description": v["description"],
            "role": "supply_grid_electricity",
            "representation": "supply_grid_electricity__pathway_bundle",
        }
        for v in ARCHETYPE_CATALOGUE.values()
    ]
    df = pd.DataFrame(rows)
    path = out_dir / "methods.csv"
    df.to_csv(path, index=False)
    return path


def _emit_method_years_csv(
    rows: list[dict], out_dir: Path
) -> Path:
    """Emit one row per (method, year) to method_years.csv.

    JSON-serialise list/dict cells so the CSV stays parseable downstream."""
    df = pd.DataFrame(rows)
    for col in ("input_commodities", "input_coefficients", "input_units",
                "energy_emissions_by_pollutant", "process_emissions_by_pollutant"):
        if col in df.columns:
            df[col] = df[col].apply(json.dumps)
    path = out_dir / "method_years.csv"
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    return path


def _emit_diagnostics_csv(rows: list[dict], out_dir: Path) -> Path:
    """Emit decoupling diagnostics — the part the team most needs to inspect."""
    diag = pd.DataFrame([
        {
            "method_id": r["method_id"],
            "year": r["year"],
            "output_cost_per_unit_excl_fuel_AUD_per_MWh": r["output_cost_per_unit"],
            "diagnostic_bundled_cost_AUD_per_MWh": r["diagnostic_bundled_cost_per_unit"],
            "diagnostic_fuel_cost_AUD_per_MWh": r["diagnostic_fuel_cost_per_unit"],
            "fuel_share_of_total_cost": (
                r["diagnostic_fuel_cost_per_unit"] / r["diagnostic_bundled_cost_per_unit"]
                if r["diagnostic_bundled_cost_per_unit"] else float("nan")
            ),
            "annual_mwh_delivered": r["diagnostic_annual_mwh_delivered"],
            "co2e_tonnes_per_MWh": (
                r["energy_emissions_by_pollutant"]["total_CO2e"]
                if isinstance(r["energy_emissions_by_pollutant"], dict)
                else float("nan")
            ),
        }
        for r in rows
    ])
    path = out_dir / "diagnostics.csv"
    diag.to_csv(path, index=False)
    return path


def _emit_provenance_csv(out_dir: Path) -> Path:
    """Emit the NGER factor cross-walk as a provenance artefact."""
    path = out_dir / "nger_factor_table.csv"
    nger_factor_table().to_csv(path, index=False)
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs-dir", required=True, type=Path)
    parser.add_argument("--workbook-cache", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args.out.mkdir(parents=True, exist_ok=True)

    runs = _find_archetype_runs(args.runs_dir)
    log.info(f"Found archetype runs: {list(runs)}")

    all_rows: list[dict] = []
    for arch_id, run_dir in runs.items():
        catalog = ARCHETYPE_CATALOGUE[arch_id]
        bounds = dict(
            max_share=catalog["default_max_share"],
            min_share=catalog["default_min_share"],
            max_activity=catalog["default_max_activity"],
            availability_conditions="national_frontier",
        )
        network = _load_solved_network(run_dir)
        pypsa_friendly = run_dir / "pypsa_friendly"
        for period in network.investment_periods:
            row = extract_method_year_row(
                network, pypsa_friendly, args.workbook_cache,
                int(period), arch_id, bounds,
            )
            row["method_id"] = catalog["method_id"]
            all_rows.append(row)
            log.info(f"  {arch_id} {int(period)}: "
                     f"{row['output_cost_per_unit']:.2f} AUD/MWh, "
                     f"{row['energy_emissions_by_pollutant']['total_CO2e']:.3f} tCO2e/MWh")

    methods_path = _emit_methods_csv(args.out)
    method_years_path = _emit_method_years_csv(all_rows, args.out)
    diagnostics_path = _emit_diagnostics_csv(all_rows, args.out)
    provenance_path = _emit_provenance_csv(args.out)

    log.info(f"Wrote: {methods_path}")
    log.info(f"Wrote: {method_years_path}")
    log.info(f"Wrote: {diagnostics_path}")
    log.info(f"Wrote: {provenance_path}")


if __name__ == "__main__":
    main()
