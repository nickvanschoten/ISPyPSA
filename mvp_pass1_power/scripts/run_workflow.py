"""Run an ISPyPSA workflow end-to-end for a single archetype.

Usage:
    uv run python mvp_pass1_power/scripts/run_workflow.py \\
        --config mvp_pass1_power/configs/baseline.yaml \\
        --archetype cost_optimal

The archetype name controls a post-templater mutation pass that edits ISPyPSA
input CSVs (and/or custom_constraints_rhs.csv) to enforce archetype-defining
constraints before translation to PyPSA-friendly tables.

Archetypes are defined in mvp_pass1_power/archetypes/<archetype>.py and exposed
through the archetype registry.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure mvp_pass1_power is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.iasr_table_caching import build_local_cache
from ispypsa.logging import configure_logging
from ispypsa.pypsa_build import build_pypsa_network, save_pypsa_network
from ispypsa.results import (
    extract_regions_and_zones_mapping,
    extract_tabular_results,
)
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeseries_inputs,
)

from mvp_pass1_power.archetypes import APPLY_ARCHETYPE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--archetype", required=True,
                        help="Archetype id from mvp_pass1_power.archetypes")
    args = parser.parse_args()

    configure_logging()
    log = logging.getLogger(__name__)

    config_path = Path(args.config)
    config = load_config(config_path)

    # Override the run name with archetype so different archetypes write to
    # different output folders.
    archetype_run_name = f"{config.paths.ispypsa_run_name}__{args.archetype}"
    run_root = Path(config.paths.run_directory) / archetype_run_name
    ispypsa_inputs_dir = run_root / "ispypsa_inputs"
    pypsa_inputs_dir = run_root / "pypsa_friendly"
    ce_ts_dir = pypsa_inputs_dir / "capacity_expansion_timeseries"
    outputs_dir = run_root / "outputs"
    tables_dir = outputs_dir / "capacity_expansion_tables"

    for d in (ispypsa_inputs_dir, pypsa_inputs_dir, ce_ts_dir, outputs_dir, tables_dir):
        d.mkdir(parents=True, exist_ok=True)

    parsed_workbook_cache = Path(config.paths.parsed_workbook_cache)
    parsed_traces_directory = (
        Path(config.paths.parsed_traces_directory) / f"isp_{config.trace_data.dataset_year}"
    )
    workbook_path = Path(config.paths.workbook_path)

    parsed_workbook_cache.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()

    log.info(f"=== Running archetype '{args.archetype}' with config '{config_path}' ===")

    # 1. Workbook cache.
    # Sentinel is the v7.4-canonical consolidated generator+storage summary
    # filename, which is what build_local_cache produces after normalisation
    # for both v6.0 and v7.4 source workbooks. Earlier v6.0 sentinel
    # (existing_generators_summary.csv) is consolidated away by the Phase 1
    # normalisation pass and no longer exists on a freshly-built cache.
    sentinel = parsed_workbook_cache / "existing_committed_anticipated_additional_generator_summary.csv"
    if not sentinel.exists():
        log.info("Building local IASR workbook cache (first run only)")
        build_local_cache(parsed_workbook_cache, workbook_path, config.iasr_workbook_version)
    else:
        log.info("Reusing existing workbook cache")
    iasr_tables = read_csvs(parsed_workbook_cache)
    manually_extracted_tables = load_manually_extracted_tables(config.iasr_workbook_version)

    # 2. ISPyPSA input template.
    ispypsa_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
        config.filter_by_nem_regions,
        config.filter_by_isp_sub_regions,
    )

    # 3. Apply archetype-specific mutations to ISPyPSA tables.
    log.info(f"Applying archetype mutations: {args.archetype}")
    ispypsa_tables = APPLY_ARCHETYPE[args.archetype](ispypsa_tables, config)

    write_csvs(ispypsa_tables, ispypsa_inputs_dir)

    # 4. Translate to PyPSA-friendly tables.
    pypsa_friendly = create_pypsa_friendly_inputs(config, ispypsa_tables)
    pypsa_friendly["snapshots"] = create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_friendly["generators"],
        parsed_traces_directory,
        ce_ts_dir,
    )
    write_csvs(pypsa_friendly, pypsa_inputs_dir)

    # 5. Build and solve.
    log.info("Building PyPSA network")
    network = build_pypsa_network(pypsa_friendly, ce_ts_dir)
    log.info(f"Solving with {config.solver}")
    t_solve = time.perf_counter()
    network.optimize.solve_model(solver_name=config.solver)
    log.info(f"Solve completed in {time.perf_counter() - t_solve:.1f}s")

    # 6. Save and extract.
    save_pypsa_network(network, outputs_dir, "capacity_expansion")
    results = extract_tabular_results(network, ispypsa_tables)
    results["regions_and_zones_mapping"] = extract_regions_and_zones_mapping(ispypsa_tables)
    write_csvs(results, tables_dir)

    log.info(f"=== Total elapsed: {time.perf_counter() - t0:.1f}s ===")
    log.info(f"Outputs at: {outputs_dir}")


if __name__ == "__main__":
    main()
