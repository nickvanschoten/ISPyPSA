"""7.8 (2026 ISP FINAL) NSW template + translate harness (NO solve).

Replicates the templating + static translation slice of
instrumented_runner.run (lines ~284-316) so 7.8-specific templater blockers can
be ground out without a solve or a network build.

Path:
    read_csvs(workbook_cache_final)
    -> create_ispypsa_inputs_template('Step Change', 'sub_regions', iasr,
                                      load_manually_extracted_tables('7.8'),
                                      ['NSW'], None)
    -> normalize_2026_rez_ids
    -> exclude_flagged_new_entrants
    -> create_pypsa_friendly_inputs(config, ispypsa_tables)

Run from repo root:
    uv run python analysis/benchmarks/tools/harness_78_template_translate.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.logging import configure_logging
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import create_pypsa_friendly_inputs

from analysis.benchmarks.flagged_exclusions_2026 import (
    exclude_flagged_new_entrants,
    normalize_2026_rez_ids,
)


def main():
    configure_logging()
    config_path = REPO_ROOT / "analysis/benchmarks/configs/78_nsw_final.yaml"
    config = load_config(config_path)

    cache = REPO_ROOT / config.paths.parsed_workbook_cache
    out_dir = REPO_ROOT / "analysis/benchmarks/runs/78_nsw_final_harness"
    ispypsa_dir = out_dir / "ispypsa_inputs"
    pypsa_dir = out_dir / "pypsa_friendly"
    for d in (ispypsa_dir, pypsa_dir):
        d.mkdir(parents=True, exist_ok=True)

    print(f"[harness] reading cache: {cache}")
    iasr_tables = read_csvs(cache)
    manual = load_manually_extracted_tables(config.iasr_workbook_version)

    print("[harness] create_ispypsa_inputs_template ...")
    ispypsa_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manual,
        config.filter_by_nem_regions,
        config.filter_by_isp_sub_regions,
    )

    print("[harness] normalize_2026_rez_ids + exclude_flagged_new_entrants ...")
    ispypsa_tables = normalize_2026_rez_ids(ispypsa_tables)
    ispypsa_tables["new_entrant_generators"] = exclude_flagged_new_entrants(
        ispypsa_tables["new_entrant_generators"]
    )
    write_csvs(ispypsa_tables, ispypsa_dir)

    print("[harness] create_pypsa_friendly_inputs ...")
    pypsa_friendly = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_friendly, pypsa_dir)

    cc = pypsa_friendly.get("custom_constraints_rhs")
    n_cc = 0 if cc is None else len(cc)
    print(f"[harness] DONE. pypsa_friendly tables: {sorted(pypsa_friendly)}")
    print(f"[harness] custom_constraints_rhs rows: {n_cc}")
    if cc is not None and n_cc:
        print(cc.to_string())


if __name__ == "__main__":
    main()
