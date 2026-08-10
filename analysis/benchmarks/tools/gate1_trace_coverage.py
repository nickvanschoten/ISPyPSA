"""Gate-1 trace-coverage check: confirm EVERY VRE component resolves to a trace.

Strengthened replacement for the scope-limited "10-solar coverage" check. That
check validated 10 specific draft-vintage solar farms; it could not catch a
genuine no-trace farm (Axedale) or a name mismatch (Goyder North Wind Farm 1)
elsewhere in the 242 ECAA VRE. This tool templates the full-NEM FINAL fleet,
applies the 2026 normalisations/exclusions exactly as the run path does, and then
asserts:

  - every remaining ECAA VRE generator has a matched PROJECT trace (by name), and
  - every remaining new-entrant VRE candidate has a matched ZONE trace
    (by rez_id + isp_resource_type).

PASS only when both gap sets are empty — so coverage is *confirmed to have run*
for 2026, not assumed from a named mechanism (the upstream
`filter_v74_ecaa_to_trace_coverage` is hardcoded to isp_2024 and no-ops for 2026,
which is why 2026 coverage is enforced in `flagged_exclusions_2026` + this gate).

Usage: uv run python analysis/benchmarks/tools/gate1_trace_coverage.py
       [workbook_cache] [trace_store]   (defaults: FINAL cache + FINAL traces)
"""

import sys
from pathlib import Path

import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.templater import create_ispypsa_inputs_template, load_manually_extracted_tables
from analysis.benchmarks.flagged_exclusions_2026 import (
    exclude_ecaa_without_trace,
    exclude_flagged_new_entrants,
    normalize_2026_rez_ids,
)

DEFAULT_CACHE = "analysis/data/workbook_cache_final"
DEFAULT_TRACES = "data/trace_data_final/isp_2026"


def main(cache_path: str, trace_store: str) -> str:
    tables = _build_full_nem_fleet(cache_path, trace_store)
    ecaa_gaps = _ecaa_vre_without_project_trace(tables["ecaa_generators"], trace_store)
    ne_gaps = _new_entrant_vre_without_zone_trace(
        tables["new_entrant_generators"], trace_store
    )
    print(f"ECAA VRE without project trace: {ecaa_gaps}")
    print(f"new-entrant VRE without zone trace: {ne_gaps}")
    verdict = "PASS" if not ecaa_gaps and not ne_gaps else "FAIL"
    print(f"\nVERDICT: {verdict}")
    return verdict


def _build_full_nem_fleet(cache_path: str, trace_store: str) -> dict:
    iasr = read_csvs(Path(cache_path))
    tables = create_ispypsa_inputs_template(
        "Step Change", "sub_regions", iasr, load_manually_extracted_tables("7.8"), None, None
    )
    tables = normalize_2026_rez_ids(tables)
    tables["new_entrant_generators"] = exclude_flagged_new_entrants(
        tables["new_entrant_generators"]
    )
    tables["ecaa_generators"] = exclude_ecaa_without_trace(
        tables["ecaa_generators"], trace_store
    )
    return tables


def _ecaa_vre_without_project_trace(ecaa_generators, trace_store) -> list:
    vre = ecaa_generators[
        ecaa_generators["fuel_type"].str.contains("Wind|Solar", case=False, na=False)
    ]
    trace_projects = _trace_names(trace_store, "project", "project")
    return sorted(set(vre["generator"].astype(str)) - trace_projects)


def _new_entrant_vre_without_zone_trace(new_entrant_generators, trace_store) -> list:
    vre = new_entrant_generators[
        new_entrant_generators["fuel_type"].str.contains("Wind|Solar", case=False, na=False)
    ]
    needed = set(zip(vre["rez_id"].astype(str), vre["isp_resource_type"].astype(str)))
    zone_keys = _trace_zone_keys(trace_store)
    return sorted(needed - zone_keys)


def _trace_names(trace_store, subdir, column) -> set:
    files = sorted(Path(trace_store).glob(f"{subdir}/reference_year=*/*.parquet"))
    frames = [pd.read_parquet(f, columns=[column]) for f in files]
    return set(pd.concat(frames, ignore_index=True)[column].astype(str))


def _trace_zone_keys(trace_store) -> set:
    files = sorted(Path(trace_store).glob("zone/reference_year=*/*.parquet"))
    frames = [pd.read_parquet(f, columns=["zone", "resource_type"]) for f in files]
    zones = pd.concat(frames, ignore_index=True)
    return set(zip(zones["zone"].astype(str), zones["resource_type"].astype(str)))


if __name__ == "__main__":
    cache = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CACHE
    traces = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TRACES
    sys.exit(0 if main(cache, traces) == "PASS" else 1)
