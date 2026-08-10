"""Materialize N9a/N9b zone traces as copies of the parent N9 (FINAL store).

The FINAL 2026 ISP splits N9 (Hunter-Central Coast) into N9a/N9b in the
connection-cost tables, REZ definitions, and transmission limits — a TRANSMISSION
split. But AEMO's zone *resource* traces remain under the un-split parent N9: the
wind/solar resource is geographically shared across the split halves, so AEMO
didn't publish separate N9a/N9b resource shapes. New-entrant VRE candidates exist
in both N9a and N9b (distinct connection costs + transmission limits) and key
their zone trace by rez_id, so with only "N9" in the store they have no matched
trace and the timeseries step fails.

This is the mirror of Q8: there AEMO DID split the traces (Q8a/b/c), so
`normalize_2026_rez_ids` maps the component rez_id to the split trace. For N9 the
traces aren't split, so we materialize the splits from the parent here — the
split halves inherit N9's resource profile, which is correct because the split is
transmission-only (the resource is the same geographic area). This is the same
reuse pattern as the 10-solar append (the data exists under a related key).

COVERAGE GUARD (hard gate): each N9a/N9b copy must reproduce the parent N9's
per-resource_type row count exactly, else a partial copy would resurface the
"expected time series values missing" timeseries error.
"""

import sys
from pathlib import Path

import pandas as pd

ZONE_PARTITION = Path("data/trace_data_final/isp_2026/zone")
PARENT_ZONE = "N9"
SPLIT_ZONES = ["N9a", "N9b"]


def main():
    ref_dirs = sorted(ZONE_PARTITION.glob("reference_year=*"))
    if not ref_dirs:
        sys.exit(f"FAIL: no zone partition under {ZONE_PARTITION}")
    for ref_dir in ref_dirs:
        _materialise_splits_for_partition(ref_dir)


def _materialise_splits_for_partition(ref_dir: Path) -> None:
    parent = _read_parent_zone(ref_dir / "data_0.parquet")
    parent_coverage = parent.groupby("resource_type").size().to_dict()
    print(f"[{ref_dir.name}] {PARENT_ZONE} per-resource rows: {parent_coverage}")

    appended = pd.concat(
        [parent.assign(zone=split) for split in SPLIT_ZONES], ignore_index=True
    )
    out_path = ref_dir / "data_1.parquet"
    appended.to_parquet(out_path, index=False)
    print(f"  wrote {out_path} ({len(appended):,} rows = {len(SPLIT_ZONES)} x {len(parent):,})")

    _assert_splits_cover_parent(ref_dir, parent_coverage)
    print(f"  POST-APPEND VERIFY PASS: {SPLIT_ZONES} match {PARENT_ZONE} per-resource coverage")


def _read_parent_zone(data_path: Path) -> pd.DataFrame:
    zones = pd.read_parquet(data_path)
    parent = zones[zones["zone"].astype(str) == PARENT_ZONE]
    if parent.empty:
        sys.exit(f"FAIL: no {PARENT_ZONE} zone trace in {data_path}")
    return parent


def _assert_splits_cover_parent(ref_dir: Path, parent_coverage: dict) -> None:
    full = pd.read_parquet(ref_dir)
    for split in SPLIT_ZONES:
        split_coverage = (
            full[full["zone"].astype(str) == split].groupby("resource_type").size().to_dict()
        )
        if split_coverage != parent_coverage:
            sys.exit(
                f"FAIL post-append: {split} coverage {split_coverage} "
                f"!= {PARENT_ZONE} {parent_coverage}"
            )


if __name__ == "__main__":
    main()
