"""Append 10 solar farms' DRAFT traces into the FINAL trace store (bounded hybrid).

AEMO's FINAL 2026 ISP solar trace set dropped the individual traces for 10 solar
farms that are still in the FINAL fleet (and were in the committed v7.5 fleet).
The draft RAW solar files are gone, but the parsed DRAFT store
(data/trace_data/isp_2026) retains them (long-format: datetime, value, project,
resource_type; keyed by project name). We append those 10 projects' draft rows
into the FINAL store's project partition as a sibling parquet (data_1.parquet);
the partitioned-dataset read concatenates it, so the translator resolves the 10
by name -- no mapping override needed.

Bounded, documented hybrid: 10 named projects carry draft-vintage solar shapes
(VRE-trace vintage differences were shown immaterial to the renewable-share
frontier); everything else is FINAL-vintage.

SILENT-DROP / COVERAGE GUARD (hard gate): a partial-coverage append would
resurface the "expected time series values missing" trace-check error (cf. the
Glanmire gap). So we confirm the 10 appended rows are schema-compatible AND cover
the SAME temporal span and row-count as the rest of the FINAL store, then
re-read the partition as a dataset to confirm the 10 resolve complete.
"""

import sys
from pathlib import Path

import pandas as pd

TEN = [
    "Mortlake Energy Hub Solar Farm", "Aldoga Solar Farm", "Broadsound Solar Farm",
    "Bundaberg Solar Farm", "Kingaroy Solar Farm", "Solar River Solar Farm",
    "Punch's Creek Renewable Energy Solar Farm", "Goorambat East Solar Farm",
    "Goulburn River Solar Farm", "Maryvale Solar Farm",
]
DRAFT = Path("data/trace_data/isp_2026/project/reference_year=2018/data_0.parquet")


def _final_partition() -> Path:
    base = Path("data/trace_data_final/isp_2026/project")
    parts = sorted(base.glob("reference_year=*"))
    if not parts:
        sys.exit(f"FAIL: no FINAL project partition under {base} (parse not done?)")
    if len(parts) > 1:
        print(f"NOTE: multiple ref-year partitions {[p.name for p in parts]}; using all")
    return base


def main():
    final_base = _final_partition()
    ref_dirs = sorted(final_base.glob("reference_year=*"))

    # 1. extract the 10 from the draft store (filtered read)
    draft10 = pd.read_parquet(DRAFT, filters=[("project", "in", TEN)])
    got = sorted(draft10["project"].unique())
    missing = [p for p in TEN if p not in got]
    if missing:
        sys.exit(f"FAIL: draft store missing {len(missing)}/10: {missing}")
    print(f"draft rows for the 10: {len(draft10):,} across {len(got)} projects")

    for ref_dir in ref_dirs:
        d0 = ref_dir / "data_0.parquet"
        final0 = pd.read_parquet(d0)

        # 2. schema compatibility. Reading a hive partition directory adds
        # the partition key (`reference_year`) to final0; it is not a physical
        # trace column and must not be required of the source rows.
        physical_final_columns = [c for c in final0.columns if c != "reference_year"]
        if list(draft10.columns) != physical_final_columns:
            sys.exit(f"FAIL schema: draft {list(draft10.columns)} != final {physical_final_columns}")
        final0 = final0[draft10.columns]

        # 3. coverage reference from the FINAL store's existing projects
        fcounts = final0.groupby("project").size()
        exp_rows = int(fcounts.median())
        fmin, fmax = final0["datetime"].min(), final0["datetime"].max()
        print(f"\n[{ref_dir.name}] FINAL: {final0['project'].nunique()} projects, "
              f"span {fmin}..{fmax}, median rows/project {exp_rows:,}")

        # 4. COVERAGE GUARD (hard gate) on each of the 10
        bad = []
        for p in TEN:
            sub = draft10[draft10["project"] == p]
            n = len(sub)
            span_ok = (sub["datetime"].min() == fmin and sub["datetime"].max() == fmax)
            rows_ok = abs(n - exp_rows) <= max(1, int(exp_rows * 0.001))
            if not (span_ok and rows_ok):
                bad.append((p, n, str(sub["datetime"].min()), str(sub["datetime"].max())))
        if bad:
            print("COVERAGE GUARD FAILED (a partial append would break the trace-check):")
            for b in bad:
                print(f"  {b}")
            sys.exit("FAIL: appended traces incomplete vs FINAL span")
        print("COVERAGE GUARD PASS: all 10 match the FINAL span + row-count")

        # 5. write as a sibling parquet in the partition (dataset read concatenates)
        append_path = ref_dir / "data_1.parquet"
        draft10.to_parquet(append_path, index=False)
        print(f"wrote {append_path} ({len(draft10):,} rows)")

        # 6. post-append verify: re-read the partition as a dataset
        full = pd.read_parquet(ref_dir)
        for p in TEN:
            n = int((full["project"] == p).sum())
            want = int((draft10["project"] == p).sum())
            if n != want:
                sys.exit(f"FAIL post-append: {p} re-reads {n} rows, expected {want}")
        print(f"POST-APPEND VERIFY PASS: 10 resolve complete; partition now "
              f"{full['project'].nunique()} projects")


if __name__ == "__main__":
    main()
