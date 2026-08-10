"""v7.8-schema regression tests for the new-entrant cost/capex pipeline.

These lock the *corrected* behaviour of the schema-migration bug class that
produced plausible-but-wrong capex on v7.8 data while passing on the v6.0
fixtures the unit tests use. Each assertion here would have failed on the bug
it guards:

- LCF ~100% for new-entrant generators AND storage (the "Equipment costs" ->
  "Equipment and installation costs" rename that dropped the dominant capex
  category, understating LCF ~3x for generators and ~5x for batteries).
- CCS/CCGT build-cost premium faithful to the IASR source (~2.3-2.8x).
- ECAA REZ ids sourced (not name-derived) so V3 and V4 — both named
  "Western Victoria" in v7.8 — don't collapse (Bulgana must stay V3).
- Per-technology WACC attached (CCGT 10.5%, wind 7.5%, batteries 8%).
- No new-entrant FOM NaN (the join that dropped the whole storage fleet).

They run against the real v7.8 FINAL parsed cache under the MVP data
directory and skip when it isn't present (it is gitignored / regenerable).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)

_V78_CACHE = Path("mvp_pass1_power/data/workbook_cache_final")


@pytest.fixture(scope="module")
def v78_template():
    if not (_V78_CACHE / "wacc.csv").exists():
        pytest.skip("v7.8 FINAL cache (with wacc.csv) not present")
    iasr_tables = read_csvs(_V78_CACHE)
    manually_extracted = load_manually_extracted_tables("7.8")
    return create_ispypsa_inputs_template(
        "Step Change", "sub_regions", iasr_tables, manually_extracted
    )


def _lcf_column(df: pd.DataFrame) -> pd.Series:
    lcf_col = next(c for c in df.columns if "lcf" in c.lower())
    return pd.to_numeric(df[lcf_col], errors="coerce").dropna()


def test_new_entrant_generator_lcf_near_100_percent(v78_template):
    # Post-fix LCFs are small adjustments around 100%. The equipment-drop bug
    # collapsed non-PHES generators to ~30-46%.
    lcf = _lcf_column(v78_template["new_entrant_generators"])
    assert lcf.min() >= 90.0
    assert lcf.max() <= 140.0


def test_new_entrant_storage_lcf_near_100_percent(v78_template):
    # The storage path never had the equipment-rename fix until this sweep;
    # without it battery LCF collapsed to ~19% (equipment is ~80% of battery
    # capex).
    lcf = _lcf_column(v78_template["new_entrant_batteries"])
    assert lcf.min() >= 95.0
    assert lcf.max() <= 120.0


def test_ccs_ccgt_build_cost_premium_matches_iasr(v78_template):
    # CCS is faithfully ~2.3-2.8x the CCGT build cost in the IASR (GenCost).
    build = v78_template["new_entrant_build_costs"]
    tech_col = next(c for c in build.columns if "tech" in c.lower())
    year_cols = [c for c in build.columns if c[:4].isdigit()]
    ccgt = build.loc[build[tech_col] == "CCGT", year_cols].to_numpy().mean()
    ccs = build.loc[build[tech_col] == "CCGT with CCS", year_cols].to_numpy().mean()
    premium = ccs / ccgt
    assert 2.3 <= premium <= 2.8


def test_ecaa_rez_ids_sourced_not_name_collapsed(v78_template):
    # V3 and V4 are both named "Western Victoria" in v7.8. Name-derivation
    # collapses both onto V4; source-id-first keeps Bulgana (V3) distinct from
    # Murra Warra (V4).
    batteries = v78_template["ecaa_batteries"]
    bulgana = batteries[
        batteries["storage_name"].astype(str).str.contains("Bulgana", case=False)
    ]
    assert set(bulgana["rez_id"]) == {"V3"}


def test_new_entrant_generators_carry_per_technology_wacc(v78_template):
    gens = v78_template["new_entrant_generators"]
    wacc_by_tech = dict(zip(gens["technology_type"], gens["wacc"]))
    assert wacc_by_tech["CCGT"] == pytest.approx(0.105)
    assert wacc_by_tech["CCGT with CCS"] == pytest.approx(0.105)
    assert wacc_by_tech["Wind"] == pytest.approx(0.075)
    assert wacc_by_tech["Large scale Solar PV"] == pytest.approx(0.07)


def test_new_entrant_storage_carries_per_technology_wacc(v78_template):
    batteries = v78_template["new_entrant_batteries"]
    assert batteries["wacc"].to_numpy() == pytest.approx(0.08)


def test_transmission_wacc_is_regulated_rate(v78_template):
    # Transmission (regulated network investment) annuitises at the IASR
    # regulated electricity transmission WACC (Step Change 3.0%), not the old
    # flat config 0.07 nor the unregulated 6.5%.
    rate = v78_template["transmission_wacc"]["regulated_transmission_wacc"].iloc[0]
    assert rate == pytest.approx(0.03)


def test_no_nan_new_entrant_fom(v78_template):
    # A NaN FOM propagates to NaN capital cost and drops the candidate silently.
    for table_name in ("new_entrant_generators", "new_entrant_batteries"):
        fom = v78_template[table_name]["fom_$/kw/annum"]
        assert fom.notna().all(), f"NaN FOM in {table_name}"
