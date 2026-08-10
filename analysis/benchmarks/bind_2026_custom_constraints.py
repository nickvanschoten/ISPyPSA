"""Bind the extracted 2026 (v7.5) group-constraint term labels to PyPSA component
names and emit the manual custom-constraint tables.

Input : _2026_constraints_raw.csv (one row per (constraint, term), coefficient
        separated) from extract_2026_custom_constraints.py.
Output: candidate custom_constraints_{lhs,rhs}.csv + a per-term classification
        report. Every term resolves to one of:
          link_flow       -> matches a link isp_name (subregion corridor or
                             REZ->subregion link "<rez>-<subregion>")
          generator_output-> matches an existing-generator name (multi-unit and
                             area-aggregate labels expand to several)
          storage_output  -> matches a battery name
          DROP-<reason>    -> classified drop (load-unimplemented / undocumented /
                             unresolved-name), never silent.

Term-type/coefficient conventions follow the existing v7.4 manual table and the
translator contract (see translator/custom_constraints.py).

Run: PYTHONPATH=. uv run python analysis/benchmarks/bind_2026_custom_constraints.py
"""

import re
from pathlib import Path

import pandas as pd

RAW = Path("analysis/benchmarks/_2026_constraints_raw.csv")
TEMPLATED = Path("analysis/data/_v75_templated_nem")
# Canonical committed artifact: the v7.5 manual custom-constraint tables.
_MANUAL = Path("src/ispypsa/templater/manually_extracted_template_tables/7.5")
OUT_LHS = _MANUAL / "custom_constraints_lhs.csv"
OUT_RHS = _MANUAL / "custom_constraints_rhs.csv"

# ---- model name sets (from the v7.5 full-NEM templated tables) ----------------
gens = set(pd.read_csv(TEMPLATED / "ecaa_generators.csv")["generator"])
batteries = set(pd.read_csv(TEMPLATED / "ecaa_batteries.csv")["storage_name"])
flow_links = set(pd.read_csv(TEMPLATED / "flow_paths.csv")["flow_path"])
_rez = pd.read_csv(TEMPLATED / "renewable_energy_zones.csv")
REZ_SUBREGION = dict(zip(_rez["rez_id"], _rez["isp_sub_region_id"]))

# ---- mappings sourced / verified upstream -------------------------------------
# Interconnector labels -> model flow-path isp_name. Verified against
# flow_path_transfer_capability + augmentation_options notes (QNI=NNSW-SQ,
# Heywood=WNV-SESA), not name-similarity.
FLOW_REMAP = {"NSW-QLD": "NNSW-SQ", "Heywood Interconnector": "WNV-SESA"}

# Generation-area aggregates (SWQLD1 note row 217). Each is a named-generator
# list + the area's new-entrant VRE, represented by the area's 2026 REZ link
# (Q8a=Darling Downs, Q8b=Southern Downs, Q8c=Western Downs; all -> SQ).
AREA_MEMBERS = {
    "WD": (["Tarong", "Tarong North", "Tarong BESS", "Borumba Pumped Hydro",
            "Coopers Gap WF", "Wambo WF"], "Q8c"),
    "DD": (["Chinchilla BESS", "Ulinda Park BESS", "Wandoan South BESS",
            "Western Downs BESS", "Kogan Creek", "Kogan Gas", "Condamine",
            "Darling Downs", "Darling Downs SF", "Braemar", "Braemar 2", "Roma",
            "Bluegrass SF", "Columboola SF", "Edenvale SF", "Gangarri SF",
            "Wandoan South SF", "Western Downs SF", "Dulacca WF"], "Q8a"),
    "SD": (["Milmerran"], "Q8b"),
}
# S5 West sub-aggregate (NSA1 note).
S5_WEST_MEMBERS = ["Mt Millar WF", "Cathedral Rocks WF", "Lincoln Gap WF",
                   "Lincoln Gap BESS", "Cultana SF", "Port Lincoln GT",
                   "SA Hydrogen Turbine"]

# Explicit name aliases: workbook label -> [model generator/battery names].
# Conservative: only where the correspondence is unambiguous. Anything not here
# and not an exact match is flagged, not prefix-guessed.
GEN_ALIAS = {
    "Loy Yang": ["Loy Yang A Power Station", "Loy Yang B"],
    "Murray": ["Murray 1", "Murray 2"],
    "Valley Power": ["Valley Power"],
    "Braemar 2": ["Braemar 2 Power Station"],
    "Condamine": ["Condamine A"],
    "Milmerran": ["Millmerran"],
    "Coopers Gap WF": ["Coopers Gap Wind Farm"],
    "Wambo WF": ["Wambo Wind Farm"],
    "Dulacca WF": ["Dulacca Wind Farm"],
    "Darling Downs SF": ["Darling Downs Solar Farm"],
    "Bluegrass SF": ["Bluegrass Solar Farm"],
    "Columboola SF": ["Columboola Solar Farm"],
    "Edenvale SF": ["Edenvale Solar Park"],
    "Gangarri SF": ["Gangarri Solar Farm"],
    "Wandoan South SF": ["Wandoan South Solar Farm - stage 1"],
    "Western Downs SF": ["Western Downs Green Power Hub"],
    "Limondale SF": ["Limondale Solar Farm 1"],
    "Limondale 2 SF": ["Limondale Solar Farm 2"],
    "Sunraysia SF": ["Sunraysia Solar Farm"],
    "Coleambally SF": ["Coleambally Solar Farm"],
    "Finley SF": ["Finley Solar Farm"],
    "Darlington Pt SF": ["Darlington Point Solar Farm"],
    "Hillston SF": ["Hillston Sun Farm"],
    "Broken Hill SF": ["Broken Hill Solar Farm"],
    "Silverton WF": ["Silverton Wind Farm"],
    "Jemalong Solar": ["Jemalong Solar"],
    "Wellington North Solar Farm": ["Wellington North Solar Farm (Lightsource)"],
    "West Kiewa": ["West Kiewa"],
    "Dartmouth": ["Dartmouth"],
    "Bayswater": ["Bayswater"],
    "Mt Piper": ["Mt Piper"],
    "Tallawarra": ["Tallawarra"],
    "Goulburn River Solar Farm": ["Goulburn River Solar Farm"],
    # S5 West members (NSA1 note) — abbreviation/spelling fixes
    "Mt Millar WF": ["Mount Millar Wind Farm"],
    "Cathedral Rocks WF": ["Cathedral Rocks Wind Farm"],
    "Lincoln Gap WF": ["Lincoln Gap Wind Farm - stage 1",
                       "Lincoln Gap Wind Farm - stage 2"],
    # Bogong and McKay are one combined model generator; map Bogong, fold McKay
    # in (see COMBINED_ELSEWHERE) so the single 0.81*output isn't double-counted.
    "Bogong": ["Bogong / Mackay"],
}

# Workbook label -> documented drop: combined into another generator already
# mapped (avoid double-counting the shared model unit).
COMBINED_ELSEWHERE = {
    "McKay": "combined into 'Bogong / Mackay' (mapped via Bogong term)",
}

# Workbook constraint members genuinely absent from the v7.5 templated existing
# set (verified by search). Classified drops, not silent misses.
ABSENT_V75 = {
    "Borumba Pumped Hydro": "committed PHES, not in v7.5 existing set (and storage dispatch unimplemented)",
    "BPH": "= Borumba Pumped Hydro (committed PHES, absent; storage unimplemented)",
    "Kogan Gas": "gas peaker, not in v7.5 existing ECAA set (Kogan Creek coal is present)",
    "Narromine Solar Farm": "not in v7.5 existing ECAA set",
    "Dubbo Solar Hub": "not in v7.5 existing ECAA set",
    "Cultana SF": "not in v7.5 existing ECAA set",
    "SA Hydrogen Turbine": "new H2 tech, not an existing generator",
}
BESS_ALIAS = {
    "Tarong BESS": ["Tarong BESS"],
    "Chinchilla BESS": ["Chinchilla BESS"],
    "Western Downs BESS": ["Western Downs Battery"],
    "Darlington Point BESS": ["Darlington Point Energy Storage System"],
    "Riverina BESS 1 & 2": ["Riverina Energy Storage System 1",
                            "Riverina Energy Storage System 2"],
    "Broken Hill BESS": ["Broken Hill BESS"],
    "Silver City Energy Storage": ["Silver City Energy Storage"],
    "Orana BESS": ["Orana BESS"],
    "Lincoln Gap BESS": ["Lincoln Gap Wind Farm BESS"],
}
# Load terms -> per-constraint disposition (materiality reasoned in the writeup).
# DROP_TERM keeps the constraint minus this term (flagged approximate);
# DROP_CONSTRAINT drops the whole constraint (flagged unrepresentable).
LOAD_TERMS = {"NSA demand", "NQ Load", "CNSW Demand", "SNW Load"}
DROP_CONSTRAINT = {"SNW1"}  # material CNSW Demand load term + undocumented GPG aggregates

_REZ_RE = re.compile(r"^(Q|N|V|S|T|DN)\d+[a-c]?$")


def _is_bare_rez(term):
    return bool(_REZ_RE.match(term)) and term in REZ_SUBREGION


def _rez_link(term):
    sub = REZ_SUBREGION[term]
    return f"{term}-{sub}" if pd.notna(sub) else None


def classify_term(constraint, term, coef):
    """Return (list of (term_type, term_id, coef) lhs rows, classification str)."""
    # 1. flow corridor (direct or sourced remap)
    if term in flow_links:
        return [("link_flow", term, coef)], f"link_flow:{term}"
    if term in FLOW_REMAP:
        tgt = FLOW_REMAP[term]
        return [("link_flow", tgt, coef)], f"link_flow:{term}->{tgt} (sourced remap)"
    # 2. bare REZ -> REZ->subregion link
    if _is_bare_rez(term):
        link = _rez_link(term)
        if link:
            return [("link_flow", link, coef)], f"link_flow:{term}->{link} (REZ link)"
        return [], f"DROP-rez-no-subregion:{term}"
    # 3. load term -> unimplementable
    if term in LOAD_TERMS:
        return [], f"DROP-load-unimplemented:{term}"
    if term in COMBINED_ELSEWHERE:
        return [], f"DROP-combined-elsewhere:{term} ({COMBINED_ELSEWHERE[term]})"
    if term in ABSENT_V75:
        return [], f"DROP-absent-v75:{term} ({ABSENT_V75[term]})"
    # 4. generation-area aggregate
    if term in AREA_MEMBERS:
        members, rez = AREA_MEMBERS[term]
        rows, notes = [], []
        for m in members:
            mrows, mcls = classify_term(constraint, m, coef)
            rows += mrows
            notes.append(mcls)
        link = _rez_link(rez)
        if link:
            rows.append(("link_flow", link, coef))
            notes.append(f"new-entrant-VRE->{link}")
        return rows, f"AREA[{term}]{{ " + " | ".join(notes) + " }}"
    if term == "S5 West":
        rows, notes = [], []
        for m in S5_WEST_MEMBERS:
            mrows, mcls = classify_term(constraint, m, coef)
            rows += mrows
            notes.append(mcls)
        return rows, f"S5West{{ " + " | ".join(notes) + " }}"
    # 5. battery
    if term in BESS_ALIAS:
        names = [n for n in BESS_ALIAS[term] if n in batteries]
        miss = [n for n in BESS_ALIAS[term] if n not in batteries]
        rows = [("storage_output", n, coef) for n in names]
        if miss:
            return rows, f"storage_output:{term}->{names} DROP-missing:{miss}"
        return rows, f"storage_output:{term}->{names}"
    if term in batteries:
        return [("storage_output", term, coef)], f"storage_output:{term}"
    # 6. named generator (alias then exact)
    if term in GEN_ALIAS:
        names = [n for n in GEN_ALIAS[term] if n in gens]
        miss = [n for n in GEN_ALIAS[term] if n not in gens]
        rows = [("generator_output", n, coef) for n in names]
        if miss:
            return rows, f"generator_output:{term}->{names} DROP-missing:{miss}"
        return rows, f"generator_output:{term}->{names}"
    if term in gens:
        return [("generator_output", term, coef)], f"generator_output:{term}"
    # 7. unresolved
    return [], f"DROP-unresolved-name:{term}"


def main():
    raw = pd.read_csv(RAW)
    lhs_rows, rhs_rows, report = [], [], []
    for cid, grp in raw.groupby("constraint_id", sort=False):
        if cid in DROP_CONSTRAINT:
            report.append(f"\n### {cid}  ==> DROPPED (flagged unrepresentable)")
            for _, r in grp.iterrows():
                report.append(f"    {r['coefficient']:+g} * {r['term']}")
            continue
        report.append(f"\n### {cid}")
        any_term = False
        for _, r in grp.iterrows():
            rows, cls = classify_term(cid, r["term"], r["coefficient"])
            report.append(f"    {r['coefficient']:+g} * {r['term']:<28} -> {cls}")
            for tt, tid, co in rows:
                lhs_rows.append({"constraint_id": cid, "term_type": tt,
                                 "term_id": tid, "coefficient": co})
                any_term = True
        if any_term:
            rhs = grp["rhs_summer_peak"].iloc[0]
            imp = grp["import_limit"].iloc[0]
            val = rhs if str(rhs) not in ("nan", "N/A") else imp
            rhs_rows.append({"constraint_id": cid, "constraint_type": "<=",
                             "rhs": val})

    pd.DataFrame(lhs_rows).to_csv(OUT_LHS, index=False)
    pd.DataFrame(rhs_rows).to_csv(OUT_RHS, index=False)
    print("\n".join(report))
    print(f"\n=> {len(lhs_rows)} LHS rows, {len(rhs_rows)} constraints emitted")
    print(f"   {OUT_LHS}\n   {OUT_RHS}")


if __name__ == "__main__":
    main()
