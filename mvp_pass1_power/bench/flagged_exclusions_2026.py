"""2026 ISP candidate-set fix-ups: REZ-id normalization + no-trace exclusions.

The v7.4 templater's REZ/subregion id normalization is incompletely migrated for
the 2026 scheme. A full-NEM audit found component `rez_id`s carrying REZ *names* or
the un-split `Q8` code, the split `Q8a/b/c` REZ nodes left unconnected to a
subregion, and a manual network constraint still referencing the old `Q8-SQ` link.
These helpers fix the data-determined parts; `instrumented_runner` applies them when
`trace_data.dataset_year == 2026` (after templating, before create_pypsa_friendly),
gated on 2026 so they never touch the 2024 store.

`normalize_2026_rez_ids` resolves the data-determined crosswalk (read from AEMO's
own 2026 trace filenames — `REZ_Q8a_Darling_Downs`, `Q8b_..._Southern_Downs`,
`Q8c_..._Western_Downs`) plus two source typos, and connects the split Q8 sub-zones
to SQ (inherited from the un-split parent `Q8 -> SQ` in the REZ table).

`exclude_flagged_new_entrants` drops only the candidates that genuinely have no
2026 trace (N10/N11 fixed-offshore — 2026 ships floating-only for those coasts).
The Q8 VRE candidates are NO LONGER excluded: `normalize_2026_rez_ids` maps them to
`Q8a`, which has real traces, so they are legitimately buildable.

STILL PENDING (topology, not data-determined): the SWQLD1 network constraint's
`Q8-SQ` link term (in the 7.4 manual custom-constraints table). Whether the split
makes it three terms (Q8a/b/c-SQ) or one shared term is a 2026-ISP transmission
definition, not a filename — resolve against the workbook before relying on the run.
"""

import logging

# Data-determined crosswalk: bad component rez_id -> 2026 REZ code.
REZ_ID_2026_NORMALIZATION = {
    "Q8": "Q8a",                 # bare Q8 == Darling Downs (cache maps Darling Downs->Q8)
    "Southern Downs": "Q8b",
    "Western Downs": "Q8c",
    "Issac": "Q4",               # source typo for Isaac
    "Central -West Orana": "N3",  # source stray-space typo for Central-West Orana
}

# (rez_id, isp_resource_type) genuinely lacking a 2026 trace -> excluded from building.
FLAGGED_NO_TRACE_VRE_CANDIDATES = {
    ("N10", "WFX"): "fixed-offshore candidate, only floating WFL trace published (Hunter Coast)",
    ("N11", "WFX"): "fixed-offshore candidate, only floating WFL trace published (Illawarra Coast)",
}

# Whole REZs with NO VRE zone trace at all (no solar, no wind) — AEMO models
# these new "DN" distribution REZs as storage-only, so VRE has no trace because
# AEMO doesn't model VRE there. The v7.5 templater over-generated solar/wind
# candidates for them; the total trace-absence is the wrong-candidate signal
# (cf. N10/N11 fixed-offshore). Exclude their VRE new entrants (this acts on
# new_entrant_generators, so storage candidates in other tables are untouched).
# Flag pending final-ISP confirmation DN zones stay storage-only (very likely
# permanent — distribution REZs host storage, not utility-scale VRE).
FLAGGED_NO_TRACE_VRE_REZS = {"DN1", "DN2", "DN3"}

# v7.5-ECAA generator name -> name matching the 2024 trace map / parsed store.
# The 2026 workbook names this "Stage 2" (uppercase) but the bundled trace map
# and the parsed store use lowercase "stage 2"; on a case-insensitive filesystem
# the two collide at parse, so the store carries only the lowercase name. Rename
# the generator to match so its trace resolves (the trace itself is correct).
GENERATOR_NAME_2026_NORMALIZATION = {
    "New England Solar Farm - Stage 2": "New England Solar Farm - stage 2",
}

# v7.5-ECAA generator name -> name matching the 2024 trace map / parsed store.
# The 2026 workbook names this "Stage 2" (uppercase) but the bundled trace map
# and the parsed store use lowercase "stage 2"; on a case-insensitive filesystem
# the two collide at parse, so the store carries only the lowercase name. Rename
# the generator to match so its trace resolves (the trace itself is correct).
GENERATOR_NAME_2026_NORMALIZATION = {
    "New England Solar Farm - Stage 2": "New England Solar Farm - stage 2",
}


def normalize_2026_rez_ids(ispypsa_tables):
    """Map component rez_ids to 2026 REZ codes and connect the split Q8 sub-zones.

    Resolves the un-split `Q8`, the sub-zone names, and the two typos to codes, and
    sets the orphaned `Q8a/b/c` REZ nodes' subregion to SQ (the parent's subregion).
    """
    for tbl in ("ecaa_generators", "ecaa_batteries", "new_entrant_generators"):
        df = ispypsa_tables.get(tbl)
        if df is not None and "rez_id" in df.columns:
            df["rez_id"] = df["rez_id"].replace(REZ_ID_2026_NORMALIZATION)

    # Reconcile the one v7.5-ECAA generator name that case-mismatches the trace map.
    eg = ispypsa_tables.get("ecaa_generators")
    if eg is not None and "generator" in eg.columns:
        eg["generator"] = eg["generator"].replace(GENERATOR_NAME_2026_NORMALIZATION)

    rez = ispypsa_tables.get("renewable_energy_zones")
    if rez is not None:
        orphaned_q8_split = rez["rez_id"].isin(["Q8a", "Q8b", "Q8c"]) & rez[
            "isp_sub_region_id"
        ].isna()
        rez.loc[orphaned_q8_split, "isp_sub_region_id"] = "SQ"
    return ispypsa_tables


def exclude_flagged_new_entrants(new_entrant_generators):
    """Drop the flagged no-trace VRE new entrants, logging each exclusion.

    Two flag sources: specific (rez_id, resource_type) candidates (N10/N11
    fixed-offshore), and whole storage-only REZs whose VRE candidates are
    templater over-generation (DN1/2/3). Both act only on new_entrant_generators
    (VRE); storage candidates in other tables are untouched.
    """
    rez = new_entrant_generators["rez_id"]
    rtype = new_entrant_generators["isp_resource_type"]
    pair_mask = [k in FLAGGED_NO_TRACE_VRE_CANDIDATES for k in zip(rez, rtype)]
    rez_mask = rez.isin(FLAGGED_NO_TRACE_VRE_REZS).tolist()
    drop_mask = [p or r for p, r in zip(pair_mask, rez_mask)]
    dropped = sorted(new_entrant_generators.loc[drop_mask, "generator"])
    if dropped:
        logging.warning(
            "Excluding flagged VRE new entrants with no 2026 trace "
            f"(pending verification): {dropped}"
        )
    return new_entrant_generators.loc[[not d for d in drop_mask]].reset_index(drop=True)
