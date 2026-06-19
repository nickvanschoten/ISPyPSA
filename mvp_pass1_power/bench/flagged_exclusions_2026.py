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

# AEMO's "Non REZ <state>" rows (REZ IDs V0=Victoria, N0=NSW) are accounting
# placeholders for generation NOT in any named REZ — not connectable zones. They
# appear in `initial_resource_limits` but have NO `initial_transmission_limits`
# row, so the templater leaves their `isp_sub_region_id` blank, then builds a
# REZ->subregion link with `isp_name=NaN` (bus1 is the missing subregion). That
# float NaN later breaks `sorted(results["isp_name"].unique())` in
# extract_transmission_expansion_results. They also carry zero new-VRE generation
# limits, so dropping them removes only non-buildable candidates. Sourced from the
# REZ Name ("Non REZ Victoria"/"Non REZ NSW"), not inferred — distinct from the
# Q8a/b/c orphans, which are real zones reconnected to SQ in normalize_2026_rez_ids.
NON_REZ_PLACEHOLDER_IDS = {"V0", "N0"}

# v7.5-ECAA generator name -> name matching the 2024 trace map / parsed store.
# The 2026 workbook names this "Stage 2" (uppercase) but the bundled trace map
# and the parsed store use lowercase "stage 2"; on a case-insensitive filesystem
# the two collide at parse, so the store carries only the lowercase name. Rename
# the generator to match so its trace resolves (the trace itself is correct).
GENERATOR_NAME_2026_NORMALIZATION = {
    "New England Solar Farm - Stage 2": "New England Solar Farm - stage 2",
}


def normalize_2026_rez_ids(ispypsa_tables):
    """Map component rez_ids to 2026 REZ codes, connect the split Q8 sub-zones, and
    drop the non-REZ placeholder rows.

    Resolves the un-split `Q8`, the sub-zone names, and the two typos to codes, sets
    the orphaned `Q8a/b/c` REZ nodes' subregion to SQ (the parent's subregion), and
    removes the `Non REZ` placeholders (V0/N0) so the translator never builds their
    malformed NaN-subregion link.
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
        # Drop the non-REZ placeholders so no malformed (NaN-subregion) link is
        # built; their VRE candidates are dropped in exclude_flagged_new_entrants.
        ispypsa_tables["renewable_energy_zones"] = rez[
            ~rez["rez_id"].isin(NON_REZ_PLACEHOLDER_IDS)
        ].reset_index(drop=True)
    return ispypsa_tables


def exclude_flagged_new_entrants(new_entrant_generators):
    """Drop VRE new entrants that cannot build, logging each reason separately.

    Three flag sources: specific (rez_id, resource_type) candidates (N10/N11
    fixed-offshore) and whole storage-only REZs (DN1/2/3) — both genuinely lacking
    a 2026 trace; and candidates sited in AEMO 'Non REZ' placeholder zones (V0/N0),
    which have no subregion and zero VRE limits. All act only on
    new_entrant_generators (VRE); storage candidates in other tables are untouched.
    """
    rez = new_entrant_generators["rez_id"]
    rtype = new_entrant_generators["isp_resource_type"]
    pair_mask = [k in FLAGGED_NO_TRACE_VRE_CANDIDATES for k in zip(rez, rtype)]
    rez_mask = rez.isin(FLAGGED_NO_TRACE_VRE_REZS).tolist()
    no_trace_mask = [p or r for p, r in zip(pair_mask, rez_mask)]
    non_rez_mask = rez.isin(NON_REZ_PLACEHOLDER_IDS).tolist()

    no_trace = sorted(new_entrant_generators.loc[no_trace_mask, "generator"])
    if no_trace:
        logging.warning(
            "Excluding flagged VRE new entrants with no 2026 trace "
            f"(pending verification): {no_trace}"
        )
    non_rez = sorted(new_entrant_generators.loc[non_rez_mask, "generator"])
    if non_rez:
        logging.warning(
            "Excluding VRE new entrants sited in AEMO 'Non REZ' placeholder "
            f"zones (V0/N0, no subregion, zero VRE limit): {non_rez}"
        )
    drop_mask = [t or n for t, n in zip(no_trace_mask, non_rez_mask)]
    return new_entrant_generators.loc[[not d for d in drop_mask]].reset_index(drop=True)
