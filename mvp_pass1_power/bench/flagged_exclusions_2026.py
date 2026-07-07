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
from pathlib import Path

import pandas as pd

# Data-determined crosswalk: bad component rez_id -> 2026 REZ code.
REZ_ID_2026_NORMALIZATION = {
    "Q8": "Q8a",                 # bare Q8 == Darling Downs (cache maps Darling Downs->Q8)
    "Southern Downs": "Q8b",
    "Western Downs": "Q8c",
    "Issac": "Q4",               # source typo for Isaac
    "Central -West Orana": "N3",  # source stray-space typo for Central-West Orana
    "Northern QLD": "Q3",        # Haughton Solar Farm Stage 2 carries the REZ name, not
                                 # the code; the summary + Stage 1 confirm Q3 (Northern Qld).
}

# Split-REZ sub-zones orphaned from their subregion link (NaN isp_sub_region_id) ->
# the parent REZ's subregion. AEMO split these REZs for transmission but the
# subregion-link source still keys the un-split parent, so the split rows have no
# subregion (a NaN link that later breaks sorted(isp_name.unique()) in post-process).
# Q8 -> SQ (Darling Downs); N9 -> CNSW (Hunter-Central Coast, per v75 + FINAL REZ table).
ORPHANED_SPLIT_REZ_SUBREGION = {
    "Q8a": "SQ", "Q8b": "SQ", "Q8c": "SQ",
    "N9a": "CNSW", "N9b": "CNSW",
}

# Source subregion typo: CSNW -> CNSW (Central NSW). Glanmire Solar Farm carries
# "CSNW"; its co-located "Glanmire Solar Farm BESS" correctly carries "CNSW".
SUBREGION_2026_NORMALIZATION = {"CSNW": "CNSW"}

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
    # FINAL 2026 ECAA lists this anticipated wind farm as "Goyder North Wind Farm 1";
    # the parsed trace store names it "Goyder North Wind Farm" (no " 1" suffix, and
    # there is no "...Farm 2", so the suffix is redundant). Rename so its trace
    # resolves — applied before the ECAA no-trace filter so it is matched, not dropped.
    "Goyder North Wind Farm 1": "Goyder North Wind Farm",
}


def normalize_2026_rez_ids(ispypsa_tables):
    """Map component rez_ids/subregions to 2026 codes, connect split sub-zones, and
    drop the non-REZ placeholder rows.

    Resolves bad rez_ids (un-split `Q8`, sub-zone names, the `Northern QLD` REZ name,
    source typos) to codes; fixes the `CSNW`->`CNSW` subregion typo; connects the
    orphaned split sub-zones (`Q8a/b/c`->SQ, `N9a/N9b`->CNSW) to their parent's
    subregion; and removes the `Non REZ` placeholders (V0/N0) so the translator never
    builds their malformed NaN-subregion link.
    """
    for tbl in ("ecaa_generators", "ecaa_batteries", "new_entrant_generators"):
        df = ispypsa_tables.get(tbl)
        if df is not None and "rez_id" in df.columns:
            df["rez_id"] = df["rez_id"].replace(REZ_ID_2026_NORMALIZATION)
        if df is not None and "sub_region_id" in df.columns:
            df["sub_region_id"] = df["sub_region_id"].replace(SUBREGION_2026_NORMALIZATION)

    # Reconcile the one v7.5-ECAA generator name that case-mismatches the trace map.
    eg = ispypsa_tables.get("ecaa_generators")
    if eg is not None and "generator" in eg.columns:
        eg["generator"] = eg["generator"].replace(GENERATOR_NAME_2026_NORMALIZATION)

    rez = ispypsa_tables.get("renewable_energy_zones")
    if rez is not None:
        for split_rez, parent_subregion in ORPHANED_SPLIT_REZ_SUBREGION.items():
            orphaned = (rez["rez_id"] == split_rez) & rez["isp_sub_region_id"].isna()
            rez.loc[orphaned, "isp_sub_region_id"] = parent_subregion
        # Drop the non-REZ placeholders so no malformed (NaN-subregion) link is
        # built; their VRE candidates are dropped in exclude_flagged_new_entrants.
        ispypsa_tables["renewable_energy_zones"] = rez[
            ~rez["rez_id"].isin(NON_REZ_PLACEHOLDER_IDS)
        ].reset_index(drop=True)

    _reconcile_split_rez_connection_cost_region(ispypsa_tables)
    return ispypsa_tables


def _reconcile_split_rez_connection_cost_region(ispypsa_tables):
    """Point split-REZ candidates at their own connection-cost row.

    The FINAL 2026 ISP splits some REZ connection costs (N9 -> N9a/N9b in
    `connection_cost_forecast_wind_and_solar`) but leaves the new-entrant
    `connection_cost_region_id` as the un-split parent (N9), so the cost merge
    misses. Where a candidate's own `rez_id` IS a connection-cost key (the split
    case) but its `connection_cost_region_id` is not, key by `rez_id`. Q8 is
    deliberately untouched: its `rez_id` (Q8a) is not a cost key, so the parent
    (Q8) is kept, matching the un-split Q8 cost row. The cost table's own REZ-ID
    set decides — no inference.
    """
    new_entrants = ispypsa_tables.get("new_entrant_generators")
    costs = ispypsa_tables.get("new_entrant_wind_and_solar_connection_costs")
    if new_entrants is None or costs is None:
        return
    cost_keys = set(costs["REZ names"].astype(str))
    rez_id_is_cost_key = new_entrants["rez_id"].astype(str).isin(cost_keys)
    region_is_not_cost_key = ~new_entrants["connection_cost_region_id"].astype(
        str
    ).isin(cost_keys)
    use_rez_id = rez_id_is_cost_key & region_is_not_cost_key
    new_entrants.loc[use_rez_id, "connection_cost_region_id"] = new_entrants.loc[
        use_rez_id, "rez_id"
    ]


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


def exclude_ecaa_without_trace(ecaa_generators, trace_store_dir):
    """Drop ECAA VRE generators with no matched project trace (class-wide).

    The upstream `filter_v74_ecaa_to_trace_coverage` hardcodes its trace lookup to
    `isp_2024` and no-ops for the 2026 traces, so 2026 ECAA VRE trace-coverage
    filtering lives here (the established 2026 trace-handling layer), mirroring
    that filter's Option-B drop. Checks every ECAA VRE generator (not a hand-listed
    subset), so it is the full-coverage filter, not a targeted patch. Must run AFTER
    `GENERATOR_NAME_2026_NORMALIZATION` (Goyder North Wind Farm 1 -> Goyder North
    Wind Farm) so a name-mismatched generator is matched against its trace, not
    dropped — the filter matches by exact name.
    """
    trace_projects = _trace_store_project_names(trace_store_dir)
    is_vre = ecaa_generators["fuel_type"].str.contains("Wind|Solar", case=False, na=False)
    drop_mask = is_vre & ~ecaa_generators["generator"].isin(trace_projects)
    dropped = sorted(ecaa_generators.loc[drop_mask, "generator"])
    if dropped:
        logging.warning(
            f"Excluding ECAA VRE generators with no 2026 project trace: {dropped}"
        )
    return ecaa_generators.loc[~drop_mask].reset_index(drop=True)


def _trace_store_project_names(trace_store_dir):
    """Project names present in the parsed trace store's project partition."""
    files = sorted(Path(trace_store_dir).glob("project/reference_year=*/*.parquet"))
    frames = [pd.read_parquet(f, columns=["project"]) for f in files]
    return set(pd.concat(frames, ignore_index=True)["project"].astype(str))
