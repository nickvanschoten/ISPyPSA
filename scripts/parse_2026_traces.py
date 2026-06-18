"""Parse AEMO Draft 2026 ISP traces into the ISPyPSA `get_data` layout.

The Draft 2026 traces differ from the 2024 conventions that isp-trace-parser
v2.0.3 expects. This script works around the differences without permanently
mutating the installed package, and produces the optimised hive-partitioned
parquet store that `isp_trace_parser.get_data` reads:

    data/trace_data/isp_2026/
        project/reference_year=2018/        # solar + wind, existing generators
        zone/reference_year=2018/           # solar + wind, REZs
        demand/scenario=Step Change/reference_year=<y>/

Workarounds, each tied to a decision recorded in the project notes:

1. VRE reference year. Solar/wind ship under the single synthetic "RefYear5000".
   ISPyPSA pairs VRE and demand on one reference-year mapping, so we relabel the
   synthetic VRE year to TARGET_REFERENCE_YEAR (2018) to pair with that year's
   historical demand. This is an INTERIM basis (see the caveat note); AEMO's
   method intends coincident VRE+demand, so re-solve when per-year VRE publishes.

2. Demand filename region prefix. 2026 demand files are
   "<REGION>_<SUBREGION>_RefYear_<y>_<scenario>_<poe>_<type>.csv"; the parser
   expects "<SUBREGION>_RefYear_...". We strip the leading region token.

3. Trace-name mapping drift. The bundled 2024 maps miss a few 2026 renames. We
   apply a small override set (Numurkah, Haughton, solar REZ V7) on top of the
   bundled maps. FLAGGED-and-unmapped pending verification: Q8 (VRE candidates
   carry the un-split id; nodes/traces use Q8a/b/c) and Goyder South (only
   Goyder_North exists in 2026). DN1-3 are storage-only, so no VRE trace is
   pulled for them.

4. Worker processes. isp-trace-parser extracts demand metadata inside joblib
   workers, which do not inherit monkeypatches, so we parse single-process.

Run:  uv run python scripts/parse_2026_traces.py
"""

import logging
import re
import shutil
from pathlib import Path

import yaml

import isp_trace_parser.demand_traces as demand_traces
import isp_trace_parser.solar_traces as solar_traces
import isp_trace_parser.wind_traces as wind_traces
from isp_trace_parser import (
    DemandMetadataFilter,
    SolarMetadataFilter,
    WindMetadataFilter,
    parse_demand_traces,
    parse_solar_traces,
    parse_wind_traces,
)
from isp_trace_parser.metadata_extractors import (
    extract_demand_trace_metadata,
)
from isp_trace_parser.optimise_parquet import partition_traces_by_columns

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

REPO = Path(__file__).resolve().parents[1]
INPUTS = REPO / "iasr inputs"
SOLAR_CSV = INPUTS / "Draft 2026 ISP Solar traces"
WIND_CSV = INPUTS / "Draft 2026 ISP Wind traces"
OUT = REPO / "data" / "trace_data" / "isp_2026"
FLAT = REPO / "data" / "trace_data" / "_flat_2026"  # transient per-type parsed files

TARGET_REFERENCE_YEAR = 2018  # relabel the synthetic VRE RefYear5000 onto this year
SYNTHETIC_VRE_YEAR = 5000

MAPPING_DIR = Path(solar_traces.__file__).parent.parent / "isp_trace_name_mapping_configs"

# 2026 trace-name overrides applied on top of the bundled 2024 maps (decision 3).
SOLAR_PROJECT_OVERRIDES = {
    "Numurkah Solar Farm": "Numurkah",  # 2024 map had "Numurkah_One"
    "Haughton Solar Farm": "Haughton_1",  # 2026 split into _1/_2; lead phase (same-site profile)
    "Haughton Solar Farm Stage 2": "Haughton_2",  # second phase
    # v7.5 ECAA committed/anticipated solar farms absent from the bundled 2024
    # map. Value = raw 2026 trace stem; the parser auto-detects the SAT/FFP/CST
    # resource type from the filename, so a stem-only value resolves any type.
    "Aldoga Solar Farm": "Aldoga",
    "Barnawartha Solar Farm": "Barnawartha",
    "Barwon Solar Farm": "Barwon",
    "Broadsound Solar Farm": "Broadsound",
    "Bullyard Solar Farm": "Bullyard",
    "Bundaberg Solar Farm": "Bundaberg",
    "Campbells Forest Solar Farm": "Campbells_Forest",
    "Ganymirra Solar Power Station": "Ganymirra",
    "Glanmire Solar Farm": "Glanmire",
    "Kingaroy Solar Farm": "Kingaroy",
    "Lancaster Solar Farm": "Lancaster",
    "Majors Creek Power Station": "Majors_Creek",
    "Maryvale Solar Farm": "Maryvale",
    "Munna Creek Solar Farm": "Munna_Creek",
    "Northern Midlands Solar Farm": "Northern_Midlands",
    "Punch's Creek Renewable Energy Solar Farm": "Punchs_Creek",
    "Sandy Creek Solar Farm": "Sandy_Creek",
    "Solar River Solar Farm": "Solar_River",
    # Fixed-tilt (FFP) sites — only an FFP trace exists for each (AEMO models
    # these as fixed-tilt); the parser reads the FFP type from the filename.
    "Elaine Solar Farm": "Elaine",
    "Hopeland Solar Farm": "Hopeland",
    "Ravenswood Solar Farm": "Ravenswood",
    # Suffix-stripped: the v7.5 generator name carries a suffix the single 2026
    # site trace lacks (verified: no _East/_River/_Energy_Hub variant exists).
    "Goorambat East Solar Farm": "Goorambat",
    "Goulburn River Solar Farm": "Goulburn",
    "Mortlake Energy Hub Solar Farm": "Mortlake",  # distinct from Mortlake South Wind Farm
    "Hay Sun Farm": "Hay",  # distinct site from the (separately mapped) Hayman Solar Farm
    # Distinct co-named sites (each has its own trace).
    "Mokoan Solar Farm": "Mokoan",
    "West Mokoan Solar Farm": "West_Mokoan",
    # Multi-stage projects sharing one site trace.
    # NB: "New England Solar Farm - stage 2" is already in the bundled map
    # (lowercase) and parses; the v7.5 ECAA uses uppercase "Stage 2", reconciled
    # at the model layer (GENERATOR_NAME_2026_NORMALIZATION) — no override here,
    # since an uppercase key would collide with the bundled lowercase on a
    # case-insensitive filesystem.
    "Warwick Solar Farm - stage 1": "Warwick",
    "Warwick Solar Farm - stage 2": "Warwick",
}
SOLAR_ZONE_OVERRIDES = {
    "V7": "V7",  # new 2026 solar REZ trace, absent from the bundled map
    # New 2026 REZ zones with VRE candidates but absent from the bundled zone
    # map (solar SAT/CST traces exist as REZ_<code>_<name>). V9 is offshore
    # (wind-only) so it has no solar zone trace and is not listed here.
    "N13": "N13",   # South Cobar
    "Q10": "Q10",   # Collinsville
    # Q8 split zones (now parseable via the split-aware extractor). Only Q8a has
    # VRE candidates today, but map all three — the raw traces exist and this is
    # the clean closure of the Q8-split.
    "Q8a": "Q8a",   # Darling Downs
    "Q8b": "Q8b",   # Southern Downs
    "Q8c": "Q8c",   # Western Downs
}
# Wind zone map is flat (code -> code), like solar zone. The parse script did
# not previously override it; the new 2026 REZ zones below need wind-zone entries
# (their WH/WM, and V9's offshore WFL/WFX, traces exist but the bundled map
# lacks the codes). Mechanism mirrors SOLAR_ZONE_OVERRIDES (nested=False).
WIND_ZONE_OVERRIDES = {
    "N13": "N13",   # South Cobar (WH/WM)
    "Q10": "Q10",   # Collinsville (WH/WM)
    "Q8a": "Q8a",   # Darling Downs (WH/WM)
    "Q8b": "Q8b",   # Southern Downs (WH/WM)
    "Q8c": "Q8c",   # Western Downs (WH/WM)
    "V9": "V9",     # Southern Ocean offshore (WFL/WFX)
}
# Wind project overrides set the nested CSVFile (the trace stem). Goyder South WF
# 1A/1B are EXISTING generators that lost their trace: 2026 ships only Goyder_North,
# not Goyder_South. Map to the same Goyder-area trace (adjacent farms share the wind
# resource) — interim, flagged pending verification that Goyder_North supersedes
# Goyder_South. Existing capacity must carry a trace (it can't be excluded like a
# new entrant), so map-to-same-region is the interim, not drop.
WIND_PROJECT_OVERRIDES = {
    "Goyder South Wind Farm 1A": "Goyder_North",
    "Goyder South Wind Farm 1B": "Goyder_North",
    # v7.5 ECAA committed/anticipated wind farms absent from the bundled 2024
    # map. Their 2026 raw files are site-named (e.g. Boulder_Creek_RefYear5000);
    # _apply creates the new nested entry (CSVFile is the only field the parser
    # consumes — see restructure_wind_project_mapping).
    "Boulder Creek Wind Farm": "Boulder_Creek",
    "Gawara Baya Wind Farm": "Gawara_Baya",
    "Golden Plains West Wind Farm": "Golden_Plains_West",
    "Goyder North Wind Farm": "Goyder_North",
    "Inverleigh Wind Farm": "Inverleigh",
    "Junction Rivers Wind Farm": "Junction_Rivers",
    "Kentbruck Green Power Hub Wind Farm": "Kentbruck",
    "Lotus Creek Wind Farm": "Lotus_Creek",
    "Palmer Wind Farm": "Palmer",
    "Port Latta Wind Farm": "Port_Latta",
    "Spicers Creek Wind Farm": "Spicers_Creek",
    "Thunderbolt Wind Farm": "Thunderbolt",
    # Valley of the Winds (Anticipated, Central-West Orana / REZ N3 / CNSW) has
    # NO own 2026 wind trace. The N3 wind-zone profile would be the ideal proxy
    # but the wind parser classifies the zone file as a zone (not project-
    # consumable). Proxy instead with co-located Spicers Creek WF (same REZ N3 +
    # subregion CNSW) — a regionally coherent stand-in, flagged pending Valley's
    # own trace in a future release.
    "Valley of the Winds": "Spicers_Creek",
}


def parse_2026_traces():
    _clean_dirs()
    with _vre_relabelled_to(TARGET_REFERENCE_YEAR), _demand_region_prefix_stripped():
        with _vre_maps_overridden():
            _parse_vre_split_by_filetype()
        _parse_step_change_demand()
    _optimise_into_get_data_layout()
    _remove_transient_flat_files()
    logging.info(f"Done. 2026 trace store written to {OUT}")


def _clean_dirs():
    for d in (OUT, FLAT):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)


def _parse_vre_split_by_filetype():
    """Parse solar + wind, keeping project and zone files in separate flat dirs.

    Solar and wind projects share the `project/` store (resource_type distinguishes
    them); likewise for zones. We parse with a file_type filter so the parser tags
    each parquet with the right `project`/`zone` column.
    """
    parse_solar_traces(SOLAR_CSV, FLAT / "project", use_concurrency=False,
                       filters=SolarMetadataFilter(file_type=["project"]))
    parse_wind_traces(WIND_CSV, FLAT / "project", use_concurrency=False,
                      filters=WindMetadataFilter(file_type=["project"]))
    parse_solar_traces(SOLAR_CSV, FLAT / "zone", use_concurrency=False,
                       filters=SolarMetadataFilter(file_type=["zone"]))
    parse_wind_traces(WIND_CSV, FLAT / "zone", use_concurrency=False,
                      filters=WindMetadataFilter(file_type=["zone"]))


def _parse_step_change_demand():
    """Parse Step Change demand (POE50, OPSO_MODELLING) across all subregion folders."""
    folders = sorted(INPUTS.glob("2026 ISP Demand Traces * Step Change"))
    demand_filter = DemandMetadataFilter(poe=["POE50"], demand_type=["OPSO_MODELLING"])
    for folder in folders:
        parse_demand_traces(folder, FLAT / "demand", use_concurrency=False,
                            filters=demand_filter)


def _optimise_into_get_data_layout():
    """Combine flat per-trace parquet into the hive-partitioned store get_data reads."""
    partition_traces_by_columns(f"{FLAT / 'project'}/*.parquet", str(OUT / "project"),
                                partition_cols=["reference_year"])
    partition_traces_by_columns(f"{FLAT / 'zone'}/*.parquet", str(OUT / "zone"),
                                partition_cols=["reference_year"])
    partition_traces_by_columns(f"{FLAT / 'demand'}/*.parquet", str(OUT / "demand"),
                                partition_cols=["scenario", "reference_year"])


def _remove_transient_flat_files():
    shutil.rmtree(FLAT)


# --- context managers: non-invasive workarounds (restored on exit) ----------------

def _extract_solar_trace_metadata_split_aware(filename):
    """isp_trace_parser's solar extractor, broadened to recognize trailing-letter
    zone codes — the 2026 Q8 split (Q8a/Q8b/Q8c). The bundled zone pattern's code
    group is `[A-Z0-9]+` (uppercase+digits only), so `REZ_Q8a_...` matches only
    `Q8` then fails the `_` boundary and falls through to the project pattern.
    The sole change is the code group -> `[A-Za-z0-9]+`; existing uppercase codes
    (N13, Q10, ...) match identically, so no regression to zones already parsing.
    """
    pattern1 = re.compile(
        r"^(?P<name>[A-Za-z0-9_\-]+)_(?P<resource_type>[A-Z]+)_RefYear(?P<reference_year>\d{4})\.csv$"
    )
    pattern2 = re.compile(
        r"^[A-Z]+_(?P<name>[A-Za-z0-9]+)_[A-Za-z0-9_\-]+_(?P<resource_type>[A-Z]+)_RefYear(?P<reference_year>\d{4})\.csv$"
    )
    m2 = pattern2.match(filename)
    if m2:
        d = m2.groupdict(); d["file_type"] = "zone"; d["reference_year"] = int(d["reference_year"]); return d
    m1 = pattern1.match(filename)
    if m1:
        d = m1.groupdict(); d["file_type"] = "project"; d["reference_year"] = int(d["reference_year"]); return d
    raise ValueError(f"Filename '{filename}' does not match the expected pattern")


def _extract_wind_trace_metadata_split_aware(filename):
    """isp_trace_parser's wind extractor, broadened to recognize trailing-letter
    zone codes (Q8a/b/c). Same single change as the solar version: the zone code
    group `[A-Z0-9]+` -> `[A-Za-z0-9]+`. This also recovers the WH/WM resource
    label for split zones (the original mis-read `Q8a_WH_...` as a project,
    losing WH/WM and tagging it generic WIND).
    """
    pattern1 = re.compile(r"^(?P<name>.*)_RefYear(?P<reference_year>\d{4})\.csv$")
    pattern2 = re.compile(
        r"^(?P<name>[A-Za-z0-9]+)_(?P<resource_type>W[A-Z]+)_[A-Za-z_\-]+_RefYear(?P<reference_year>\d{4})\.csv$"
    )
    m2 = pattern2.match(filename)
    if m2:
        d = m2.groupdict(); d["file_type"] = "zone"; d["reference_year"] = int(d["reference_year"]); return d
    m1 = pattern1.match(filename)
    if m1:
        d = m1.groupdict(); d["file_type"] = "project"; d["resource_type"] = "WIND"; d["reference_year"] = int(d["reference_year"]); return d
    raise ValueError(f"Filename '{filename}' does not match the expected pattern")


class _vre_relabelled_to:
    """Relabel the synthetic VRE reference year (5000) onto `year` during parsing.

    Also routes extraction through the split-aware extractors so the 2026 Q8-split
    zone codes (Q8a/b/c) parse as zones rather than mis-classified projects.
    """

    def __init__(self, year):
        self.year = year

    def __enter__(self):
        self._orig_solar = solar_traces.extract_solar_trace_metadata
        self._orig_wind = wind_traces.extract_wind_trace_metadata
        solar_traces.extract_solar_trace_metadata = self._relabel(_extract_solar_trace_metadata_split_aware)
        wind_traces.extract_wind_trace_metadata = self._relabel(_extract_wind_trace_metadata_split_aware)
        return self

    def __exit__(self, *exc):
        solar_traces.extract_solar_trace_metadata = self._orig_solar
        wind_traces.extract_wind_trace_metadata = self._orig_wind

    def _relabel(self, extractor):
        year = self.year

        def relabelled(filename):
            metadata = extractor(filename)
            if metadata["reference_year"] == SYNTHETIC_VRE_YEAR:
                metadata["reference_year"] = year
            return metadata

        return relabelled


class _demand_region_prefix_stripped:
    """Strip the 2026 "<REGION>_" filename prefix so the parser's regex matches."""

    def __enter__(self):
        self._orig = demand_traces.extract_demand_trace_metadata
        demand_traces.extract_demand_trace_metadata = self._strip(extract_demand_trace_metadata)
        return self

    def __exit__(self, *exc):
        demand_traces.extract_demand_trace_metadata = self._orig

    def _strip(self, extractor):
        def stripped(filename):
            # "NSW_CNSW_RefYear_..." -> "CNSW_RefYear_..."
            return extractor(filename.split("_", 1)[1])

        return stripped


class _vre_maps_overridden:
    """Apply the 2026 name overrides to the bundled maps, restoring after.

    Solar maps are flat {output: trace}; the wind project map is nested
    {output: {..., CSVFile: trace}} (or a list of such), so wind overrides set the
    CSVFile field rather than replacing the value.
    """

    def __enter__(self):
        self._backed_up = []
        self._apply("solar_project_mapping.yaml", SOLAR_PROJECT_OVERRIDES, nested=False)
        self._apply("solar_zone_mapping.yaml", SOLAR_ZONE_OVERRIDES, nested=False)
        self._apply("wind_project_mapping.yaml", WIND_PROJECT_OVERRIDES, nested=True)
        self._apply("wind_zone_mapping.yaml", WIND_ZONE_OVERRIDES, nested=False)
        return self

    def _apply(self, name, overrides, nested):
        path = MAPPING_DIR / name
        shutil.copy2(path, path.with_suffix(".yaml.bak"))
        self._backed_up.append(path)
        mapping = yaml.safe_load(path.read_text())
        for key, trace in overrides.items():
            if not nested:
                mapping[key] = trace
            elif key not in mapping:
                # New project absent from the bundled map: create the entry.
                # CSVFile is the only field consumed (restructure_wind_project_mapping).
                mapping[key] = {"CSVFile": trace}
            elif isinstance(mapping[key], list):
                for entry in mapping[key]:
                    entry["CSVFile"] = trace
            else:
                mapping[key]["CSVFile"] = trace
        path.write_text(yaml.safe_dump(mapping, sort_keys=True))

    def __exit__(self, *exc):
        for path in self._backed_up:
            shutil.move(str(path.with_suffix(".yaml.bak")), str(path))


if __name__ == "__main__":
    parse_2026_traces()
