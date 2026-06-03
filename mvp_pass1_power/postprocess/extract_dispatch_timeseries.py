"""Extract per-snapshot dispatch / storage / load time series for the dashboard.

Public interface:
  list_available_runs(runs_dir)              -> {run_id_prefix: {archetype: [years]}}
  find_run_dir(runs_dir, prefix, arch, year) -> Path | None
  extract_dispatch_timeseries(run_dir)       -> dict | None

The returned dict (one investment period's worth of snapshots) keys:
  dispatch_by_carrier — DataFrame: real generators grouped by carrier, MW, clipped >=0
  storage_dispatch    — DataFrame: storage_units_t.p clipped >=0 (positive discharge)
  storage_charge      — DataFrame: -storage_units_t.p clipped >=0 (positive charge)
  demand              — Series: loads_t.p_set summed across loads, MW
  curtailment         — DataFrame: (p_max_pu * p_nom_opt - dispatch) summed by VRE carrier
  period_hours        — Series: snapshot_weightings.generators (hours per snapshot)
  snapshot_index      — DatetimeIndex (flattened from (period, snapshot) MultiIndex)

Run-dir naming convention matches extract_granular_outputs._scan_run_directories:
{run_id_prefix}_{year}__{archetype_id} with the solved network at
outputs/capacity_expansion.nc.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pypsa

_RUN_DIR_PATTERN = re.compile(r"^(.+)_(\d{4})__(.+)$")
_VRE_CARRIERS = {"Wind", "Solar"}


# ---------- run-dir discovery helpers ----------


def _parse_run_dir_name(dir_name: str) -> tuple[str, int, str] | None:
    m = _RUN_DIR_PATTERN.match(dir_name)
    if not m:
        return None
    return m.group(1), int(m.group(2)), m.group(3)


def _network_path(run_dir: Path) -> Path:
    return run_dir / "outputs" / "capacity_expansion.nc"


def _iter_solved_runs(runs_dir: Path):
    """Yield (run_dir, (prefix, year, archetype)) for dirs with a solved NetCDF."""
    if not runs_dir.exists():
        return
    for d in sorted(runs_dir.iterdir()):
        if not d.is_dir():
            continue
        parsed = _parse_run_dir_name(d.name)
        if parsed is None or not _network_path(d).exists():
            continue
        yield d, parsed


# ---------- timeseries-extraction helpers ----------


def _period_snapshots(network: pypsa.Network, period: int) -> list:
    return [s for s in network.snapshots if s[0] == period]


def _flatten_snapshots(snaps: list) -> pd.DatetimeIndex:
    if snaps and isinstance(snaps[0], tuple):
        return pd.DatetimeIndex([s[1] for s in snaps])
    return pd.DatetimeIndex(snaps)


def _real_generator_mask(network: pypsa.Network) -> pd.Series:
    g = network.generators
    return (g["bus"] != "bus_for_custom_constraint_gens") & (g["carrier"] != "Unserved Energy")


def _dispatch_by_carrier(network, snaps, flat_idx) -> pd.DataFrame:
    real = network.generators[_real_generator_mask(network)]
    dispatch = network.generators_t.p.loc[snaps, real.index].clip(lower=0)
    by_carrier = dispatch.T.groupby(real["carrier"]).sum().T
    by_carrier.index = flat_idx
    return by_carrier


def _storage_dispatch_and_charge(network, snaps, flat_idx) -> tuple[pd.DataFrame, pd.DataFrame]:
    if network.storage_units.empty:
        empty = pd.DataFrame(index=flat_idx)
        return empty, empty
    p = network.storage_units_t.p.loc[snaps].copy()
    p.index = flat_idx
    return p.clip(lower=0), (-p).clip(lower=0)


def _demand_series(network, snaps, flat_idx) -> pd.Series:
    demand = network.loads_t.p_set.loc[snaps].sum(axis=1)
    demand.index = flat_idx
    return demand


def _curtailment_by_carrier(network, snaps, flat_idx) -> pd.DataFrame:
    real_mask = _real_generator_mask(network)
    vre = network.generators[real_mask & network.generators["carrier"].isin(_VRE_CARRIERS)]
    if vre.empty:
        return pd.DataFrame(index=flat_idx)
    available_pu = network.get_switchable_as_dense("Generator", "p_max_pu").loc[snaps, vre.index]
    available_mw = available_pu.multiply(vre["p_nom_opt"], axis=1)
    dispatched = network.generators_t.p.loc[snaps, vre.index].clip(lower=0)
    curtailed = (available_mw - dispatched).clip(lower=0)
    by_carrier = curtailed.T.groupby(vre["carrier"]).sum().T
    by_carrier.index = flat_idx
    return by_carrier


def _period_hours_series(network, snaps, flat_idx) -> pd.Series:
    hours = network.snapshot_weightings.loc[snaps, "generators"].copy()
    hours.index = flat_idx
    return hours


# ---------- public interface ----------


def list_available_runs(runs_dir: Path) -> dict[str, dict[str, list[int]]]:
    """Group solved run dirs as {run_id_prefix: {archetype: sorted [years]}}."""
    out: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for _, (prefix, year, archetype) in _iter_solved_runs(runs_dir):
        out[prefix][archetype].append(year)
    return {p: {a: sorted(ys) for a, ys in archs.items()} for p, archs in out.items()}


def find_run_dir(runs_dir: Path, run_id_prefix: str, archetype: str, year: int) -> Path | None:
    """Find the unique run directory for (prefix, archetype, year), or None."""
    for d, (prefix, y, a) in _iter_solved_runs(runs_dir):
        if prefix == run_id_prefix and a == archetype and y == year:
            return d
    return None


def extract_dispatch_timeseries(run_dir: Path) -> dict | None:
    """Load the solved NetCDF and return per-snapshot dispatch / storage / load / curtailment.

    Returns None if the NetCDF is missing, the directory name doesn't match the
    expected pattern, or the encoded year is not in network.investment_periods.
    """
    nc_path = _network_path(run_dir)
    if not nc_path.exists():
        return None
    parsed = _parse_run_dir_name(run_dir.name)
    if parsed is None:
        return None
    _, year, _ = parsed
    network = pypsa.Network(str(nc_path))
    if year not in network.investment_periods:
        return None
    snaps = _period_snapshots(network, year)
    flat_idx = _flatten_snapshots(snaps)
    storage_dispatch, storage_charge = _storage_dispatch_and_charge(network, snaps, flat_idx)
    return {
        "dispatch_by_carrier": _dispatch_by_carrier(network, snaps, flat_idx),
        "storage_dispatch": storage_dispatch,
        "storage_charge": storage_charge,
        "demand": _demand_series(network, snaps, flat_idx),
        "curtailment": _curtailment_by_carrier(network, snaps, flat_idx),
        "period_hours": _period_hours_series(network, snaps, flat_idx),
        "snapshot_index": flat_idx,
    }
