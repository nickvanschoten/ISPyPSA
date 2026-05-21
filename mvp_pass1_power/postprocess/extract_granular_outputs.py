"""Extract per-archetype per-period operational metrics from solved PyPSA Networks.

Public interface:
  extract_granular_period(network, period, archetype_id) -> dict
  emit_granular_outputs(runs_dir, workbook_cache, out_dir, archetype_catalogue) -> None

Renewable carriers (for renewable_share calculation):
  Wind, Solar, Biomass.
  Water (hydro) is excluded: ISPyPSA does not load hydro availability traces, so
  all Water generators run with p_max_pu=1.0 and zero marginal cost. The LP
  dispatches them at near-100% capacity factor (~60 TWh/year vs ~15-17 TWh
  realistic NEM-wide), making Water generation an unreliable renewable-share
  indicator. Including it would inflate renewable_share_pct by ~20 percentage
  points relative to a physically-constrained model.
  Nuclear and Hydrogen are also excluded — nuclear is debated in the Australian
  context and H2 has upstream embodied emissions outside this module's scope.

Storage units (network.storage_units) are excluded from generation totals —
they shift electricity temporally without producing it.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd
import pypsa

log = logging.getLogger(__name__)

_RENEWABLE_CARRIERS = {"Wind", "Solar", "Biomass"}


# ---------- helpers (≤10 lines each) ----------


def _period_snapshots(network: pypsa.Network, period: int) -> list:
    """Snapshots belonging to a given investment period."""
    return [s for s in network.snapshots if s[0] == period]


def _period_weightings(network: pypsa.Network, period: int) -> pd.Series:
    """Snapshot weightings (hours/snapshot) for a given period."""
    snaps = _period_snapshots(network, period)
    return network.snapshot_weightings.loc[snaps, "generators"]


def _real_generator_mask(network: pypsa.Network) -> pd.Series:
    """Boolean mask for generators that represent real assets."""
    g = network.generators
    return (g["bus"] != "bus_for_custom_constraint_gens") & (g["carrier"] != "Unserved Energy")


def _annual_generation_by_carrier(network: pypsa.Network, period: int) -> pd.Series:
    """Annual MWh dispatched per carrier for the given period."""
    mask = _real_generator_mask(network)
    real_gens = network.generators[mask]
    snaps = _period_snapshots(network, period)
    weights = _period_weightings(network, period)
    gen_t = network.generators_t.p.loc[snaps, real_gens.index].clip(lower=0)
    annual_mwh = gen_t.mul(weights, axis=0).sum()
    return annual_mwh.groupby(real_gens["carrier"]).sum()


def _annual_capacity_by_carrier(network: pypsa.Network, period: int) -> pd.Series:
    """Optimal installed capacity (MW) per carrier, active in the given period."""
    mask = _real_generator_mask(network)
    g = network.generators[mask]
    active = g[
        (g["build_year"].fillna(0) <= period)
        & (g["build_year"].fillna(0) + g["lifetime"].fillna(0) > period)
    ]
    return active.groupby("carrier")["p_nom_opt"].sum()


def _annual_demand_mwh(network: pypsa.Network, period: int) -> float:
    """Total annual load MWh for the given period."""
    snaps = _period_snapshots(network, period)
    weights = _period_weightings(network, period)
    return float((network.loads_t.p_set.loc[snaps].sum(axis=1) * weights).sum())


def _storage_totals(network: pypsa.Network) -> tuple[float, float]:
    """Total storage power (GW) and energy (GWh) from storage_units."""
    su = network.storage_units
    if su.empty or "p_nom_opt" not in su.columns:
        return 0.0, 0.0
    power_gw = float(su["p_nom_opt"].sum() / 1000.0)
    energy_gwh = float((su["p_nom_opt"] * su.get("max_hours", 0)).sum() / 1000.0)
    return power_gw, energy_gwh


def _capacity_factors(
    generation_mwh: pd.Series, capacity_mw: pd.Series, period_hours: float
) -> pd.Series:
    """Capacity factor per carrier = generation / (capacity × hours)."""
    gen, cap = generation_mwh.align(capacity_mw, fill_value=0.0)
    max_possible = cap * period_hours
    cf = gen.div(max_possible).replace([float("inf"), -float("inf")], 0.0)
    return cf[cap > 0].dropna()


def _period_hours(network: pypsa.Network, period: int) -> float:
    """Sum of snapshot weightings — the effective hours in the period."""
    return float(_period_weightings(network, period).sum())


# ---------- public orchestrators ----------


def extract_granular_period(
    network: pypsa.Network,
    period: int,
    archetype_id: str,
) -> dict:
    """Return dict of granular metrics for one archetype-period.

    Keys:
      capacity_gw           — list of {technology, capacity_gw}
      generation_twh        — list of {technology, generation_twh}
      storage_power_gw      — float
      storage_energy_gwh    — float
      demand_twh            — float
      total_generation_twh  — float
      supply_gap_pct        — float (positive = over-generation)
      capacity_factors      — list of {technology, capacity_factor}
      renewable_share_pct   — float
    """
    gen_mwh = _annual_generation_by_carrier(network, period)
    cap_mw = _annual_capacity_by_carrier(network, period)
    demand_mwh = _annual_demand_mwh(network, period)
    total_gen_mwh = float(gen_mwh.sum())
    storage_gw, storage_gwh = _storage_totals(network)
    hours = _period_hours(network, period)
    cf = _capacity_factors(gen_mwh, cap_mw, hours)

    renewable_mwh = float(gen_mwh[gen_mwh.index.isin(_RENEWABLE_CARRIERS)].sum())
    renewable_share = (renewable_mwh / total_gen_mwh * 100.0) if total_gen_mwh > 0 else 0.0
    supply_gap = ((total_gen_mwh - demand_mwh) / demand_mwh * 100.0) if demand_mwh > 0 else 0.0

    return {
        "archetype_id": archetype_id,
        "period": period,
        "capacity_gw": [
            {"technology": c, "capacity_gw": float(v) / 1000.0}
            for c, v in cap_mw.items()
        ],
        "generation_twh": [
            {"technology": c, "generation_twh": float(v) / 1e6}
            for c, v in gen_mwh.items()
        ],
        "storage_power_gw": storage_gw,
        "storage_energy_gwh": storage_gwh,
        "demand_twh": demand_mwh / 1e6,
        "total_generation_twh": total_gen_mwh / 1e6,
        "supply_gap_pct": supply_gap,
        "capacity_factors": [
            {"technology": c, "capacity_factor": float(v)}
            for c, v in cf.items()
        ],
        "renewable_share_pct": renewable_share,
    }


def emit_granular_outputs(
    runs_dir: Path,
    workbook_cache: Path,
    out_dir: Path,
    archetype_catalogue: list[str],
) -> None:
    """Scan runs_dir for per-period myopic networks, extract granular metrics, write CSVs.

    Expects run directories named {run_id}_{year}__{archetype_id}. Writes 6 CSVs to out_dir:
      capacity_gw.csv, generation_twh.csv, storage_capacity.csv,
      demand_generation.csv, capacity_factors.csv, renewable_share.csv.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    records = _scan_run_directories(runs_dir, archetype_catalogue)
    all_metrics = [_load_and_extract(rec) for rec in records if rec is not None]
    all_metrics = [m for m in all_metrics if m is not None]

    _write_capacity_csv(all_metrics, out_dir)
    _write_generation_csv(all_metrics, out_dir)
    _write_storage_csv(all_metrics, out_dir)
    _write_demand_generation_csv(all_metrics, out_dir)
    _write_capacity_factors_csv(all_metrics, out_dir)
    _write_renewable_share_csv(all_metrics, out_dir)


def _scan_run_directories(runs_dir: Path, archetype_catalogue: list[str]) -> list[dict]:
    """Find all period-level run directories matching the naming convention."""
    pattern = re.compile(r"^(.+)_(\d{4})__(.+)$")
    found = []
    for d in runs_dir.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if not m:
            continue
        archetype_id = m.group(3)
        if archetype_id not in archetype_catalogue:
            continue
        nc_path = d / "outputs" / "capacity_expansion.nc"
        if not nc_path.exists():
            continue
        found.append({
            "run_dir": d,
            "year": int(m.group(2)),
            "archetype_id": archetype_id,
            "nc_path": nc_path,
        })
    return found


def _load_and_extract(rec: dict) -> dict | None:
    """Load a period network and return granular metrics, or None on failure."""
    network = pypsa.Network(str(rec["nc_path"]))
    period = rec["year"]
    if period not in network.investment_periods:
        return None
    return extract_granular_period(network, period, rec["archetype_id"])


def _write_capacity_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write capacity_gw.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "technology": c["technology"], "capacity_gw": c["capacity_gw"]}
        for m in metrics for c in m["capacity_gw"]
    ]
    pd.DataFrame(rows).to_csv(out_dir / "capacity_gw.csv", index=False)


def _write_generation_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write generation_twh.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "technology": g["technology"], "generation_twh": g["generation_twh"]}
        for m in metrics for g in m["generation_twh"]
    ]
    pd.DataFrame(rows).to_csv(out_dir / "generation_twh.csv", index=False)


def _write_storage_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write storage_capacity.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "storage_power_gw": m["storage_power_gw"],
         "storage_energy_gwh": m["storage_energy_gwh"]}
        for m in metrics
    ]
    pd.DataFrame(rows).to_csv(out_dir / "storage_capacity.csv", index=False)


def _write_demand_generation_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write demand_generation.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "demand_twh": m["demand_twh"],
         "total_generation_twh": m["total_generation_twh"],
         "supply_gap_pct": m["supply_gap_pct"]}
        for m in metrics
    ]
    pd.DataFrame(rows).to_csv(out_dir / "demand_generation.csv", index=False)


def _write_capacity_factors_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write capacity_factors.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "technology": cf["technology"], "capacity_factor": cf["capacity_factor"]}
        for m in metrics for cf in m["capacity_factors"]
    ]
    pd.DataFrame(rows).to_csv(out_dir / "capacity_factors.csv", index=False)


def _write_renewable_share_csv(metrics: list[dict], out_dir: Path) -> None:
    """Write renewable_share.csv."""
    rows = [
        {"archetype_id": m["archetype_id"], "year": m["period"],
         "renewable_share_pct": m["renewable_share_pct"]}
        for m in metrics
    ]
    pd.DataFrame(rows).to_csv(out_dir / "renewable_share.csv", index=False)


# ---------- CLI ----------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract granular operational metrics from solved ISPyPSA myopic runs."
    )
    parser.add_argument("--runs-dir", required=True, type=Path)
    parser.add_argument("--workbook-cache", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    catalogue = [
        "cost_optimal", "fast_fossil_exit", "gas_bridge",
        "storage_led", "fossil_incumbent", "nuclear_included",
    ]
    log.info(f"Scanning {args.runs_dir} for archetypes: {catalogue}")
    emit_granular_outputs(args.runs_dir, args.workbook_cache, args.out, catalogue)
    log.info(f"Granular outputs written to {args.out}")


if __name__ == "__main__":
    main()
