"""Endogenous economic retirement of existing capacity (myopic chain).

The myopic chain models only AEMO's *scheduled* retirement: each existing unit
gets `lifetime = closure_year - build_year` and PyPSA deactivates it at end of
life (see src/ispypsa/translator/generators.py). There is no decision for the
optimiser to retire a unit *early* when a carbon price makes it uneconomic -- so
coal whose AEMO closure year sits past the modelling horizon (Callide C,
Millmerran: 2051) persists at every carbon price as idle-but-installed capacity,
the ~1.7 GW that holds the 2026 frontier ~3 pp below AEMO's all-coal-retired
outcome.

This module adds the missing decision as a CONTINUOUS (LP, not MIP) downward
capacity variable on the existing fleet:

    make_existing_reducible(): existing (non-extendable) generators become
    extendable with p_nom_min=0, p_nom_max=installed. `p_nom_opt < installed`
    IS the (partial) retirement. `capital_cost` is the per-MW cost of KEEPING
    the capacity each period -- Phase-1 strict passes 0 (confirms the mechanism
    with no economics); Phase-2 passes the AEMO fixed-OPEX so that shedding an
    idle unit pays (Phase 0 established the units are idle at $550, so the
    keeping-cost, not operation savings, is the driver).

Monotone non-increasing down the chain: the retained level from period t caps
period (t+1)'s p_nom_max via a parallel retention floor (`retention/<year>/`),
distinct from recursive_dynamic's new-build `tranches/`. Once retired, stays
retired. Existing units are re-templated each period (so scheduled closure still
applies); this layers ECONOMIC retirement on top of the scheduled retirement.

Composition: make_existing_reducible runs BEFORE inject_carried_tranches so it
targets only the templated ECAA fleet, not carried new-build vintages (which are
managed -- and retired at end-of-life -- by recursive_dynamic). Partial
retirement (retain 0.6 of a unit) is a mild physical fiction consistent with the
menu's soft compositions, and keeps the model LP rather than introducing an
integer open/closed variable.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pypsa

_FLOOR_FILENAME = "retained_existing.parquet"
_CUSTOM_CONSTRAINT_BUS = "bus_for_custom_constraint_gens"


def make_existing_reducible(
    generators: pd.DataFrame,
    existing_names: set[str] | list[str],
    retention_floor: dict[str, float] | None = None,
    keeping_cost_per_mw: float | dict[str, float] = 0.0,
) -> dict:
    """Turn the existing (ECAA) generators into a downward-only capacity
    decision. Mutates `generators` in place; returns a diagnostics dict.

    Targets EXACTLY the named ECAA fleet (`existing_names`, the templated
    ecaa_generators station names), NOT every `p_nom_extendable == False` row.
    That distinction matters: the unserved-energy / load-shedding pseudo-
    generators are also non-extendable (~100 GW each), and a Phase-2 FOM on
    those would let the LP "retire" load-shedding capacity. Targeting by ECAA
    name also keeps this consistent with extract_retained_existing (which uses
    the same name set). Call BEFORE inject_carried_tranches so carried new-build
    vintages are left fixed.
    """
    names_set = {str(n) for n in existing_names}
    existing = generators["name"].astype(str).isin(names_set)
    if "bus" in generators.columns:
        existing &= generators["bus"] != _CUSTOM_CONSTRAINT_BUS

    # Add bound columns for the whole table (inf/0 == PyPSA defaults, so
    # extendable new entrants and fixed carried rows are unaffected).
    if "p_nom_max" not in generators.columns:
        generators["p_nom_max"] = np.inf
    if "p_nom_min" not in generators.columns:
        generators["p_nom_min"] = 0.0

    installed = generators.loc[existing, "p_nom"].astype(float)
    cap = installed
    if retention_floor:
        prior = generators.loc[existing, "name"].map(retention_floor).astype(float)
        # Cap each unit at min(installed, prior-retained). A unit absent from the
        # floor (newly committed since the prior period) has NaN prior -> min
        # skips it -> keeps its installed cap.
        cap = pd.concat([installed, prior], axis=1).min(axis=1)

    generators.loc[existing, "p_nom_max"] = cap.to_numpy()
    generators.loc[existing, "p_nom_min"] = 0.0
    generators.loc[existing, "p_nom"] = cap.to_numpy()
    generators.loc[existing, "p_nom_extendable"] = True

    # Keeping cost: a per-unit FOM dict (Phase 2: each unit's own $/MW/yr) or a
    # scalar (Phase 1 strict: 0). As capital_cost it is multiplied by the
    # investment-period objective weighting (so it is a per-period recurring
    # cost) -- see the span-weighting override in instrumented_runner.
    if isinstance(keeping_cost_per_mw, dict):
        kc = generators.loc[existing, "name"].astype(str).map(keeping_cost_per_mw)
        kc = kc.fillna(0.0)
        generators.loc[existing, "capital_cost"] = kc.to_numpy()
        kc_summary = {"min": float(kc.min()), "max": float(kc.max()),
                      "mean": float(kc.mean())}
    else:
        generators.loc[existing, "capital_cost"] = float(keeping_cost_per_mw)
        kc_summary = float(keeping_cost_per_mw)

    return {
        "reducible_existing_units": int(existing.sum()),
        "reducible_existing_installed_mw": float(installed.sum()),
        "capped_below_installed_mw": float(
            (installed.to_numpy() - cap.to_numpy()).sum()
        ),
        "keeping_cost_per_mw": kc_summary,
        "monotone_floor_applied": bool(retention_floor),
    }


def extract_retained_existing(run_root: Path, year: int) -> dict[str, float]:
    """After a period solve, return `{existing_unit: retained p_nom_opt}` for the
    ECAA fleet.

    Existing units are identified by the period's templated `ecaa_generators`
    list (re-templated each period with stable station names), intersected with
    the solved network's generators. `year` is accepted for symmetry with
    recursive_dynamic and to make the call site self-documenting.
    """
    nc_path = run_root / "outputs" / "capacity_expansion.nc"
    ecaa_path = run_root / "ispypsa_inputs" / "ecaa_generators.csv"
    network = pypsa.Network(nc_path)
    ecaa_names = set(pd.read_csv(ecaa_path)["generator"].astype(str))
    gens = network.generators
    retained = gens.index[gens.index.astype(str).isin(ecaa_names)]
    return {str(n): float(gens.at[n, "p_nom_opt"]) for n in retained}


def save_retention_floor(
    floor: dict[str, float], retention_root: Path, year: int
) -> None:
    """Persist a year-T retained-existing floor under `retention_root/<year>/`."""
    out_dir = retention_root / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {"name": list(floor.keys()), "retained_mw": list(floor.values())}
    )
    df.to_parquet(out_dir / _FLOOR_FILENAME, index=False)


def load_retention_floor(
    retention_root: Path, before_year: int
) -> dict[str, float]:
    """Return the most-recent prior period's retained levels (the tightest
    monotone cap). Empty if no prior period exists.

    Because retention is monotone non-increasing by construction (each period
    caps at the prior), the latest prior period's floor is the binding one.
    """
    if not retention_root.exists():
        return {}
    priors = sorted(
        int(d.name)
        for d in retention_root.iterdir()
        if d.is_dir() and d.name.isdigit() and int(d.name) < before_year
    )
    if not priors:
        return {}
    df = pd.read_parquet(retention_root / str(priors[-1]) / _FLOOR_FILENAME)
    return dict(zip(df["name"].astype(str), df["retained_mw"].astype(float)))
