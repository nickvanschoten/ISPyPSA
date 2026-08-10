"""Derive AEMO 2026 draft ISP (CDP4 ODP) annual capacity factors per carrier.

Phase 7.3.1 evidence. Reads the team-supplied AEMO 2026 draft capacity + energy
exports and derives annual CF = energy_TWh / (capacity_GW x 8.76) per carrier
per year, for the step_change scenario (the catalogue's anchoring scenario).

Caveat: AEMO publishes capacity (GW) and energy (TWh) rounded to integers, so
CFs for small/declining carriers (late-life coal, gas peakers) carry rounding
noise. Direction and scale are robust; treat 2-decimal CFs as indicative.

AEMO category -> deliverable carrier mapping is NOT 1:1 (AEMO has a single
"Coal"; the deliverable splits Black/Brown. AEMO "Hydro" bundles conventional +
PHES. AEMO has no Nuclear / Hydrogen / Hyblend / Battery). See
PHASE7_3_1_CF_INVENTORY.md for the per-carrier fallback decisions.
"""

import pandas as pd

OUT_DIR = "iasr outputs"
SCENARIO = "step_change"
MILESTONES = [2025, 2030, 2035, 2040, 2045, 2050]
CARRIERS = ["Coal", "Gas", "Hydro", "Wind", "Solar (Utility)", "Bioenergy", "Distillate"]


def _load(kind: str) -> pd.DataFrame:
    path = f"{OUT_DIR}/NEM-aemo2026draft-{SCENARIO}-CDP4 (ODP)-{kind}.csv"
    df = pd.read_csv(path)
    df["yr"] = pd.to_datetime(df["date"], format="%d %b %Y %I:%M %p").dt.year
    return df.set_index("yr")


def main():
    cap = _load("capacity")  # GW
    en = _load("energy")     # TWh
    print(f"AEMO 2026 draft ISP ({SCENARIO}) — annual CF = energy / (capacity x 8.76)\n")
    print(f"{'carrier':16}" + "".join(f"{y:>8}" for y in MILESTONES))
    for c in CARRIERS:
        row = f"{c:16}"
        for y in MILESTONES:
            cg, eg = cap.loc[y, c], en.loc[y, c]
            row += f"{eg / (cg * 8.76):8.3f}" if cg > 0 else f"{'ret/0':>8}"
        print(row)


if __name__ == "__main__":
    main()
