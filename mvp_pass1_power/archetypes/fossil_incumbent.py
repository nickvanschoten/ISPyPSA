"""Fossil-incumbent archetype.

Pathway: constrains the model away from rapid renewable build, so the LP is
forced to rely on extended life of existing thermal and new gas to meet load.

Levers:
  1. Push every coal closure_year out by 10 years (subject to AEMO's legislated
     hard caps).
  2. Drop solar new-entrants and 75% of wind new-entrants from
     new_entrant_generators, simulating a constrained renewable-build pathway
     (limited transmission, social licence, planning bottlenecks).

This is a deliberately structural archetype — the IASR-economics-optimal
solution will still be renewables-dominated in the unconstrained REZ-build
sense, so we have to constrain the *option set* available to the LP. Drop-75%
on wind is a coarse lever; a production deployment would express the same
intent via custom_constraints_rhs build-rate caps that scale by year.
"""


def apply(ispypsa_tables, config):
    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"] + 10
    ispypsa_tables["ecaa_generators"] = ecaa

    ne = ispypsa_tables["new_entrant_generators"]
    drop_solar = ne["fuel_type"] == "Solar"
    wind_rows = ne[ne["fuel_type"] == "Wind"].sample(frac=0.75, random_state=0).index
    keep = ~drop_solar & ~ne.index.isin(wind_rows)
    ispypsa_tables["new_entrant_generators"] = ne[keep].copy()

    return ispypsa_tables
