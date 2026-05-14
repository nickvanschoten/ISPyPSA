"""Renewables-led archetype.

Pathway: forces the system away from new thermal new-entrants and accelerates
coal retirement, so the LP is pushed toward a wind+solar+storage+gas-peaking mix.

Levers (operate on ISPyPSA input CSVs between templater and translator):
  1. Drop new-entrant CCGT and OCGT (Gas) entries from new_entrant_generators
     so the LP cannot build new mid-merit/base-load gas. We keep new-entrant
     Hydrogen reciprocating engines as the firming option.
  2. Bring forward closure_year of every existing coal unit to 2035.

The IASR Step Change scenario already disallows new coal, so no separate coal
new-entrant lever is required. Renewable-share *enforcement* via a generation
share constraint is intentionally not added here — IASR-cost economics under
the above two levers produce a high-renewable pathway, and adding share
constraints risks over-determining the LP for an MVP. A production deployment
would add an explicit RES-share floor via custom_constraints_lhs.
"""


def apply(ispypsa_tables, config):
    ne = ispypsa_tables["new_entrant_generators"]
    ispypsa_tables["new_entrant_generators"] = ne[
        ~ne["technology_type"].isin(["OCGT (small GT)", "OCGT (large GT)", "CCGT"])
    ].copy()

    ecaa = ispypsa_tables["ecaa_generators"].copy()
    coal_mask = ecaa["fuel_type"].isin(["Black Coal", "Brown Coal"])
    ecaa.loc[coal_mask, "closure_year"] = ecaa.loc[coal_mask, "closure_year"].clip(upper=2035)
    ispypsa_tables["ecaa_generators"] = ecaa

    return ispypsa_tables
