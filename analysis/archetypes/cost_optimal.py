"""Cost-optimal archetype: the unmodified Step Change configuration.

Serves as both the calibration baseline (against AEMO's published 2024 ISP
Step Change outputs) and as the reference archetype for the simple-msm
output_cost_per_unit benchmark.
"""


def apply(ispypsa_tables, config):
    return ispypsa_tables
