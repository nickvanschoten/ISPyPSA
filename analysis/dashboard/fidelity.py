"""Fidelity-state registry for the interactive product.

Every result shown to the user carries a fidelity badge: `solved` (a real
converged frontier cell), `interpolated` (Stage 1+: between grid points), or
`screening` (Stage 2+: a reduced-fidelity fast solve). Stage 0 only produces
`solved` results, but the registry and badge helper exist now so later stages
add manifest rows, not new UI plumbing.
"""

from __future__ import annotations

import streamlit as st

FIDELITY_STATES = {
    "solved": {
        "label": "Solved",
        "colour": "#1f77b4",
        "help": "A real converged full-year LP solve at this exact carbon price and year.",
    },
    "interpolated": {
        "label": "Interpolated",
        "colour": "#9467bd",
        "help": "Estimated between solved grid points — not an independent solve.",
    },
    "screening": {
        "label": "Screening",
        "colour": "#d62728",
        "help": "A reduced-fidelity fast solve for exploration only — not production-grade.",
    },
}


def render_fidelity_badge(fidelity: str) -> None:
    """Render a small coloured badge + help tooltip next to a displayed result."""
    state = FIDELITY_STATES[fidelity]
    st.markdown(
        f"<span style='background-color:{state['colour']}; color:white; "
        f"padding:2px 8px; border-radius:10px; font-size:0.8em;' "
        f"title='{state['help']}'>{state['label']}</span>",
        unsafe_allow_html=True,
    )
