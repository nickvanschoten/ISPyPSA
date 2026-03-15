"""
Temporal Clustering & Profile Shaping for Multi-Period NEM Optimization.

Provides:
- K-Medoids representative week selection with extreme event preservation
- Synthetic EV charging and rooftop solar profiles with volume conservation
"""

import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. K-Medoids Representative Week Selection
# ---------------------------------------------------------------------------

def _daily_profiles(series: pd.DataFrame, bus_weightings: pd.Series | None = None) -> np.ndarray:
    """
    Reshape an hourly time-series DataFrame into a (n_days, 24) matrix.
    Each row is one day's 24-hour profile.
    If bus_weightings is provided, calculates a weighted sum across buses
    to ensure peak signals (10% POE) are prioritized in clustering.
    """
    if bus_weightings is not None:
        # Align indices and multiply
        common = series.columns.intersection(bus_weightings.index)
        weighted = series[common].multiply(bus_weightings[common], axis=1)
        total_hourly = weighted.sum(axis=1).values
    else:
        total_hourly = series.sum(axis=1).values

    n_full_days = len(total_hourly) // 24
    return total_hourly[: n_full_days * 24].reshape(n_full_days, 24)


def _kmedoids_select(daily_matrix: np.ndarray, n_clusters: int, max_iter: int = 100) -> np.ndarray:
    """
    Simple K-Medoids (PAM-style) clustering on daily profiles.
    Returns the indices of the n_clusters medoid days.

    Unlike K-Means, medoids are always actual observed days, preserving
    real temporal correlation within each representative day.
    """
    n_days = daily_matrix.shape[0]
    if n_days <= n_clusters:
        return np.arange(n_days)

    rng = np.random.RandomState(42)
    medoid_indices = rng.choice(n_days, size=n_clusters, replace=False)

    for _ in range(max_iter):
        # Assign each day to nearest medoid (L2 distance)
        distances = np.array([
            np.linalg.norm(daily_matrix - daily_matrix[m], axis=1)
            for m in medoid_indices
        ])  # shape: (n_clusters, n_days)
        assignments = distances.argmin(axis=0)

        # Update medoids: for each cluster, pick the day that minimises
        # sum of distances to all other days in the cluster
        new_medoids = np.copy(medoid_indices)
        for k in range(n_clusters):
            cluster_members = np.where(assignments == k)[0]
            if len(cluster_members) == 0:
                continue
            intra_dist = np.array([
                np.sum(np.linalg.norm(
                    daily_matrix[cluster_members] - daily_matrix[c], axis=1
                ))
                for c in cluster_members
            ])
            new_medoids[k] = cluster_members[intra_dist.argmin()]

        if np.array_equal(new_medoids, medoid_indices):
            break
        medoid_indices = new_medoids

    return np.sort(medoid_indices)


def _find_extreme_days(daily_matrix: np.ndarray, vre_daily: np.ndarray | None = None) -> list[int]:
    """
    Identify the 3 critical extreme days:
      1. Peak demand day (highest daily sum)
      2. Minimum VRE day (lowest daily VRE generation sum, if available)
      3. Maximum ramp day (largest hour-to-hour demand change)
    Returns a list of day indices.
    """
    extremes = []

    # 1. Peak demand day
    daily_sums = daily_matrix.sum(axis=1)
    peak_day = int(np.argmax(daily_sums))
    extremes.append(peak_day)

    # 2. Minimum VRE day
    if vre_daily is not None and len(vre_daily) > 0:
        vre_sums = vre_daily.sum(axis=1)
        min_vre_day = int(np.argmin(vre_sums))
        if min_vre_day not in extremes:
            extremes.append(min_vre_day)

    # 3. Maximum ramp day (largest absolute hour-to-hour change within a day)
    max_ramps = np.max(np.abs(np.diff(daily_matrix, axis=1)), axis=1)
    max_ramp_day = int(np.argmax(max_ramps))
    if max_ramp_day not in extremes:
        extremes.append(max_ramp_day)

    return extremes


def cluster_to_representative_weeks(
    loads_t: pd.DataFrame,
    n_weeks: int = 3,
    vre_t: pd.DataFrame | None = None,
    bus_weightings: pd.Series | None = None,
) -> tuple[list[int], dict[int, float]]:
    """
    Select representative days via K-Medoids + forced extreme-day append.
    Supports both single-period (flat index) and multi-period (MultiIndex).
    Uses bus_weightings to ensure peak demand signals (10% POE) are preserved.

    Parameters
    ----------
    loads_t : pd.DataFrame
        Hourly load profiles (snapshots × buses).
    n_weeks : int
        Number of representative weeks to select per period.
    vre_t : pd.DataFrame or None
        Optional hourly VRE generation profiles for extreme-event detection.
    bus_weightings : pd.Series or None
        Optional weights per bus to prioritize peak signals in clustering.

    Returns
    -------
    selected_hours : list[int]
        Indices into the original hourly index for the selected snapshots.
    snapshot_weightings : dict[int, float]
        Mapping from original hour index → snapshot weighting.
    """
    is_multi = isinstance(loads_t.index, pd.MultiIndex)
    
    if not is_multi:
        return _cluster_single_period(loads_t, n_weeks, vre_t, bus_weightings)
    
    # Multi-period: iterate through periods and aggregate
    periods = loads_t.index.get_level_values(0).unique()
    all_selected_hours = []
    all_snapshot_weightings = {}
    
    # We must track the integer position in the original index
    original_indices = np.arange(len(loads_t))
    
    for period in periods:
        period_mask = loads_t.index.get_level_values(0) == period
        period_loads = loads_t.loc[period_mask]
        period_vre = vre_t.loc[period_mask] if vre_t is not None else None
        period_int_indices = original_indices[period_mask]
        
        # Cluster this period
        sel_hours_local, weightings_local = _cluster_single_period(
            period_loads, n_weeks, period_vre, bus_weightings
        )
        
        # Map local integer hours back to global integer hours
        for h_local in sel_hours_local:
            global_h = period_int_indices[h_local]
            all_selected_hours.append(global_h)
            all_snapshot_weightings[global_h] = weightings_local[h_local]
            
    logger.info(
        f"Multi-period clustering: {len(periods)} periods, "
        f"{len(all_selected_hours)} total snapshots selected."
    )
    return all_selected_hours, all_snapshot_weightings


def _cluster_single_period(
    loads_t: pd.DataFrame,
    n_weeks: int = 3,
    vre_t: pd.DataFrame | None = None,
    bus_weightings: pd.Series | None = None,
) -> tuple[list[int], dict[int, float]]:
    """Internal helper to cluster a single contiguous block of hours."""
    daily_matrix = _daily_profiles(loads_t, bus_weightings)
    n_representative_days = n_weeks * 7

    # VRE daily profiles for extreme detection
    vre_daily = None
    if vre_t is not None and not vre_t.empty:
        vre_daily = _daily_profiles(vre_t)

    # Stage 1: K-Medoids selection
    medoid_days = _kmedoids_select(daily_matrix, n_representative_days)

    # Stage 2: Extreme event append
    extreme_days = _find_extreme_days(daily_matrix, vre_daily)
    all_selected_days = list(medoid_days)
    for ed in extreme_days:
        if ed not in all_selected_days:
            all_selected_days.append(ed)

    all_selected_days = sorted(all_selected_days)

    # Compute cluster assignments for weighting
    distances = np.array([
        np.linalg.norm(daily_matrix - daily_matrix[m], axis=1)
        for m in medoid_days
    ])
    assignments = distances.argmin(axis=0)
    cluster_sizes = np.bincount(assignments, minlength=len(medoid_days))

    # Build hour-level indices and weightings
    selected_hours = []
    snapshot_weightings = {}

    for day_idx in all_selected_days:
        start_hour = day_idx * 24
        end_hour = start_hour + 24
        hours = list(range(start_hour, min(end_hour, len(loads_t))))

        if day_idx in medoid_days:
            medoid_pos = list(medoid_days).index(day_idx)
            weight = float(cluster_sizes[medoid_pos])
        else:
            # Extreme day: represents only itself
            weight = 1.0

        for h in hours:
            selected_hours.append(h)
            snapshot_weightings[h] = weight

    return selected_hours, snapshot_weightings


# ---------------------------------------------------------------------------
# 2. Synthetic Load Profile Shapes (Volume-Conserving)
# ---------------------------------------------------------------------------

# Normalised 24h EV charging shape: low overnight, ramp AM, evening peak 5-10pm
_EV_HOURLY_SHAPE = np.array([
    0.02, 0.02, 0.02, 0.02, 0.02, 0.03,   # 00:00 – 05:00
    0.04, 0.05, 0.06, 0.04, 0.03, 0.03,   # 06:00 – 11:00
    0.03, 0.03, 0.04, 0.05, 0.07, 0.10,   # 12:00 – 17:00
    0.12, 0.10, 0.07, 0.05, 0.03, 0.02,   # 18:00 – 23:00
])
_EV_HOURLY_SHAPE = _EV_HOURLY_SHAPE / _EV_HOURLY_SHAPE.sum()

# Normalised 24h rooftop solar shape: zero at night, midday peak
_SOLAR_HOURLY_SHAPE = np.array([
    0.00, 0.00, 0.00, 0.00, 0.00, 0.01,   # 00:00 – 05:00
    0.04, 0.08, 0.12, 0.14, 0.15, 0.15,   # 06:00 – 11:00
    0.14, 0.12, 0.08, 0.04, 0.01, 0.00,   # 12:00 – 17:00
    0.00, 0.00, 0.00, 0.00, 0.00, 0.00,   # 18:00 – 23:00
])
_SOLAR_HOURLY_SHAPE = _SOLAR_HOURLY_SHAPE / _SOLAR_HOURLY_SHAPE.sum()


def generate_ev_charging_profile(n_hours: int) -> np.ndarray:
    """
    Generate a normalised EV charging profile (evening-peaked).
    The returned array has unit sum per day, tiled to n_hours.
    """
    return np.tile(_EV_HOURLY_SHAPE, n_hours // 24 + 1)[:n_hours]


def generate_rooftop_solar_profile(n_hours: int) -> np.ndarray:
    """
    Generate a normalised rooftop solar profile (midday-peaked).
    The returned array has unit sum per day, tiled to n_hours.
    """
    return np.tile(_SOLAR_HOURLY_SHAPE, n_hours // 24 + 1)[:n_hours]


def scale_profile_to_volume(
    profile: np.ndarray,
    target_annual_mwh: float,
) -> np.ndarray:
    """
    Rescale a normalised profile so its total sum equals target_annual_mwh.
    Raises AssertionError if volume conservation is violated.
    """
    if profile.sum() == 0:
        logger.warning("Profile sums to zero; returning zeros.")
        return np.zeros_like(profile)

    scaled = profile * (target_annual_mwh / profile.sum())

    assert abs(scaled.sum() - target_annual_mwh) < 1e-3, (
        f"Volume conservation violated: {scaled.sum():.6f} MWh vs "
        f"{target_annual_mwh:.6f} MWh target"
    )

    return scaled
