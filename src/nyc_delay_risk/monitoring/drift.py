import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from ..db import get_conn


def compute_psi(actual_values, expected_values, bins=10):
    """Compute Population Stability Index (PSI) between actual and expected distributions.
    
    PSI = sum((actual_pct - expected_pct) * ln(actual_pct / expected_pct))
    
    Args:
        actual_values: Array of actual feature values
        expected_values: Array of expected/baseline feature values
        bins: Number of bins for discretization (default 10)
    
    Returns:
        float: PSI value
    """
    epsilon = 1e-6
    
    # Determine bin edges from baseline (expected) distribution
    # Use equal-width bins based on min/max of expected values only
    if len(expected_values) == 0 or len(actual_values) == 0:
        return float('nan')
    
    # Convert to numpy arrays
    expected_array = np.array(expected_values, dtype=float)
    actual_array = np.array(actual_values, dtype=float)
    
    min_val = expected_array.min()
    max_val = expected_array.max()
    
    if min_val == max_val:
        return 0.0
    
    bin_edges = np.linspace(min_val, max_val, bins + 1)
    
    # Compute histograms
    expected_hist, _ = np.histogram(expected_array, bins=bin_edges)
    actual_hist, _ = np.histogram(actual_array, bins=bin_edges)
    
    # Convert to percentages
    expected_total = len(expected_array)
    actual_total = len(actual_array)
    
    if expected_total == 0 or actual_total == 0:
        return float('nan')
    
    expected_pct = expected_hist / expected_total
    actual_pct = actual_hist / actual_total
    
    # Add epsilon to avoid divide by zero and log(0)
    expected_pct = expected_pct + epsilon
    actual_pct = actual_pct + epsilon
    
    # Normalize again after adding epsilon
    expected_pct = expected_pct / expected_pct.sum()
    actual_pct = actual_pct / actual_pct.sum()
    
    # Compute PSI per bin
    psi_per_bin = (actual_pct - expected_pct) * np.log(actual_pct / expected_pct)
    psi = psi_per_bin.sum()
    
    return float(psi)


def get_feature_values(feature_name, time_window_start, time_window_end):
    """Get feature values from database for a given time window.
    
    Computes rolling sums similar to dataset builder for features:
    - alerts_sum_15m
    - trip_updates_sum_15m
    - vehicle_positions_sum_15m
    
    Args:
        feature_name: Name of the feature (e.g., 'alerts_sum_15m')
        time_window_start: Start timestamp (inclusive)
        time_window_end: End timestamp (exclusive)
    
    Returns:
        numpy array of feature values
    """
    sql = """
        SELECT bucket_start, line_id, stop_id, alerts_count, 
               trip_updates_count, vehicle_positions_count
        FROM mta.station_minute_facts
        WHERE bucket_size_seconds = 60
          AND bucket_start >= %s
          AND bucket_start < %s
        ORDER BY bucket_start, line_id, stop_id
    """
    
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=(time_window_start, time_window_end))
    
    if len(df) == 0:
        return np.array([])
    
    df['bucket_start'] = pd.to_datetime(df['bucket_start'])
    df = df[~(df['line_id'].isna() & df['stop_id'].isna())]
    
    if len(df) == 0:
        return np.array([])
    
    # Set index to bucket_start for time-based rolling
    df = df.set_index('bucket_start')
    
    # Group by station identifiers
    grouped = df.groupby(['line_id', 'stop_id'], dropna=False)
    
    # Compute 15-minute rolling sums
    alerts_sum_15m = grouped['alerts_count'].rolling('15min', closed='left').sum()
    trip_updates_sum_15m = grouped['trip_updates_count'].rolling('15min', closed='left').sum()
    vehicle_positions_sum_15m = grouped['vehicle_positions_count'].rolling('15min', closed='left').sum()
    
    # Reset index and add rolling features
    df = df.reset_index()
    df['alerts_sum_15m'] = alerts_sum_15m.reset_index(level=[0, 1], drop=True).reindex(df.index)
    df['trip_updates_sum_15m'] = trip_updates_sum_15m.reset_index(level=[0, 1], drop=True).reindex(df.index)
    df['vehicle_positions_sum_15m'] = vehicle_positions_sum_15m.reset_index(level=[0, 1], drop=True).reindex(df.index)
    
    # Extract feature values (remove NaN from rolling window start)
    feature_values = df[feature_name].dropna().values
    
    return feature_values.astype(float)

