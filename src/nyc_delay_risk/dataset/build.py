import pandas as pd
from ..db import get_conn


def load_facts():
    """Load 60-second bucket facts from database, filtered and sorted."""
    sql = """
        SELECT *
        FROM mta.station_minute_facts
        WHERE bucket_size_seconds = 60
        ORDER BY bucket_start
    """
    
    with get_conn() as conn:
        df = pd.read_sql(sql, conn)
    
    # Convert bucket_start to datetime if not already
    df['bucket_start'] = pd.to_datetime(df['bucket_start'])
    
    # Drop rows where both line_id and stop_id are NULL
    df = df[~(df['line_id'].isna() & df['stop_id'].isna())]
    
    return df


def create_features(df):
    """Add features to dataframe: direct counts, time features, and rolling sums."""
    df = df.copy()
    
    # Direct features (already in dataframe)
    # alerts_count, trip_updates_count, vehicle_positions_count
    
    # Time features from bucket_start (UTC)
    df['hour_of_day'] = df['bucket_start'].dt.hour
    df['day_of_week'] = df['bucket_start'].dt.dayofweek  # Monday=0
    
    # Rolling features per (line_id, stop_id) group
    # Set index to bucket_start for time-based rolling
    df = df.set_index('bucket_start')
    
    # Group by station identifiers
    grouped = df.groupby(['line_id', 'stop_id'], dropna=False)
    
    # Rolling backward windows (past 15min and 60min)
    rolling_features = {}
    for window in ['15min', '60min']:
        suffix = window.replace('min', 'm')
        
        rolling_features[f'alerts_sum_{suffix}'] = grouped['alerts_count'].rolling(
            window, closed='left'
        ).sum()
        
        rolling_features[f'trip_updates_sum_{suffix}'] = grouped['trip_updates_count'].rolling(
            window, closed='left'
        ).sum()
        
        rolling_features[f'vehicle_positions_sum_{suffix}'] = grouped['vehicle_positions_count'].rolling(
            window, closed='left'
        ).sum()
    
    # Add rolling features to dataframe and reset index
    for col_name, series in rolling_features.items():
        # Reset groupby index levels (line_id, stop_id) but keep bucket_start index
        df[col_name] = series.reset_index(level=[0, 1], drop=True)
    
    # Reset index to bring bucket_start back as column
    df = df.reset_index()
    
    return df


def create_label(df):
    """Create binary label: 1 if alerts_count > 0 occurs in next 15 minutes.
    
    Method: For each row at time t, check if any future row exists where
    bucket_start is in (t, t+15min] and alerts_count > 0, for the same
    (line_id, stop_id). Uses groupby and time-based filtering per station.
    """
    df = df.copy()
    
    # Ensure sorted by bucket_start
    df = df.sort_values(['line_id', 'stop_id', 'bucket_start']).reset_index(drop=True)
    
    # Create labels by checking future alerts within 15 minutes for each group
    labels = []
    
    for (line_id, stop_id), group in df.groupby(['line_id', 'stop_id'], dropna=False):
        group = group.sort_values('bucket_start').reset_index(drop=True)
        
        # Get rows with alerts for this station
        alert_rows = group[group['alerts_count'] > 0].copy()
        
        group_labels = []
        for idx, row in group.iterrows():
            current_time = row['bucket_start']
            future_window_end = current_time + pd.Timedelta(minutes=15)
            
            # Check if any alert exists in the future window
            if len(alert_rows) > 0:
                future_alerts = alert_rows[
                    (alert_rows['bucket_start'] > current_time) &
                    (alert_rows['bucket_start'] <= future_window_end)
                ]
                has_future_alert = len(future_alerts) > 0
            else:
                has_future_alert = False
            
            group_labels.append(1 if has_future_alert else 0)
        
        labels.extend(group_labels)
    
    df['label'] = labels
    
    return df

