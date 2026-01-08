from datetime import datetime, timedelta
from ..db import get_conn
import logging

logger = logging.getLogger(__name__)


def compute_features_online(line_id: str, stop_id: str):
    """Compute features for a station matching the training pipeline.
    
    Returns:
        tuple: (features_dict, latest_bucket_start)
    
    Raises:
        ValueError: If no data found for the station
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Get latest bucket_start for this station
            cur.execute("""
                SELECT bucket_start, alerts_count, major_alerts_count,
                       trip_updates_count, vehicle_positions_count
                FROM mta.station_minute_facts
                WHERE line_id = %s AND stop_id = %s AND bucket_size_seconds = 60
                ORDER BY bucket_start DESC
                LIMIT 1
            """, (line_id, stop_id))
            
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No data found for line_id={line_id}, stop_id={stop_id}")
            
            latest_bucket_start = row[0]
            alerts_count = row[1] or 0
            major_alerts_count = row[2] or 0
            trip_updates_count = row[3] or 0
            vehicle_positions_count = row[4] or 0
            
            # Compute rolling sums for 15min and 60min windows
            # Window is (latest - window_size, latest] (closed interval on right)
            window_15min_start = latest_bucket_start - timedelta(minutes=15)
            window_60min_start = latest_bucket_start - timedelta(minutes=60)
            
            # 15-minute rolling sums
            cur.execute("""
                SELECT 
                    COALESCE(SUM(alerts_count), 0) as alerts_sum,
                    COALESCE(SUM(trip_updates_count), 0) as trip_updates_sum,
                    COALESCE(SUM(vehicle_positions_count), 0) as vehicle_positions_sum
                FROM mta.station_minute_facts
                WHERE line_id = %s 
                  AND stop_id = %s 
                  AND bucket_size_seconds = 60
                  AND bucket_start > %s
                  AND bucket_start <= %s
            """, (line_id, stop_id, window_15min_start, latest_bucket_start))
            
            row_15m = cur.fetchone()
            alerts_sum_15m = row_15m[0] or 0
            trip_updates_sum_15m = row_15m[1] or 0
            vehicle_positions_sum_15m = row_15m[2] or 0
            
            # 60-minute rolling sums
            cur.execute("""
                SELECT 
                    COALESCE(SUM(alerts_count), 0) as alerts_sum,
                    COALESCE(SUM(trip_updates_count), 0) as trip_updates_sum,
                    COALESCE(SUM(vehicle_positions_count), 0) as vehicle_positions_sum
                FROM mta.station_minute_facts
                WHERE line_id = %s 
                  AND stop_id = %s 
                  AND bucket_size_seconds = 60
                  AND bucket_start > %s
                  AND bucket_start <= %s
            """, (line_id, stop_id, window_60min_start, latest_bucket_start))
            
            row_60m = cur.fetchone()
            alerts_sum_60m = row_60m[0] or 0
            trip_updates_sum_60m = row_60m[1] or 0
            vehicle_positions_sum_60m = row_60m[2] or 0
            
            # Time features from latest bucket_start
            hour_of_day = latest_bucket_start.hour
            day_of_week = latest_bucket_start.weekday()  # Monday=0
            
            # Build features dict matching training feature order
            features = {
                'alerts_count': int(alerts_count),
                'major_alerts_count': int(major_alerts_count),
                'trip_updates_count': int(trip_updates_count),
                'vehicle_positions_count': int(vehicle_positions_count),
                'hour_of_day': int(hour_of_day),
                'day_of_week': int(day_of_week),
                'alerts_sum_15m': int(alerts_sum_15m),
                'alerts_sum_60m': int(alerts_sum_60m),
                'trip_updates_sum_15m': int(trip_updates_sum_15m),
                'trip_updates_sum_60m': int(trip_updates_sum_60m),
                'vehicle_positions_sum_15m': int(vehicle_positions_sum_15m),
                'vehicle_positions_sum_60m': int(vehicle_positions_sum_60m),
            }
            
            return features, latest_bucket_start

