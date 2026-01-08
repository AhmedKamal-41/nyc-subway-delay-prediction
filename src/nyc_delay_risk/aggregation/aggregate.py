import logging
from ..db import get_conn

logger = logging.getLogger(__name__)


def upsert_facts(bucket_size_seconds: int, window_minutes: int) -> int:
    """Aggregate raw events into station_minute_facts using SQL aggregation.
    
    Args:
        bucket_size_seconds: Either 60 or 300 seconds
        window_minutes: Number of minutes to look back from now()
    
    Returns:
        Number of affected rows (inserted or updated), or -1 if unavailable
    """
    if bucket_size_seconds == 60:
        bucket_function = "date_trunc('minute', event_ts)"
    elif bucket_size_seconds == 300:
        bucket_function = "date_bin('5 minutes', event_ts, TIMESTAMPTZ '1970-01-01 00:00:00+00')"
    else:
        raise ValueError(f"Unsupported bucket_size_seconds: {bucket_size_seconds}. Must be 60 or 300.")
    
    sql = f"""
        INSERT INTO mta.station_minute_facts 
            (bucket_start, bucket_size_seconds, line_id, stop_id, 
             alerts_count, major_alerts_count, trip_updates_count, vehicle_positions_count)
        SELECT 
            {bucket_function} as bucket_start,
            {bucket_size_seconds} as bucket_size_seconds,
            line_id,
            stop_id,
            COUNT(*) FILTER (WHERE feed_type='service_alerts') as alerts_count,
            0 as major_alerts_count,
            COUNT(*) FILTER (WHERE feed_type='trip_updates') as trip_updates_count,
            COUNT(*) FILTER (WHERE feed_type='vehicle_positions') as vehicle_positions_count
        FROM mta.raw_events
        WHERE event_ts >= now() - INTERVAL '{window_minutes} minutes'
        GROUP BY bucket_start, line_id, stop_id
        ON CONFLICT (bucket_start, bucket_size_seconds, line_id, stop_id)
        DO UPDATE SET
            alerts_count=EXCLUDED.alerts_count,
            major_alerts_count=EXCLUDED.major_alerts_count,
            trip_updates_count=EXCLUDED.trip_updates_count,
            vehicle_positions_count=EXCLUDED.vehicle_positions_count,
            created_at=now()
    """
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            rowcount = cur.rowcount
            if rowcount is None or rowcount < 0:
                logger.warning("rowcount not available or unreliable")
                return -1
            return rowcount

