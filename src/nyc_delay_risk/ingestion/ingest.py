import os
import json
import uuid
import logging
from ..db import get_conn
from .client import fetch_bytes
from .parser import parse_feed

logger = logging.getLogger(__name__)


def start_run(source: str, notes: str | None = None) -> uuid.UUID:
    """Create a new ingestion run with status 'running' and return run_id."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mta.ingest_runs (status, source, notes) VALUES ('running', %s, %s) RETURNING run_id",
                (source, notes)
            )
            run_id = cur.fetchone()[0]
            conn.commit()
            return run_id


def finish_run(run_id: uuid.UUID, status: str, notes: str | None = None) -> None:
    """Update ingestion run status and ended_at timestamp."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE mta.ingest_runs SET status=%s, ended_at=now(), notes=%s WHERE run_id=%s",
                (status, notes, run_id)
            )
            conn.commit()


def insert_raw_events(run_id: uuid.UUID, feed_type: str, feed_ts, entities: list[dict]) -> int:
    """Insert raw events using executemany. Returns count of inserted rows."""
    if not entities:
        return 0
    
    insert_sql = """
        INSERT INTO mta.raw_events 
        (run_id, feed_type, entity_id, event_ts, line_id, stop_id, trip_id, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    
    rows = []
    for entity in entities:
        rows.append((
            run_id,
            feed_type,
            entity.get("entity_id"),
            feed_ts,
            entity.get("line_id"),
            entity.get("stop_id"),
            entity.get("trip_id"),
            json.dumps(entity.get("payload"))
        ))
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
            conn.commit()
            return len(rows)


def ingest_once() -> dict:
    """Run a single ingestion cycle: fetch, parse, and store all three feeds."""
    api_key = os.getenv("MTA_API_KEY")
    service_alerts_url = os.getenv("SERVICE_ALERTS_URL")
    trip_updates_url = os.getenv("TRIP_UPDATES_URL")
    vehicle_positions_url = os.getenv("VEHICLE_POSITIONS_URL")
    
    if not service_alerts_url or not trip_updates_url or not vehicle_positions_url:
        raise ValueError("Required feed URLs not configured in environment variables")
    
    run_id = start_run(source="mta_gtfs_rt", notes="ingest_once")
    summary = {
        "run_id": str(run_id),
        "feeds": {}
    }
    
    try:
        feed_configs = [
            ("service_alerts", service_alerts_url),
            ("trip_updates", trip_updates_url),
            ("vehicle_positions", vehicle_positions_url),
        ]
        
        total_entities = 0
        
        for feed_type, url in feed_configs:
            logger.info(f"Fetching {feed_type} from {url}")
            feed_bytes = fetch_bytes(url, api_key)
            
            logger.info(f"Parsing {feed_type}")
            feed_ts, entities = parse_feed(feed_bytes)
            
            logger.info(f"Inserting {len(entities)} entities for {feed_type}")
            count = insert_raw_events(run_id, feed_type, feed_ts, entities)
            total_entities += count
            
            summary["feeds"][feed_type] = {
                "entities": count,
                "feed_ts": feed_ts.isoformat()
            }
        
        notes = f"Successfully ingested {total_entities} total entities"
        finish_run(run_id, "success", notes)
        summary["status"] = "success"
        summary["total_entities"] = total_entities
        logger.info(f"Ingestion completed successfully: {total_entities} entities")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ingestion failed: {error_msg}")
        finish_run(run_id, "failed", error_msg)
        summary["status"] = "failed"
        summary["error"] = error_msg
        raise
    
    return summary

