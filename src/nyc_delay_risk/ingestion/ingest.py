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
    """Run a single ingestion cycle: fetch, parse, and store all feeds.
    
    Fetches:
    - Service Alerts (single feed)
    - Realtime feeds (multiple line-specific feeds, each containing trip updates and vehicle positions)
    """
    api_key = os.getenv("MTA_API_KEY")
    service_alerts_url = os.getenv("SERVICE_ALERTS_URL")
    realtime_feeds_urls = os.getenv("REALTIME_FEEDS_URLS", "")
    
    if not service_alerts_url:
        raise ValueError("SERVICE_ALERTS_URL environment variable is required")
    if not realtime_feeds_urls:
        raise ValueError("REALTIME_FEEDS_URLS environment variable is required (comma-separated list)")
    
    # Parse comma-separated list of realtime feed URLs
    realtime_urls = [url.strip() for url in realtime_feeds_urls.split(",") if url.strip()]
    
    if not realtime_urls:
        raise ValueError("REALTIME_FEEDS_URLS must contain at least one URL")
    
    run_id = start_run(source="mta_gtfs_rt", notes="ingest_once")
    summary = {
        "run_id": str(run_id),
        "feeds": {}
    }
    
    try:
        total_entities = 0
        
        # Fetch and process service alerts
        logger.info(f"Fetching service alerts from {service_alerts_url}")
        feed_bytes = fetch_bytes(service_alerts_url, api_key)
        feed_ts, entities = parse_feed(feed_bytes)
        
        # Filter only service_alerts entities
        alerts_entities = [e for e in entities if e.get("entity_type") == "service_alerts"]
        logger.info(f"Inserting {len(alerts_entities)} service alerts")
        alerts_count = insert_raw_events(run_id, "service_alerts", feed_ts, alerts_entities)
        total_entities += alerts_count
        summary["feeds"]["service_alerts"] = {
            "entities": alerts_count,
            "feed_ts": feed_ts.isoformat()
        }
        
        # Fetch and process all realtime feeds (trip updates + vehicle positions)
        trip_updates_total = 0
        vehicle_positions_total = 0
        
        for feed_url in realtime_urls:
            logger.info(f"Fetching realtime feed from {feed_url}")
            feed_bytes = fetch_bytes(feed_url, api_key)
            feed_ts, entities = parse_feed(feed_bytes)
            
            # Separate entities by type
            trip_updates_entities = [e for e in entities if e.get("entity_type") == "trip_updates"]
            vehicle_positions_entities = [e for e in entities if e.get("entity_type") == "vehicle_positions"]
            
            # Insert trip updates
            if trip_updates_entities:
                logger.info(f"Inserting {len(trip_updates_entities)} trip updates from {feed_url}")
                count = insert_raw_events(run_id, "trip_updates", feed_ts, trip_updates_entities)
                trip_updates_total += count
                total_entities += count
            
            # Insert vehicle positions
            if vehicle_positions_entities:
                logger.info(f"Inserting {len(vehicle_positions_entities)} vehicle positions from {feed_url}")
                count = insert_raw_events(run_id, "vehicle_positions", feed_ts, vehicle_positions_entities)
                vehicle_positions_total += count
                total_entities += count
        
        summary["feeds"]["trip_updates"] = {
            "entities": trip_updates_total,
            "feeds_processed": len(realtime_urls)
        }
        summary["feeds"]["vehicle_positions"] = {
            "entities": vehicle_positions_total,
            "feeds_processed": len(realtime_urls)
        }
        
        notes = f"Successfully ingested {total_entities} total entities from {len(realtime_urls) + 1} feeds"
        finish_run(run_id, "success", notes)
        summary["status"] = "success"
        summary["total_entities"] = total_entities
        logger.info(f"Ingestion completed successfully: {total_entities} entities from {len(realtime_urls) + 1} feeds")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ingestion failed: {error_msg}")
        finish_run(run_id, "failed", error_msg)
        summary["status"] = "failed"
        summary["error"] = error_msg
        raise
    
    return summary

