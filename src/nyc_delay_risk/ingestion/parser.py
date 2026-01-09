from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from ..utils.time import from_epoch_seconds


def parse_feed(feed_bytes: bytes) -> tuple:
    """Parse GTFS-RT feed and extract entities with metadata and type.
    
    Returns:
        tuple: (feed_ts: datetime, entities: list[dict])
        Each entity dict contains: entity_id, payload, line_id, stop_id, trip_id, entity_type
        entity_type is one of: 'service_alerts', 'trip_updates', 'vehicle_positions'
    """
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.ParseFromString(feed_bytes)
    
    # Extract feed timestamp
    feed_ts = from_epoch_seconds(feed_message.header.timestamp if feed_message.header.HasField("timestamp") else None)
    
    entities = []
    
    for entity in feed_message.entity:
        entity_id = entity.id
        entity_type = None
        
        # Convert entity to dict for JSONB storage
        payload = MessageToDict(entity, preserving_proto_field_name=True)
        
        # Extract line_id, stop_id, trip_id based on entity type
        line_id = None
        stop_id = None
        trip_id = None
        
        # Service alerts
        if entity.HasField("alert"):
            entity_type = "service_alerts"
            alert = entity.alert
            if alert.informed_entity:
                for informed_entity in alert.informed_entity:
                    if line_id is None and informed_entity.HasField("route_id"):
                        line_id = informed_entity.route_id
                    if stop_id is None and informed_entity.HasField("stop_id"):
                        stop_id = informed_entity.stop_id
        
        # Trip updates
        elif entity.HasField("trip_update"):
            entity_type = "trip_updates"
            trip_update = entity.trip_update
            if trip_update.trip.HasField("route_id"):
                line_id = trip_update.trip.route_id
            if trip_update.trip.HasField("trip_id"):
                trip_id = trip_update.trip.trip_id
            if trip_update.stop_time_update:
                for stop_time_update in trip_update.stop_time_update:
                    if stop_time_update.HasField("stop_id"):
                        stop_id = stop_time_update.stop_id
                        break
        
        # Vehicle positions
        elif entity.HasField("vehicle"):
            entity_type = "vehicle_positions"
            vehicle = entity.vehicle
            if vehicle.trip.HasField("route_id"):
                line_id = vehicle.trip.route_id
            if vehicle.trip.HasField("trip_id"):
                trip_id = vehicle.trip.trip_id
            if vehicle.HasField("stop_id"):
                stop_id = vehicle.stop_id
        
        if entity_type:
            entities.append({
                "entity_id": entity_id,
                "payload": payload,
                "line_id": line_id if line_id else None,
                "stop_id": stop_id if stop_id else None,
                "trip_id": trip_id if trip_id else None,
                "entity_type": entity_type,
            })
    
    return feed_ts, entities

