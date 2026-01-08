from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return current UTC datetime with timezone awareness."""
    return datetime.now(timezone.utc)


def from_epoch_seconds(ts: int | None) -> datetime:
    """Convert epoch seconds to UTC datetime. If None, return current UTC time."""
    if ts is None:
        return utc_now()
    return datetime.fromtimestamp(ts, tz=timezone.utc)

