import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nyc_delay_risk.db import get_conn, query_one

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Connecting to database...")
        with get_conn() as conn:
            logger.info("Connection established.")
        
        logger.info("Running connection test query...")
        result = query_one("SELECT 1 as ok")
        logger.info(f"Connection test passed: {result}")
        
        logger.info("Checking schema...")
        count_result = query_one("SELECT COUNT(*) AS n FROM mta.raw_events")
        logger.info(f"Table mta.raw_events exists. Current row count: {count_result['n']}")
        
        logger.info("Database check completed successfully.")
        return 0
    
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

