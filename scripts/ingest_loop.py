import sys
import os
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nyc_delay_risk.ingestion.ingest import ingest_once

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    logger.info(f"Starting ingestion loop (interval: {poll_interval}s)")
    
    try:
        while True:
            try:
                ingest_once()
            except Exception as e:
                logger.error(f"Ingestion cycle failed: {e}")
            
            logger.info(f"Sleeping {poll_interval} seconds...")
            time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        logger.info("Ingestion loop stopped by user")
        return 0


if __name__ == "__main__":
    sys.exit(main())

