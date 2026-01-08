import sys
import os
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nyc_delay_risk.aggregation.aggregate import upsert_facts

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    window_minutes = int(os.getenv("WINDOW_MINUTES", "120"))
    logger.info(f"Aggregating facts for last {window_minutes} minutes")
    
    bucket_sizes = [60, 300]
    
    for bucket_size in bucket_sizes:
        logger.info(f"Processing {bucket_size}-second buckets...")
        rowcount = upsert_facts(bucket_size, window_minutes)
        if rowcount >= 0:
            logger.info(f"  {bucket_size}s buckets: {rowcount} rows affected")
        else:
            logger.info(f"  {bucket_size}s buckets: completed (rowcount unavailable)")
    
    logger.info("Aggregation completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

