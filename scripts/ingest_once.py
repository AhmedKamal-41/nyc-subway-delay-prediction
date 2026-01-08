import sys
import logging
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nyc_delay_risk.ingestion.ingest import ingest_once

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    try:
        summary = ingest_once()
        logger.info("Ingestion summary:")
        logger.info(json.dumps(summary, indent=2))
        return 0
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

