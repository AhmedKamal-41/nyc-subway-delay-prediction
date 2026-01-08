import requests
import logging

logger = logging.getLogger(__name__)


def fetch_bytes(url: str, api_key: str | None = None) -> bytes:
    """Fetch bytes from URL with optional API key authentication.
    
    Attempts header 'x-api-key' first. On 401/403, retries once with
    query parameter ?api_key=...
    """
    timeout = 20
    headers = {}
    
    if api_key:
        headers["x-api-key"] = api_key
    
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.content
    
    except (requests.HTTPError, requests.RequestException) as e:
        # Retry once with query param on 401/403
        if isinstance(e, requests.HTTPError) and e.response.status_code in (401, 403):
            if api_key:
                logger.info(f"Header auth failed ({e.response.status_code}), retrying with query param")
                try:
                    retry_url = f"{url}?api_key={api_key}"
                    response = requests.get(retry_url, timeout=timeout)
                    response.raise_for_status()
                    return response.content
                except requests.RequestException as retry_err:
                    logger.error(f"Retry with query param failed: {retry_err}")
                    raise
        raise

