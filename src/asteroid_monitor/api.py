"""Bounded, single-day NASA NeoWs ingestion."""

import logging
import os
from datetime import date

import requests

LOGGER = logging.getLogger(__name__)
API_URL = "https://api.nasa.gov/neo/rest/v1/feed"


def fetch_feed(query_date: str) -> dict:
    """Fetch one UTC date; redact request URLs because they contain the API key."""
    date.fromisoformat(query_date)
    try:
        response = requests.get(
            API_URL,
            params={
                "start_date": query_date,
                "end_date": query_date,
                "api_key": os.getenv("NASA_API_KEY") or "DEMO_KEY",
            },
            timeout=(10, 60),
        )
        response.raise_for_status()
    except requests.Timeout:
        raise RuntimeError("NASA request timed out; retry later") from None
    except requests.RequestException:
        raise RuntimeError(
            "NASA HTTP request failed; check connectivity, API quota and key"
        ) from None
    try:
        payload = response.json()
    except ValueError:
        raise ValueError("NASA response is not valid JSON") from None
    if not isinstance(payload, dict):
        raise ValueError("NASA response must be a JSON object")
    LOGGER.info("Fetched NASA feed for %s", query_date)
    return payload
