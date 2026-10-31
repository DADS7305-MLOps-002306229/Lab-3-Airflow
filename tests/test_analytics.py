import pytest

from asteroid_monitor.analytics import summarize
from asteroid_monitor.processing import transform_feed


def test_statistics(feed):
    report = summarize(transform_feed(feed, "2025-01-01"), "2025-01-01")
    assert report["total_asteroids"] == 2
    assert report["hazardous_count"] == 1
    assert report["fastest_asteroid"]["asteroid_id"] == "001"
    assert report["closest_asteroid"]["asteroid_id"] == "002"
    assert report["average_velocity_km_s"] == 15
    assert report["average_diameter_km"] == pytest.approx(0.2)


def test_empty():
    frame = transform_feed({"near_earth_objects": {"2025-01-01": []}}, "2025-01-01")
    report = summarize(frame, "2025-01-01")
    assert report["total_asteroids"] == report["hazardous_count"] == 0
    assert report["closest_asteroid"] is None
    assert report["fastest_asteroid"] is None
    assert report["average_velocity_km_s"] is None
