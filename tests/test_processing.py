import pytest

from asteroid_monitor.processing import COLUMNS, transform_feed, validate_feed


def test_conversion(feed):
    validate_feed(feed, "2025-01-01")
    frame = transform_feed(feed, "2025-01-01")
    assert list(frame.columns) == COLUMNS
    assert frame.iloc[0]["diameter_km"] == pytest.approx(0.2)
    assert frame.iloc[0]["velocity_km_s"] == 20.0
    assert frame.iloc[0]["asteroid_id"] == "001"
    assert len(frame) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"near_earth_objects": []},
        {"near_earth_objects": {"2025-01-01": None}},
        {"near_earth_objects": {"2025-01-01": [{}]}},
    ],
)
def test_invalid_structure(payload):
    with pytest.raises(ValueError, match="Invalid NASA feed"):
        validate_feed(payload, "2025-01-01")


@pytest.mark.parametrize("value", [None, "NaN", "inf", "invalid", -1, True])
def test_bad_measurements(feed, value):
    feed["near_earth_objects"]["2025-01-01"][0]["estimated_diameter"]["kilometers"][
        "estimated_diameter_min"
    ] = value
    with pytest.raises(ValueError):
        validate_feed(feed, "2025-01-01")


@pytest.mark.parametrize(
    "field,value",
    [("name", None), ("is_potentially_hazardous_asteroid", "false"), ("close_approach_data", [])],
)
def test_missing_values(feed, field, value):
    feed["near_earth_objects"]["2025-01-01"][0][field] = value
    with pytest.raises(ValueError):
        validate_feed(feed, "2025-01-01")


def test_duplicate_records(feed):
    feed["near_earth_objects"]["2025-01-01"] *= 2
    with pytest.raises(ValueError, match="Duplicate"):
        validate_feed(feed, "2025-01-01")
