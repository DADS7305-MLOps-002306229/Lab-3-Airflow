"""Validate and flatten NASA approach records without silent data loss."""

from datetime import date
from math import isfinite

import pandas as pd

COLUMNS = [
    "asteroid_id",
    "name",
    "diameter_min_km",
    "diameter_max_km",
    "diameter_km",
    "approach_date",
    "velocity_km_s",
    "miss_distance_km",
    "is_hazardous",
]
NUMERIC_COLUMNS = [
    "diameter_min_km",
    "diameter_max_km",
    "diameter_km",
    "velocity_km_s",
    "miss_distance_km",
]


def _number(value: object) -> float:
    if isinstance(value, bool):
        raise ValueError("Boolean is not a numeric measurement")
    number = float(value)
    if not isfinite(number) or number < 0:
        raise ValueError("Measurements must be finite and nonnegative")
    return number


def _records(payload: dict, query_date: str) -> list[dict]:
    date.fromisoformat(query_date)
    try:
        groups = payload["near_earth_objects"]
        if not isinstance(groups, dict) or query_date not in groups:
            raise ValueError("Requested date is missing")
        if set(groups) != {query_date} or not isinstance(groups[query_date], list):
            raise ValueError("Expected one date containing a list of asteroids")
        rows = []
        for asteroid in groups[query_date]:
            identifier = asteroid["id"]
            name = asteroid["name"]
            hazard = asteroid["is_potentially_hazardous_asteroid"]
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError("Missing asteroid identifier")
            if not isinstance(name, str) or not name.strip() or not isinstance(hazard, bool):
                raise ValueError("Missing name or invalid hazardous classification")
            diameter = asteroid["estimated_diameter"]["kilometers"]
            low = _number(diameter["estimated_diameter_min"])
            high = _number(diameter["estimated_diameter_max"])
            if low > high:
                raise ValueError("Diameter minimum exceeds maximum")
            approaches = asteroid["close_approach_data"]
            if not isinstance(approaches, list):
                raise ValueError("Approaches must be a list")
            matching = [
                a
                for a in approaches
                if a["close_approach_date"] == query_date and a["orbiting_body"] == "Earth"
            ]
            if not matching:
                raise ValueError("No Earth approach for the requested date")
            for approach in matching:
                rows.append(
                    dict(
                        zip(
                            COLUMNS,
                            [
                                identifier,
                                name.strip(),
                                low,
                                high,
                                (low + high) / 2,
                                query_date,
                                _number(approach["relative_velocity"]["kilometers_per_second"]),
                                _number(approach["miss_distance"]["kilometers"]),
                                hazard,
                            ],
                            strict=True,
                        )
                    )
                )
        keys = [(row["asteroid_id"], row["approach_date"]) for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate asteroid/date records")
        return rows
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
        raise ValueError(f"Invalid NASA feed for {query_date}: {exc}") from exc


def validate_feed(payload: dict, query_date: str) -> None:
    """Reject incomplete measurements; an explicitly empty date list is valid."""
    _records(payload, query_date)


def transform_feed(payload: dict, query_date: str) -> pd.DataFrame:
    """One row per asteroid/Earth approach date; diameter is the range midpoint."""
    return pd.DataFrame(_records(payload, query_date), columns=COLUMNS)


def validate_frame(frame: pd.DataFrame) -> None:
    """Validate files again when consuming them, including header-only datasets."""
    if list(frame.columns) != COLUMNS:
        raise ValueError("Unexpected processed CSV columns")
    if frame.isna().any().any():
        raise ValueError("Processed CSV contains missing values")
    for column in NUMERIC_COLUMNS:
        for value in frame[column]:
            _number(value)
    if not frame["is_hazardous"].isin([True, False]).all():
        raise ValueError("Invalid hazardous classification")
    for value in frame["approach_date"]:
        date.fromisoformat(str(value))
    if frame.duplicated(["asteroid_id", "approach_date"]).any():
        raise ValueError("Duplicate processed records")
