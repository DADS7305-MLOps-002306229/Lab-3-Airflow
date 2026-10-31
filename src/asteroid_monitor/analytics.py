"""Descriptive statistics, never collision predictions."""

from datetime import UTC, datetime

import pandas as pd

from asteroid_monitor.processing import validate_frame


def summarize(frame: pd.DataFrame, query_date: str) -> dict:
    validate_frame(frame)

    def extreme(column: str, largest: bool) -> dict | None:
        if frame.empty:
            return None
        index = frame[column].idxmax() if largest else frame[column].idxmin()
        row = frame.loc[index]
        return {
            "asteroid_id": str(row["asteroid_id"]),
            "name": str(row["name"]),
            column: float(row[column]),
        }

    return {
        "query_date": query_date,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "NASA NeoWs",
        "total_asteroids": len(frame),
        "hazardous_count": int(frame["is_hazardous"].sum()),
        "fastest_asteroid": extreme("velocity_km_s", True),
        "closest_asteroid": extreme("miss_distance_km", False),
        "average_velocity_km_s": None if frame.empty else float(frame["velocity_km_s"].mean()),
        "average_diameter_km": None if frame.empty else float(frame["diameter_km"].mean()),
    }
