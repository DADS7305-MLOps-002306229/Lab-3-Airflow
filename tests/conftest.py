import os
from copy import deepcopy
from tempfile import mkdtemp

import pytest

os.environ.setdefault("AIRFLOW_HOME", mkdtemp(prefix="asteroid-airflow-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")


@pytest.fixture
def feed():
    asteroid = {
        "id": "001",
        "name": "Example Alpha",
        "is_potentially_hazardous_asteroid": True,
        "estimated_diameter": {
            "kilometers": {"estimated_diameter_min": "0.1", "estimated_diameter_max": "0.3"}
        },
        "close_approach_data": [
            {
                "close_approach_date": "2025-01-01",
                "orbiting_body": "Earth",
                "relative_velocity": {"kilometers_per_second": "20"},
                "miss_distance": {"kilometers": "1000"},
            }
        ],
    }
    other = deepcopy(asteroid)
    other.update(id="002", name="Example Beta", is_potentially_hazardous_asteroid=False)
    other["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"] = "10"
    other["close_approach_data"][0]["miss_distance"]["kilometers"] = "500"
    return {"near_earth_objects": {"2025-01-01": [asteroid, other]}}


@pytest.fixture
def training_feed(feed):
    """Synthetic separable labels for software tests, never a scientific dataset."""
    records = []
    for i in range(80):
        asteroid = deepcopy(feed["near_earth_objects"]["2025-01-01"][0])
        hazardous = i % 2 == 0
        asteroid.update(
            id=f"test-{i:03}", name=f"Synthetic {i}", is_potentially_hazardous_asteroid=hazardous
        )
        low = (1.0 if hazardous else 0.01) + i / 10000
        asteroid["estimated_diameter"]["kilometers"] = {
            "estimated_diameter_min": low,
            "estimated_diameter_max": low * 1.5,
        }
        records.append(asteroid)
    return {"near_earth_objects": {"2025-01-01": records}}


@pytest.fixture
def training_frame(training_feed):
    from asteroid_monitor.processing import transform_feed

    return transform_feed(training_feed, "2025-01-01")
