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
