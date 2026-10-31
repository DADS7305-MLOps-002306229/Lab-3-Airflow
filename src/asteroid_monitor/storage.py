"""Atomic local files and a manifest published only after output validation."""

import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from asteroid_monitor.analytics import summarize
from asteroid_monitor.processing import validate_frame

LOGGER = logging.getLogger(__name__)


def root_path() -> Path:
    return Path(os.getenv("ASTEROID_ROOT", Path(__file__).resolve().parents[2]))


def atomic_write(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return str(path)


def write_json(path: Path, payload: dict) -> str:
    return atomic_write(path, json.dumps(payload, indent=2, allow_nan=False) + "\n")


def read_json(path: str | Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def save_csv(frame: pd.DataFrame, path: Path) -> str:
    validate_frame(frame)
    return atomic_write(path, frame.to_csv(index=False))


def read_csv(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype={"asteroid_id": str})
    validate_frame(frame)
    return frame


def validate_outputs(csv_path: str, report_path: str) -> dict:
    frame = read_csv(csv_path)
    report = read_json(report_path)
    expected = summarize(frame, report["query_date"])
    for key, value in expected.items():
        if key != "generated_at" and report.get(key) != value:
            raise ValueError(f"Report disagrees with CSV: {key}")
    datetime.fromisoformat(report["generated_at"])
    if not frame.empty and set(frame["approach_date"]) != {report["query_date"]}:
        raise ValueError("CSV date disagrees with report")
    LOGGER.info(
        "Validated %s records, %s hazardous; query date %s",
        report["total_asteroids"],
        report["hazardous_count"],
        report["query_date"],
    )
    return report
