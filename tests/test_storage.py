import pytest

from asteroid_monitor.analytics import summarize
from asteroid_monitor.processing import transform_feed
from asteroid_monitor.storage import read_csv, read_json, save_csv, validate_outputs, write_json


@pytest.mark.parametrize("empty", [False, True])
def test_round_trip(feed, tmp_path, empty):
    if empty:
        feed["near_earth_objects"]["2025-01-01"] = []
    frame = transform_feed(feed, "2025-01-01")
    csv = tmp_path / "nested/records.csv"
    report_file = tmp_path / "reports/report.json"
    report = summarize(frame, "2025-01-01")
    for _ in range(2):
        save_csv(frame, csv)
        write_json(report_file, report)
    assert len(read_csv(csv)) == len(frame)
    assert read_json(report_file) == report
    assert validate_outputs(str(csv), str(report_file)) == report
    if not empty:
        assert read_csv(csv).iloc[0]["asteroid_id"] == "001"
    report["hazardous_count"] = 999
    write_json(report_file, report)
    with pytest.raises(ValueError, match="disagrees"):
        validate_outputs(str(csv), str(report_file))


def test_missing_and_corrupt(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_json(tmp_path / "missing.json")
    (tmp_path / "bad.json").write_text("not json")
    with pytest.raises(ValueError):
        read_json(tmp_path / "bad.json")
