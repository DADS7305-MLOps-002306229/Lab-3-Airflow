from pathlib import Path

from streamlit.testing.v1 import AppTest

from asteroid_monitor.analytics import summarize
from asteroid_monitor.processing import transform_feed
from asteroid_monitor.storage import save_csv, write_json

APP = Path(__file__).resolve().parents[1] / "app/streamlit_app.py"


def test_missing_outputs(monkeypatch, tmp_path):
    monkeypatch.setenv("ASTEROID_ROOT", str(tmp_path))
    app = AppTest.from_file(str(APP)).run(timeout=30)
    assert not app.exception
    assert "Execute nasa_asteroid_monitor" in app.info[-1].value


def test_populated_dashboard(feed, monkeypatch, tmp_path):
    monkeypatch.setenv("ASTEROID_ROOT", str(tmp_path))
    frame = transform_feed(feed, "2025-01-01")
    save_csv(frame, tmp_path / "data/processed/test.csv")
    write_json(tmp_path / "reports/test.json", summarize(frame, "2025-01-01"))
    write_json(
        tmp_path / "reports/latest.json",
        {"csv_path": "data/processed/test.csv", "report_path": "reports/test.json"},
    )
    app = AppTest.from_file(str(APP)).run(timeout=30)
    assert not app.exception
    assert app.metric[0].value == "2"
    app.text_input[0].set_value("Beta").run()
    assert len(app.dataframe[0].value) == 1
    app.selectbox[0].set_value("Potentially hazardous").run()
    assert len(app.dataframe[0].value) == 0
