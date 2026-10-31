from datetime import timedelta
from pathlib import Path

import pytest
from airflow.models import DagBag


def test_dag_import_and_dependencies():
    bag = DagBag(
        dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False
    )
    assert bag.import_errors == {}
    dag = bag.dags["nasa_asteroid_monitor"]
    tasks = [
        "fetch_asteroid_data",
        "validate_data",
        "transform_data",
        "save_processed_data",
        "generate_analytics",
        "train_model",
        "evaluate_model",
        "validate_outputs",
    ]
    assert set(dag.task_ids) == set(tasks)
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    for before, after in zip(tasks, tasks[1:], strict=False):
        assert dag.get_task(before).downstream_task_ids == {after}
    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=1)
        assert task.execution_timeout == timedelta(minutes=5)


def test_query_date_semantics():
    from types import SimpleNamespace

    import pendulum

    from dags.asteroid_monitor_dag import query_date_from_context

    run = SimpleNamespace(run_after=pendulum.datetime(2025, 2, 3, 14, tz="UTC"))
    assert query_date_from_context({"dag_run": run}) == "2025-02-02"
    logical = pendulum.datetime(2024, 1, 10, tz="UTC")
    assert query_date_from_context({"logical_date": logical, "dag_run": run}) == "2024-01-10"
    interval = pendulum.datetime(2024, 1, 9, tz="UTC")
    assert (
        query_date_from_context(
            {"data_interval_start": interval, "logical_date": logical, "dag_run": run}
        )
        == "2024-01-09"
    )


@pytest.mark.parametrize("fixture_name", ["feed", "training_feed"])
def test_task_handoffs_publish_outputs(request, fixture_name, monkeypatch, tmp_path):
    from types import SimpleNamespace

    import pendulum

    import dags.asteroid_monitor_dag as module
    from asteroid_monitor.storage import read_json

    feed = request.getfixturevalue(fixture_name)
    records = len(feed["near_earth_objects"]["2025-01-01"])
    monkeypatch.setenv("ASTEROID_ROOT", str(tmp_path))
    monkeypatch.setattr(module, "fetch_feed", lambda query_date: feed)
    monkeypatch.setattr(
        module,
        "get_current_context",
        lambda: {
            "run_id": "manual__fixture",
            "dag_run": SimpleNamespace(run_after=pendulum.datetime(2025, 1, 2, 12, tz="UTC")),
        },
    )
    dag = module.asteroid_monitor()
    for _ in range(2):
        result = dag.get_task("fetch_asteroid_data").python_callable()
        for task_id in [
            "validate_data",
            "transform_data",
            "save_processed_data",
            "generate_analytics",
            "train_model",
            "evaluate_model",
            "validate_outputs",
        ]:
            result = dag.get_task(task_id).python_callable(result)
        assert result == {"records": records, "query_date": "2025-01-01"}
    manifest = read_json(tmp_path / "reports/latest.json")
    assert not Path(manifest["csv_path"]).is_absolute()
    assert (tmp_path / manifest["csv_path"]).is_file()
    assert len(list((tmp_path / "data/processed").glob("*.csv"))) == 1
    model = read_json(tmp_path / manifest["model_path"])
    evaluation = read_json(tmp_path / manifest["evaluation_path"])
    assert model["status"] == ("skipped" if records == 2 else "trained")
    assert evaluation["status"] == ("skipped" if records == 2 else "evaluated")
