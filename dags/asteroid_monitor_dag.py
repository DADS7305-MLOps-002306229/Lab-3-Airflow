"""Eight small TaskFlow tasks; only paths and metadata cross XCom."""

import hashlib
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from asteroid_monitor.analytics import summarize
from asteroid_monitor.api import fetch_feed
from asteroid_monitor.modeling import evaluate_classifier, train_classifier
from asteroid_monitor.processing import transform_feed, validate_feed
from asteroid_monitor.storage import (
    read_csv,
    read_json,
    root_path,
    save_csv,
    write_json,
)
from asteroid_monitor.storage import validate_outputs as check_outputs

LOGGER = logging.getLogger(__name__)


def query_date_from_context(context: dict) -> str:
    """Use the interval/logical date, or infer the prior UTC day for undated manual runs."""
    interval = context.get("data_interval_start") or context.get("logical_date")
    if interval is None:
        interval = (
            pendulum.instance(context["dag_run"].run_after).in_timezone("UTC").subtract(days=1)
        )
    return pendulum.instance(interval).in_timezone("UTC").date().isoformat()


@dag(
    dag_id="nasa_asteroid_monitor",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=5),
    },
    dagrun_timeout=timedelta(minutes=45),
    tags=["NASA", "university"],
    description="Fetch, validate and summarize one UTC day of NASA Earth approaches.",
)
def asteroid_monitor():
    @task(doc_md="Fetch one UTC date derived from data_interval_start; produce a raw JSON path.")
    def fetch_asteroid_data() -> dict:
        context = get_current_context()
        query_date = query_date_from_context(context)
        run_key = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
        filename = f"asteroids_{query_date}_{run_key}"
        path = write_json(root_path() / "data/raw" / f"{filename}.json", fetch_feed(query_date))
        return {"raw_path": path, "query_date": query_date, "filename": filename}

    @task(doc_md="Validate required fields and finite measurements in raw JSON; return metadata.")
    def validate_data(metadata: dict) -> dict:
        validate_feed(read_json(metadata["raw_path"]), metadata["query_date"])
        return metadata

    @task(doc_md="Flatten validated JSON; write a staging CSV and return its path.")
    def transform_data(metadata: dict) -> dict:
        frame = transform_feed(read_json(metadata["raw_path"]), metadata["query_date"])
        path = root_path() / "data/raw" / f"{metadata['filename']}_staging.csv"
        return {**metadata, "staging_path": save_csv(frame, path)}

    @task(doc_md="Read staging CSV and atomically save a predictable processed CSV.")
    def save_processed_data(metadata: dict) -> dict:
        path = root_path() / "data/processed" / f"{metadata['filename']}.csv"
        return {**metadata, "csv_path": save_csv(read_csv(metadata["staging_path"]), path)}

    @task(doc_md="Compute descriptive statistics from processed CSV and write a JSON report.")
    def generate_analytics(metadata: dict) -> dict:
        report = summarize(read_csv(metadata["csv_path"]), metadata["query_date"])
        path = root_path() / "reports" / f"{metadata['filename']}.json"
        return {**metadata, "report_path": write_json(path, report)}

    @task(doc_md="Train a classifier on a stratified training split, or record insufficient data.")
    def train_model(metadata: dict) -> dict:
        model = train_classifier(read_csv(metadata["csv_path"]))
        LOGGER.info("Model training status=%s records=%s", model["status"], model["record_count"])
        path = root_path() / "reports" / f"{metadata['filename']}_model.json"
        return {**metadata, "model_path": write_json(path, model)}

    @task(doc_md="Evaluate the saved model on held-out asteroids and report a majority baseline.")
    def evaluate_model(metadata: dict) -> dict:
        evaluation = evaluate_classifier(
            read_csv(metadata["csv_path"]), read_json(metadata["model_path"])
        )
        LOGGER.info(
            "Model evaluation status=%s metrics=%s", evaluation["status"], evaluation["metrics"]
        )
        path = root_path() / "reports" / f"{metadata['filename']}_evaluation.json"
        return {**metadata, "evaluation_path": write_json(path, evaluation)}

    @task(doc_md="Verify CSV and report agree; publish a manifest and log record statistics.")
    def validate_outputs(metadata: dict) -> dict:
        report = check_outputs(metadata["csv_path"], metadata["report_path"])
        evaluation = evaluate_classifier(
            read_csv(metadata["csv_path"]), read_json(metadata["model_path"])
        )
        saved = read_json(metadata["evaluation_path"])
        if any(
            saved.get(key) != value for key, value in evaluation.items() if key != "generated_at"
        ):
            raise ValueError("Saved model evaluation disagrees with held-out results")
        # Relative paths work both on the host and inside either container.
        root = root_path()
        write_json(
            root / "reports/latest.json",
            {
                "csv_path": str(Path(metadata["csv_path"]).relative_to(root)),
                "report_path": str(Path(metadata["report_path"]).relative_to(root)),
                "model_path": str(Path(metadata["model_path"]).relative_to(root)),
                "evaluation_path": str(Path(metadata["evaluation_path"]).relative_to(root)),
                "query_date": metadata["query_date"],
                "generated_at": report["generated_at"],
            },
        )
        return {"records": report["total_asteroids"], "query_date": metadata["query_date"]}

    # TaskFlow arguments establish the explicit linear dependencies.
    fetched = fetch_asteroid_data()
    validated = validate_data(fetched)
    transformed = transform_data(validated)
    processed = save_processed_data(transformed)
    analyzed = generate_analytics(processed)
    trained = train_model(analyzed)
    evaluated = evaluate_model(trained)
    validate_outputs(evaluated)


asteroid_monitor()
