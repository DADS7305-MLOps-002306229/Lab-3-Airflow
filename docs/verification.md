# Verification record

Verified locally on 2026-10-31 using the Docker image with Python 3.11 and Airflow 3.1.8. Host Python was 3.14, so the host interpreter was not used to run Airflow.

| Check | Result |
|---|---|
| `docker compose config --quiet` | Passed |
| `docker compose build` | Passed for both services |
| `pip check` inside image | Passed: no broken requirements |
| `ruff check .` | Passed |
| `ruff format --check .` | Passed |
| `pytest -q` | 32 passed; one upstream Requests dependency warning |
| Airflow `DagBag` parsing | Passed, six expected tasks and linear dependencies |
| Offline six-task handoff test | Passed twice, including stable retry paths and manifest publication |
| Streamlit `AppTest` | Missing outputs and populated/filter interaction tests passed |
| Standalone Airflow startup | Passed, SQLite and LocalExecutor |
| Streamlit health endpoint | HTTP 200 |
| Live NASA NeoWs integration | Passed with development fallback key |
| Manual DAG execution | Airflow state `success`, completed at 2026-10-31T19:57:05 UTC |
| Scheduled DAG execution | Airflow state `success`, completed at 2026-10-31T19:57:30 UTC |
| Dashboard against real output | Rendered four metrics without application exceptions |
| Git diff whitespace check | Passed |

Both live runs queried 2026-10-30 and produced two asteroid rows, with zero potentially hazardous classifications. This is an observed verification result, not a guarantee of future API contents. Generated data and reports are ignored by Git.

The initial manual execution exposed Airflow 3's nullable logical date/data interval; the corrected image handled that same run successfully on retry. A regression test now covers interval, logical-date, and undated-manual semantics.

## Remaining verification limits

- No browser screenshots or visual viewport inspection were performed; screenshot capture instructions are in `docs/screenshots/README.md`. Streamlit was tested through its testing interface and HTTP health endpoint.
- GitHub Actions workflows were authored but have not executed on GitHub. Equivalent lint, test, Compose, and image build checks ran locally.
- Native Python 3.11 installation was not exercised; the supported runtime was verified in Docker.
- The upstream Airflow image currently supplies chardet 6 alongside Requests 2.32.5, which emits a dependency warning. Dependency checks, mocked tests, and live HTTP ingestion still passed. This warning is not suppressed.
- API quotas, external connectivity and source revisions remain outside the project's control.

The local Airflow and Streamlit services were left running for inspection. Stop them with `docker compose down`; generated host artifacts and the metadata volume persist. No Git commit, remote creation, image publication or push was performed.

## Follow-up: readonly metadata database

A later restart without the temporary UID override ran the service as UID 50000,
while `airflow.db` remained owned by UID 1000 with mode 0644. Triggering a run
therefore failed with `sqlite3.OperationalError: attempt to write a readonly database`.
Persisted `AIRFLOW_UID=1000` in the ignored local `.env` and recreated both services.
A rollback-only SQLite UPDATE transaction then passed under UID 1000. No database,
run history, credentials, or output artifacts were deleted. README recovery guidance
and the environment example now explain keeping the UID stable.

## Machine learning extension

Verified the eight-task DAG and scikit-learn 1.7.2 extension in Python 3.11:

- 44 Pytest tests passed, including deterministic training, held-out evaluation,
  JSON model round trips, train/test separation, training-only scaling,
  insufficient-data handling, invalid artifacts, and the dashboard metrics view.
- All eight task functions ran twice with both a small feed (ML skipped) and an
  80-object synthetic feed (model trained/evaluated); manifests published correctly.
- Ruff linting and formatting checks passed.
- Both Docker images built with scikit-learn 1.7.2; `pip check` passed.
- The existing CI job runs the new tests automatically; image smoke checks also
  import scikit-learn. No GitHub workflow execution or release was triggered here.

The synthetic training fixture verifies software behavior, not scientific accuracy.
No live NASA training performance is claimed. Real daily feeds may have too few
examples of both classes, in which case JSON artifacts explicitly record that no
model was trained. The existing upstream Requests warning remains unrelated to
these ML changes.
