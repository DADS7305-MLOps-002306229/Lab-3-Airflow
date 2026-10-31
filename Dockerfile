FROM apache/airflow:3.1.8-python3.11
WORKDIR /opt/project
COPY --chown=airflow:root requirements.txt pyproject.toml README.md ./
COPY --chown=airflow:root src ./src
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r requirements.txt .
COPY --chown=airflow:root dags ./dags
COPY --chown=airflow:root app ./app
COPY --chown=airflow:root .streamlit/config.toml ./.streamlit/config.toml
ENV ASTEROID_ROOT=/opt/project
