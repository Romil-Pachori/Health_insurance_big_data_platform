from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/project"
CONFIG_PATH = "/opt/project/src/config/pipeline.yaml"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
}


with DAG(
    dag_id="health_insurance_pipeline_v1",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["health", "spark", "postgres", "quality"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.ingestion.run --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    stage_clean = BashOperator(
        task_id="stage_clean",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.transformations.stage_clean --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    synthesize_events = BashOperator(
        task_id="synthesize_events",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.transformations.synthesize_events --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    build_dimensions = BashOperator(
        task_id="build_dimensions",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.transformations.build_dimensions --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    build_facts = BashOperator(
        task_id="build_facts",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.transformations.build_facts --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.quality.run --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.transformations.load_postgres --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    publish_notebooks_metadata = BashOperator(
        task_id="publish_notebooks_metadata",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"python -m src.utils.publish_metadata --config {CONFIG_PATH} "
            "--run-date '{{ ds }}'"
        ),
    )

    ingest_raw >> stage_clean >> synthesize_events >> build_dimensions >> build_facts
    build_facts >> quality_checks >> load_postgres >> publish_notebooks_metadata
