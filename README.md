# Health Insurance Big Data Platform

Docker-first MVP data platform for France/EU open-health style analytics using Python, Spark, Airflow, PostgreSQL, and Jupyter.

## Quick Recruiter Scan

- Architecture: [docs/architecture.png](docs/architecture.png)
- DAG success proof: [artifacts/golden-run/2025-02-01/task_run_summary.md](artifacts/golden-run/2025-02-01/task_run_summary.md)
- Curated row counts: [artifacts/golden-run/2025-02-01/curated_table_row_counts.csv](artifacts/golden-run/2025-02-01/curated_table_row_counts.csv)
- Quality checks snapshot: [artifacts/golden-run/2025-02-01/quality_check_results.csv](artifacts/golden-run/2025-02-01/quality_check_results.csv)
- KPI SQL output: [artifacts/golden-run/2025-02-01/sql_kpi_claims_trend.csv](artifacts/golden-run/2025-02-01/sql_kpi_claims_trend.csv)
- Evidence mapping: [docs/showcase-evidence.md](docs/showcase-evidence.md)

## Reproducible-First Data Strategy

Default demo runs are intentionally stable and repeatable:

- `src/config/pipeline.yaml` keeps `ingestion.use_sample_on_failure: true`.
- If a public endpoint is unavailable, the pipeline falls back to local fixtures in `data/raw/sample`.
- Ingestion writes both `manifest.json` and `source_health_report.md` so fallback vs live source usage is explicit.

Optional live connector experiments are isolated in:

- `src/config/pipeline.live.yaml` (fallback disabled).

## Golden Run (Docker, Recommended)

### 1) Start services

```bash
cp .env.example .env
docker compose down --remove-orphans
docker compose build airflow-init
docker compose up -d
```

### 2) Validate Airflow health

```bash
# PowerShell
(Invoke-WebRequest -Uri http://localhost:8080/health -UseBasicParsing).StatusCode
```

Expected: `200`.

### 3) Run the full DAG for baseline date

```bash
docker compose exec -T airflow-webserver airflow dags test health_insurance_pipeline_v1 2025-02-01
```

Expected task chain success:

- `ingest_raw`
- `stage_clean`
- `synthesize_events`
- `build_dimensions`
- `build_facts`
- `quality_checks`
- `load_postgres`
- `publish_notebooks_metadata`

### 4) Verify evidence artifacts

Artifacts are written under:

- `artifacts/golden-run/2025-02-01/`
- `artifacts/screenshots/`

Required screenshot filenames:

- `01_airflow_dag_graph.png`
- `02_airflow_task_success.png`
- `03_postgres_curated_tables.png`
- `04_quality_check_results.png`
- `05_notebook_kpi_output.png`

## Local Development (.venv)

Use local `.venv` for CLI development/testing. Airflow runtime remains Docker-only.

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Core CLI contracts:

```bash
python -m src.ingestion.run --config src/config/pipeline.yaml --run-date 2025-02-01
python -m src.transformations.run --layer staging --config src/config/pipeline.yaml --run-date 2025-02-01
python -m src.transformations.run --layer curated --config src/config/pipeline.yaml --run-date 2025-02-01
python -m src.quality.run --config src/config/pipeline.yaml --run-date 2025-02-01
python -m src.transformations.load_postgres --config src/config/pipeline.yaml --run-date 2025-02-01
```

Optional metadata publication:

```bash
python -m src.utils.publish_metadata --config src/config/pipeline.yaml --run-date 2025-02-01
```

## Optional Live Source Mode

For non-demo experiments only:

```bash
python -m src.ingestion.run --config src/config/pipeline.live.yaml --run-date 2025-02-01
```

This may fail if public links are unavailable, by design.

## Docker Recovery Commands

If stale containers/networks cause startup conflicts:

```bash
docker compose down --remove-orphans
docker rm -f airflow_init airflow_webserver airflow_scheduler health_postgres health_spark health_jupyter 2>/dev/null || true
```

## Data Quality Guarantees

Python checks and SQL checks are persisted to `quality.check_results`.

Rules include:

- schema presence and mandatory fields
- ID uniqueness
- enum validity (`claim_status`)
- non-negative and bounded amounts (`reimbursed_amount <= billed_amount`)
- FK integrity checks after load
- staging-to-curated reconciliation

## Notes

- No PHI is used.
- Patient-level rows are deterministic pseudonymized synthetic derivations from open aggregates.
- Pipeline is idempotent per `run_date` partition.
