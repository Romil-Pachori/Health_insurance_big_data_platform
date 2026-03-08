# Showcase Evidence Map

This document links each portfolio claim to concrete artifacts generated in the golden run.

## Baseline

- Golden run date: `2025-02-01`
- DAG: `health_insurance_pipeline_v1`
- Artifact root: `artifacts/golden-run/2025-02-01/`

## Claim-to-Evidence

| Claim | Evidence artifact | Verification query or note |
|---|---|---|
| Full Airflow DAG runs end-to-end in Docker | [task_run_summary.md](../artifacts/golden-run/2025-02-01/task_run_summary.md), [02_airflow_task_success.png](../artifacts/screenshots/02_airflow_task_success.png) | Contains 8/8 task successes for run `manual__2025-02-01T00:00:00+00:00`. |
| Runtime includes Spark support in Airflow containers | [docker/airflow/Dockerfile](../docker/airflow/Dockerfile) | Installs `openjdk-17-jre-headless` and `procps`, sets `JAVA_HOME`. |
| Curated star-schema tables are loaded in PostgreSQL | [curated_table_row_counts.csv](../artifacts/golden-run/2025-02-01/curated_table_row_counts.csv), [03_postgres_curated_tables.png](../artifacts/screenshots/03_postgres_curated_tables.png) | Includes dimensions + facts with non-zero row counts. |
| Critical quality gates pass | [quality_check_results.csv](../artifacts/golden-run/2025-02-01/quality_check_results.csv), [04_quality_check_results.png](../artifacts/screenshots/04_quality_check_results.png) | All recorded statuses are `PASS` for the run date. |
| Reimbursement constraints hold | [quality_check_results.csv](../artifacts/golden-run/2025-02-01/quality_check_results.csv) | `sql_reimbursed_le_billed` has `fail_count=0`. |
| KPI analytics are queryable from curated model | [sql_kpi_claims_trend.csv](../artifacts/golden-run/2025-02-01/sql_kpi_claims_trend.csv), [05_notebook_kpi_output.png](../artifacts/screenshots/05_notebook_kpi_output.png) | Query defined in [sql/analytics/claims_kpis.sql](../sql/analytics/claims_kpis.sql). |
| Anomaly-prep features are available | [sql_anomaly_provider_features.csv](../artifacts/golden-run/2025-02-01/sql_anomaly_provider_features.csv) | Provider-level rejection-rate and volume features are materialized as SQL output snapshot. |
| Demo is reproducible even when live URLs fail | [ingestion_manifest.json](../artifacts/golden-run/2025-02-01/ingestion_manifest.json), [source_health_report.md](../artifacts/golden-run/2025-02-01/source_health_report.md) | Manifest records fallback usage by source and fallback ratio. |
| Architecture and orchestration are documented visually | [architecture.mmd](architecture.mmd), [architecture.png](architecture.png), [01_airflow_dag_graph.png](../artifacts/screenshots/01_airflow_dag_graph.png) | High-level flow and execution graph are both captured. |

## SQL Checks Used in Evidence

```sql
-- Non-negative reimbursement constraint
SELECT COUNT(*) AS fail_count
FROM curated.fact_claims
WHERE reimbursed_amount > billed_amount;

-- FK check example
SELECT COUNT(*) AS fail_count
FROM curated.fact_claims f
LEFT JOIN curated.dim_patient d ON f.patient_sk = d.patient_sk
WHERE d.patient_sk IS NULL;
```

## Screenshot Index

- [01_airflow_dag_graph.png](../artifacts/screenshots/01_airflow_dag_graph.png)
- [02_airflow_task_success.png](../artifacts/screenshots/02_airflow_task_success.png)
- [03_postgres_curated_tables.png](../artifacts/screenshots/03_postgres_curated_tables.png)
- [04_quality_check_results.png](../artifacts/screenshots/04_quality_check_results.png)
- [05_notebook_kpi_output.png](../artifacts/screenshots/05_notebook_kpi_output.png)
