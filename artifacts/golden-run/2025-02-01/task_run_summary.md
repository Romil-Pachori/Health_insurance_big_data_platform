# Golden Run Task Summary

- `dag_id`: `health_insurance_pipeline_v1`
- `run_date`: `2025-02-01`
- `run_id`: `manual__2025-02-01T00:00:00+00:00`
- `dag_state`: `success`
- `task_count`: `8`
- `successful_tasks`: `8`

## Task States

| dag_id | execution_date | task_id | state | start_date | end_date |
| --- | --- | --- | --- | --- | --- |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | build_dimensions | success |  | 2026-03-08T10:13:19.917241+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | build_facts | success |  | 2026-03-08T10:13:40.783378+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | ingest_raw | success |  | 2026-03-08T10:12:15.193997+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | load_postgres | success |  | 2026-03-08T10:14:02.361544+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | publish_notebooks_metadata | success |  | 2026-03-08T10:14:02.925082+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | quality_checks | success |  | 2026-03-08T10:13:59.728487+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | stage_clean | success |  | 2026-03-08T10:12:37.935892+00:00 |
| health_insurance_pipeline_v1 | 2025-02-01T00:00:00+00:00 | synthesize_events | success |  | 2026-03-08T10:12:58.985594+00:00 |
