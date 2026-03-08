| run_id | run_date | check_name | status | severity | details | checked_at |
| --- | --- | --- | --- | --- | --- | --- |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_schema_fact_claims | PASS | error | ok | 2026-03-08 10:13:51.886531 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_claim_id_uniqueness | PASS | error | total=126, unique=126 | 2026-03-08 10:13:54.575761 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_fact_claims_mandatory_not_null | PASS | error | violations=0 | 2026-03-08 10:13:55.026652 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_claim_status_enum | PASS | error | violations=0 | 2026-03-08 10:13:55.419812 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_amount_constraints | PASS | error | violations=0 | 2026-03-08 10:13:55.820611 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_referential_integrity | PASS | error | fact_claims.patient_fk=0, fact_claims.provider_fk=0, fact_prescriptions.treatment_fk=0 | 2026-03-08 10:13:57.503635 |
| 2025-02-01_quality_20260308T101342Z | 2025-02-01 | python_row_count_reconciliation | PASS | error | stg_claims=126, fact_claims=126, stg_prescriptions=133, fact_prescriptions=133 | 2026-03-08 10:13:58.527314 |
| 2025-02-01_load_20260308T101401Z | 2025-02-01 | sql_fact_claims_patient_fk | PASS | error | fail_count=0 | 2026-03-08 10:14:02.144953 |
| 2025-02-01_load_20260308T101401Z | 2025-02-01 | sql_fact_claims_provider_fk | PASS | error | fail_count=0 | 2026-03-08 10:14:02.144953 |
| 2025-02-01_load_20260308T101401Z | 2025-02-01 | sql_reimbursed_le_billed | PASS | error | fail_count=0 | 2026-03-08 10:14:02.144953 |
| 2025-02-01_load_20260308T101401Z | 2025-02-01 | sql_reimbursement_claim_totals_match | PASS | error | fail_count=0 | 2026-03-08 10:14:02.144953 |