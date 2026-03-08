-- SQL checks executed by src.transformations.load_postgres

-- reimbursed_amount must not exceed billed_amount
SELECT COUNT(*) AS fail_count
FROM curated.fact_claims
WHERE reimbursed_amount > billed_amount;

-- all fact_claims patient keys should exist in dim_patient
SELECT COUNT(*) AS fail_count
FROM curated.fact_claims f
LEFT JOIN curated.dim_patient d ON f.patient_sk = d.patient_sk
WHERE d.patient_sk IS NULL;

-- all fact_claims provider keys should exist in dim_provider
SELECT COUNT(*) AS fail_count
FROM curated.fact_claims f
LEFT JOIN curated.dim_provider d ON f.provider_sk = d.provider_sk
WHERE d.provider_sk IS NULL;

