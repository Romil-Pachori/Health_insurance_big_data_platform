SELECT
    d.year,
    d.month,
    SUM(f.total_claims) AS claims_volume,
    ROUND(SUM(f.total_billed)::numeric, 2) AS billed_total,
    ROUND(SUM(f.total_reimbursed)::numeric, 2) AS reimbursed_total,
    ROUND((SUM(f.total_reimbursed) / NULLIF(SUM(f.total_billed), 0))::numeric, 4) AS reimbursement_rate
FROM curated.fact_reimbursements f
JOIN curated.dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
