CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS quality.check_results (
    run_id TEXT NOT NULL,
    run_date DATE NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT,
    checked_at TIMESTAMP NOT NULL DEFAULT NOW()
);

