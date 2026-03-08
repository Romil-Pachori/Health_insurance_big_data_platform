import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.quality.persistence import build_engine, ensure_schemas_and_quality_table, persist_check_results
from src.utils.config import load_config, resolve_path, run_date_or_default
from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)

CURATED_TABLES = [
    "dim_region",
    "dim_provider",
    "dim_treatment",
    "dim_patient",
    "dim_date",
    "fact_claims",
    "fact_prescriptions",
    "fact_visits",
    "fact_reimbursements",
]


def _run_post_load_sql_checks(curated_schema: str) -> dict[str, str]:
    return {
        "sql_reimbursed_le_billed": (
            f"SELECT COUNT(*) AS fail_count FROM {curated_schema}.fact_claims "
            "WHERE reimbursed_amount > billed_amount"
        ),
        "sql_fact_claims_patient_fk": (
            f"SELECT COUNT(*) AS fail_count FROM {curated_schema}.fact_claims f "
            f"LEFT JOIN {curated_schema}.dim_patient d ON f.patient_sk = d.patient_sk "
            "WHERE d.patient_sk IS NULL"
        ),
        "sql_fact_claims_provider_fk": (
            f"SELECT COUNT(*) AS fail_count FROM {curated_schema}.fact_claims f "
            f"LEFT JOIN {curated_schema}.dim_provider d ON f.provider_sk = d.provider_sk "
            "WHERE d.provider_sk IS NULL"
        ),
        "sql_reimbursement_claim_totals_match": (
            f"SELECT ABS((SELECT COALESCE(SUM(total_claims), 0) FROM {curated_schema}.fact_reimbursements) "
            f"- (SELECT COUNT(*) FROM {curated_schema}.fact_claims)) AS fail_count"
        ),
    }


def _load_one_table(table_name: str, table_path: Path, curated_schema: str, engine: Any) -> int:
    df = pd.read_parquet(table_path)
    df.to_sql(
        name=table_name,
        con=engine,
        schema=curated_schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)


def load_curated_to_postgres(config_path: str, run_date: str | None = None) -> dict[str, int]:
    config = load_config(config_path)
    resolved_run_date = run_date_or_default(config, run_date)
    run_id = f"{resolved_run_date}_load_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    curated_root = resolve_path(config["paths"]["curated_root"]) / resolved_run_date
    postgres_cfg = config["postgres"]
    schemas = postgres_cfg["schemas"]
    curated_schema = schemas["curated"]

    engine = build_engine(postgres_cfg)
    ensure_schemas_and_quality_table(engine, schemas)

    load_counts: dict[str, int] = {}
    for table in CURATED_TABLES:
        table_path = curated_root / table
        if not table_path.exists():
            raise FileNotFoundError(f"Expected curated table path missing: {table_path}")
        loaded = _load_one_table(table, table_path, curated_schema=curated_schema, engine=engine)
        load_counts[table] = loaded
        LOGGER.info("Loaded %s rows into %s.%s", loaded, curated_schema, table)

    sql_checks = _run_post_load_sql_checks(curated_schema=curated_schema)
    results = []
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    with engine.begin() as conn:
        for check_name, query in sql_checks.items():
            fail_count = int(conn.execute(text(query)).scalar() or 0)
            results.append(
                {
                    "run_id": run_id,
                    "run_date": resolved_run_date,
                    "check_name": check_name,
                    "status": "PASS" if fail_count == 0 else "FAIL",
                    "severity": "error",
                    "details": f"fail_count={fail_count}",
                    "checked_at": checked_at,
                }
            )

    persist_check_results(engine, results)
    LOGGER.info("Post-load SQL quality checks completed; results persisted for run_id=%s", run_id)
    return load_counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Load curated parquet outputs into PostgreSQL.")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    load_curated_to_postgres(config_path=args.config, run_date=args.run_date)


if __name__ == "__main__":
    main()

