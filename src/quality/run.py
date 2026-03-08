import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from src.quality.checks import evaluate_python_checks
from src.quality.persistence import build_engine, ensure_schemas_and_quality_table, persist_check_results
from src.utils.config import load_config, resolve_path, run_date_or_default
from src.utils.logging_utils import get_logger
from src.utils.spark import build_spark

LOGGER = get_logger(__name__)

CURATED_TABLES = [
    "fact_claims",
    "fact_prescriptions",
    "fact_visits",
    "fact_reimbursements",
    "dim_patient",
    "dim_provider",
    "dim_region",
    "dim_treatment",
    "dim_date",
]

STAGING_TABLES = [
    "stg_claims_events",
    "stg_prescriptions_events",
]


def _load_parquet_tables(spark: SparkSession, root: Path, table_names: list[str]) -> dict:
    tables = {}
    for name in table_names:
        path = root / name
        if not path.exists():
            raise FileNotFoundError(f"Missing expected table path: {path}")
        tables[name] = spark.read.parquet(str(path))
    return tables


def run_quality_checks(config_path: str, run_date: str | None = None) -> list[dict]:
    config = load_config(config_path)
    resolved_run_date = run_date_or_default(config, run_date)
    run_id = f"{resolved_run_date}_quality_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    curated_root = resolve_path(config["paths"]["curated_root"]) / resolved_run_date
    staging_root = resolve_path(config["paths"]["staging_root"]) / resolved_run_date

    spark = build_spark("health-insurance-quality")
    try:
        curated = _load_parquet_tables(spark, curated_root, CURATED_TABLES)
        staging = _load_parquet_tables(spark, staging_root, STAGING_TABLES)
        results = evaluate_python_checks(
            spark=spark,
            curated=curated,
            staging=staging,
            run_id=run_id,
            run_date=resolved_run_date,
            allowed_claim_status=config["quality"]["allowed_claim_status"],
        )
    finally:
        spark.stop()

    out_path = curated_root / "quality_python_results.json"
    out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    LOGGER.info("Saved python quality results to %s", out_path)

    try:
        engine = build_engine(config["postgres"])
        ensure_schemas_and_quality_table(engine, config["postgres"]["schemas"])
        persist_check_results(engine, results)
        LOGGER.info("Persisted python quality results to PostgreSQL quality.check_results")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Could not persist python quality results to PostgreSQL: %s", exc)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run native python quality checks.")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    run_quality_checks(config_path=args.config, run_date=args.run_date)


if __name__ == "__main__":
    main()

