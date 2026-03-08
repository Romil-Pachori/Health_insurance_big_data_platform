import argparse
from typing import Any

from src.transformations.curated import run_curated_transformations
from src.transformations.staging import run_staging_transformations
from src.utils.config import load_config, run_date_or_default
from src.utils.logging_utils import get_logger
from src.utils.spark import build_spark

LOGGER = get_logger(__name__)


def run_transformations(config_path: str, layer: str, run_date: str | None = None) -> dict[str, Any]:
    config = load_config(config_path)
    resolved_run_date = run_date_or_default(config, run_date)
    spark = build_spark(app_name=f"health-insurance-{layer}")
    outputs: dict[str, Any] = {}
    try:
        if layer in {"staging", "all"}:
            outputs["staging"] = run_staging_transformations(spark, config, resolved_run_date)
        if layer in {"curated", "all"}:
            outputs["curated"] = run_curated_transformations(spark, config, resolved_run_date)
    finally:
        spark.stop()
    LOGGER.info("Transformation run completed for layer=%s run_date=%s", layer, resolved_run_date)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run staging/curated Spark transformations.")
    parser.add_argument("--layer", choices=["staging", "curated", "all"], default="all")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    run_transformations(config_path=args.config, layer=args.layer, run_date=args.run_date)


if __name__ == "__main__":
    main()

