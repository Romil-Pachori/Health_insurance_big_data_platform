import argparse

from src.transformations.staging import run_stage_cleaning
from src.utils.config import load_config, run_date_or_default
from src.utils.spark import build_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Run staging clean-only transforms.")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    config = load_config(args.config)
    resolved_run_date = run_date_or_default(config, args.run_date)
    spark = build_spark("health-insurance-stage-clean")
    try:
        run_stage_cleaning(spark, config, resolved_run_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

