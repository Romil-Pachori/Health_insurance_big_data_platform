import argparse
from pathlib import Path

from src.ingestion.sources import ingest_one_source, write_manifest, write_source_health_report
from src.utils.config import load_config, resolve_path, run_date_or_default
from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


def run_ingestion(config_path: str, run_date: str | None = None) -> Path:
    config = load_config(config_path)
    resolved_run_date = run_date_or_default(config, run_date)

    raw_root = resolve_path(config["paths"]["raw_root"]) / resolved_run_date
    sample_root = resolve_path(config["paths"]["sample_root"])
    ingestion_cfg = config["ingestion"]
    sources_cfg = config["sources"]

    results = []
    for source_name, source_cfg in sources_cfg.items():
        output_file = raw_root / f"{source_name}.csv"
        result = ingest_one_source(
            source_name=source_name,
            source_cfg=source_cfg,
            sample_root=sample_root,
            output_path=output_file,
            timeout_seconds=int(ingestion_cfg["timeout_seconds"]),
            use_sample_on_failure=bool(ingestion_cfg["use_sample_on_failure"]),
        )
        results.append(result)

    manifest_path = write_manifest(raw_root, results)
    report_path = write_source_health_report(raw_root, results)
    LOGGER.info(
        "Ingestion complete for run_date=%s; manifest=%s; source_health_report=%s",
        resolved_run_date,
        manifest_path,
        report_path,
    )
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest healthcare source datasets.")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    run_ingestion(config_path=args.config, run_date=args.run_date)


if __name__ == "__main__":
    main()
