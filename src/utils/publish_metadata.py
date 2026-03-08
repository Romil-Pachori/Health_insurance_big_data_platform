import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.utils.config import load_config, resolve_path, run_date_or_default
from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


def publish_metadata(config_path: str, run_date: str | None = None) -> Path:
    config = load_config(config_path)
    resolved_run_date = run_date_or_default(config, run_date)
    curated_root = resolve_path(config["paths"]["curated_root"]) / resolved_run_date
    curated_root.mkdir(parents=True, exist_ok=True)

    table_dirs = sorted([p.name for p in curated_root.iterdir() if p.is_dir()])
    payload = {
        "run_date": resolved_run_date,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "curated_tables": table_dirs,
        "notebooks": [
            "notebooks/claims_kpi_trends.ipynb",
            "notebooks/anomaly_prep_features.ipynb",
        ],
    }

    metadata_path = curated_root / "metadata.json"
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("Published metadata to %s", metadata_path)
    return metadata_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish run metadata for notebooks.")
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config.")
    parser.add_argument("--run-date", required=False, help="Run date partition (YYYY-MM-DD).")
    args = parser.parse_args()

    publish_metadata(config_path=args.config, run_date=args.run_date)


if __name__ == "__main__":
    main()

