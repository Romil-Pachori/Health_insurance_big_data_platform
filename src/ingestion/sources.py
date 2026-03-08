import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


@dataclass
class IngestionResult:
    source_name: str
    source_url: str
    status: str
    used_fallback: bool
    fallback_file: str
    output_path: str
    records: int
    note: str
    error: str


def _download_csv(url: str, timeout_seconds: int) -> pd.DataFrame:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))


def _load_fallback(sample_path: Path) -> pd.DataFrame:
    return pd.read_csv(sample_path)


def validate_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def ingest_one_source(
    source_name: str,
    source_cfg: dict[str, Any],
    sample_root: Path,
    output_path: Path,
    timeout_seconds: int,
    use_sample_on_failure: bool,
) -> IngestionResult:
    note = ""
    status = "downloaded"
    used_fallback = False
    error_message = ""

    try:
        df = _download_csv(source_cfg["url"], timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        if not use_sample_on_failure:
            raise RuntimeError(f"Source {source_name} failed and fallback disabled") from exc
        fallback = sample_root / source_cfg["fallback_file"]
        df = _load_fallback(fallback)
        status = "fallback_sample"
        used_fallback = True
        error_message = str(exc)
        note = f"Download failed. Used sample file {fallback.name}."
        LOGGER.warning("Using fallback for %s due to: %s", source_name, exc)

    validate_columns(df, source_cfg["required_columns"])

    df = df[source_cfg["required_columns"]].copy()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    LOGGER.info("Ingested %s records for %s to %s", len(df), source_name, output_path)

    return IngestionResult(
        source_name=source_name,
        source_url=source_cfg["url"],
        status=status,
        used_fallback=used_fallback,
        fallback_file=source_cfg["fallback_file"],
        output_path=str(output_path),
        records=len(df),
        note=note,
        error=error_message,
    )


def write_manifest(run_root: Path, results: list[IngestionResult]) -> Path:
    downloaded_count = len([r for r in results if not r.used_fallback])
    fallback_count = len([r for r in results if r.used_fallback])
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_health_summary": {
            "total_sources": len(results),
            "downloaded_count": downloaded_count,
            "fallback_count": fallback_count,
            "fallback_ratio": round((fallback_count / len(results)) if results else 0.0, 4),
        },
        "results": [result.__dict__ for result in results],
    }
    path = run_root / "manifest.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def write_source_health_report(run_root: Path, results: list[IngestionResult]) -> Path:
    lines = [
        "# Source Health Report",
        "",
        "| source_name | status | used_fallback | records | source_url | fallback_file |",
        "|---|---|---|---:|---|---|",
    ]
    for result in results:
        lines.append(
            "| {name} | {status} | {fallback} | {records} | {url} | {fallback_file} |".format(
                name=result.source_name,
                status=result.status,
                fallback=str(result.used_fallback).lower(),
                records=result.records,
                url=result.source_url,
                fallback_file=result.fallback_file,
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "Fallback is expected in reproducible demo mode when public links are unavailable.",
        ]
    )
    path = run_root / "source_health_report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
