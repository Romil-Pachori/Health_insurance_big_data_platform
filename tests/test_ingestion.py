from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.sources import ingest_one_source, validate_columns


def test_validate_columns_raises_on_missing_fields():
    df = pd.DataFrame({"a": [1], "b": [2]})
    with pytest.raises(ValueError):
        validate_columns(df, ["a", "c"])


def test_ingest_fallback_sample(tmp_path: Path):
    sample_root = tmp_path / "sample"
    sample_root.mkdir(parents=True, exist_ok=True)
    sample_file = sample_root / "providers_sample.csv"
    sample_file.write_text(
        "provider_id,provider_name,provider_type,region_id\nP1,N1,hospital,R1\n",
        encoding="utf-8",
    )

    source_cfg = {
        "url": "http://127.0.0.1:9/not_available.csv",
        "fallback_file": "providers_sample.csv",
        "required_columns": ["provider_id", "provider_name", "provider_type", "region_id"],
    }
    output = tmp_path / "raw" / "providers.csv"
    result = ingest_one_source(
        source_name="providers",
        source_cfg=source_cfg,
        sample_root=sample_root,
        output_path=output,
        timeout_seconds=1,
        use_sample_on_failure=True,
    )

    assert result.status == "fallback_sample"
    assert output.exists()
    assert "provider_id" in output.read_text(encoding="utf-8")

