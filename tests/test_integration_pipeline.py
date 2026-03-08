from pathlib import Path

import pandas as pd
import yaml

from src.ingestion.run import run_ingestion
from src.transformations.run import run_transformations
from src.utils.config import project_root


def test_end_to_end_pipeline_with_sample_fallback(tmp_path: Path):
    root = project_root()
    sample_root = root / "data" / "raw" / "sample"
    raw_root = tmp_path / "raw"
    staging_root = tmp_path / "staging"
    curated_root = tmp_path / "curated"
    run_date = "2025-03-01"

    cfg = {
        "project": {"name": "test", "default_run_date": run_date},
        "paths": {
            "raw_root": str(raw_root),
            "staging_root": str(staging_root),
            "curated_root": str(curated_root),
            "sample_root": str(sample_root),
        },
        "sources": {
            "providers": {
                "url": "http://127.0.0.1:9/providers.csv",
                "fallback_file": "providers_sample.csv",
                "required_columns": ["provider_id", "provider_name", "provider_type", "region_id"],
            },
            "claims": {
                "url": "http://127.0.0.1:9/claims.csv",
                "fallback_file": "claims_agg_sample.csv",
                "required_columns": [
                    "provider_id",
                    "region_id",
                    "treatment_category",
                    "claim_month",
                    "claim_count",
                    "total_billed",
                    "total_reimbursed",
                    "claim_status",
                ],
            },
            "prescriptions": {
                "url": "http://127.0.0.1:9/prescriptions.csv",
                "fallback_file": "prescriptions_agg_sample.csv",
                "required_columns": [
                    "provider_id",
                    "region_id",
                    "treatment_category",
                    "claim_month",
                    "prescription_count",
                    "total_quantity",
                ],
            },
            "regions": {
                "url": "http://127.0.0.1:9/regions.csv",
                "fallback_file": "regions_sample.csv",
                "required_columns": ["region_id", "region_name", "population_group"],
            },
        },
        "ingestion": {"timeout_seconds": 1, "use_sample_on_failure": True, "batch_mode": "snapshot"},
        "synthesis": {
            "seed": 123,
            "default_claim_status": "approved",
            "visit_share_hash_threshold": 3,
        },
        "quality": {
            "allowed_claim_status": ["approved", "pending", "rejected"],
            "fail_on_error": True,
        },
        "postgres": {
            "host": "localhost",
            "port": "5432",
            "database": "health_insurance",
            "user": "health_user",
            "password": "health_password",
            "schemas": {"raw": "raw", "staging": "staging", "curated": "curated", "quality": "quality"},
        },
    }

    cfg_path = tmp_path / "pipeline.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    run_ingestion(str(cfg_path), run_date=run_date)
    run_transformations(config_path=str(cfg_path), layer="all", run_date=run_date)

    fact_claims_path = curated_root / run_date / "fact_claims"
    assert fact_claims_path.exists()
    fact_claims = pd.read_parquet(fact_claims_path)
    assert len(fact_claims) > 0

