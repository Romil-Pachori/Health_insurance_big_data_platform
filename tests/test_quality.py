from pyspark.sql import Row

from src.quality.checks import evaluate_python_checks


def _build_test_frames(spark, bad_amount: bool = False):
    fact_claims_rows = [
        Row(
            claim_sk=1,
            claim_id="C1",
            patient_sk=1,
            provider_sk=1,
            treatment_sk=1,
            date_sk=20250101,
            billed_amount=100.0,
            reimbursed_amount=120.0 if bad_amount else 90.0,
            claim_status="approved",
        )
    ]
    fact_prescription_rows = [
        Row(
            prescription_sk=1,
            prescription_id="RX1",
            patient_sk=1,
            provider_sk=1,
            treatment_sk=1,
            date_sk=20250101,
            quantity=2,
        )
    ]
    curated = {
        "fact_claims": spark.createDataFrame(fact_claims_rows),
        "fact_prescriptions": spark.createDataFrame(fact_prescription_rows),
        "dim_patient": spark.createDataFrame([Row(patient_sk=1, patient_id="P1")]),
        "dim_provider": spark.createDataFrame([Row(provider_sk=1, provider_id="PR1")]),
        "dim_treatment": spark.createDataFrame([Row(treatment_sk=1, treatment_id="T1")]),
    }
    staging = {
        "stg_claims_events": spark.createDataFrame([Row(claim_id="C1")]),
        "stg_prescriptions_events": spark.createDataFrame([Row(prescription_id="RX1")]),
    }
    return curated, staging


def test_quality_checks_fail_on_bad_amount(spark):
    curated, staging = _build_test_frames(spark, bad_amount=True)
    results = evaluate_python_checks(
        spark=spark,
        curated=curated,
        staging=staging,
        run_id="rid",
        run_date="2025-01-01",
        allowed_claim_status=["approved", "pending", "rejected"],
    )
    amount_result = [r for r in results if r["check_name"] == "python_amount_constraints"][0]
    assert amount_result["status"] == "FAIL"


def test_quality_checks_pass_on_valid_amount(spark):
    curated, staging = _build_test_frames(spark, bad_amount=False)
    results = evaluate_python_checks(
        spark=spark,
        curated=curated,
        staging=staging,
        run_id="rid",
        run_date="2025-01-01",
        allowed_claim_status=["approved", "pending", "rejected"],
    )
    amount_result = [r for r in results if r["check_name"] == "python_amount_constraints"][0]
    assert amount_result["status"] == "PASS"

