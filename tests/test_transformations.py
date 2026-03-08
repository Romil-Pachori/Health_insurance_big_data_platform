from pyspark.sql import Row

from src.transformations.staging import synthesize_claim_events


def test_synthesized_claim_ids_are_deterministic(spark):
    rows = [
        Row(
            provider_id="P1",
            region_id="R1",
            treatment_category="cardiology",
            claim_month="2025-01",
            claim_count=3,
            total_billed=150.0,
            total_reimbursed=120.0,
            claim_status="approved",
        )
    ]
    claims_agg = spark.createDataFrame(rows)

    first = synthesize_claim_events(claims_agg, seed=111).select("claim_id").orderBy("claim_id")
    second = synthesize_claim_events(claims_agg, seed=111).select("claim_id").orderBy("claim_id")
    first_ids = [r["claim_id"] for r in first.collect()]
    second_ids = [r["claim_id"] for r in second.collect()]

    assert first_ids == second_ids


def test_reimbursement_never_exceeds_billed(spark):
    rows = [
        Row(
            provider_id="P1",
            region_id="R1",
            treatment_category="cardiology",
            claim_month="2025-01",
            claim_count=2,
            total_billed=100.0,
            total_reimbursed=130.0,
            claim_status="approved",
        )
    ]
    claims_agg = spark.createDataFrame(rows)
    events = synthesize_claim_events(claims_agg, seed=111)
    invalid = events.filter(events.reimbursed_amount > events.billed_amount).count()
    assert invalid == 0

