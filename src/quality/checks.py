from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _result(run_id: str, run_date: str, check_name: str, passed: bool, details: str, severity: str = "error") -> dict:
    return {
        "run_id": run_id,
        "run_date": run_date,
        "check_name": check_name,
        "status": "PASS" if passed else "FAIL",
        "severity": severity,
        "details": details,
        "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def _count(df: DataFrame) -> int:
    return int(df.count())


def evaluate_python_checks(
    spark: SparkSession,
    curated: dict[str, DataFrame],
    staging: dict[str, DataFrame],
    run_id: str,
    run_date: str,
    allowed_claim_status: list[str],
) -> list[dict]:
    results: list[dict] = []

    fact_claims = curated["fact_claims"]
    dim_patient = curated["dim_patient"]
    dim_provider = curated["dim_provider"]
    fact_prescriptions = curated["fact_prescriptions"]
    dim_treatment = curated["dim_treatment"]

    expected_fact_claim_cols = {
        "claim_sk",
        "claim_id",
        "patient_sk",
        "provider_sk",
        "treatment_sk",
        "date_sk",
        "billed_amount",
        "reimbursed_amount",
        "claim_status",
    }
    missing = sorted(expected_fact_claim_cols - set(fact_claims.columns))
    results.append(
        _result(
            run_id,
            run_date,
            "python_schema_fact_claims",
            passed=(len(missing) == 0),
            details="missing_columns=" + ",".join(missing) if missing else "ok",
        )
    )

    total_claims = _count(fact_claims)
    unique_claims = _count(fact_claims.select("claim_id").dropDuplicates(["claim_id"]))
    results.append(
        _result(
            run_id,
            run_date,
            "python_claim_id_uniqueness",
            passed=(total_claims == unique_claims),
            details=f"total={total_claims}, unique={unique_claims}",
        )
    )

    null_violations = _count(
        fact_claims.filter(
            F.col("claim_id").isNull()
            | F.col("patient_sk").isNull()
            | F.col("provider_sk").isNull()
            | F.col("treatment_sk").isNull()
            | F.col("date_sk").isNull()
        )
    )
    results.append(
        _result(
            run_id,
            run_date,
            "python_fact_claims_mandatory_not_null",
            passed=(null_violations == 0),
            details=f"violations={null_violations}",
        )
    )

    bad_status = _count(
        fact_claims.filter(~F.col("claim_status").isin([status.lower() for status in allowed_claim_status]))
    )
    results.append(
        _result(
            run_id,
            run_date,
            "python_claim_status_enum",
            passed=(bad_status == 0),
            details=f"violations={bad_status}",
        )
    )

    bad_amounts = _count(
        fact_claims.filter(
            (F.col("billed_amount") < 0)
            | (F.col("reimbursed_amount") < 0)
            | (F.col("reimbursed_amount") > F.col("billed_amount"))
        )
    )
    results.append(
        _result(
            run_id,
            run_date,
            "python_amount_constraints",
            passed=(bad_amounts == 0),
            details=f"violations={bad_amounts}",
        )
    )

    fk_patient_fail = _count(
        fact_claims.alias("f")
        .join(dim_patient.alias("d"), F.col("f.patient_sk") == F.col("d.patient_sk"), "left")
        .filter(F.col("d.patient_sk").isNull())
    )
    fk_provider_fail = _count(
        fact_claims.alias("f")
        .join(dim_provider.alias("d"), F.col("f.provider_sk") == F.col("d.provider_sk"), "left")
        .filter(F.col("d.provider_sk").isNull())
    )
    fk_treatment_fail = _count(
        fact_prescriptions.alias("f")
        .join(dim_treatment.alias("d"), F.col("f.treatment_sk") == F.col("d.treatment_sk"), "left")
        .filter(F.col("d.treatment_sk").isNull())
    )
    results.append(
        _result(
            run_id,
            run_date,
            "python_referential_integrity",
            passed=(fk_patient_fail == 0 and fk_provider_fail == 0 and fk_treatment_fail == 0),
            details=(
                f"fact_claims.patient_fk={fk_patient_fail}, "
                f"fact_claims.provider_fk={fk_provider_fail}, "
                f"fact_prescriptions.treatment_fk={fk_treatment_fail}"
            ),
        )
    )

    stg_claims = _count(staging["stg_claims_events"])
    stg_prescriptions = _count(staging["stg_prescriptions_events"])
    curated_claims = _count(curated["fact_claims"])
    curated_prescriptions = _count(curated["fact_prescriptions"])
    results.append(
        _result(
            run_id,
            run_date,
            "python_row_count_reconciliation",
            passed=(stg_claims == curated_claims and stg_prescriptions == curated_prescriptions),
            details=(
                f"stg_claims={stg_claims}, fact_claims={curated_claims}, "
                f"stg_prescriptions={stg_prescriptions}, fact_prescriptions={curated_prescriptions}"
            ),
        )
    )

    return results

