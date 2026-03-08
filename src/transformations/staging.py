from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import resolve_path
from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


def _write_parquet(df: DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(target))


def _safe_month(month_col: F.Column) -> F.Column:
    return F.regexp_extract(month_col, r"^(\d{4}-\d{2})", 1)


def _allocate_decimal(total_col: str, count_col: str, seq_col: str) -> F.Column:
    unit = F.bround(F.col(total_col) / F.col(count_col), 2)
    return F.when(F.col(seq_col) < F.col(count_col), unit).otherwise(
        F.bround(F.col(total_col) - unit * (F.col(count_col) - 1), 2)
    )


def _allocate_integer(total_col: str, count_col: str, seq_col: str) -> F.Column:
    unit = F.floor(F.col(total_col) / F.col(count_col))
    return F.when(F.col(seq_col) < F.col(count_col), unit).otherwise(
        F.col(total_col) - unit * (F.col(count_col) - 1)
    )


def clean_providers(df: DataFrame) -> DataFrame:
    return (
        df.select("provider_id", "provider_name", "provider_type", "region_id")
        .withColumn("provider_id", F.upper(F.trim(F.col("provider_id"))))
        .withColumn("provider_name", F.trim(F.col("provider_name")))
        .withColumn("provider_type", F.lower(F.trim(F.col("provider_type"))))
        .withColumn("region_id", F.upper(F.trim(F.col("region_id"))))
        .dropDuplicates(["provider_id"])
    )


def clean_regions(df: DataFrame) -> DataFrame:
    return (
        df.select("region_id", "region_name", "population_group")
        .withColumn("region_id", F.upper(F.trim(F.col("region_id"))))
        .withColumn("region_name", F.trim(F.col("region_name")))
        .withColumn("population_group", F.lower(F.trim(F.col("population_group"))))
        .dropDuplicates(["region_id"])
    )


def clean_claims_agg(df: DataFrame, default_status: str) -> DataFrame:
    return (
        df.select(
            "provider_id",
            "region_id",
            "treatment_category",
            "claim_month",
            "claim_count",
            "total_billed",
            "total_reimbursed",
            "claim_status",
        )
        .withColumn("provider_id", F.upper(F.trim(F.col("provider_id"))))
        .withColumn("region_id", F.upper(F.trim(F.col("region_id"))))
        .withColumn("treatment_category", F.lower(F.trim(F.col("treatment_category"))))
        .withColumn("claim_month", _safe_month(F.col("claim_month")))
        .withColumn("claim_count", F.when(F.col("claim_count").cast("int") > 0, F.col("claim_count").cast("int")).otherwise(F.lit(1)))
        .withColumn("total_billed", F.col("total_billed").cast("double"))
        .withColumn("total_reimbursed", F.col("total_reimbursed").cast("double"))
        .withColumn(
            "claim_status",
            F.when(F.col("claim_status").isNull(), F.lit(default_status)).otherwise(
                F.lower(F.trim(F.col("claim_status")))
            ),
        )
    )


def clean_prescriptions_agg(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "provider_id",
            "region_id",
            "treatment_category",
            "claim_month",
            "prescription_count",
            "total_quantity",
        )
        .withColumn("provider_id", F.upper(F.trim(F.col("provider_id"))))
        .withColumn("region_id", F.upper(F.trim(F.col("region_id"))))
        .withColumn("treatment_category", F.lower(F.trim(F.col("treatment_category"))))
        .withColumn("claim_month", _safe_month(F.col("claim_month")))
        .withColumn(
            "prescription_count",
            F.when(F.col("prescription_count").cast("int") > 0, F.col("prescription_count").cast("int")).otherwise(F.lit(1)),
        )
        .withColumn("total_quantity", F.when(F.col("total_quantity").cast("int") >= 0, F.col("total_quantity").cast("int")).otherwise(F.lit(0)))
    )


def synthesize_claim_events(claims_agg: DataFrame, seed: int) -> DataFrame:
    base = (
        claims_agg.withColumn(
            "source_row_key",
            F.concat_ws(
                "|",
                F.col("provider_id"),
                F.col("region_id"),
                F.col("treatment_category"),
                F.col("claim_month"),
                F.col("claim_status"),
            ),
        )
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("claim_count"))))
        .withColumn(
            "claim_id",
            F.substring(
                F.sha2(
                    F.concat_ws("|", F.lit(str(seed)), F.lit("claim"), F.col("source_row_key"), F.col("seq")),
                    256,
                ),
                1,
                16,
            ),
        )
        .withColumn(
            "patient_id",
            F.substring(
                F.sha2(
                    F.concat_ws("|", F.lit(str(seed)), F.lit("patient"), F.col("source_row_key"), F.col("seq")),
                    256,
                ),
                1,
                16,
            ),
        )
        .withColumn("treatment_id", F.substring(F.sha2(F.col("treatment_category"), 256), 1, 12))
        .withColumn(
            "claim_date",
            F.date_add(
                F.to_date(F.concat_ws("-", F.col("claim_month"), F.lit("01")), "yyyy-MM-dd"),
                F.pmod(F.col("seq") - F.lit(1).cast("bigint"), F.lit(28).cast("bigint")).cast("int"),
            ),
        )
        .withColumn("billed_amount", _allocate_decimal("total_billed", "claim_count", "seq"))
        .withColumn("raw_reimbursed_amount", _allocate_decimal("total_reimbursed", "claim_count", "seq"))
        .withColumn(
            "reimbursed_amount",
            F.when(F.col("raw_reimbursed_amount") > F.col("billed_amount"), F.col("billed_amount")).otherwise(
                F.col("raw_reimbursed_amount")
            ),
        )
    )

    return base.select(
        "claim_id",
        "patient_id",
        "provider_id",
        "region_id",
        "treatment_id",
        "treatment_category",
        "claim_date",
        "billed_amount",
        "reimbursed_amount",
        "claim_status",
        "source_row_key",
    )


def synthesize_prescription_events(prescriptions_agg: DataFrame, seed: int) -> DataFrame:
    base = (
        prescriptions_agg.withColumn(
            "source_row_key",
            F.concat_ws(
                "|",
                F.col("provider_id"),
                F.col("region_id"),
                F.col("treatment_category"),
                F.col("claim_month"),
            ),
        )
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("prescription_count"))))
        .withColumn(
            "prescription_id",
            F.substring(
                F.sha2(
                    F.concat_ws("|", F.lit(str(seed)), F.lit("prescription"), F.col("source_row_key"), F.col("seq")),
                    256,
                ),
                1,
                16,
            ),
        )
        .withColumn(
            "patient_id",
            F.substring(
                F.sha2(
                    F.concat_ws("|", F.lit(str(seed)), F.lit("patient"), F.col("source_row_key"), F.col("seq")),
                    256,
                ),
                1,
                16,
            ),
        )
        .withColumn("treatment_id", F.substring(F.sha2(F.col("treatment_category"), 256), 1, 12))
        .withColumn(
            "prescription_date",
            F.date_add(
                F.to_date(F.concat_ws("-", F.col("claim_month"), F.lit("01")), "yyyy-MM-dd"),
                F.pmod(F.col("seq") - F.lit(1).cast("bigint"), F.lit(28).cast("bigint")).cast("int"),
            ),
        )
        .withColumn("quantity", _allocate_integer("total_quantity", "prescription_count", "seq").cast("int"))
    )

    return base.select(
        "prescription_id",
        "patient_id",
        "provider_id",
        "region_id",
        "treatment_id",
        "treatment_category",
        "prescription_date",
        "quantity",
        "source_row_key",
    )


def synthesize_patients(claim_events: DataFrame, prescription_events: DataFrame, providers: DataFrame) -> DataFrame:
    patient_provider = (
        claim_events.select("patient_id", "provider_id")
        .unionByName(prescription_events.select("patient_id", "provider_id"))
        .dropDuplicates(["patient_id", "provider_id"])
    )

    patient_region = (
        patient_provider.join(providers.select("provider_id", "region_id"), on="provider_id", how="left")
        .groupBy("patient_id")
        .agg(F.first("region_id", ignorenulls=True).alias("region_id"))
    )

    hashed = F.abs(F.hash("patient_id"))
    return (
        patient_region.withColumn("region_id", F.coalesce(F.col("region_id"), F.lit("R11")))
        .withColumn("gender", F.when(F.pmod(hashed, F.lit(2)) == 0, F.lit("F")).otherwise(F.lit("M")))
        .withColumn("birth_year", (F.lit(1940) + F.pmod(hashed, F.lit(55))).cast("int"))
        .withColumn("birth_month", F.lpad((F.pmod(hashed, F.lit(12)) + F.lit(1)).cast("string"), 2, "0"))
        .withColumn("birth_day", F.lpad((F.pmod(hashed, F.lit(28)) + F.lit(1)).cast("string"), 2, "0"))
        .withColumn(
            "birth_date",
            F.to_date(F.concat_ws("-", F.col("birth_year").cast("string"), F.col("birth_month"), F.col("birth_day"))),
        )
        .withColumn("registration_year", (F.lit(2018) + F.pmod(hashed, F.lit(6))).cast("int"))
        .withColumn(
            "registration_date",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("registration_year").cast("string"),
                    F.col("birth_month"),
                    F.col("birth_day"),
                )
            ),
        )
        .select("patient_id", "birth_date", "gender", "region_id", "registration_date")
        .dropDuplicates(["patient_id"])
    )


def synthesize_visits(claim_events: DataFrame, providers: DataFrame, seed: int, threshold: int) -> DataFrame:
    provider_types = providers.select("provider_id", "provider_type")
    with_provider = claim_events.join(provider_types, on="provider_id", how="left")

    is_hospital = F.lower(F.coalesce(F.col("provider_type"), F.lit(""))).rlike("hospital|hopital|clinique|clinic")
    hash_pick = F.pmod(F.abs(F.hash(F.col("claim_id"), F.lit(seed))), F.lit(10)) < F.lit(threshold)

    filtered = with_provider.filter(is_hospital | hash_pick)
    hashed = F.abs(F.hash("claim_id"))

    return (
        filtered.withColumn(
            "visit_id",
            F.substring(F.sha2(F.concat_ws("|", F.lit(str(seed)), F.lit("visit"), F.col("claim_id")), 256), 1, 16),
        )
        .withColumn("admission_date", F.col("claim_date"))
        .withColumn("discharge_date", F.date_add(F.col("claim_date"), F.pmod(hashed, F.lit(5)) + F.lit(1)))
        .withColumn(
            "diagnosis_code",
            F.concat(F.lit("D"), F.lpad((F.pmod(F.abs(F.hash("patient_id")), F.lit(900)) + F.lit(100)).cast("string"), 3, "0")),
        )
        .select(
            "visit_id",
            "patient_id",
            "provider_id",
            "region_id",
            "treatment_id",
            "admission_date",
            "discharge_date",
            "diagnosis_code",
            "claim_id",
        )
    )


def run_stage_cleaning(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    raw_root = resolve_path(config["paths"]["raw_root"]) / run_date
    staging_root = resolve_path(config["paths"]["staging_root"]) / run_date
    default_status = config["synthesis"]["default_claim_status"]

    providers_raw = spark.read.option("header", True).csv(str(raw_root / "providers.csv"))
    regions_raw = spark.read.option("header", True).csv(str(raw_root / "regions.csv"))
    claims_raw = spark.read.option("header", True).csv(str(raw_root / "claims.csv"))
    prescriptions_raw = spark.read.option("header", True).csv(str(raw_root / "prescriptions.csv"))

    providers = clean_providers(providers_raw)
    regions = clean_regions(regions_raw)
    claims_agg = clean_claims_agg(claims_raw, default_status=default_status)
    prescriptions_agg = clean_prescriptions_agg(prescriptions_raw)

    outputs: dict[str, Path] = {
        "stg_providers": staging_root / "stg_providers",
        "stg_regions": staging_root / "stg_regions",
        "stg_claims_agg": staging_root / "stg_claims_agg",
        "stg_prescriptions_agg": staging_root / "stg_prescriptions_agg",
    }

    _write_parquet(providers, outputs["stg_providers"])
    _write_parquet(regions, outputs["stg_regions"])
    _write_parquet(claims_agg, outputs["stg_claims_agg"])
    _write_parquet(prescriptions_agg, outputs["stg_prescriptions_agg"])

    LOGGER.info("Stage cleaning completed for run_date=%s", run_date)
    return outputs


def run_synthesis(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    staging_root = resolve_path(config["paths"]["staging_root"]) / run_date
    seed = int(config["synthesis"]["seed"])
    visit_threshold = int(config["synthesis"]["visit_share_hash_threshold"])

    providers = spark.read.parquet(str(staging_root / "stg_providers"))
    claims_agg = spark.read.parquet(str(staging_root / "stg_claims_agg"))
    prescriptions_agg = spark.read.parquet(str(staging_root / "stg_prescriptions_agg"))

    claim_events = synthesize_claim_events(claims_agg, seed=seed)
    prescription_events = synthesize_prescription_events(prescriptions_agg, seed=seed)
    patients = synthesize_patients(claim_events, prescription_events, providers)
    visits = synthesize_visits(claim_events, providers, seed=seed, threshold=visit_threshold)

    outputs: dict[str, Path] = {
        "stg_claims_events": staging_root / "stg_claims_events",
        "stg_prescriptions_events": staging_root / "stg_prescriptions_events",
        "stg_patients": staging_root / "stg_patients",
        "stg_visits_events": staging_root / "stg_visits_events",
    }

    _write_parquet(claim_events, outputs["stg_claims_events"])
    _write_parquet(prescription_events, outputs["stg_prescriptions_events"])
    _write_parquet(patients, outputs["stg_patients"])
    _write_parquet(visits, outputs["stg_visits_events"])

    LOGGER.info("Synthesis completed for run_date=%s", run_date)
    return outputs


def run_staging_transformations(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    cleaned = run_stage_cleaning(spark, config, run_date)
    synthesized = run_synthesis(spark, config, run_date)
    outputs = {**cleaned, **synthesized}
    LOGGER.info("Staging transformations completed for run_date=%s", run_date)
    return outputs
