from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.utils.config import resolve_path
from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


def _write_parquet(df: DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(target))


def _add_surrogate(df: DataFrame, sk_name: str, order_cols: list[str]) -> DataFrame:
    window = Window.orderBy(*order_cols)
    return df.withColumn(sk_name, F.row_number().over(window))


def _load_staging_table(spark: SparkSession, root: Path, table: str) -> DataFrame:
    return spark.read.parquet(str(root / table))


def build_dimensions(staging_root: Path, spark: SparkSession) -> dict[str, DataFrame]:
    stg_regions = _load_staging_table(spark, staging_root, "stg_regions")
    stg_providers = _load_staging_table(spark, staging_root, "stg_providers")
    stg_patients = _load_staging_table(spark, staging_root, "stg_patients")
    stg_claims_events = _load_staging_table(spark, staging_root, "stg_claims_events")
    stg_prescriptions_events = _load_staging_table(spark, staging_root, "stg_prescriptions_events")
    stg_visits_events = _load_staging_table(spark, staging_root, "stg_visits_events")

    dim_region = _add_surrogate(stg_regions.dropDuplicates(["region_id"]), "region_sk", ["region_id"]).select(
        "region_sk",
        "region_id",
        "region_name",
        "population_group",
    )

    dim_provider = (
        _add_surrogate(stg_providers.dropDuplicates(["provider_id"]), "provider_sk", ["provider_id"])
        .join(dim_region.select("region_id", "region_sk"), on="region_id", how="left")
        .select(
            "provider_sk",
            "provider_id",
            "provider_name",
            "provider_type",
            "region_sk",
        )
    )

    treatment_source = (
        stg_claims_events.select("treatment_id", "treatment_category")
        .unionByName(stg_prescriptions_events.select("treatment_id", "treatment_category"))
        .dropDuplicates(["treatment_id"])
    )
    dim_treatment = _add_surrogate(treatment_source, "treatment_sk", ["treatment_id"]).select(
        "treatment_sk",
        "treatment_id",
        "treatment_category",
        F.initcap(F.regexp_replace("treatment_category", "_", " ")).alias("treatment_name"),
    )

    dim_patient = (
        _add_surrogate(stg_patients.dropDuplicates(["patient_id"]), "patient_sk", ["patient_id"])
        .join(dim_region.select("region_id", "region_sk"), on="region_id", how="left")
        .select(
            "patient_sk",
            "patient_id",
            "birth_date",
            "gender",
            "region_sk",
            "registration_date",
        )
    )

    all_dates = (
        stg_claims_events.select(F.col("claim_date").alias("date"))
        .unionByName(stg_prescriptions_events.select(F.col("prescription_date").alias("date")))
        .unionByName(stg_visits_events.select(F.col("admission_date").alias("date")))
        .unionByName(stg_visits_events.select(F.col("discharge_date").alias("date")))
        .where(F.col("date").isNotNull())
        .dropDuplicates(["date"])
    )

    dim_date = (
        all_dates.withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .select("date_sk", "date", "year", "quarter", "month", "day", "week_of_year", "is_weekend")
    )

    return {
        "dim_region": dim_region,
        "dim_provider": dim_provider,
        "dim_treatment": dim_treatment,
        "dim_patient": dim_patient,
        "dim_date": dim_date,
    }


def build_facts(staging_root: Path, dimensions: dict[str, DataFrame], spark: SparkSession) -> dict[str, DataFrame]:
    stg_claims_events = _load_staging_table(spark, staging_root, "stg_claims_events")
    stg_prescriptions_events = _load_staging_table(spark, staging_root, "stg_prescriptions_events")
    stg_visits_events = _load_staging_table(spark, staging_root, "stg_visits_events")

    dim_patient = dimensions["dim_patient"].select("patient_id", "patient_sk")
    dim_provider = dimensions["dim_provider"].select("provider_id", "provider_sk")
    dim_treatment = dimensions["dim_treatment"].select("treatment_id", "treatment_sk")
    dim_date = dimensions["dim_date"].select("date", "date_sk")

    fact_claims = (
        stg_claims_events.join(dim_patient, on="patient_id", how="left")
        .join(dim_provider, on="provider_id", how="left")
        .join(dim_treatment, on="treatment_id", how="left")
        .join(dim_date.withColumnRenamed("date", "claim_date"), on="claim_date", how="left")
        .withColumn("claim_sk", F.row_number().over(Window.orderBy("claim_id")))
        .select(
            "claim_sk",
            "claim_id",
            "patient_sk",
            "provider_sk",
            "treatment_sk",
            F.col("date_sk"),
            "billed_amount",
            "reimbursed_amount",
            "claim_status",
        )
    )

    fact_prescriptions = (
        stg_prescriptions_events.join(dim_patient, on="patient_id", how="left")
        .join(dim_provider, on="provider_id", how="left")
        .join(dim_treatment, on="treatment_id", how="left")
        .join(dim_date.withColumnRenamed("date", "prescription_date"), on="prescription_date", how="left")
        .withColumn("prescription_sk", F.row_number().over(Window.orderBy("prescription_id")))
        .select(
            "prescription_sk",
            "prescription_id",
            "patient_sk",
            "provider_sk",
            "treatment_sk",
            F.col("date_sk"),
            "quantity",
        )
    )

    date_adm = dim_date.withColumnRenamed("date", "admission_date").withColumnRenamed("date_sk", "admission_date_sk")
    date_dis = dim_date.withColumnRenamed("date", "discharge_date").withColumnRenamed("date_sk", "discharge_date_sk")

    fact_visits = (
        stg_visits_events.join(dim_patient, on="patient_id", how="left")
        .join(dim_provider, on="provider_id", how="left")
        .join(date_adm, on="admission_date", how="left")
        .join(date_dis, on="discharge_date", how="left")
        .withColumn("length_of_stay_days", F.datediff(F.col("discharge_date"), F.col("admission_date")))
        .withColumn("visit_sk", F.row_number().over(Window.orderBy("visit_id")))
        .select(
            "visit_sk",
            "visit_id",
            "patient_sk",
            "provider_sk",
            "admission_date_sk",
            "discharge_date_sk",
            "diagnosis_code",
            "length_of_stay_days",
        )
    )

    fact_reimbursements = (
        fact_claims.groupBy("date_sk", "provider_sk", "treatment_sk")
        .agg(
            F.count("*").alias("total_claims"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.sum("reimbursed_amount"), 2).alias("total_reimbursed"),
        )
        .withColumn(
            "reimbursement_rate",
            F.when(F.col("total_billed") > 0, F.round(F.col("total_reimbursed") / F.col("total_billed"), 4)).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn("reimbursement_sk", F.row_number().over(Window.orderBy("date_sk", "provider_sk", "treatment_sk")))
        .select(
            "reimbursement_sk",
            "date_sk",
            "provider_sk",
            "treatment_sk",
            "total_claims",
            "total_billed",
            "total_reimbursed",
            "reimbursement_rate",
        )
    )

    return {
        "fact_claims": fact_claims,
        "fact_prescriptions": fact_prescriptions,
        "fact_visits": fact_visits,
        "fact_reimbursements": fact_reimbursements,
    }


def run_build_dimensions(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    staging_root = resolve_path(config["paths"]["staging_root"]) / run_date
    curated_root = resolve_path(config["paths"]["curated_root"]) / run_date
    dimensions = build_dimensions(staging_root, spark=spark)
    outputs: dict[str, Path] = {}
    for name, df in dimensions.items():
        target = curated_root / name
        _write_parquet(df, target)
        outputs[name] = target
    LOGGER.info("Dimension build completed for run_date=%s", run_date)
    return outputs


def run_build_facts(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    staging_root = resolve_path(config["paths"]["staging_root"]) / run_date
    curated_root = resolve_path(config["paths"]["curated_root"]) / run_date
    dimensions = {
        "dim_region": spark.read.parquet(str(curated_root / "dim_region")),
        "dim_provider": spark.read.parquet(str(curated_root / "dim_provider")),
        "dim_treatment": spark.read.parquet(str(curated_root / "dim_treatment")),
        "dim_patient": spark.read.parquet(str(curated_root / "dim_patient")),
        "dim_date": spark.read.parquet(str(curated_root / "dim_date")),
    }
    facts = build_facts(staging_root, dimensions=dimensions, spark=spark)
    outputs: dict[str, Path] = {}
    for name, df in facts.items():
        target = curated_root / name
        _write_parquet(df, target)
        outputs[name] = target
    LOGGER.info("Fact build completed for run_date=%s", run_date)
    return outputs


def run_curated_transformations(spark: SparkSession, config: dict[str, Any], run_date: str) -> dict[str, Path]:
    outputs = {}
    outputs.update(run_build_dimensions(spark, config, run_date))
    outputs.update(run_build_facts(spark, config, run_date))
    LOGGER.info("Curated transformations completed for run_date=%s", run_date)
    return outputs
