"""Bronze-to-Silver standardization and deduplication."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@dataclass(frozen=True, slots=True)
class SilverTransformResult:
    """Accepted records and quarantined records from one Silver transform."""

    accepted: DataFrame
    rejected: DataFrame


def _normalized_records(bronze: DataFrame) -> DataFrame:
    """Normalize source fields before applying business rules."""
    return bronze.select(
        F.trim("property_id").alias("property_id"),
        F.to_date("sale_date").alias("sale_date"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.regexp_replace(F.lower(F.trim("property_type")), r"[- ]+", "_").alias("property_type"),
        F.col("bedrooms").cast("int").alias("bedrooms"),
        F.col("bathrooms").cast("double").alias("bathrooms"),
        F.col("square_feet").cast("int").alias("square_feet"),
        F.col("year_built").cast("int").alias("year_built"),
        F.col("sale_price").cast("long").alias("sale_price"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.lower(F.trim("source")).alias("source"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
    )


def _with_rejection_reasons(records: DataFrame) -> DataFrame:
    """Attach stable, machine-readable reasons to invalid records."""
    reason_checks = (
        (F.col("property_id").isNull() | (F.length("property_id") == 0), "missing_property_id"),
        (F.col("sale_date").isNull(), "invalid_sale_date"),
        (F.col("city").isNull() | (F.length("city") == 0), "missing_city"),
        (F.col("state").isNull() | (F.length("state") != 2), "invalid_state"),
        (F.col("sale_price").isNull() | (F.col("sale_price") <= 0), "invalid_sale_price"),
        (F.col("square_feet").isNull() | (F.col("square_feet") <= 0), "invalid_square_feet"),
        (F.col("bedrooms").isNull() | (F.col("bedrooms") < 0), "invalid_bedrooms"),
        (F.col("bathrooms").isNull() | (F.col("bathrooms") <= 0), "invalid_bathrooms"),
        (
            F.col("latitude").isNull() | ~F.col("latitude").between(-90, 90),
            "invalid_latitude",
        ),
        (
            F.col("longitude").isNull() | ~F.col("longitude").between(-180, 180),
            "invalid_longitude",
        ),
    )
    reasons = F.array(*[F.when(condition, F.lit(reason)) for condition, reason in reason_checks])
    return records.withColumn("rejection_reasons", F.array_compact(reasons))


def build_silver_with_rejects(bronze: DataFrame) -> SilverTransformResult:
    """Build Silver records while preserving invalid and superseded rows."""
    evaluated = _with_rejection_reasons(_normalized_records(bronze))
    invalid = evaluated.filter(F.size("rejection_reasons") > 0)
    valid = evaluated.filter(F.size("rejection_reasons") == 0).drop("rejection_reasons")

    latest_record = Window.partitionBy("property_id").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("sale_date").desc()
    )
    ranked = valid.withColumn("_record_rank", F.row_number().over(latest_record))
    duplicates = (
        ranked.filter(F.col("_record_rank") > 1)
        .drop("_record_rank")
        .withColumn("rejection_reasons", F.array(F.lit("duplicate_superseded")))
    )
    accepted = (
        ranked.filter(F.col("_record_rank") == 1)
        .drop("_record_rank")
        .withColumn("price_per_sqft", F.round(F.col("sale_price") / F.col("square_feet"), 2))
        .withColumn("property_age_at_sale", F.year("sale_date") - F.col("year_built"))
        .withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.month("sale_date"))
    )
    return SilverTransformResult(accepted=accepted, rejected=invalid.unionByName(duplicates))

BRONZE_SCHEMA = StructType(
    [
        StructField("property_id", StringType(), False),
        StructField("sale_date", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("property_type", StringType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", DoubleType(), True),
        StructField("square_feet", IntegerType(), True),
        StructField("year_built", IntegerType(), True),
        StructField("sale_price", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)


def build_silver(bronze: DataFrame) -> DataFrame:
    """Return typed, normalized, valid, and latest-version housing records."""
    return build_silver_with_rejects(bronze).accepted
