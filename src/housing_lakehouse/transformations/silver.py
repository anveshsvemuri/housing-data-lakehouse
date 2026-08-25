"""Bronze-to-Silver standardization and deduplication."""

from __future__ import annotations

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
    normalized = bronze.select(
        F.trim("property_id").alias("property_id"),
        F.to_date("sale_date").alias("sale_date"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.regexp_replace(F.lower(F.trim("property_type")), r"[s-]+", "_").alias("property_type"),
        F.col("bedrooms").cast("int").alias("bedrooms"),
        F.col("bathrooms").cast("double").alias("bathrooms"),
        F.col("square_feet").cast("int").alias("square_feet"),
        F.col("year_built").cast("int").alias("year_built"),
        F.col("sale_price").cast("long").alias("sale_price"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.lower(F.trim("source")).alias("source"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
    ).filter(
        F.col("property_id").isNotNull()
        & (F.length("property_id") > 0)
        & F.col("sale_date").isNotNull()
        & F.col("city").isNotNull()
        & (F.length("state") == 2)
        & (F.col("sale_price") > 0)
        & (F.col("square_feet") > 0)
        & (F.col("bedrooms") >= 0)
        & (F.col("bathrooms") > 0)
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )

    latest_record = Window.partitionBy("property_id").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("sale_date").desc()
    )

    return (
        normalized.withColumn("_record_rank", F.row_number().over(latest_record))
        .filter(F.col("_record_rank") == 1)
        .drop("_record_rank")
        .withColumn("price_per_sqft", F.round(F.col("sale_price") / F.col("square_feet"), 2))
        .withColumn("property_age_at_sale", F.year("sale_date") - F.col("year_built"))
        .withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.month("sale_date"))
    )
