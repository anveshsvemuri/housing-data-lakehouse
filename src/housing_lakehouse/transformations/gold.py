"""Analytics-ready Gold datasets."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_market_metrics(silver: DataFrame) -> DataFrame:
    """Aggregate annual housing KPIs by city and state."""
    return (
        silver.groupBy("state", "city", "sale_year")
        .agg(
            F.countDistinct("property_id").alias("properties_sold"),
            F.sum("sale_price").alias("total_sales_volume"),
            F.round(F.avg("sale_price"), 2).alias("average_sale_price"),
            F.expr("percentile_approx(sale_price, 0.5)").alias("median_sale_price"),
            F.round(F.avg("price_per_sqft"), 2).alias("average_price_per_sqft"),
            F.round(F.avg("square_feet"), 2).alias("average_square_feet"),
            F.min("sale_price").alias("minimum_sale_price"),
            F.max("sale_price").alias("maximum_sale_price"),
        )
        .orderBy("sale_year", "state", "city")
    )
