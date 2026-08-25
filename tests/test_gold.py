from datetime import UTC, date, datetime

from housing_lakehouse.transformations import build_market_metrics


def test_build_market_metrics_calculates_city_kpis(spark):
    columns = [
        "property_id",
        "state",
        "city",
        "sale_year",
        "sale_date",
        "sale_price",
        "price_per_sqft",
        "square_feet",
        "ingested_at",
    ]
    rows = [
        (
            "A",
            "NJ",
            "Jersey City",
            2025,
            date(2025, 1, 1),
            400_000,
            400.0,
            1_000,
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        (
            "B",
            "NJ",
            "Jersey City",
            2025,
            date(2025, 2, 1),
            600_000,
            300.0,
            2_000,
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        (
            "C",
            "TX",
            "Austin",
            2025,
            date(2025, 3, 1),
            300_000,
            300.0,
            1_000,
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    silver = spark.createDataFrame(rows, columns)

    metrics = {(row.state, row.city): row for row in build_market_metrics(silver).collect()}

    jersey_city = metrics[("NJ", "Jersey City")]
    assert jersey_city.properties_sold == 2
    assert jersey_city.total_sales_volume == 1_000_000
    assert jersey_city.average_sale_price == 500_000.0
    assert jersey_city.median_sale_price == 400_000
    assert jersey_city.average_price_per_sqft == 350.0
    assert jersey_city.average_square_feet == 1_500.0
    assert jersey_city.minimum_sale_price == 400_000
    assert jersey_city.maximum_sale_price == 600_000
