from housing_lakehouse.transformations import BRONZE_SCHEMA, build_silver


def test_build_silver_normalizes_filters_and_keeps_latest_record(spark):
    rows = [
        (
            " H-1 ",
            "2025-03-01",
            "jersey CITY",
            "nj",
            "Single Family",
            3,
            2.0,
            1_500,
            2000,
            600_000,
            40.72,
            -74.04,
            "SYNTHETIC",
            "2026-01-01T00:00:00+00:00",
        ),
        (
            "H-1",
            "2025-03-01",
            "jersey city",
            "NJ",
            "single-family",
            3,
            2.0,
            1_500,
            2000,
            630_000,
            40.72,
            -74.04,
            "synthetic",
            "2026-01-02T00:00:00+00:00",
        ),
        (
            "H-2",
            "2025-04-01",
            "Austin",
            "TX",
            "condo",
            2,
            1.5,
            900,
            2015,
            -1,
            30.27,
            -97.74,
            "synthetic",
            "2026-01-02T00:00:00+00:00",
        ),
        (
            "H-3",
            "2025-05-01",
            "Seattle",
            "wa",
            "Townhouse",
            2,
            2.5,
            1_000,
            2010,
            500_000,
            47.61,
            -122.33,
            "synthetic",
            "2026-01-02T00:00:00+00:00",
        ),
    ]
    bronze = spark.createDataFrame(rows, BRONZE_SCHEMA)

    result = {row.property_id: row for row in build_silver(bronze).collect()}

    assert set(result) == {"H-1", "H-3"}
    assert result["H-1"].city == "Jersey City"
    assert result["H-1"].state == "NJ"
    assert result["H-1"].property_type == "single_family"
    assert result["H-1"].sale_price == 630_000
    assert result["H-1"].price_per_sqft == 420.0
    assert result["H-1"].property_age_at_sale == 25
    assert result["H-1"].sale_year == 2025
    assert result["H-1"].sale_month == 3
