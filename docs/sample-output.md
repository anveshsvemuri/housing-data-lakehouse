# Sample pipeline output

This preview was captured from a real local run on August 27, 2026:

```bash
housing-lakehouse --rows 25 --seed 42 --data-root /tmp/housing-demo --full
```

The pipeline produced 25 Bronze rows, 25 Silver rows, no rejected rows, and 10 Gold market
aggregates. The audit manifest reported `reconciled: true`.

## Gold preview

| Sale year | State | City | Properties sold | Total sales volume | Average price | Median price | Average $/sq ft |
|---:|---|---|---:|---:|---:|---:|---:|
| 2025 | TX | Austin | 4 | $2,903,336 | $725,834.00 | $615,614 | $338.00 |
| 2025 | WA | Seattle | 4 | $5,206,439 | $1,301,609.75 | $1,260,936 | $402.00 |
| 2026 | NJ | Jersey City | 3 | $3,083,320 | $1,027,773.33 | $1,079,610 | $535.67 |
| 2026 | TX | Austin | 5 | $4,374,015 | $874,803.00 | $908,523 | $362.40 |

Synthetic values are deterministic for a given seed and generation timestamp. The repository does
not commit generated datasets; the command above recreates the full output locally.

## Audit preview

```json
{
  "mode": "full_refresh",
  "processed_bronze_rows": 25,
  "reconciled": true,
  "row_counts": {
    "bronze": 25,
    "gold": 10,
    "rejected": 0,
    "silver": 25
  },
  "status": "succeeded"
}
```

Audit files also include the content-derived run ID, selected input files, output locations, and UTC
completion timestamp.
