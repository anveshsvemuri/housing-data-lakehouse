# Databricks deployment guide

The checked-in job template turns the same console entry point used locally into a Databricks Python
wheel task. It intentionally contains placeholders because cluster IDs and Unity Catalog names are
workspace-specific and must not be invented or committed as credentials.

## Prerequisites

- A Databricks workspace and CLI authenticated through a profile or environment variables
- An existing cluster with a runtime compatible with Python 3.11 and Spark 3.5
- A Unity Catalog Volume writable by the job identity
- Permission to upload a wheel and create or update a job

## Build and upload

```bash
python -m pip install build
python -m build --wheel
databricks fs cp dist/housing_data_lakehouse-0.1.0-py3-none-any.whl \
  dbfs:/FileStore/wheels/housing_data_lakehouse-0.1.0-py3-none-any.whl --overwrite
```

Copy `deployment/databricks-job.template.json` to a temporary file outside the repository and replace:

- `<DATABRICKS_CLUSTER_ID>` with an existing cluster ID
- `<CATALOG>`, `<SCHEMA>`, and `<VOLUME>` with a writable Unity Catalog Volume

Then validate the JSON and create the job:

```bash
python -m json.tool /tmp/databricks-job.json >/dev/null
databricks jobs create --json @/tmp/databricks-job.json
```

For repeatable production releases, store the job ID in deployment tooling and use
`databricks jobs reset --job-id <JOB_ID> --json @/tmp/databricks-job.json`.

## Production considerations

The template demonstrates packaging, parameterization, retries, concurrency control, Unity Catalog
storage, and incremental execution. Before processing concurrent or high-volume feeds, replace local
Parquet overwrite semantics with a transactional table format such as Delta Lake, use managed secrets
for external sources, and add environment-specific compute policies and observability destinations.
