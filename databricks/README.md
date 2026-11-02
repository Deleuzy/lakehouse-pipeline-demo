# Running on Databricks (Free Edition)

The same code runs on Databricks. The only difference is the Spark session:
`lakehouse.spark_session.get_spark()` reuses the notebook's `spark` when it sees
`DATABRICKS_RUNTIME_VERSION`, and Delta Lake is built in.

**Status: not run.** The notebook has been written but not executed on Databricks.
The local tests and the local pipeline run pass (see the main README).

## Steps (to be done by you, in your own workspace)

1. Create a Databricks Free Edition workspace (sign-up is not done by this repo).
2. Clone or import this repository into **Workspace > Repos** (or upload the folder).
3. Open `databricks/lakehouse_pipeline_notebook.py` as a notebook. Set `REPO_PATH`
   to where the repo lives in your workspace.
4. Run all cells. Tables are created in the `lakehouse_demo` schema.

The notebook uses `source="sample"`, so it needs no outbound calls. Change it to
`source="live"` with a start and end time to fetch from the Energinet API, if
the workspace allows outbound internet.

## Things that may differ on Databricks

- Table names are `schema.table`. On Free Edition the schema lands in the default catalog.
- `ANSI` mode is on by default on recent runtimes. The SQL uses `try_to_timestamp`
  and avoids other casts that would raise on bad input.
- File paths: the notebook reads the reference CSV with plain Python, not `spark.read`,
  to avoid depending on how `/Workspace` paths are exposed to Spark.
