astro_orders DAG

Purpose
- Extract orders data from an S3 CSV, filter, join to customers, merge into a Snowflake reporting table, and run a small dataframe transform.

Key files
- `dags/astro_orders.py` — the DAG implementation using Astronomer/astro SDK transformations and dataframe operators.

Inputs and outputs
- Input file (S3): `s3://hv-airflow-2/orders_data_header.csv` (see `S3_FILE_PATH` in the DAG).
- Output: Snowflake tables: `reporting_table` (configured via `SNOWFLAKE_REPORTING` constant).

Required Airflow connections
- `aws_default` — AWS credentials (access key/secret or role) for S3 access.
- `snowflake_default` — Snowflake connection used by the astro SQL table operators.

Notes and important bits
- The DAG uses astro SDK decorators: `@aql.transform`, `@aql.dataframe` and `aql.load_file`, `aql.merge`, `aql.cleanup()`.
- The `aql.cleanup()` call must be invoked (with parentheses) to return a task/operator for Airflow to wire downstream tasks. If referenced without calling, Airflow will treat it as a plain function and the DAG will be marked broken (AttributeError during task linking).
- SQL in `join_orders_customers` is defined as a raw string returned by the transform decorator. Edit carefully and keep template variable names (e.g., `{{filtered_orders_table}}`).

How to run locally (short)
1. Ensure provider packages are installed in your Airflow environment, e.g. `apache-airflow-providers-amazon` and `apache-airflow-providers-snowflake` if you use Snowflake.
2. Populate `airflow_settings.yaml` or your environment with `aws_default` and `snowflake_default` connections.
3. Restart Airflow (webserver + scheduler) or your local dev environment so the DAG reloads.
4. In the Airflow UI: open `astro_orders` DAG and trigger a DAG run or examine the task graph.

Troubleshooting
- If the DAG appears as "Broken DAG" in the UI: open the DAG file in the Airflow logs or the web UI error message; check for function-vs-task wiring mistakes (see note about `aql.cleanup()`), and ensure all variables/constants used are defined.
- If the Test button in Admin -> Connections is grayed out: ensure the connection entry has `conn_type` and is not provided via a read-only secrets backend. Installing the appropriate provider packages may be necessary for the connection test to work.

- XCom serialization errors: Airflow serializes XCom values to JSON by default. Returning pandas objects (DataFrame or Series) from tasks will cause a "not JSON serializable" error when Airflow tries to push the task return value to XCom.
	- Fixes:
		- Convert the pandas object to a JSON-serializable Python type before returning (e.g., `series.tolist()` or `df.to_dict()`).
		- Or enable pickle-based XCom by configuring `enable_xcom_pickling` in your Airflow configuration (not recommended for untrusted or production environments).
	- In this DAG, `transform_dataframe` now returns a Python list of purchase dates to avoid the serialization error.

Contact / Next steps
- Replace placeholder secrets in connections with secure stores (Secrets Manager, Environment Variables, or Airflow Variables) for production.
- If you want, I can add sample `airflow_settings.yaml` connection snippets for AWS and Snowflake with placeholders.
