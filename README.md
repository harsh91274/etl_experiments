# etl_experiments
data pipeline and warehousing experiments with Snowflake, Airflow and DBT

## astro_orders DAG
Small example DAG that extracts an orders CSV from S3, filters and joins it to a customers table, and merges results into a Snowflake reporting table. It also demonstrates using the Astro SDK (`aql.transform`, `aql.dataframe`, `aql.load_file`, `aql.merge`).

Key points:
- Input: `s3://hv-airflow-2/orders_data_header.csv` (S3 path configured in `dags/astro_orders.py`).
- Required Airflow connections: `aws_default` (S3 access) and `snowflake_default` (Snowflake access).
- The DAG returns small results as JSON-serializable types (lists/dicts) to avoid XCom serialization issues.

See `dags/README_astro_orders.md` for more details and troubleshooting notes.

EMR instance type configuration
- For the EMR demo DAG (`dags/airflow_empr_spark_s3_snowflake.py`) the cluster instance types are configurable via environment variables:
	- `EMR_MASTER_INSTANCE_TYPE` (default: `m5.large`)
	- `EMR_CORE_INSTANCE_TYPE` (default: `m5.large`)

These defaults were chosen to balance availability and cost. To change them add the variables to your `.env` or environment and restart your Airflow environment (for example `astro dev restart`).

EMR workflow (snowflake_automation_dag)
- File: `dags/airflow_empr_spark_s3_snowflake.py`
- Purpose: Launch a transient EMR cluster, run ingest and transform steps (via script/spark-submit), then merge/refresh data in Snowflake.
- Important environment variables:
	- `EMR_MASTER_INSTANCE_TYPE` / `EMR_CORE_INSTANCE_TYPE` — instance types (defaults: `m5.large`).
	- `EMR_EC2_KEY_NAME` — SSH key pair name for the EC2 instances (default: `helloairflow`).
	- `EMR_EC2_ROLE` / `EMR_JOBFLOW_ROLE` — IAM roles for EMR (defaults: `EMR_EC2_DefaultRole`/`EMR_DefaultRole`).
	- `EMR_WAIT_TIMEOUT_SECONDS` / `EMR_POLL_INTERVAL_SECONDS` — tune how long the DAG waits for EMR to become RUNNING before failing (useful for capacity delays).
- Logs: EMR logs are written to the configured S3 `LogUri` (default in the DAG: `s3://hv-emr-logs/`) — check `steps/<step-id>/` and YARN/container logs for errors.

Quick run tip:
- Set any needed env vars in `.env` or your environment, then restart the dev environment:
	```bash
	export EMR_MASTER_INSTANCE_TYPE=m5.large
	export EMR_CORE_INSTANCE_TYPE=m5.large
	export EMR_EC2_KEY_NAME=helloairflow
	export EMR_WAIT_TIMEOUT_SECONDS=900
	astro dev restart
	```

Troubleshooting pointers:
- If the cluster stays in `STARTING` with `Provisioning Amazon EC2 capacity`, increase `EMR_WAIT_TIMEOUT_SECONDS` or change instance types/AZ.
- If the cluster immediately terminates with validation errors (invalid subnet, key name, or role), ensure the corresponding resource exists or set the correct env var.
