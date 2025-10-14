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

snowflake_automation DAG (EMR workflow)

Purpose
- Launch a transient EMR cluster, run ingest and transform steps (script + spark-submit), wait for steps to finish, terminate the cluster, and refresh data in Snowflake.

Key file
- `dags/airflow_empr_spark_s3_snowflake.py` — creates the EMR cluster, adds steps, polls step status, and triggers a Snowflake refresh.

Flow (high level)
```mermaid
flowchart LR
  A[create_emr_cluster] --> B[ingest_layer\n(add step)]
  B --> C[poll_step_layer\n(wait for step)]
  C --> D[transform_layer\n(add step)]
  D --> E[poll_step_layer2\n(wait for step)]
  E --> F[terminate_emr_cluster]
  F --> G[snowflake_load\n(ALTER EXTERNAL TABLE REFRESH)]
```

Inputs and outputs
- Input scripts/data: S3 scripts (example path in DAG: `s3://hv-irisdata/scripts/ingest.sh` and `transform.py`).
- Logs: EMR writes cluster/step logs to the S3 `LogUri` configured in the DAG (default `s3://hv-emr-logs/`). Check `steps/<step-id>/` and YARN/container logs for application errors.

Important environment variables
- `EMR_MASTER_INSTANCE_TYPE` / `EMR_CORE_INSTANCE_TYPE` — instance types (defaults: `m5.large`).
- `EMR_EC2_KEY_NAME` — SSH key pair name for the EC2 instances (default: `helloairflow`).
- `EMR_EC2_ROLE` / `EMR_JOBFLOW_ROLE` — IAM roles for EMR (defaults: `EMR_EC2_DefaultRole` / `EMR_DefaultRole`).
- `EMR_WAIT_TIMEOUT_SECONDS` / `EMR_POLL_INTERVAL_SECONDS` — tune how long the DAG waits for EMR to become RUNNING before failing (useful for capacity delays).
- `EMR_SUBNET_ID` (recommended) — optional subnet id to run EMR in a specific VPC/AZ (validate before use).

How to run locally (short)
1. Set required env vars (or add them to `.env`) like `EMR_EC2_KEY_NAME`, instance types, and wait timeout.
2. Restart your dev Airflow environment so env vars are picked up (e.g., `astro dev restart`).
3. Trigger the `snowflake_automation_dag` in the Airflow UI.

Troubleshooting
- If the cluster stays in `STARTING` with `Provisioning Amazon EC2 capacity`, increase `EMR_WAIT_TIMEOUT_SECONDS` or change instance types/AZ.
- If the cluster immediately terminates with validation errors (invalid subnet, key name, or role), validate or update the corresponding env var (subnet/key/roles) and re-run.
- When a step fails, run `aws emr describe-step --cluster-id <id> --step-id <id>` and inspect the S3 step logs under the configured `LogUri` to find the error details.
