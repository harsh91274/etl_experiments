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
