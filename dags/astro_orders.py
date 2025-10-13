from datetime import datetime 
from airflow.models import DAG
import pandas as pd
from pandas import DataFrame
from astro import sql as aql 
from astro.files import File
from astro.sql.table import Table

S3_FILE_PATH="s3://hv-airflow-2/orders_data_header.csv"
S3_CONN_ID="aws_default"
SNOWFLAKE_CONN_ID="snowflake_default2"
SNOWFLAKE_ORDERS="orders_table"
SNOWFLAKE_FILTERED_ORDERS="filtered_table"
SNOWFLAKE_JOINED="joined_table"
SNOWFLAKE_CUSTOMERS="customers_table"
SNOWFLAKE_REPORTING="reporting_table"

@aql.transform 
def filter_orders(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE amount>150"

@aql.transform
def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date,  amount, TYPE 
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
    on f.customer_id=c.customer_id    
    """

@aql.dataframe
def transform_dataframe(df: DataFrame):
    purchase_dates=df.loc[:, "purchase_date"]
    # XCom uses JSON serialization by default; pandas Series are not JSON serializable.
    # Convert to a plain Python list (or dict) before returning so Airflow can push via XCom.
    purchase_dates_list = purchase_dates.tolist()
    print("purchase dates:", purchase_dates_list)
    return purchase_dates_list


with DAG(dag_id='astro_orders', start_date=datetime(2022, 1, 1), schedule="@daily", catchup=False):
    orders_data=aql.load_file(
        input_file=File(path=S3_FILE_PATH, conn_id=S3_CONN_ID),
        output_table=Table(conn_id=SNOWFLAKE_CONN_ID)
    )

    customers_table=Table(name=SNOWFLAKE_CUSTOMERS, conn_id=SNOWFLAKE_CONN_ID)

    joined_data=join_orders_customers(filter_orders(orders_data), customers_table)

    reporting_table=aql.merge(target_table=Table(name=SNOWFLAKE_REPORTING, conn_id=SNOWFLAKE_CONN_ID), 
                              source_table=joined_data, 
                              target_conflict_columns=["order_id"],
                              columns=["customer_id", "customer_name"],
                              if_conflicts="update",
    )

    purchase_dates=transform_dataframe(reporting_table)

    # cleanup is callable and returns an operator/task — call it to get a task object
    purchase_dates >> aql.cleanup()
