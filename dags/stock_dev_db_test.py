import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'

SNOWFLAKE_TABLE = 'group_3_stock_symbol'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_TABLE} AS SELECT * FROM US_STOCKS_DAILY.PUBLIC.SYMBOLS;"
)

with DAG(
    "stock_dev_db_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['test'],
    catchup=False,
) as dag:

    stock_to_dev_db = SnowflakeOperator(
        task_id='stock_to_dev_db',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE, 
    )

    stock_to_dev_db