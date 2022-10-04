import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'

SNOWFLAKE_TABLE = 'group_3_stock_test_1'

# SQL commands
CREATE_TABLE = (
    f"CREATE OR REPLACE TABLE {SNOWFLAKE_TABLE}" +
    "(id number identity(1,1), symbol varchar, price_date date, " +
    "open_price number, high_price number, low_price number, " +
    "close_price number, volume number, adj_close_price number);"
)

INSERT_LAST_DATA = (
    f"INSERT INTO {SNOWFLAKE_TABLE} (symbol,price_date,open_price,high_price," +
    "low_price,close_price,volume,adj_close_price)" +
    "SELECT symbol,date,open,high,low,close,volume,adjclose" +
    "FROM US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY" +
    "WHERE date = current_date()"
)

with DAG(
    'stock_test_1',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['stock'],
    catchup=False,
) as dag:

    snowflake_create_table = SnowflakeOperator(
        task_id='snowflake_create_table',
        sql=CREATE_TABLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_insert_data = SnowflakeOperator(
        task_id='snowflake_insert_data',
        sql=INSERT_LAST_DATA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_create_table >> snowflake_insert_data