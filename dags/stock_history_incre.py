"""
Example use of Snowflake related operators.
"""
# test
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'beaconfire_stage'

DAG_ID = "group1_stock_history_increload"


with DAG(
    DAG_ID,
    start_date=datetime(2022, 10, 17),
    schedule_interval='0 8 * * 1-5',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['group1'],
    catchup=False,
) as dag:

    snowflake_stock_history_fact = SnowflakeOperator(
        task_id="stock_history_incre",
        sql='''
            INSERT INTO fact_stock_history
                SELECT
                    MD5_NUMBER_LOWER64(symbol) as symbol_id
                    ,symbol
                    ,date
                    ,open
                    ,high
                    ,low
                    ,close
                    ,volume
                    ,ADJCLOSE
                    ,current_date() as load_date
                FROM US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY
                WHERE date='{{ ds }}';
            ''',
    )

    snowflake_company_profile_dim = SnowflakeOperator(
        task_id='company_profile_incre',
        sql='./company_profile_incre.sql',
    )


    (
        [
            snowflake_company_profile_dim >> snowflake_stock_history_fact

        ]

    )

