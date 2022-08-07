"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'

with DAG(
    "user_query_rt_rpt",
    start_date=datetime(2021, 1, 1),
    schedule_interval='30 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_op_template_file',
       sql='./user_query_rt_rpt.sql',
    )

    snowflake_op_template_file        
    

