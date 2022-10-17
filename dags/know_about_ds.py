"""
Example use of Snowflake related operators.
"""
# test
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator

# from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# hello
# hello again

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'beaconfire_stage'
# S3_FILE_PATH = '</path/to/file/sample_file.csv'

SNOWFLAKE_SAMPLE_TABLE = 'airflow_ds_figureout'

EXEC_DATE = '{{ execution_date.strftime("%d%m%Y") }}'

with DAG(
    "ds_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    user_query_rt_rpt = SnowflakeOperator(
       task_id='user_query_rt_rpt',
       sql=f"insert into {SNOWFLAKE_SAMPLE_TABLE} values (1,'abc',{EXEC_DATE})",
    )

    (
        user_query_rt_rpt
    )