"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'beaconfire_stage'
 #S3_FILE_PATH = '</path/to/file/sample_file.csv'

with DAG(
    "s3_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval='30 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_op_template_file',
       sql='./beaconfire_dev_db_test.sql',
    )



    copy_into_table = S3ToSnowflakeOperator(
        task_id='copy_into_table',
        s3_keys=[S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ';')",
    )



    snowflake_op_template_file >> copy_into_table
          
        
    

