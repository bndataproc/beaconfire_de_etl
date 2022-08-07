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
S3_FILE_PATH = 's3://de-july-airflow/product-order/product_order_trans_07152022.csv'

with DAG(
    "s3_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval='* * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_product_order_trans',
        s3_keys=[S3_FILE_PATH],
        table='prestg_product_order_trans',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ';')",
    )

    copy_into_prestg
          
        
    

