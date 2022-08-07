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
SNOWFLAKE_STAGE = 's3_stage_trans_order'
S3_FILE_PATH = 'product_order_trans_07152022.csv'

with DAG(
    "s3_data_copy_test",
    start_date=datetime(2022, 7, 12),
    end_date = datetime(2022, 7, 14),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_product_order_trans',
        s3_keys=['product_order_trans_{{ task.start_date.strftime("%m%d%Y") }}.csv'],
        table='prestg_product_order_trans',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg
          
        
    

