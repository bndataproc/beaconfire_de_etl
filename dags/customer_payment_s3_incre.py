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
SNOWFLAKE_STAGE = 's3_stage_customer_payment'
# S3_FILE_PATH = 'customer_payment_10182022.csv'

with DAG(
        "group1_s3_copy_tesing",
        start_date=datetime(2022, 10, 19),
        end_date=datetime(2022, 10, 23),
        schedule_interval='0 7 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['test'],
        catchup=False,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_customer_payment',
        s3_keys=['customer_payment_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        table='customer_payment_prestg',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )



    copy_into_prestg




