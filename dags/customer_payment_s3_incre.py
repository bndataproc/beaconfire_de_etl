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

EXEC_DATE = '{{ ds.strftime("%d%m%Y") }}'
SNOWFLAKE_PRESTAGE_TABLE = 'customer_payment_prestg'
SNOWFLAKE_STAGE_TABLE = 'customer_payment_stg'

CREATE_TABLE_SQL_STRING = (
    f'''CREATE OR REPLACE TEMP TABLE {SNOWFLAKE_PRESTAGE_TABLE} (
        InvoiceNo varchar
        ,StockCode varchar
        ,Description varchar
        ,Quantity int
        ,InvoiceDate timestamp
        ,UnitPrice number(10,2)
        ,CustomerID int
        ,Country varchar);
    '''
)

GET_LOAD_DATE = (
    f'''CREATE OR REPLACE TABLE {SNOWFLAKE_STAGE_TABLE} AS
        SELECT 
        *
        , {EXEC_DATE} as load_date
        FROM {SNOWFLAKE_PRESTAGE_TABLE};
    '''
)


with DAG(
        "group1_s3_copy_test",
        start_date=datetime(2022, 10, 18),
        end_date=datetime(2022, 10, 21),
        schedule_interval='0 7 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['test'],
        catchup=True,
) as dag:
    create_prestg_table = SnowflakeOperator(
        task_id='snowflake_create_prestg_table',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

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

    create_stg_table = SnowflakeOperator(
        task_id='snowflake_create_stg_with_load_date',
        sql=GET_LOAD_DATE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    create_prestg_table >> copy_into_prestg >> create_stg_table




