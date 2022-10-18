"""
Example use of Snowflake related operators.
"""
# test
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
# hello
# hello again

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'beaconfire_stage'

DAG_ID = "stock_history_increload"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 8 * * 2,4,6',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['test'],
    catchup=False,
) as dag:
    # # [START snowflake_example_dag]
    # snowflake_op_sql_str = SnowflakeOperator(
    #     task_id='snowflake_op_sql_str',
    #     sql=CREATE_TABLE_SQL_STRING,
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )
    #
    # snowflake_op_with_params = SnowflakeOperator(
    #     task_id='snowflake_op_with_params',
    #     sql=SQL_INSERT_STATEMENT,
    #     parameters={"id": 5},
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )

    #snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)

    # snowflake_op_sql_multiple_stmts = SnowflakeOperator(
    #     task_id='snowflake_op_sql_multiple_stmts',
    #     sql=SQL_MULTIPLE_STMTS,
    # )
    snowflake_stock_history_historical = SnowflakeOperator(
        task_id="stock_history_historical",
        sql="./stock_history_historical.sql",
    )

    snowflake_stock_history_fact = SnowflakeOperator(
        task_id="stock_history_incre",
        sql="insert into fact_stock_history "
            "select symbol_id, symbol, date, open, high, low, close, volume, ADJCLOSE "
            "from stock_history_historical where load_id=current_date() and date>='{{ ds }}'",
    )

    snowflake_company_profile_temp = SnowflakeOperator(
        task_id='company_profile_deduplicated',
        sql='./company_profile_incre.sql',
    )



    # [END howto_operator_snowflake]

    # [START howto_operator_s3_to_snowflake]

    # copy_into_table = S3ToSnowflakeOperator(
    #     task_id='copy_into_table',
    #     s3_keys=[S3_FILE_PATH],
    #     table=SNOWFLAKE_SAMPLE_TABLE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     stage=SNOWFLAKE_STAGE,
    #     file_format="(type = 'CSV',field_delimiter = ';')",
    # )

    # [END howto_operator_s3_to_snowflake]


    (
        [
            snowflake_stock_history_historical >> snowflake_stock_history_fact,
            snowflake_company_profile_temp

        ]

        # >> [
        #     snowflake_op_with_params,
        #     snowflake_op_sql_list,
        #     snowflake_op_template_file,
        #     # copy_into_table,
        #     snowflake_op_sql_multiple_stmts,
        # ]

    )
    # [END snowflake_example_dag]

