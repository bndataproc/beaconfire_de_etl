"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
#branch_group2_test1

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'beaconfire'
SNOWFLAKE_SCHEMA = 'dev_db'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'beaconfire_stage'
 #S3_FILE_PATH = '</path/to/file/sample_file.csv'

SNOWFLAKE_SAMPLE_TABLE = 'airflow_group2_SYMBOLS'
FACT_TABLE = 'GROUP2_STOCK'
DIM_TABLE = 'GROUP2_DIM_COMPANY'


# SQL commands
#CREATE_FACT_TABLE_SQL_STRING = (
#    f"CREATE OR REPLACE TRANSIENT TABLE {FACT_TABLE} as select * from US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY;"
#)

#CREATE_DIM_TABLE_SQL_STRING = (
#    [f"CREATE OR REPLACE TRANSIENT TABLE {DIM_TABLE} as select  from US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY;",
#     f"ALTER TABLE {DIM_TABLE} MODIFY ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY"]
#)

UPDATE_DIM_TABLE = f"CREATE OR REPLACE TRANSIENT TABLE {DIM_TABLE} as select * from US_STOCKS_DAILY.PUBLIC.COMPANY_PROFILE;"


UPDATE_FACT_TABLE = f"INSERT INTO {FACT_TABLE} select * from US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY WHERE DATE = getdate();"


# SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} select * from US_STOCKS_DAILY.PUBLIC.SYMBOLS"
# SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
# SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
#ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "beaconfire_dev_db_test"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 12 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    #snowflake_op_sql_str = SnowflakeOperator(
    #    task_id='snowflake_op_sql_str',
    #    sql=CREATE_TABLE_SQL_STRING,
    #    warehouse=SNOWFLAKE_WAREHOUSE,
    #    database=SNOWFLAKE_DATABASE,
    #    schema=SNOWFLAKE_SCHEMA,
    #    role=SNOWFLAKE_ROLE,
    #)

    snowflake_dim_update = SnowflakeOperator(
        task_id='snowflake_dim_update',
        sql=UPDATE_DIM_TABLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_fact_update = SnowflakeOperator(
        task_id='snowflake_fact_update',
        sql=UPDATE_FACT_TABLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # snowflake_op_with_params = SnowflakeOperator(
    #     task_id='snowflake_op_with_params',
    #     sql=SQL_INSERT_STATEMENT,
    #     parameters={"id": 11},
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )
    #
    # snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)
    #
    # snowflake_op_sql_multiple_stmts = SnowflakeOperator(
    #     task_id='snowflake_op_sql_multiple_stmts',
    #     sql=SQL_MULTIPLE_STMTS,
    # )

    # snowflake_op_template_file = SnowflakeOperator(
    #    task_id='snowflake_op_template_file',
    #    sql='./beaconfire_dev_db_test.sql',
    # )

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
        snowflake_dim_update >> snowflake_fact_update

        #snowflake_op_sql_str
        # >> [
        #     snowflake_op_with_params,
        #     snowflake_op_sql_list,
        #     snowflake_op_template_file,
        #     # copy_into_table,
        #     snowflake_op_sql_multiple_stmts,
        # ]

    )
    # [END snowflake_example_dag]

