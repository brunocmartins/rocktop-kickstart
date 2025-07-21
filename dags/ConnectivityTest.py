from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import pandas as pd
import logging
from airflow.decorators import task
import io

task_logger = logging.getLogger("airflow.task")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="Connectivity_Test",
    default_args=default_args,
    schedule_interval=None,
    tags=["snowflake", "stored_procedure", "sqlserver"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def fetch_from_snowflake(**context):
        sf_hook = SnowflakeHook(snowflake_conn_id='snowflake-rocktop-nonprod', warehouse='XSMALL_WH')
        conn = sf_hook.get_conn()
        cursor_sf = conn.cursor()

        query = "select top 10 lOAN from FADATA.MATCH.ADDRESS"
        df = sf_hook.get_pandas_df(query)

        task_logger.info(f"Result from Snowflake:\n{df}")

        if df.empty:
            task_logger.warning("No data to insert into Snowflake.")

        task_logger.info("Snowflake test Completed Successfully.")

    @task # added @task decorator
    def fetch_from_sqlserver(**context):
    
        sql_hook = MsSqlHook(mssql_conn_id='mssql-rocktop-test-nonprod')
        conn = sql_hook.get_conn()
        cursor_sql = conn.cursor()

        query = "select top 10 lOAN from FADATA.MATCH.ADDRESS"
        df = sql_hook.get_pandas_df(query)
        # results = sql_hook.get_records(query)
        # df = pd.DataFrame(results, columns=['LOAN'])

        task_logger.info(f"Result from SQL:\n{df}")
        print(df)

        if df.empty:
            task_logger.warning("No data to insert into SQL Server.")

        task_logger.info("SQL Server test Completed Successfully.")

    # added "()" at the end of fetch_from_snowflake and fetch_from_sqlserver to call the task functions
    start >> fetch_from_snowflake()  >> fetch_from_sqlserver() >> end
