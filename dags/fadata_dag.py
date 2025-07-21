from pendulum import datetime

from airflow.models import DAG

from include.custom_operators.snowflake_to_mssql_operator import SnowflakeToMssqlOperator
from include.custom_operators.snowflake_to_odbc_operator import SnowflakeToOdbcOperator


with DAG(
    dag_id="fadata_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # transfer_task = SnowflakeToMssqlOperator(
    #     task_id="transfer_data",
    #     sql="SELECT * FROM sample_table",
    #     table="MDR.DBO.SAMPLE_TABLE",
    #     # snowflake_conn_id="snowflake-rocktop-nonprod",
    #     # mssql_conn_id="mssql-rocktop-test-nonprod",
    #     batch_size=1000,
    # )

    another_transfer_task = SnowflakeToOdbcOperator(
        task_id="transfer_data_odbc",
        sql="SELECT * FROM sample_table",
        odbc_conn_id="odbc_default",
        table="MDR.DBO.SAMPLE_TABLE",
        batch_size=1000,
        method="TRUNCATE_INSERT",
    )

    another_transfer_task
