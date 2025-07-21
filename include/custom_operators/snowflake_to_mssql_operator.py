import logging
from typing import Any, Sequence, Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.context import Context


log = logging.getLogger(__name__)

class SnowflakeToMssqlOperator(BaseOperator):
    """
    Airflow Operator to transfer data from Snowflake to MSSQL.

    Runs a SQL query on Snowflake and inserts the resulting data into a target MSSQL table.
    """
    template_fields: Sequence[str] = ("sql", "snowflake_conn_id", "mssql_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        sql: str,
        table: str,
        snowflake_conn_id: str = "snowflake_default",
        mssql_conn_id: str = "mssql_default",
        batch_size: int = 1000,
        method: str = "INSERT",
        **kwargs: Any,
    ) -> None:
        """
        :param sql: SQL query to execute on Snowflake.
        :param table: Target MSSQL table to insert data into.
        :param snowflake_conn_id: Airflow connection ID for Snowflake.
        :param mssql_conn_id: Airflow connection ID for MSSQL.
        :param batch_size: Number of rows per batch insert.
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.table = table
        self.snowflake_conn_id = snowflake_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.batch_size = batch_size

    def execute(self, context: Context) -> None:
        """
        Executes the SQL on Snowflake and inserts the results into MSSQL in batches.
        """
        log.info(f"Transferring data from Snowflake to MSSQL table '{self.table}'")
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

        # Open Snowflake connection and execute query
        with snowflake_hook.get_conn() as sf_conn:
            with sf_conn.cursor() as sf_cursor:
                sf_cursor.execute(self.sql)
                columns = [col[0] for col in sf_cursor.description]
                placeholders = ", ".join(["%s" for _ in columns])
                insert_sql = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({placeholders})"

                batch = []
                with mssql_hook.get_conn() as ms_conn:
                    with ms_conn.cursor() as ms_cursor:
                        for row in sf_cursor:
                            batch.append(row)
                            if len(batch) >= self.batch_size:
                                ms_cursor.executemany(insert_sql, batch)
                                batch.clear()
                        # Insert any remaining rows
                        if batch:
                            ms_cursor.executemany(insert_sql, batch)
                        ms_conn.commit()
        log.info("Data transfer complete.")
