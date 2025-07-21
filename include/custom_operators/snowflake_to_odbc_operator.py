import logging
from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.context import Context
import time

log = logging.getLogger(__name__)

class SnowflakeToOdbcOperator(BaseOperator):
    """
    Airflow Operator to transfer data from Snowflake to a SQL Server database using OdbcHook and pyodbc's fast_executemany.

    Runs a SQL query on Snowflake and inserts the resulting data into a target table via ODBC in efficient batches.
    """
    template_fields: Sequence[str] = ("sql", "snowflake_conn_id", "odbc_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        sql: str,
        table: str,
        snowflake_conn_id: str = "snowflake_default",
        odbc_conn_id: str = "odbc_default",
        batch_size: int = 10000,
        **kwargs: Any,
    ) -> None:
        """
        :param sql: SQL query to execute on Snowflake.
        :param table: Target table to insert data into (should be fully qualified if needed).
        :param snowflake_conn_id: Airflow connection ID for Snowflake.
        :param odbc_conn_id: Airflow ODBC connection ID for SQL Server.
        :param batch_size: Number of rows per batch insert.
        """
        super().__init__(**kwargs)
        self.sql = sql
        self.table = table
        self.snowflake_conn_id = snowflake_conn_id
        self.odbc_conn_id = odbc_conn_id
        self.batch_size = batch_size

    def execute(self, context: Context) -> None:
        """
        Executes the SQL on Snowflake and inserts the results into SQL Server via ODBC in batches using fast_executemany.
        Adds debug logging for batch sizes, progress, and timing.
        """
        log.info(f"Transferring data from Snowflake to SQL Server table '{self.table}' using ODBC (fast_executemany)")
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        odbc_hook = OdbcHook(odbc_conn_id=self.odbc_conn_id)

        with snowflake_hook.get_conn() as sf_conn:
            with sf_conn.cursor() as sf_cursor:
                sf_cursor.execute(self.sql)
                columns = [col[0] for col in sf_cursor.description]
                placeholders = ", ".join(["?" for _ in columns])
                insert_sql = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({placeholders})"

                batch = []
                with odbc_hook.get_conn() as odbc_conn:
                    with odbc_conn.cursor() as odbc_cursor:
                        # Enable fast_executemany for pyodbc
                        try:
                            odbc_cursor.fast_executemany = True
                            log.info("Enabled fast_executemany on ODBC cursor.")
                        except Exception as e:
                            log.warning(f"Could not enable fast_executemany: {e}")
                        for row in sf_cursor:
                            batch.append(row)
                            if len(batch) >= self.batch_size:
                                odbc_cursor.executemany(insert_sql, batch)
                                batch.clear()
                        # Insert any remaining rows
                        if batch:
                            odbc_cursor.executemany(insert_sql, batch)
                        odbc_conn.commit()
