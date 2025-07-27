import logging
import time
from typing import Any, Literal, Sequence

from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.context import Context

log = logging.getLogger(__name__)


class SnowflakeToOdbcOperator(BaseOperator):
    """
    Transfers data from a Snowflake database to a target database accessible via
    an ODBC connection.

    This operator executes a query on Snowflake, fetches the results, and then
    efficiently loads them into a target table using the pyodbc `fast_executemany`
    feature for high-performance batch inserts. It supports both appending data
    and truncating the target table before loading.

    :param sql: The SQL query to execute on the Snowflake database.
    :param table: The name of the target table in the ODBC database.
    :param snowflake_conn_id: Airflow connection ID for the Snowflake connection.
    :param odbc_conn_id: Airflow connection ID for the ODBC target connection.
    :param batch_size: The number of rows to insert in each batch.
    :param method: The loading method. Options are:
        - "INSERT": Appends data to the target table.
        - "TRUNCATE_INSERT": Truncates the target table before inserting data.
    """

    template_fields: Sequence[str] = ("sql", "snowflake_conn_id", "odbc_conn_id", "method")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        sql: str,
        table: str,
        snowflake_conn_id: str = "snowflake_default",
        odbc_conn_id: str = "odbc_default",
        batch_size: int = 10000,
        method: Literal["INSERT", "TRUNCATE_INSERT"] = "INSERT",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.table = table
        self.snowflake_conn_id = snowflake_conn_id
        self.odbc_conn_id = odbc_conn_id
        self.batch_size = batch_size
        self.method = method

    def execute(self, context: Context) -> None:
        """
        Executes the data transfer from Snowflake to the ODBC target.

        This method connects to both Snowflake and the ODBC database, fetches data
        in a streaming fashion, and performs batch inserts. If `TRUNCATE_INSERT`
        is specified, it ensures the truncate operation is performed only if data
        is returned from the source query.
        """
        log.info(
            f"Transferring data from Snowflake to table '{self.table}' "
            f"using ODBC (fast_executemany), method={self.method}"
        )
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        odbc_hook = OdbcHook(odbc_conn_id=self.odbc_conn_id)

        with snowflake_hook.get_conn() as sf_conn:
            with sf_conn.cursor() as sf_cursor:
                sf_cursor.execute(self.sql)
                columns = [col[0] for col in sf_cursor.description]
                placeholders = ", ".join(["?" for _ in columns])
                insert_sql = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({placeholders})"

                with odbc_hook.get_conn() as odbc_conn:
                    try:
                        with odbc_conn.cursor() as odbc_cursor:
                            try:
                                odbc_cursor.fast_executemany = True
                                log.info("Enabled fast_executemany on ODBC cursor.")
                            except Exception as e:
                                log.warning(f"Could not enable fast_executemany: {e}")

                            first_row = None
                            batch = []
                            total_rows = 0
                            batch_num = 0
                            start_time = time.time()
                            got_data = False
                            for row in sf_cursor:
                                if not got_data:
                                    first_row = row
                                    got_data = True
                                    if self.method == "TRUNCATE_INSERT":
                                        log.info(f"Truncating table {self.table} (method=TRUNCATE_INSERT)")
                                        odbc_cursor.execute(f"TRUNCATE TABLE {self.table}")
                                        log.info(f"Table {self.table} truncated.")
                                    batch.append(first_row)
                                else:
                                    batch.append(row)
                                if len(batch) >= self.batch_size:
                                    batch_num += 1
                                    batch_start = time.time()
                                    odbc_cursor.executemany(insert_sql, batch)
                                    batch_time = time.time() - batch_start
                                    total_rows += len(batch)
                                    log.info(
                                        f"Loaded batch {batch_num}: {len(batch)} rows in "
                                        f"{batch_time:.2f}s (total loaded: {total_rows})"
                                    )
                                    batch.clear()

                            if batch:
                                batch_num += 1
                                batch_start = time.time()
                                odbc_cursor.executemany(insert_sql, batch)
                                batch_time = time.time() - batch_start
                                total_rows += len(batch)
                                log.info(
                                    f"Loaded final batch {batch_num}: {len(batch)} rows in "
                                    f"{batch_time:.2f}s (total loaded: {total_rows})"
                                )

                            if not got_data:
                                log.warning("No data returned from Snowflake. No changes made to target table.")
                                odbc_conn.rollback()
                                return

                            odbc_conn.commit()
                            elapsed = time.time() - start_time
                            log.info(f"Data transfer complete. Total rows loaded: {total_rows} in {elapsed:.2f}s")
                    except Exception as e:
                        log.error(f"Error during data transfer: {e}")
                        odbc_conn.rollback()
                        raise
