import asyncio
from typing import AsyncIterator, List, Tuple

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async


class SqlTrigger(BaseTrigger):
    """
    A deferrable trigger that repeatedly polls a database with a specified SQL
    query and yields an event when a success or failure condition is met.

    This trigger is designed to be used by deferrable sensors to wait for a
    database state without holding a worker slot. It evaluates the first cell
    of the query's result set to determine the outcome.

    :param conn_id: The Airflow connection ID to use for the database.
    :param sql: The SQL query to execute.
    :param parameters: Parameters to pass to the SQL query.
    :param poke_interval: The time in seconds to wait between polls.
    :param success: A callable or value to evaluate for success. If a callable,
                    it receives the first cell's value and should return True for success.
                    If not a callable, the cell's value is simply evaluated as a boolean.
    :param failure: A callable to evaluate for failure. It receives the first
                    cell's value and should return True if the condition is met,
                    which will raise an exception.
    :param fail_on_empty: If True, the trigger will fail if the query returns no rows.
    :param dag_id: The DAG ID for logging purposes.
    :param task_id: The Task ID for logging purposes.
    :param run_id: The Run ID for logging purposes.
    """

    def __init__(
        self,
        *,
        sql: str,
        parameters: str,
        success: str,
        failure: str,
        fail_on_empty: bool,
        dag_id: str,
        task_id: str,
        run_id: str,
        conn_id: str,
        poke_interval: int = 60,
    ):
        super().__init__()
        self._sql = sql
        self._parameters = parameters
        self._success = success
        self._failure = failure
        self._fail_on_empty = fail_on_empty
        self._dag_id = dag_id
        self._task_id = task_id
        self._run_id = run_id
        self._conn_id = conn_id
        self._poke_interval = poke_interval

    def serialize(self) -> Tuple[str, dict]:
        """
        Serializes the SqlTrigger arguments so it can be stored in the database.
        """
        return (
            "include.custom_triggers.sql_trigger.SqlTrigger",
            {
                "sql": self._sql,
                "parameters": self._parameters,
                "poke_interval": self._poke_interval,
                "success": self._success,
                "failure": self._failure,
                "fail_on_empty": self._fail_on_empty,
                "dag_id": self._dag_id,
                "task_id": self._task_id,
                "run_id": self._run_id,
                "conn_id": self._conn_id,
            },
        )

    def get_hook(self) -> BaseHook:
        """
        Retrieves the database hook for the specified connection.
        Ensures the hook is a subclass of DbApiHook.
        """
        conn = BaseHook.get_connection(self._conn_id)
        hook = conn.get_hook()
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        return hook

    def validate_result(self, result: List[Tuple]) -> bool:
        """
        Validates the query result based on success/failure criteria.

        :param result: The list of tuples returned by the database hook.
        :return: True if the success criteria is met, False otherwise.
        :raises AirflowException: If the failure criteria is met or if
                                 fail_on_empty is True and no rows are returned.
        """
        if not result:
            if self._fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False

        first_cell = result[0][0]
        if self._failure is not None:
            if callable(self._failure):
                if self._failure(first_cell):
                    raise AirflowException(f"Failure criteria met. self.failure({first_cell}) returned True")
            else:
                raise AirflowException(f"self.failure is present, but not callable -> {self._failure}")
        
        if self._success is not None:
            if callable(self._success):
                return self._success(first_cell)
            else:
                raise AirflowException(f"self.success is present, but not callable -> {self._success}")
        
        return bool(first_cell)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        The main execution loop for the trigger.

        It polls the database at the specified interval, validates the result,
        and yields a TriggerEvent upon success or error.
        """
        try:
            hook = self.get_hook()
            while True:
                # Run the sync hook method in a separate thread
                result = await sync_to_async(hook.get_records)(self._sql, self._parameters)
                self.log.info(
                    "Raw query result = %s <DAG id = %s, task id = %s, run id = %s>",
                    result,
                    self._dag_id,
                    self._task_id,
                    self._run_id,
                )

                # Validate the result asynchronously
                if await sync_to_async(self.validate_result)(result):
                    yield TriggerEvent(
                        {"status": "success", "message": "Found expected markers."}
                    )
                    return
                else:
                    self.log.info(
                        "No success yet. Checking again in %s seconds. <DAG id = %s, task id = %s, run id = %s>",
                        self._poke_interval,
                        self._dag_id,
                        self._task_id,
                        self._run_id,
                    )
                    await asyncio.sleep(self._poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
