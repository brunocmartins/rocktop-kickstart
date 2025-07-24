import asyncio
from typing import List, Tuple, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async


class SqlTrigger(BaseTrigger):
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

    def serialize(self):
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
    
    def validate_result(self, result: List[Tuple]) -> bool:
        if not result:
            if self._fail_on_empty:
                raise AirflowException(
                    "No rows returned, raising as per fail_on_empty flag"
                )
            else:
                return False

        first_cell = result[0][0]
        if self._failure is not None:
            if callable(self._failure):
                if self._failure(first_cell):
                    raise AirflowException(
                        f"Failure criteria met. self.failure({first_cell}) returned True"
                    )
            else:
                raise AirflowException(
                    f"self.failure is present, but not callable -> {self._failure}"
                )
            if self._success is not None:
                if callable(self._success):
                    return self._success(first_cell)
                else:
                    raise AirflowException(
                        f"self.success is present, but not callable -> {self._success}"
                    )
        return bool(first_cell)

    def get_hook(self) -> BaseHook:
        conn = BaseHook.get_connection(self._conn_id)
        hook = conn.get_hook()
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        return hook

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        try:
            hook = self.get_hook()
            while True:
                result = hook.get_records(self._sql, self._parameters)

                self.log.info(
                    "Raw query result = %s <DAG id = %s, task id = %s, run id = %s>",
                    result,
                    self._dag_id,
                    self._task_id,
                    self._run_id,
                )

                if await sync_to_async(self.validate_result)(result):
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Found expected markers.",
                        }
                    )

                else:

                    self.log.info(
                        (
                            "No success yet. Checking again in %s seconds. "
                            "<DAG id = %s, task id = %s, run id = %s>"
                        ),
                        self._poke_interval,
                        self._dag_id,
                        self._task_id,
                        self._run_id,
                    )
                    await asyncio.sleep(self._poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
