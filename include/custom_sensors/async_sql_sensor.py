from datetime import timedelta
from typing import Sequence, Optional, List, Union, Dict

from airflow import AirflowException
from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from include.custom_triggers.sql_trigger import SqlTrigger


class AsyncSqlSensor(BaseSensorOperator):
    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (
        ".sql",
    )

    def __init__(
        self,
        *,
        conn_id,
        sql,
        parameters=None,
        success=None,
        failure=None,
        fail_on_empty=False,
        hook_params=None,
        poll_interval: int = 60,
        **kwargs,
    ):
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        self.hook_params = hook_params
        if poll_interval:
            self.poke_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: Context, **kwargs) -> None:
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SqlTrigger(
                sql=self.sql,
                poke_interval=self.poke_interval,
                parameters=self.parameters,
                success=self.success,
                failure=self.failure,
                fail_on_empty=self.fail_on_empty,
                dag_id=context["dag"].dag_id,
                task_id=context["task"].task_id,
                run_id=context["dag_run"].run_id,
                conn_id=self.conn_id,
            ),
            method_name=self.execute_complete.__name__,
        )


    def execute_complete(
        self,
        context: Context,
        event: Optional[Dict[str, Union[str, List[str]]]] = None,
    ) -> None:
        if "status" in event and event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info(event["message"])
