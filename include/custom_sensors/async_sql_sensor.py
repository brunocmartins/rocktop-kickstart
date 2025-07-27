from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from include.custom_triggers.sql_trigger import SqlTrigger


class AsyncSqlSensor(BaseSensorOperator):
    """
    An asynchronous sensor that waits for an SQL query to return a 'True' value.

    This sensor leverages the deferrable trigger mechanism in Airflow to free up
    worker slots while it waits. It periodically runs a SQL query and evaluates
    the first cell of the result. The task succeeds if the value evaluates to
    True.

    :param conn_id: The Airflow connection ID for the database.
    :param sql: The SQL query to be executed.
    :param parameters: (Optional) The parameters to pass to the SQL query.
    :param success: (Optional) A callable that receives the first cell of the
                    query result and returns True for success.
    :param failure: (Optional) A callable that receives the first cell of the
                    query result and returns True for failure, raising an exception.
    :param fail_on_empty: If True, the sensor will fail if the query returns no rows.
    :param poll_interval: The interval in seconds between database checks.
    :param hook_params: (Optional) Extra parameters to pass to the DB hook.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)

    def __init__(
        self,
        *,
        conn_id: str,
        sql: str,
        parameters: Optional[Union[List, Dict]] = None,
        success: Optional[callable] = None,
        failure: Optional[callable] = None,
        fail_on_empty: bool = False,
        hook_params: Optional[Dict] = None,
        poll_interval: int = 5,
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

    def execute(self, context: Context) -> None:
        """
        Defers the task to the SqlTrigger.

        This method creates an instance of the SqlTrigger with the provided
        parameters and defers the execution. The Airflow triggerer service will
        then take over and run the trigger's async loop.
        """
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SqlTrigger(
                conn_id=self.conn_id,
                sql=self.sql,
                poke_interval=self.poke_interval,
                parameters=self.parameters,
                success=self.success,
                failure=self.failure,
                fail_on_empty=self.fail_on_empty,
                dag_id=context["dag"].dag_id,
                task_id=context["task"].task_id,
                run_id=context["dag_run"].run_id,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(
        self,
        context: Context,
        event: Optional[Dict[str, Union[str, List[str]]]] = None,
    ) -> None:
        """
        Callback method executed when the trigger fires.

        This method is called by the triggerer when the sensing condition is met
        or an error occurs. It handles the final state of the task.

        :param context: The task context.
        :param event: The event payload from the trigger.
        """
        if event and event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("Success criteria met. Resuming task.")
        if event:
            self.log.info(event["message"])
