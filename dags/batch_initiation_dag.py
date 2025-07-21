from pendulum import datetime
import logging

from airflow.models import DAG, Variable
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


task_logger = logging.getLogger("airflow.task")

with DAG(
    dag_id="batch_initiation",
    schedule=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["continuours_monitoring", "sql_sensor"],
) as dag:

    start = EmptyOperator(task_id="start_task")

    @task
    def collect_batch_ids() -> list[str]:
        hook = MsSqlHook(mssql_conn_id="mssql_default")
        sql = """
            SELECT DISTINCT id
            FROM mdr.dbo.batch
        """
        rows = hook.get_records(sql)
        return [row[0] for row in rows]

    @task_group
    def wait_for_batch_task_group(batch_id):
        @task.sensor(
            task_id='wait_for_batch_completion',
            poke_interval=60,
            timeout=300,
            do_xcom_push=True,
            mode='reschedule'
        )
        def wait_for_batch(batch_id: int, ti):
            #Getting batchlog ids for all the batche ids where batch_state is sucess and dag_batchstate is null 
            #(using dag_batchstate in order to tackle multiple batch id entries in batchlog table for a current date)
        
            sql = f"""
                SELECT DISTINCT ID FROM MDR.DBO.BATCHLOG
                WHERE BATCHID = '{batch_id}' 
                    AND CAST(CREATEDAT AS DATE) = CAST(GETDATE() AS DATE) 
                    AND BATCHSTATE='Success' 
                    AND DAG_BATCHSTATE IS NULL
            """
            hook = MsSqlHook(mssql_conn_id="mssql_default")
            rows = hook.get_records(sql)
            if not rows:
                return None

            Variable.set(f"batchlog_id_batch_{batch_id}", rows[0][0])
            return rows[0][0] if rows else None

        trigger_raw_prep = TriggerDagRunOperator(
            task_id="trigger_Raw_Layer_Preparation",
            trigger_dag_id="Raw_Layer_Preparation",
            wait_for_completion=False,
            reset_dag_run=True,
            conf={
                "batch_id": batch_id,
            },
        )

        wait_for_batch(batch_id=batch_id) >> trigger_raw_prep

    start >> wait_for_batch_task_group.expand(batch_id=collect_batch_ids())
