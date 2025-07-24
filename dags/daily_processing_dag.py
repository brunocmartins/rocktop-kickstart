from pendulum import datetime, duration

from airflow.models import DAG
from airflow.decorators import task

from include.custom_sensors.async_sql_sensor import AsyncSqlSensor


with DAG(
    "daily_processing_dag",
    schedule=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
) as dag:
    
    deferrable_sensor = AsyncSqlSensor.partial(
        task_id="wait_metadata",
        conn_id="mssql_default",
        sql="select * from mdr.dbo.tasks where task_id = %(task_id)s and status = 'success'",
        poke_interval=60,
        map_index_template="""{{ task.parameters['task_id'] }}""",
        timeout=duration(seconds=120) # 2m
    ).expand(
        parameters=[
            {"task_id": 123},
            {"task_id": 231},
            {"task_id": 312},
        ]
    )


