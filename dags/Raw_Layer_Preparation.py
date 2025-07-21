from pendulum import datetime

from airflow.models import DAG, Variable
from airflow.operators.python import get_current_context
from airflow.decorators import task

with DAG(
    dag_id="Raw_Layer_Preparation",
    schedule=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["raw_preparation", "print_conf"],
) as dag:

    @task
    def print_conf():
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}
        batch_id = conf.get("batch_id")

        # build your variable key dynamically
        var_key = f"batchlog_id_batch_{batch_id}"

        # read it at runtime
        batchlogid = Variable.get(var_key, default_var=None)

        print(f"batch_id = {batch_id}")
        print(f"{var_key} = {batchlogid}")
    # invoke the task
    print_conf()
