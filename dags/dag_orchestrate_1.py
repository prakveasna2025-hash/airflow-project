from airflow.sdk import dag, task
from pendulum import datetime
import os


@dag(
    dag_id="first_orchestrator_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def first_orchestrator_dag():

    @task.python
    def first_task():
        print("This is the first task of DAG 1")

    @task.python
    def second_task():
        # Optional: writing file output like the tutorial's structure
        os.makedirs(os.path.dirname(
            "/opt/airflow/logs/data/output_1.txt"), exist_ok=True)
        with open("/opt/airflow/logs/data/output_1.txt", "w") as f:
            f.write("DAG 1 completed")
        print("This is the second task of DAG 1")

    first_task() >> second_task()


first_orchestrator_dag()
