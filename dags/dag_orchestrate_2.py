from airflow.sdk import dag, task
from pendulum import datetime
import os


@dag(
    dag_id="second_orchestrator_dag",
    schedule=None,
    catchup=False,
)
def second_orchestrator_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        path = "/opt/airflow/logs/data/output_2.txt"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("Data processed successfully")
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


second_orchestrator_dag()
