from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="oparator_dag")
def oparator_dag():
    @task.python
    def task_1():
        print("Task 1 executed")

    @task.python
    def task_2():
        print("Task 2 executed")

    @task.bash
    def bash_task() -> str:
        return "echo https://airflow.apache.org/"

    Bash_Run = BashOperator(
        task_id="Bash_Run",
        bash_command="echo https://airflow.apache.org/",
    )

    # Difining task dependencies using the bitshift operator (>>). This means that task_1 will run first, followed by task_2, and finally task_3.
    first_dag = task_1()
    second_dag = task_2()
    Bash_modern = bash_task()
    Bash_for_run = Bash_Run

    first_dag >> second_dag >> Bash_modern >> Bash_for_run


# Instantiate the DAG
oparator_dag()
