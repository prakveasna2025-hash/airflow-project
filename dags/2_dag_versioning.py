from airflow.sdk import dag, task


@dag(
    dag_id="version_dag")
def versioned_dag():

    @task.python
    def task_1():
        print("Task 1 executed")

    @task.python
    def task_2():
        print("Task 2 executed")

    @task.python
    def task_3():
        print("Task 3 executed")

    @task.python
    def version_task():
        print("This is a version task. DAG version 2.0!")

    # Difining task dependencies using the bitshift operator (>>). This means that task_1 will run first, followed by task_2, and finally task_3.
    first_dag = task_1()
    second_dag = task_2()
    third_dag = task_3()
    version = version_task()

    first_dag >> second_dag >> third_dag


# Instantiate the DAG
versioned_dag()
