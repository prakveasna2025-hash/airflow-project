from airflow.sdk import dag, task


@dag(
    dag_id="XCOMS_auto",
    schedule=None,
    catchup=False,
)
def first_dag():

    @task.python
    def first_task():
        return {"data": [1, 2, 3, 4, 5]}

    @task.python
    def second_task(data: dict):
        fetched_data = data['data']
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"trans_data": transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    # Defining task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

    first >> second >> third


# Instantiate the DAG
first_dag()
