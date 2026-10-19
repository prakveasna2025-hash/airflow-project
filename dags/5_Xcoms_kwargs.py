from airflow.sdk import dag, task


@dag(
    dag_id="xcoms_dag_kwargs",
    schedule=None,
    catchup=False,
)
def xcoms_dag_kwargs():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = [1, 2, 3, 4, 5]
        ti.xcom_push(key='return_result', value=fetched_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids='first_task', key='return_result')
        print("Transforming data... This is the second task")

        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"trans_data": transformed_data}

        ti.xcom_push(key='return_result', value=transformed_data_dict)

    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    # Defining task dependencies
    f_task = first_task()
    s_task = second_task()
    t_task = third_task(s_task)

    f_task >> s_task >> t_task


# Instantiate the DAG
xcoms_dag_kwargs()
