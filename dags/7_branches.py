from airflow.sdk import dag, task


@dag(
    dag_id="branching_dag",
    schedule=None,
    catchup=False,
)
def branching_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs['ti']
        api_data = [1, 2, 3, 4, 5]
        db_data = [10, 20, 30, 40, 50]
        s3_data = [100, 200, 300, 400, 500]

        extracted_data = {
            "api_extracted_data": api_data,
            "db_extracted_data": db_data,
            "s3_extracted_data": s3_data,
            "weekend_flag": "true",
        }
        ti.xcom_push(key='return_value', value=extracted_data)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task')[
            'api_extracted_data']
        transformed_api_data = [i * 10 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task')[
            'db_extracted_data']
        transformed_db_data = [i * 100 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids='extract_task')[
            's3_extracted_data']
        transformed_s3_data = [i * 1000 for i in s3_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)

    # Creating the Decider Node using Airflow 3 @task.branch decorator
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task')['weekend_flag']
        if weekend_flag == "true":
            return 'no_load_task'
        else:
            return 'load_task'

    @task.bash
    def no_load_task():
        return "echo 'Weekend detected: Skipping load task!'"

    @task.bash
    def load_task():
        return "echo 'Weekday detected: Proceeding with load task!'"

    # Defining dependencies
    extract = extract_task()

    t_api = transform_task_api()
    t_db = transform_task_db()
    t_s3 = transform_task_s3()

    decider = decider_task()
    no_load = no_load_task()
    load = load_task()

    extract >> [t_api, t_db, t_s3] >> decider >> [no_load, load]


# Instantiate the DAG
branching_dag()
