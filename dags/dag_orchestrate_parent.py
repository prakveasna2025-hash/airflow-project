from airflow.sdk import dag
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="dag_orchestrate_parent",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def dag_orchestrate_parent():

    trigger_first = TriggerDagRunOperator(
        task_id="trigger_first_dag",
        trigger_dag_id="first_orchestrator_dag",
        # Wait for the first DAG to complete before triggering the second
        wait_for_completion=True,
    )

    trigger_second = TriggerDagRunOperator(
        task_id="trigger_second_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True,  # Wait for the second DAG to complete before finishing
    )

    trigger_first >> trigger_second


dag_orchestrate_parent()
