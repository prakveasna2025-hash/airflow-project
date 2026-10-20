
from airflow.sdk import dag, task, Asset
from pendulum import datetime
import os

# Define the asset reference
my_asset = Asset(name="fetch_data",
                 uri="/opt/airflow/logs/data/data_extract.txt")


@dag(
    dag_id="13_assets",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def fetch_data_dag():

    @task(outlets=[my_asset])
    def fetch_data():
        uri = "/opt/airflow/logs/data/data_extract.txt"
        os.makedirs(os.path.dirname(uri), exist_ok=True)

        with open(uri, "w") as f:
            f.write("Data fetched successfully")

        print(f"Data written to {uri}")

    fetch_data()


fetch_data_dag()
