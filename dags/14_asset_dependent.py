from airflow.sdk import dag, task, asset, Asset
from pendulum import datetime
import os

# Define the matching asset reference URI to watch
my_asset = Asset(name="fetch_data",
                 uri="/opt/airflow/logs/data/data_extract.txt")


@dag(
    dag_id="14_asset_dependent",
    schedule=[my_asset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def process_data_dag():

    @task
    def process_data():
        uri = "/opt/airflow/logs/data/data_processed.txt"
        os.makedirs(os.path.dirname(uri), exist_ok=True)

        with open(uri, 'w') as f:
            f.write("Data processed successfully")

        print(f"Data processed to {uri}")

    process_data()


process_data_dag()
