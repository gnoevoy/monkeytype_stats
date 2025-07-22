from airflow.sdk import dag, task
from pathlib import Path
import pendulum
import sys

root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from pipeline.extract_data import extract_data


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def monkeytype_pipeline():

    @task()
    def extract_data_task():
        return extract_data()

    extract_data_task()


monkeytype_pipeline()
