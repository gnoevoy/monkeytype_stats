from airflow.sdk import dag, task
from pathlib import Path
import pendulum
import sys

root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from pipeline.extract_data import extract_data
from pipeline.transform_data import transform_activity_data, transform_profile_data, transform_results_data


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    # tags=["example"],
)
def monkeytype_pipeline():

    @task()
    def _extract_data():
        extract_data()

    @task()
    def _transform_activity_data():
        return transform_activity_data()

    @task()
    def _transform_profile_data():
        return transform_profile_data()

    @task()
    def _transform_results_data():
        return transform_results_data()

    t1 = _extract_data()
    t2 = _transform_activity_data()
    t3 = _transform_profile_data()
    t4 = _transform_results_data()

    t1 >> [t2, t3, t4]


monkeytype_pipeline()
