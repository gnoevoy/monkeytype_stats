from airflow.sdk import dag, task
from pathlib import Path
import pendulum
import sys

root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from pipeline.extract_data import extract_data
from pipeline.transform_data import transform_activity_data, transform_profile_data, transform_results_data
from pipeline.load_data import load_data_to_bigquery


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def monkeytype_pipeline():
    @task()
    def _extract_data():
        extract_data()

    @task()
    def _transform_activity_data():
        transform_activity_data()

    @task()
    def _transform_profile_data():
        transform_profile_data()

    @task()
    def _transform_results_data():
        transform_results_data()

    t_load_activity_data = load_data_to_bigquery("load_activity_data", "clean/activity.csv", "activity")
    t_load_best_results_data = load_data_to_bigquery("load_best_results_data", "clean/best_results.csv", "best_results")
    t_load_results_data = load_data_to_bigquery("load_results_data", "clean/results.csv", "results")
    t_load_stats_data = load_data_to_bigquery("load_stats_data", "clean/stats.csv", "stats")

    t_extract_data = _extract_data()
    t_transform_activity_data = _transform_activity_data()
    t_transform_profile_data = _transform_profile_data()
    t_transform_results_data = _transform_results_data()

    t_extract_data >> [t_transform_activity_data, t_transform_profile_data, t_transform_results_data]
    t_transform_activity_data >> t_load_activity_data
    t_transform_results_data >> t_load_results_data
    t_transform_profile_data >> [t_load_best_results_data, t_load_stats_data]


monkeytype_pipeline()
