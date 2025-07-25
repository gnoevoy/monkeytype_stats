from airflow.sdk import dag, task
from pathlib import Path
import pendulum
import sys

# Add path to import scripts
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from python_code.extract_data import _extract_data
from python_code.transform_data import _transform_activity_data, _transform_profile_data, _transform_results_data
from python_code.load_data import _load_data
from python_code.file_update import _check_file_update, _update_env_variable


@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def monkeytype_pipeline():

    # Define tasks to extract and transform data
    @task()
    def extract_data():
        _extract_data()

    @task()
    def transform_activity_data():
        _transform_activity_data()

    @task()
    def transform_profile_data():
        _transform_profile_data()

    @task()
    def transform_results_data():
        _transform_results_data()

    t_extract_data = extract_data()
    t_transform_activity_data = transform_activity_data()
    t_transform_profile_data = transform_profile_data()
    t_transform_results_data = transform_results_data()

    # Load to BigQuery tasks
    t_load_activity_data = _load_data("load_activity_data", "clean/activity.csv", "activity")
    t_load_best_results_data = _load_data("load_best_results_data", "clean/best_results.csv", "best_results")
    t_load_results_data = _load_data("load_results_data", "clean/results.csv", "results")
    t_load_stats_data = _load_data("load_stats_data", "clean/stats.csv", "stats")

    @task()
    def check_file_update():
        return _check_file_update()

    @task.short_circuit
    def is_file_updated(dct):
        return dct["is_file_updated"]

    @task()
    def update_env_variable(dct):
        _update_env_variable(dct["file_timestamp"])

    t_check_file_update = check_file_update()
    t_update_env_variable = update_env_variable(t_check_file_update)
    t_is_file_updated = is_file_updated(t_check_file_update)

    t_check_file_update >> t_is_file_updated >> t_transform_results_data >> t_load_results_data >> t_update_env_variable

    # Set task dependencies
    t_extract_data >> [t_transform_activity_data, t_transform_profile_data]
    t_transform_activity_data >> t_load_activity_data
    t_transform_profile_data >> [t_load_best_results_data, t_load_stats_data]


monkeytype_pipeline()
