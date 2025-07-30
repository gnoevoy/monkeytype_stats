from airflow.decorators import task_group
from airflow.sdk import dag
import pendulum
import sys
import os

# Add path to import scripts / tasks
HOME_DIR = os.getenv("AIRFLOW_HOME")
sys.path.append(HOME_DIR)

from python_code.extract_data import extract_data
from python_code.transform_data import transform_activity_data, transform_profile_data, transform_results_data
from python_code.load_data import load_data
from python_code.update_file import check_file_update, is_file_updated, update_env_variable
from dbt_code.dbt_setup import dbt_group, empty_task


@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def monkeytype_pipeline():

    @task_group(group_id="python_group")
    def python_group():

        @task_group(group_id="api_group")
        def api_group():
            t_extract_data = extract_data()

            # Transformation tasks
            t_transform_activity_data = transform_activity_data()
            t_transform_profile_data = transform_profile_data()

            # Load to GCS tasks
            t_load_activity_data = load_data("load_activity_data", "clean/activity.csv", "activity")
            t_load_best_results_data = load_data("load_best_results_data", "clean/best_results.csv", "best_results")
            t_load_stats_data = load_data("load_stats_data", "clean/stats.csv", "stats")

            # Dependecies inside the group
            t_extract_data >> [t_transform_activity_data, t_transform_profile_data]
            t_transform_activity_data >> t_load_activity_data
            t_transform_profile_data >> [t_load_best_results_data, t_load_stats_data]

            # Return the last tasks in the group
            return [t_load_activity_data, t_load_best_results_data, t_load_stats_data]

        @task_group(group_id="file_group", prefix_group_id=False)
        def file_group():
            # Run scripts if a file was updated otherwise skip tasks
            t_check_file_update = check_file_update()
            t_is_file_updated = is_file_updated(t_check_file_update)
            t_transform_results_data = transform_results_data()
            t_load_results_data = load_data("load_results_data", "clean/results.csv", "results")
            t_update_env_variable = update_env_variable(t_check_file_update)

            t_check_file_update >> t_is_file_updated >> t_transform_results_data >> t_load_results_data >> t_update_env_variable
            return t_update_env_variable

        g_api_group = api_group()
        g_file_group = file_group()

    # Chain the task groups together
    g_python = python_group()
    g_dbt = dbt_group(group_id="dbt_group")
    t_empty_task = empty_task()
    g_python >> t_empty_task >> g_dbt


monkeytype_pipeline()
