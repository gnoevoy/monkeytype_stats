from airflow.sdk import dag
import pendulum
import sys
import os

# Add path to import scripts
HOME_DIR = os.getenv("AIRFLOW_HOME")
sys.path.append(HOME_DIR)

from dbt_code.dbt_setup import dbt_group


@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def dbt_example():
    dbt_task_group = dbt_group(group_id="dbt_group")


dbt_example()
