from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from airflow.sdk import Variable
import os


# Add paths
HOME_DIR = os.getenv("AIRFLOW_HOME")
DBT_PROJECT_PATH = f"{HOME_DIR}/dbt_code"
DBT_EXECUTABLE_PATH = f"{HOME_DIR}/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    # env_vars={"MY_ENV_VAR": "my_env_value"},
    # dbt_vars={
    #     "my_dbt_var": "my_value",
    #     "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
    #     "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    # },
)

profile_config = ProfileConfig(
    profile_name="monkeytype",
    target_name="airflow",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud",
        profile_args={
            "project": Variable.get("GOOGLE_CLOUD_PROJECT_ID"),
            "dataset": Variable.get("BIGQUERY_DATASET"),
            "threads": 64,
        },
    ),
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)


def dbt_tasks(group_id):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        # operator_args={
        #     "vars": '{"my_name": {{ params.my_name }} }',
        # },
        # default_args={"retries": 2},
    )
