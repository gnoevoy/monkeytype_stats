# from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, LoadMode
# from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
# from airflow.sdk import Variable, task
# import os


# # Add paths
# HOME_DIR = os.getenv("AIRFLOW_HOME")
# DBT_PROJECT_PATH = f"{HOME_DIR}/dbt_code"
# DBT_EXECUTABLE_PATH = f"{HOME_DIR}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="monkeytype",
#     target_name="airflow",
#     profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
#         conn_id="google_cloud",
#         profile_args={
#             "project": Variable.get("GOOGLE_CLOUD_PROJECT_ID"),
#             "dataset": Variable.get("BIGQUERY_DATASET"),
#             "threads": 64,
#         },
#     ),
# )

# project_config = ProjectConfig(
#     dbt_project_path=DBT_PROJECT_PATH,
#     env_vars={"GOOGLE_CLOUD_PROJECT_ID": Variable.get("GOOGLE_CLOUD_PROJECT_ID")},
# )

# execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)
# render_config = RenderConfig(load_method=LoadMode.DBT_LS, dbt_deps=False)


# def dbt_group(group_id):
#     return DbtTaskGroup(
#         group_id=group_id,
#         project_config=project_config,
#         profile_config=profile_config,
#         execution_config=execution_config,
#         render_config=render_config,
#         default_args={"retries": 0},
#         operator_args={"install_deps": False},
#     )


# @task(trigger_rule="none_failed")
# def empty_task():
#     pass
