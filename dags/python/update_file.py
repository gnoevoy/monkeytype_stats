from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task, Variable
from datetime import datetime
import logging
import os


logger = logging.getLogger(__name__)


@task
def check_file_update():
    # Get file timestamp from GCS
    bucket_name = os.getenv("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    file_timestamp = hook.get_blob_update_time(bucket_name, "raw/results.csv")

    # Get env variable value -> convert to timestamp
    var_value = Variable.get("RESULTS_FILE_LAST_UPDATE")
    var_timestamp = datetime.fromisoformat(var_value)

    # Compare timestamps
    result = file_timestamp > var_timestamp

    logger.info(f"File timestamp: {file_timestamp}")
    logger.info(f"Variable timestamp: {var_timestamp}")
    logger.info(f"Is file updated: {result}")
    return {"is_file_updated": result, "file_timestamp": file_timestamp}


# Conditional task to check if the file was updated
@task.branch
def is_file_updated(dct):
    flag = dct["is_file_updated"]
    if flag:
        return ["transform_results_data"]
    else:
        return []


@task
def update_env_variable(dct):
    # Retrieve variable from airflow
    current_env_var_value = Variable.get("RESULTS_FILE_LAST_UPDATE")
    new_value = dct["file_timestamp"].isoformat()

    logger.info(f"Old value: {current_env_var_value}")
    logger.info(f"New value: {new_value}")
    Variable.set("RESULTS_FILE_LAST_UPDATE", new_value)
