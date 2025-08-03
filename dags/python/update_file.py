from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task
from datetime import datetime
import logging
import os


logger = logging.getLogger(__name__)


# Helper function to get variable and create GCS hook
def get_bucket_name_with_hook():
    bucket_name = os.getenv("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    return bucket_name, hook


@task
def check_file_update():
    bucket_name, hook = get_bucket_name_with_hook()

    # Get file and variable timestamp from GCS
    file_timestamp = hook.get_blob_update_time(bucket_name, "raw/results.csv")
    var_timestamp_txt = hook.download(bucket_name=bucket_name, object_name="variable_timestamp.txt")
    var_timestamp = datetime.fromisoformat(var_timestamp_txt.decode("utf-8").strip())

    # Compare timestamps
    result = file_timestamp > var_timestamp

    logger.info(f"File timestamp: {file_timestamp}")
    logger.info(f"Variable timestamp: {var_timestamp}")
    logger.info(f"Is file updated: {result}")
    return {"is_file_updated": result, "file_timestamp": file_timestamp, "variable_timestamp": var_timestamp}


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
    bucket_name, hook = get_bucket_name_with_hook()

    # Get timestamps from a task
    new_timestamp = dct["file_timestamp"]
    old_timestamp = dct["variable_timestamp"]

    # Update the variable in GCS
    hook.upload(
        bucket_name=bucket_name,
        object_name="variable_timestamp.txt",
        data=new_timestamp.isoformat(),
        mime_type="text/plain",
    )

    logger.info(f"Old value: {old_timestamp}")
    logger.info(f"New value: {new_timestamp}")
