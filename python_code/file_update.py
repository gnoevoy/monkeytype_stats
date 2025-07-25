from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def _check_file_update():
    # Get file timestamp from GCS
    bucket_name = Variable.get("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    file_timestamp = hook.get_blob_update_time(bucket_name, "raw/results.csv")

    # Get var value converted to datetime
    var_value = Variable.get("RESULTS_FILE_LAST_UPDATE")
    var_timestamp = datetime.fromisoformat(var_value)

    # Compare timestamps
    result = file_timestamp > var_timestamp

    logger.info(f"File timestamp: {file_timestamp}")
    logger.info(f"Variable timestamp: {var_timestamp}")
    logger.info(f"Is file updated: {result}")
    return {"is_file_updated": result, "file_timestamp": file_timestamp}


def _update_env_variable(file_timestamp):
    old_value = Variable.get("RESULTS_FILE_LAST_UPDATE")
    logger.info(f"Old value: {old_value}, New value: {file_timestamp}")
    str_value = file_timestamp.isoformat()
    Variable.set("RESULTS_FILE_LAST_UPDATE", str_value)
