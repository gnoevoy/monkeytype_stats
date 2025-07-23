from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging
import json


logger = logging.getLogger(__name__)


def read_from_bucket(blob_name):
    bucket_name = Variable.get("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    data = hook.download(bucket_name=bucket_name, object_name=blob_name)
    json_data = json.loads(data)
    logger.info(f"File {blob_name} - successfully retrieved from GCS bucket")
    return json_data


def write_to_bucket(data, blob_name):
    bucket_name = Variable.get("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    hook.upload(bucket_name=bucket_name, object_name=blob_name, data=data, mime_type="text/csv")
    logger.info(f"File {blob_name} - successfully uploaded to GCS bucket")


def transform_activity_data():
    # Get json data from GCS
    json_data = read_from_bucket("raw/activity.json")
    data = json_data["data"]
    values, num = data["testsByDays"], data["lastDay"]

    # Convert number to UTC date
    date = datetime.fromtimestamp(num / 1000, tz=timezone.utc).date()
    dates_lst = [date]

    # Generate dates -> assing for each day the number of tests
    for i in range(len(values) - 1):
        value = dates_lst[-1]
        previous_day = value - timedelta(days=1)
        dates_lst.append(previous_day)

    dataset = zip(dates_lst[::-1], values)

    # Create DataFrame
    df = pd.DataFrame(dataset, columns=["date", "tests"])
    df["tests"] = df["tests"].fillna(0).astype(int)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Filter table
    filter_date = datetime(2025, 1, 10).date()
    df = df[df["date"] > filter_date]

    logger.info(f"Activity table - successfully created, {len(df)} rows")
    write_to_bucket(df.to_csv(index=False), "clean/activity")
