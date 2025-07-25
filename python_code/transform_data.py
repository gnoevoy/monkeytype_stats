from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task, Variable
from datetime import datetime
from io import BytesIO
import pandas as pd
import logging
import json


logger = logging.getLogger(__name__)


# 2 helper functions to read / write data to GCS


def read_from_bucket(blob_name, file_type="json"):
    bucket_name = Variable.get("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    data = hook.download(bucket_name=bucket_name, object_name=blob_name)

    # Handle CSV and JSON files differently
    if file_type == "csv":
        csv_buffer = BytesIO(data)
        logger.info(f"File {blob_name} - successfully retrieved from GCS bucket")
        return pd.read_csv(csv_buffer)
    else:
        json_data = json.loads(data)
        logger.info(f"File {blob_name} - successfully retrieved from GCS bucket")
        return json_data


def write_to_bucket(data, blob_name):
    bucket_name = Variable.get("BUCKET_NAME")
    hook = GCSHook(gcp_conn_id="google_cloud")
    hook.upload(bucket_name=bucket_name, object_name=blob_name, data=data, mime_type="text/csv")
    logger.info(f"File {blob_name} - successfully uploaded to GCS bucket")


# 3 tasks to transform raw data


@task
def transform_activity_data():
    # Get json data from GCS
    json_data = read_from_bucket("raw/activity.json")
    data = json_data["data"]
    values, num = data["testsByDays"], data["lastDay"]

    # Convert timestamp from number -> assing for each day the number of tests
    last_date = pd.to_datetime(num, unit="ms", utc=True).normalize()
    dates = pd.date_range(end=last_date, periods=len(values), freq="D")
    df = pd.DataFrame({"date": dates, "tests": values})

    # Format columns
    df["tests"] = df["tests"].fillna(0).astype(int)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Filter table by date
    filter_date = datetime(2025, 1, 10).date()
    df = df[df["date"] > filter_date]

    logger.info(f"Activity table - successfully created, {len(df)} rows")
    write_to_bucket(df.to_csv(index=False), "clean/activity.csv")


@task
def transform_profile_data():
    json_data = read_from_bucket("raw/profile.json")
    data = json_data["data"]

    # Create a table with general stats
    def get_general_stats(data):
        typing_stats = data["typingStats"]
        dct = {
            "current_streak": data["streak"],
            "max_streak": data["maxStreak"],
            "completed_tests": typing_stats["completedTests"],
            "started_tests": typing_stats["startedTests"],
            "time_spent_in_minutes": typing_stats["timeTyping"] // 60,
        }
        df = pd.DataFrame([dct]).astype(int)
        return df

    # Create a table with best results
    def get_best_results(data):
        best_results = data["personalBests"]
        lst = []

        # Loop through nested dct and get all entries
        for mode, categories in best_results.items():
            for category, entries in categories.items():
                for entry in entries:
                    row = {"mode": mode, "category": category, **entry}
                    lst.append(row)

        # Create DataFrame -> formatting + filtering
        df = pd.DataFrame(lst)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.floor("s")
        df = df[df["language"].str.contains(r"english.*", case=False)]
        df.drop(columns=["lazyMode", "difficulty"], inplace=True)

        return df

    # Load tables to GCS
    stats = get_general_stats(data)
    best_resulst = get_best_results(data)

    logger.info(f"General stats table - successfully created, {len(stats)} rows")
    logger.info(f"Best results table - successfully created, {len(best_resulst)} rows")
    write_to_bucket(stats.to_csv(index=False), "clean/stats.csv")
    write_to_bucket(best_resulst.to_csv(index=False), "clean/best_results.csv")


@task
def transform_results_data():
    # Read csv file from GCS
    df = read_from_bucket("raw/results.csv", file_type="csv")

    # Convert timestamp to datetime and filter by language
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.floor("s")
    df = df[df["language"].str.contains(r"english.*", case=False)]

    # Create 4 new columns from charStats
    char_stats = df["charStats"].str.split(";", expand=True)
    df["correctChars"] = char_stats[0].astype(int)
    df["incorrectChars"] = char_stats[1].astype(int)
    df["extraChars"] = char_stats[2].astype(int)
    df["missedChars"] = char_stats[3].astype(int)

    # Function to round values (bankers rounding)
    def rounding_fun(x):
        i, f = divmod(x, 1)
        return int(i + ((f >= 0.5) if (x > 0) else (f > 0.5)))

    df["testDuration"] = df["testDuration"].apply(rounding_fun).astype(int)
    df["incompleteTestSeconds"] = df["incompleteTestSeconds"].apply(rounding_fun).astype(int)

    # Drop unnecessary columns
    cols_to_drop = ["charStats", "quoteLength", "funbox", "difficulty", "lazyMode", "blindMode", "bailedOut", "tags"]
    df.drop(columns=cols_to_drop, inplace=True)

    logger.info(f"General stats table - successfully created, {len(df)} rows")
    write_to_bucket(df.to_csv(index=False), "clean/results.csv")
