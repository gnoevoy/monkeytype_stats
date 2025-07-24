from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
import requests
import logging
import json


logger = logging.getLogger(__name__)


def extract_data():
    # Get env variables
    api_key = Variable.get("MONKEYTYPE_API_KEY")
    base_url = Variable.get("BASE_URL")
    bucket_name = Variable.get("BUCKET_NAME")

    # Set up the API header and endpoints
    header = {"Authorization": f"ApeKey {api_key}"}
    endpoints = {"activity": f"{base_url}/users/currentTestActivity", "profile": f"{base_url}/users/bars1k/profile"}

    # Loop through each endpoint -> make a request -> fetch data -> upload to GCS
    for key, endpoint in endpoints.items():
        response = requests.get(endpoint, headers=header)
        data = response.json()
        json_data = json.dumps(data, indent=4)

        # Upload to GCS
        blob_name = f"raw/{key}.json"
        hook = GCSHook(gcp_conn_id="google_cloud")
        hook.upload(bucket_name=bucket_name, object_name=blob_name, data=json_data, mime_type="application/json")

        logger.info(f"Data from {endpoint} successfully extracted and uploaded to GCS bucket as {blob_name}")
