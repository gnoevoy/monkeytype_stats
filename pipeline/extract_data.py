from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
import requests
import logging
import json


# Set up logging
logger = logging.getLogger(__name__)

# Get env variables
api_key = Variable.get("MONKEYTYPE_API_KEY")
base_url = Variable.get("BASE_URL")
bucket_name = Variable.get("BUCKET_NAME")

# Set up the API header and endpoints
header = {"Authorization": f"ApeKey {api_key}"}
endpoints = {
    "results": f"{base_url}/results",
    "activity": f"{base_url}/users/currentTestActivity",
    "profile": f"{base_url}/users/bars1k/profile",
}


def extract_data():
    # Loop through each endpoint -> make a request -> fetch data -> upload to GCS
    for key, endpoint in endpoints.items():
        response = requests.get(endpoint, headers=header)
        data = response.json()
        json_data = json.dumps(data, indent=4)

        # Construct the blob name
        blob_name = f"raw/{key}.json"

        # Upload to GCS
        hook = GCSHook(gcp_conn_id="google_cloud")
        hook.upload(bucket_name=bucket_name, object_name=blob_name, data=json_data)

        logger.info(f"Data from {endpoint} successfully extracted and uploaded to GCS bucket as {blob_name}")
