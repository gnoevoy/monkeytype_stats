from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import Variable


def load_data_to_bigquery(task_id, source_object, table):
    bucket_name = Variable.get("BUCKET_NAME")
    project_id = Variable.get("GOOGLE_CLOUD_PROJECT_ID")
    dataset = Variable.get("BIGQUERY_DATASET")

    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=bucket_name,
        source_objects=[source_object],
        destination_project_dataset_table=f"{project_id}.{dataset}.{table}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="google_cloud",
    )
