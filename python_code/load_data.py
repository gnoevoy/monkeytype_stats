from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def _load_data(task_id, source_object, table):
    # Import env variables
    # Somehow Variable.get() does not work in "airflow tasks test" command
    bucket_name = "{{var.value.BUCKET_NAME}}"
    project_id = "{{var.value.GOOGLE_CLOUD_PROJECT_ID}}"
    dataset = "{{var.value.BIGQUERY_DATASET}}"

    # Return an operator that transfer data from bucket to BigQuery
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
