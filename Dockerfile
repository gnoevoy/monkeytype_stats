FROM apache/airflow:3.0.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Create separate venv for dbt 
RUN python -m venv /opt/airflow/dbt_venv && \
    source /opt/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate



