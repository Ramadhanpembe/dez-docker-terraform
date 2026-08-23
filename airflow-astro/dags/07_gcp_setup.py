from airflow.sdk import Variable, dag

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

@dag(
    dag_id="07_gcp_setup",
    tags=["zoomcamp"],
)
def gcp_setup():
    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=Variable.get("GCP_BUCKET_NAME"),
        storage_class="REGIONAL",
        location=Variable.get("GCP_LOCATION"),
        project_id=Variable.get("GCP_PROJECT_ID"),
        gcp_conn_id="google_cloud_default",
    )

    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=Variable.get("GCP_DATASET"),
        location=Variable.get("GCP_LOCATION"),
        project_id=Variable.get("GCP_PROJECT_ID"),
        gcp_conn_id="google_cloud_default",
    )

    create_gcs_bucket >> create_bq_dataset

gcp_setup()
