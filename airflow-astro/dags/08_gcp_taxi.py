import os
from datetime import timedelta

from airflow.sdk import dag, task, Param, Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


@dag(
    dag_id="08_gcp_taxi",
    tags=["zoomcamp"],
    params={
        "taxi": Param(default="green", type="string", enum=["yellow", "green"], title="Select taxi type",),
        "year": Param(default="2019", type="string", enum=["2019", "2020"], title="Select year",),
        "month": Param(
            default="01", 
            type="string", 
            enum=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
            title="Select month",
        )
    }
)
def implement_gcp_taxi():
    @task
    def build_vars(**ctx):
        p = ctx["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]

        gcp_bucket_name = Variable.get("GCP_BUCKET_NAME")
        gcp_dataset = Variable.get("GCP_DATASET")

        file = f"{taxi}_tripdata_{year}-{month}.csv"
        gcs_file = f"gs://{gcp_bucket_name}/{file}"
        table = f"{gcp_dataset}.{taxi}_tripdata_{year}_{month}"

        return {
            "taxi": taxi,
            "file": file,
            "gcs_file": gcs_file,
            "table": table,
        }

    @task.bash(cwd="/tmp/airflow_data")
    def extract(vars):
        os.makedirs("/tmp/airflow_data", exist_ok=True)
        url = url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{vars['taxi']}/{vars['file']}.gz"
        return f"curl -sL {url} | gunzip > {vars['file']} && echo /tmp/airflow_data/{vars['file']}"

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ ti.xcom_pull(task_ids='extract') }}",
        dst="{{ ti.xcom_pull(task_ids='build_vars')['file'] }}",
        bucket=Variable.get("GCP_BUCKET_NAME"),
        gcp_conn_id="google_cloud_default",
        chunk_size=1024 * 1024,
        retries=5,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
    )

    @task.branch
    def choose_bq_task(vars):
        return "bq_yellow_tripdata" if vars["taxi"] == "yellow" else "bq_green_tripdata"

    GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
    GCP_DATASET = Variable.get("GCP_DATASET")

    bq_yellow_tripdata = BigQueryInsertJobOperator(
        task_id="bq_yellow_tripdata",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{GCP_DATASET}.yellow_tripdata`
                    (
                        unique_row_id BYTES,
                        filename STRING,
                        VendorID STRING,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count INTEGER,
                        trip_distance NUMERIC,
                        RatecodeID STRING,
                        store_and_fwd_flag STRING,
                        PULocationID STRING,
                        DOLocationID STRING,
                        payment_type INTEGER,
                        fare_amount NUMERIC,
                        extra NUMERIC,
                        mta_tax NUMERIC,
                        tip_amount NUMERIC,
                        tolls_amount NUMERIC,
                        improvement_surcharge NUMERIC,
                        total_amount NUMERIC,
                        congestion_surcharge NUMERIC
                    )
                PARTITION BY DATE(tpep_pickup_datetime);
                """,
                "useLegacySql": False,
            },
        },
    )

    bq_yellow_table_ext = BigQueryInsertJobOperator(
        task_id="bq_yellow_table_ext",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE EXTERNAL TABLE `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}_ext`\n"
                    "(\n"
                    "    VendorID STRING,\n"
                    "    tpep_pickup_datetime TIMESTAMP,\n"
                    "    tpep_dropoff_datetime TIMESTAMP,\n"
                    "    passenger_count INTEGER,\n"
                    "    trip_distance NUMERIC,\n"
                    "    RatecodeID STRING,\n"
                    "    store_and_fwd_flag STRING,\n"
                    "    PULocationID STRING,\n"
                    "    DOLocationID STRING,\n"
                    "    payment_type INTEGER,\n"
                    "    fare_amount NUMERIC,\n"
                    "    extra NUMERIC,\n"
                    "    mta_tax NUMERIC,\n"
                    "    tip_amount NUMERIC,\n"
                    "    tolls_amount NUMERIC,\n"
                    "    improvement_surcharge NUMERIC,\n"
                    "    total_amount NUMERIC,\n"
                    "    congestion_surcharge NUMERIC\n"
                    ")\n"
                    "OPTIONS (\n"
                    "    format = 'CSV',\n"
                    "    uris = ['{{ ti.xcom_pull(task_ids='build_vars')['gcs_file'] }}'],\n"
                    "    skip_leading_rows = 1,\n"
                    "    ignore_unknown_values = TRUE\n"
                    ");"
                ),
                "useLegacySql": False,
            },
        },
    )

    bq_yellow_table_tmp = BigQueryInsertJobOperator(
        task_id="bq_yellow_table_tmp",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE TABLE `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}`\n"
                    "AS\n"
                    "SELECT\n"
                    "  MD5(CONCAT(\n"
                    "    COALESCE(CAST(VendorID AS STRING), \"\"),\n"
                    "    COALESCE(CAST(tpep_pickup_datetime AS STRING), \"\"),\n"
                    "    COALESCE(CAST(tpep_dropoff_datetime AS STRING), \"\"),\n"
                    "    COALESCE(CAST(PULocationID AS STRING), \"\"),\n"
                    "    COALESCE(CAST(DOLocationID AS STRING), \"\")\n"
                    "  )) AS unique_row_id,\n"
                    "  '{{ ti.xcom_pull(task_ids='build_vars')['file'] }}' AS filename,\n"
                    "  *\n"
                    "FROM `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}_ext`;"
                ),
                "useLegacySql": False,

            },
        },
    )

    bq_yellow_merge = BigQueryInsertJobOperator(
        task_id="bq_yellow_merge",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "MERGE INTO `"
                    + GCP_PROJECT_ID
                    + f".{GCP_DATASET}.yellow_tripdata` T\n"
                    "USING `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}` S\n"
                    "ON T.unique_row_id = S.unique_row_id\n"
                    "WHEN NOT MATCHED THEN\n"
                    "  INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, "
                    "passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, "
                    "payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, "
                    "total_amount, congestion_surcharge)\n"
                    "  VALUES (S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, "
                    "S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID, S.DOLocationID, "
                    "S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.improvement_surcharge, "
                    "S.total_amount, S.congestion_surcharge);"
                ),
                "useLegacySql": False,
            }
        },
    )

    bq_green_tripdata = BigQueryInsertJobOperator(
        task_id="bq_green_tripdata",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{GCP_DATASET}.green_tripdata`
                    (
                        unique_row_id BYTES,
                        filename STRING,
                        VendorID STRING,
                        lpep_pickup_datetime TIMESTAMP,
                        lpep_dropoff_datetime TIMESTAMP,
                        store_and_fwd_flag STRING,
                        RatecodeID STRING,
                        PULocationID STRING,
                        DOLocationID STRING,
                        passenger_count INT64,
                        trip_distance NUMERIC,
                        fare_amount NUMERIC,
                        extra NUMERIC,
                        mta_tax NUMERIC,
                        tip_amount NUMERIC,
                        tolls_amount NUMERIC,
                        ehail_fee NUMERIC,
                        improvement_surcharge NUMERIC,
                        total_amount NUMERIC,
                        payment_type INTEGER,
                        trip_type STRING,
                        congestion_surcharge NUMERIC
                    )
                    PARTITION BY DATE(lpep_pickup_datetime);
                """,
                "useLegacySql": False,
            }
        },
    )

    bq_green_table_ext = BigQueryInsertJobOperator(
        task_id="bq_green_table_ext",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE EXTERNAL TABLE `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}_ext`\n"
                    "(\n"
                    "    VendorID STRING,\n"
                    "    lpep_pickup_datetime TIMESTAMP,\n"
                    "    lpep_dropoff_datetime TIMESTAMP,\n"
                    "    store_and_fwd_flag STRING,\n"
                    "    RatecodeID STRING,\n"
                    "    PULocationID STRING,\n"
                    "    DOLocationID STRING,\n"
                    "    passenger_count INT64,\n"
                    "    trip_distance NUMERIC,\n"
                    "    fare_amount NUMERIC,\n"
                    "    extra NUMERIC,\n"
                    "    mta_tax NUMERIC,\n"
                    "    tip_amount NUMERIC,\n"
                    "    tolls_amount NUMERIC,\n"
                    "    ehail_fee NUMERIC,\n"
                    "    improvement_surcharge NUMERIC,\n"
                    "    total_amount NUMERIC,\n"
                    "    payment_type INTEGER,\n"
                    "    trip_type STRING,\n"
                    "    congestion_surcharge NUMERIC\n"
                    ")\n"
                    "OPTIONS (\n"
                    "    format = 'CSV',\n"
                    "    uris = ['{{ ti.xcom_pull(task_ids='build_vars')['gcs_file'] }}'],\n"
                    "    skip_leading_rows = 1,\n"
                    "    ignore_unknown_values = TRUE\n"
                    ");"
                ),
                "useLegacySql": False,
            }
        },
    )

    bq_green_table_tmp = BigQueryInsertJobOperator(
        task_id="bq_green_table_tmp",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE TABLE `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}`\n"
                    "AS\n"
                    "SELECT\n"
                    "  MD5(CONCAT(\n"
                    "    COALESCE(CAST(VendorID AS STRING), \"\"),\n"
                    "    COALESCE(CAST(lpep_pickup_datetime AS STRING), \"\"),\n"
                    "    COALESCE(CAST(lpep_dropoff_datetime AS STRING), \"\"),\n"
                    "    COALESCE(CAST(PULocationID AS STRING), \"\"),\n"
                    "    COALESCE(CAST(DOLocationID AS STRING), \"\")\n"
                    "  )) AS unique_row_id,\n"
                    "  '{{ ti.xcom_pull(task_ids='build_vars')['file'] }}' AS filename,\n"
                    "  *\n"
                    "FROM `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}_ext`;"
                ),
                "useLegacySql": False,
            }
        },
    )

    bq_green_merge = BigQueryInsertJobOperator(
        task_id="bq_green_merge",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": (
                    "MERGE INTO `"
                    + GCP_PROJECT_ID
                    + f".{GCP_DATASET}.green_tripdata` T\n"
                    "USING `"
                    + GCP_PROJECT_ID
                    + ".{{ ti.xcom_pull(task_ids='build_vars')['table'] }}` S\n"
                    "ON T.unique_row_id = S.unique_row_id\n"
                    "WHEN NOT MATCHED THEN\n"
                    "  INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, "
                    "store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, "
                    "fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, "
                    "total_amount, payment_type, trip_type, congestion_surcharge)\n"
                    "  VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, "
                    "S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count, S.trip_distance, "
                    "S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee, S.improvement_surcharge, "
                    "S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge);"
                ),
                "useLegacySql": False,
            }
        },
    )

    vars = build_vars()
    filepath = extract(vars)

    filepath >> upload_to_gcs

    branch = choose_bq_task(vars)
    upload_to_gcs >> branch
    branch >> [bq_yellow_tripdata, bq_green_tripdata]

    bq_yellow_tripdata >> bq_yellow_table_ext >> bq_yellow_table_tmp >> bq_yellow_merge
    bq_green_tripdata >> bq_green_table_ext >> bq_green_table_tmp >> bq_green_merge


implement_gcp_taxi()
