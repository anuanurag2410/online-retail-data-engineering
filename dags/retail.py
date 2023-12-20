from airflow.decorators import dag,task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

#using Astro SDK to Load the CSV file to BQ Table 
from astro import sql as aql 
from astro.files import File 
from astro.constants import FileType 


@dag(
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)

def retail():
    upload_csv_to_gcs=LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='online_retail_anurag',
        gcp_conn_id='GCP',
        mime_type='text/csv'
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='GCP'
    )

    gcs_to_raw=aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://online_retail_anurag/raw/online_retail.csv',
            conn_id='GCP',
            filetype=FileType.CSV
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='GCP'
        ))

retail()