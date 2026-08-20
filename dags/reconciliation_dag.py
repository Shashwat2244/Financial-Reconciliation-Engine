from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'financial_reconciliation_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    upload_orders = LocalFilesystemToS3Operator(
        task_id='upload_orders_to_bronze',
        filename='/opt/airflow/project/internal_orders.csv',
        dest_key='bronze/internal_orders.csv',
        dest_bucket='s3-fintech-recon-bronze-ss',
        aws_conn_id='aws_default',
        replace=True,
    )

    upload_settlements = LocalFilesystemToS3Operator(
        task_id='upload_settlements_to_bronze',
        filename='/opt/airflow/project/gateway_settlements.csv',
        dest_key='bronze/gateway_settlements.csv',
        dest_bucket='s3-fintech-recon-bronze-ss',
        aws_conn_id='aws_default',
        replace=True,
    )

    # Trigger PySpark Job
    run_spark_processor = BashOperator(
        task_id='run_spark_processor',
        bash_command='python /opt/airflow/project/spark_processor.py',
    )

    # Run Snowflake Query
    with open('/opt/airflow/project/reconciliation_engine.sql', 'r') as f:
        sql_query = f.read()
        
    run_snowflake_reconciliation = SnowflakeOperator(
        task_id='run_snowflake_reconciliation',
        snowflake_conn_id='snowflake_default',
        sql=sql_query,
    )

    [upload_orders, upload_settlements] >> run_spark_processor >> run_snowflake_reconciliation
