from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors import S3PrefixSensor
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'jackyho',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=30),
    'retries': 3,
	'depends_on_past': False,
    'catchup': False,
    'email_on_retry': False,
    'email_on_failure': False
}

dag_name = 'goodreads_pipeline'
dag = DAG(
	'us_import_dag',
	default_args=default_args,
	description='Load, transform, and save US import data',
	schedule_interval='@weekly',
	max_active_runs = 1
)

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

s3_bucket_sensor = S3PrefixSensor(
	task_id='check_s3_bucket_availability',
	bucket_name=Variable.get('s3_raw_data_bucket'),
	prefix=Variable.get('s3_raw_data_folder'),
	aws_conn_id='S3_bucket_us_import_connection',
	timeout=300,
	poke_interval=300,
	dag=dag
)

start_operator >> s3_bucket_sensor