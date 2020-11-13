from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks import S3_hook
from airflow.sensors import S3PrefixSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import LoadInputToS3Operator, LoadScriptsToS3Operator

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

load_input_to_s3_bucket_operator = LoadInputToS3Operator(
	task_id='load_input_to_s3_bucket',
	dataset_id=Variable.get('data_exchange_dataset_id'),
	bucket_name=Variable.get('s3_raw_data_bucket'),
	region_name=Variable.get('data_exchange_dataset_regionn'),
	timeout=600,
	poke_interval=300,
	dag=dag
)

load_scripts_to_s3_bucket_operator = LoadScriptsToS3Operator(
	task_id='load_scripts_to_s3_bucket',
	bucket_name=Variable.get('s3_raw_data_bucket'),
	timeout=600,
	poke_interval=300,
	dag=dag
)

start_operator >> s3_bucket_sensor
s3_bucket_sensor >> [load_input_to_s3_bucket_operator, load_scripts_to_s3_bucket_operator]
