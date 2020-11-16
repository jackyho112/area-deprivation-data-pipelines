from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks import S3_hook
from airflow.sensors import S3PrefixSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators import LoadInputToS3Operator, LoadScriptsToS3Operator, ClearS3OutputOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

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

aws_emr_job_flow_overrides = {
    "Name": "US Import ETL",
    "ReleaseLabel": "emr-6.1.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "LogUri": f"s3://{Variable.get('s3_raw_data_bucket')}-logs/",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

spark_steps = [ # Note the params values are supplied to the operator
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.bucket }}/input",
                "--dest=hdfs:///input",
            ],
        },
    },
    {
        "Name": "Process data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.bucket }}/scripts/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=hdfs:///output",
                "--dest=s3://{{ params.bucket }}/output",
            ],
        },
    }
]

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

clear_s3_output_operator = ClearS3OutputOperator(
    task_id='clear_s3_output',
    bucket_name=Variable.get('s3_raw_data_bucket'),
    timeout=600,
    poke_interval=300,
    dag=dag
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=aws_emr_job_flow_overrides,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

add_emr_steps = EmrAddStepsOperator(
    task_id="add_emr_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}", 
    aws_conn_id="aws_default",
    steps=spark_steps,
    params={
        "bucket": Variable.get('s3_raw_data_bucket'),
        "s3_script": 'assemble_contacts.py'
    },
    dag=dag
)

last_emr_step = len(spark_steps) - 1
step_checker = EmrStepSensor(
    task_id="watch_last_emr_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_emr_steps', key='return_value')["
    + str(last_emr_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag
)

start_operator >> s3_bucket_sensor
s3_bucket_sensor >> [
	load_input_to_s3_bucket_operator, 
	load_scripts_to_s3_bucket_operator,
    clear_s3_output_operator,
	create_emr_cluster
]
[
	load_input_to_s3_bucket_operator, 
	load_scripts_to_s3_bucket_operator,
    clear_s3_output_operator,
	create_emr_cluster
] >> add_emr_steps


