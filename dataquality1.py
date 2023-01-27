from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun
from airflow.models import DagRun
from datetime import timedelta
import os
import json
#Define params for Submit Run Operator

new_cluster = {
  'spark_version': '10.4.x-scala2.12',
  "spark_conf": {
        "spark.databricks.hive.metastore.glueCatalog.enabled":"true"
  },
  'num_workers': 2,
  'node_type_id': 'i3.xlarge',
   "aws_attributes": {
      "instance_profile_arn": "arn:aws:iam::832766144784:instance-profile/ser-exp-databricks-instance-profile"
  },
  "init_scripts": [{ "s3": { "destination" : "s3://expseradatabricks-root-bucket/artifacts/installDeequ.sh", "region" : "us-east-1" } }]
}

notebook_task = {
  'notebook_path': '/Users/proservesdt@gmail.com/dataqualitynotebook',
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('datalake_databricks_runnow_dag',
  start_date=datetime(2022, 10, 6),
  schedule_interval='@daily',
  catchup=False,
  default_args=default_args
  ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_workspace1_dev',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
