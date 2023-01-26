
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

dag_root = os.path.dirname(__file__)
dags_folder = os.path.join(dag_root, "airflow-databricks-exec-config")
dags = os.listdir(dags_folder)

new_cluster = {
  'spark_version': '10.4.x-scala2.12',
  "spark_conf": {
        "spark.databricks.hive.metastore.glueCatalog.enabled":"true"
  },
  'num_workers': 2,
  'node_type_id': 'i3.xlarge',
   "aws_attributes": {
      "instance_profile_arn": "arn:aws:iam::294381644777:instance-profile/ser-poc-databricks-instance-profile"
  },
  "init_scripts": [{ "s3": { "destination" : "s3://ser-poc-databricks-root-bucket/artifacts/installDeequ.sh", "region" : "us-east-1" } }]
}

notebook_task = {
  'notebook_path': '/Users/goneswet@amazon.com/covid-raw',
}

notebook_task1 = {
  'notebook_path': '/Users/goneswet@amazon.com/covid-bronze',
}

#Define params for Run Now Operator
notebook_params = {
  "Variable":5
}

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=2)
}

#Parameter
sm_secretId_connection_det = "{{ dag_run.conf['smdatabricksconndetails'] }}"
print(sm_secretId_connection_det)

hook = AwsBaseHook(client_type='secretsmanager')
secretclient = hook.get_client_type('secretsmanager')

@provide_session
def _get_execution_date_of_dag_addconnection(exec_date, session=None,  **kwargs):
    databricks_connection_details = secretclient.get_secret_value(SecretId=sm_secretId_connection_det)
    print(databricks_connection_details)
    databricksconndagid = databricks_connection_details['dagid']
    dag_runs = DagRun.find(dag_id=databricksconndagid)
    print(dag_runs)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0].execution_date if dag_runs else None

def submit_run_raw_notebook(**kwargs):
    print(kwargs)
    databricks_connection_details = secretclient.get_secret_value(SecretId=sm_secretId_connection_det)
    print(databricks_connection_details)

    databricksconnid = databricks_connection_details['connectionid']
    print(databricksconnid)
    submitrunoperator = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id=databricksconnid,
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    submitrunoperator.execute(context=kwargs)

def create_dag(item, schedule, default_args):
    with DAG(dag_id=item["dag_id"],
      start_date=datetime(2022, 11, 4),
      schedule_interval= item['schedule'],
      catchup=False,
      default_args=item['default_args'],
      params = {

      }
      ) as dag:

      wait_for_add_databricks_connection = ExternalTaskSensor(
        task_id='wait_for_add_databricks_connection',
        external_dag_id=item['external_dag_id'],
        allowed_states=['success'],
        execution_date_fn=_get_execution_date_of_dag_addconnection,
        poke_interval=30
      )
      submit_run_raw = PythonOperator(
           task_id="submit_run_raw",
           python_callable=submit_run_raw_notebook,
           provide_context=True
      )

      wait_for_add_databricks_connection >> submit_run_raw
    return dag

for dag in dags:
    dag_path = os.path.join(dags_folder, dag)
    dag_name, dag_extension = os.path.splitext(dag)
    dag_id = dag_name

    with open(dag_path) as dag_file:
        dag_config = json.load(dag_file)

    print(dag_config)

    for item in dag_config['config']:
        if item['runexec']:
            default_args = item['default_args']
            default_args['start_date'] = days_ago(1)
            schedule = item['schedule']

            globals()[dagid] = create_dag(item, schedule, default_args)
