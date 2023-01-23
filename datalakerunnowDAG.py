from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta

#Define params for Submit Run Operator
new_cluster = {
  'spark_version': '10.4.x-scala2.12',
  'num_workers': 2,
  'node_type_id': 'i3.xlarge',
   "aws_attributes": {
      "instance_profile_arn": "arn:aws:iam::683819638661:instance-profile/databricks-bruno-instance-profile"
  }
}

notebook_task = {
  'notebook_path': '/Users/goneswet@amazon.com/covid-raw',
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
        databricks_conn_id='databricks_default',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    opr_run_now = DatabricksRunNowOperator(
        task_id='run_now',
        databricks_conn_id='databricks_default',
        job_id=72871988420711
    )

    opr_submit_run >> opr_run_now
