from airflow import DAG, settings, secrets
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.bash_operator import BashOperator

from datetime import timedelta
import os
import json
import boto3
from airflow.models.connection import Connection


dag_root = os.path.dirname(__file__)
dags_folder = os.path.join(dag_root, "airflow-connection-configs")
dags = os.listdir(dags_folder)


def add_connection(**kwargs):
    print(kwargs)
    sm_secretId_connection_details = kwargs['secretsmanagerconndetails']
    connid = kwargs['connectionid']
    conntype = kwargs['connectiontype']

    # Retreive connection secrets from AWS Secrets Manager
    hook = AwsBaseHook(client_type='secretsmanager')
    client = hook.get_client_type('secretsmanager')
    connectiondetails = client.get_secret_value(SecretId=sm_secretId_connection_details)
    print(connectiondetails)

    # Prepare the Connection function arguments such as host=<>, login=<>, extra=<>
    connectiondict = json.loads(connectiondetails['SecretString'])
    connection_checks = ["host","login","password","extra","port","schema"]
    connparams = {}

    for check in connection_checks:
        if check in connectiondict:
            print(connectiondict[check])
            connparams[check] = connectiondict[check]

    print(connparams)
    connparams['conn_id'] = connid
    connparams['conn_type'] = conntype
    print(connparams)

    # Retrieve the Amazon MWAA Connection URI
    myconn = Connection(**connparams)
    connectionurl = myconn.get_uri()
    print(connectionurl)

    # Add Amazon MWAA connection using airflow CLI command
    bash_operator = BashOperator(
            task_id='add_connection',
            bash_command="airflow connections add '%s' --conn-uri '%s'" % (connid, connectionurl)
            )
    bash_operator.execute(context=kwargs)

def create_dag(dagid,schedule,item,default_args):
    with DAG(
            dagid,
            default_args=default_args,
            dagrun_timeout=timedelta(hours=2),
            start_date=days_ago(1),
            schedule_interval=schedule
    ) as dag:

      databricks_connection = PythonOperator(
            task_id=f"add_{dagid}",
            python_callable=add_connection,
            op_kwargs=item,
            provide_context=True
       )
    return dag

for dag in dags:
    dag_path = os.path.join(dags_folder, dag)
    dag_name, dag_extension = os.path.splitext(dag)
    dag_id = dag_name

    with open(dag_path) as dag_file:
        dag_config = json.load(dag_file)

    print(dag_config)

    for item in dag_config['config']:
        if item['establishconnection']:
            dagid = 'dag_{}_connection'.format(item['connectionid'])
            default_args = item['default_args']
            default_args['start_date'] = days_ago(1)
            schedule = item['schedule']

            globals()[dagid] = create_dag(dagid,schedule,item,default_args)
