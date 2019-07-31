import airflow
from pod_plugin import simplePodOperator
from airflow.models import DAG
from datetime import timedelta

args = {
    'owner': 'Data',
    'retries': 3,
    'retry_delay':timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(1)
    'email_on_failure': True,
    'email': 'minal@boxed.com'
}


dag = DAG(dag_id='braintree_job',
schedule_interval='45 9 * * *',
catchup=False,
default_args=args)

resources_settings={
    'prod': {
        'request_cpu': '500m',
        'request_memory': '500Mi',
        'limit_cpu': '1000m',
        'limit_memory': '1000Mi',
   }
    ,'staging': None
}

from airflow.models import Variable
app_env = Variable.get("APP_ENV")
app_resources=resources_settings.get(app_env)
from airflow.contrib.kubernetes.pod import Resources
resources = Resources(**app_resources) if app_resources else None

t1 = simplePodOperator(
    task_id='braintree-to-redshift',
    image_name='external-data-importer',
    secret_name='external-data-importer',
    cmds=["python","/srv/app/src/braintree/braintree_report.py"],
    resources=resources,
    dag=dag)

t1