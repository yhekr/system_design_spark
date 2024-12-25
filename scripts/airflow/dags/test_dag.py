from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.dag_parsing_context import get_parsing_context
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from modules.cdm_order import cdm_order
from modules.cdm_user import cdm_user
from modules.dds_drivers import dds_drivers
from modules.dds_events import dds_events
from modules.dds_users import dds_users
from modules.ods_drivers import ods_drivers
from modules.ods_event_log import ods_event_log
from modules.ods_users import ods_users

with DAG(
    dag_id="test_dag",
    start_date=days_ago(30),
    schedule_interval='* * * * *',
    catchup=False,
    tags=["test_dag"],
) as dag:

    ods_drivers_task = PythonOperator(
        task_id='ods_drivers_task',
        python_callable=ods_drivers,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        show_return_value_in_logs=False,
    )

    ods_drivers_task