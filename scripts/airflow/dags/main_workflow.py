from datetime import timedelta
import os
import sys

from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dag_parsing_context import get_parsing_context
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

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
    dag_id="main_workflow",
    catchup=False,
    tags=["main_workflow"],
    start_date=timezone.parse("1970-1-1 00", 'Europe/Moscow'),
    schedule_interval='*/5 * * * *',
) as dag:
    cdm_order_task = PythonOperator(
        task_id='cdm_order_task',
        dag=dag,
        python_callable=cdm_order,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    cdm_user_task = PythonOperator(
        task_id='cdm_user_task',
        dag=dag,
        python_callable=cdm_user,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_drivers_task = PythonOperator(
        task_id='dds_drivers_task',
        dag=dag,
        python_callable=dds_drivers,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_events_task = PythonOperator(
        task_id='dds_events_task',
        dag=dag,
        python_callable=dds_events,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_users_task = PythonOperator(
        task_id='dds_users_task',
        dag=dag,
        python_callable=dds_users,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_drivers_task = PythonOperator(
        task_id='ods_drivers_task',
        dag=dag,
        python_callable=ods_drivers,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_event_log_task = PythonOperator(
        task_id='ods_event_log_task',
        dag=dag,
        python_callable=ods_event_log,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_users_task = PythonOperator(
        task_id='ods_users_task',
        dag=dag,
        python_callable=ods_users,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_event_log_task >> dds_events_task
    ods_drivers_task >> dds_drivers_task
    ods_users_task >> dds_users_task

    [dds_events_task, dds_drivers_task, dds_users_task] >> cdm_order_task

    [dds_users_task, cdm_order_task] >> cdm_user_task
