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
from modules.dds_devices import dds_devices
from modules.ods_drivers import ods_drivers
from modules.ods_event_log import ods_event_log
from modules.ods_users import ods_users
from modules.ods_currencies import ods_currencies

# from modules.generate_jsons.devices_dump import devices_dump
from modules.generate_jsons.users_dump import users_dump
from modules.generate_jsons.drivers_dump import drivers_dump
from modules.generate_jsons.event_logs import event_logs

with DAG(
    dag_id="main_workflow",
    catchup=False,
    tags=["main_workflow"],
    start_date=timezone.parse("1970-1-1 00", 'Europe/Moscow'),
    schedule_interval='@daily',
) as dag:
    cdm_order_task = PythonOperator(
        task_id='cdm_order_task',
        dag=dag,
        python_callable=cdm_order,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    cdm_user_task = PythonOperator(
        task_id='cdm_user_task',
        dag=dag,
        python_callable=cdm_user,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_drivers_task = PythonOperator(
        task_id='dds_drivers_task',
        dag=dag,
        python_callable=dds_drivers,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_events_task = PythonOperator(
        task_id='dds_events_task',
        dag=dag,
        python_callable=dds_events,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_users_task = PythonOperator(
        task_id='dds_users_task',
        dag=dag,
        python_callable=dds_users,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    dds_devices_task = PythonOperator(
        task_id='dds_devices_task',
        dag=dag,
        python_callable=dds_devices,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_drivers_task = PythonOperator(
        task_id='ods_drivers_task',
        dag=dag,
        python_callable=ods_drivers,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_event_log_task = PythonOperator(
        task_id='ods_event_log_task',
        dag=dag,
        python_callable=ods_event_log,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_users_task = PythonOperator(
        task_id='ods_users_task',
        dag=dag,
        python_callable=ods_users,
        op_kwargs={'execution_date': "{{ ds }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    ods_currencies_task = PythonOperator(
        task_id='ods_currencies_task',
        dag=dag,
        python_callable=ods_currencies,
        op_kwargs={'execution_dttm': "{{ ts }}"},
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # devices_dump_task = PythonOperator(
    #     task_id='devices_dump_task',
    #     dag=dag,
    #     python_callable=devices_dump,
    #     op_kwargs={'execution_date': "{{ ts }}"},
    #     provide_context=True,
    #     retries=3,
    #     retry_delay=timedelta(minutes=2),
    # )

    # drivers_dump_task = PythonOperator(
    #     task_id='drivers_dump_task',
    #     dag=dag,
    #     python_callable=drivers_dump,
    #     op_kwargs={'execution_date': "{{ ts }}"},
    #     provide_context=True,
    #     retries=3,
    #     retry_delay=timedelta(minutes=2),
    # )

    # users_dump_task = PythonOperator(
    #     task_id='users_dump_task',
    #     dag=dag,
    #     python_callable=users_dump,
    #     op_kwargs={'execution_date': "{{ ts }}"},
    #     provide_context=True,
    #     retries=3,
    #     retry_delay=timedelta(minutes=2),
    # )

    # generate_event_logs_task = PythonOperator(
    #     task_id='generate_event_logs_task',
    #     dag=dag,
    #     python_callable=event_logs,
    #     op_kwargs={'execution_date': "{{ ts }}"},
    #     provide_context=True,
    #     retries=3,
    #     retry_delay=timedelta(minutes=2),
    # )

    ods_event_log_task >> dds_events_task
    ods_drivers_task >> dds_drivers_task
    ods_users_task >> dds_users_task

    [dds_events_task, dds_drivers_task] >> cdm_order_task

    [dds_users_task, cdm_order_task] >> cdm_user_task
