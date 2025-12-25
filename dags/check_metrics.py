
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
import glob
from pyspark.sql.functions import col, count, when, isnull
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import logging


# Настройки DAG
default_args = {
    'owner': 'kzinovyeva',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=10),
}

dag = DAG(
    'check_metrics',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['team_9']
)

SCHEMA_NAME = 'dwh'
POSTGRES_CONN_ID = 'de_postgres'


start_task = DummyOperator(task_id='start', dag=dag)


def check_metrics_with_print(**context):
    import logging
    logger = logging.getLogger(__name__)

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql_query = """
    SELECT
        COUNT(*) as total_orders,
        SUM(CASE WHEN canceled_at IS NOT NULL THEN 1 ELSE 0 END) as canceled_orders,
        AVG(delivery_cost) as avg_delivery_cost
    FROM dwh.orders
    """
    # sql_query = """
    #    SELECT
    #        COUNT(*) as total
    #
    #    FROM dwh.products
    #    """

    logger.info("Выполняем SQL запрос...")
    logger.info(f"SQL: {sql_query}")

    # Выполняем запрос
    result = postgres_hook.get_first(sql_query)
    print('!!!!!!!!!!!!!!!!!!!!!', result)

    if result:
        total_orders, canceled_orders, avg_delivery_cost = result

        logger.info("=" * 50)
        logger.info("РЕЗУЛЬТАТЫ МЕТРИК:")
        logger.info(f"Всего заказов: {total_orders}")
        logger.info(f"Отмененных заказов: {canceled_orders}")
        logger.info(f"Средняя стоимость доставки: {avg_delivery_cost:.2f}")
        cancel_rate = (canceled_orders / total_orders * 100) if total_orders > 0 else 0
        logger.info(f"Процент отмен: {cancel_rate:.2f}%")

        return {
            'total_orders': total_orders,
            'canceled_orders': canceled_orders,
            'avg_delivery_cost': avg_delivery_cost,
            'cancel_rate': cancel_rate
        }
    else:
        logger.error("Не удалось получить результаты из БД")
        return None


check_metrics_task = PythonOperator(
    task_id='check_metrics',
    python_callable=check_metrics_with_print,
    provide_context=True,
    dag=dag,
)

start_task >> check_metrics_task