# dags/Metrics_upd.py
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "team_9",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="metrics_upd",
    default_args=default_args,
    description="Создание витрин dwh.product_performance_data_mart и dwh.order_performance_data_mart",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["team_9"],
) as dag:

    start = EmptyOperator(task_id="start")

    assert_sources_not_empty = PostgresOperator(
        task_id="assert_sources_not_empty",
        postgres_conn_id="de_postgres",
        sql="""
        DO $$
        BEGIN
          IF (SELECT COUNT(*) FROM dwh.orders) = 0 THEN
            RAISE EXCEPTION 'dwh.orders is empty';
          END IF;

          IF (SELECT COUNT(*) FROM dwh.order_items) = 0 THEN
            RAISE EXCEPTION 'dwh.order_items is empty';
          END IF;
        END $$;
        """,
    )

    delete_for_day = PostgresOperator(
        task_id="delete_for_day",
        postgres_conn_id="de_postgres",
        sql="""
          DELETE FROM dwh.product_performance_data_mart WHERE load_date = DATE '{{ ds }}';
          DELETE FROM dwh.order_performance_data_mart   WHERE load_date = DATE '{{ ds }}';
        """,
    )

    # PRODUCT mart (default mode)
    create_product_mart = SparkSubmitOperator(
        task_id="create_product_mart",
        application="/opt/airflow/dags/pyspark_scripts.py",
        application_args=["{{ ds }}"],
        name="product-performance-datamart",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
        packages="org.postgresql:postgresql:42.7.4",
    )

    # ORDER mart (order mode)
    create_order_mart = SparkSubmitOperator(
        task_id="create_order_mart",
        application="/opt/airflow/dags/pyspark_scripts.py",
        application_args=["{{ ds }}", "order"],
        name="order-performance-datamart",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
        packages="org.postgresql:postgresql:42.7.4",
    )

    end = EmptyOperator(task_id="end")

    start >> assert_sources_not_empty >> delete_for_day >> create_product_mart >> create_order_mart >> end
