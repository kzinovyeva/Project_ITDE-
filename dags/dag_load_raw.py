# dags/dag_load_raw.py
from __future__ import annotations

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import os
import glob
import zipfile
import requests
import pandas as pd
from sqlalchemy import create_engine, text


def _make_engine():
    db = os.getenv("POSTGRES_DB", "de_db")
    user = os.getenv("POSTGRES_USER", "de_user")
    pwd = os.getenv("POSTGRES_PASSWORD", "de_password")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")


def _get_raw_count(engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(*) FROM dwh.raw_data")).scalar() or 0)


def _resolve_extract_dir() -> str:
    # Хотим писать в /data (как у тебя в логах), но если нет прав — падаем в /tmp/data
    desired = os.getenv("DATA_DIR", "/data")
    try:
        os.makedirs(desired, exist_ok=True)
        test_path = os.path.join(desired, ".write_test")
        with open(test_path, "w") as f:
            f.write("ok")
        os.remove(test_path)
        return desired
    except Exception:
        fallback = "/tmp/data"
        os.makedirs(fallback, exist_ok=True)
        return fallback


def _yandex_public_download_href(public_url: str) -> str:
    api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    r = requests.get(api_url, params={"public_key": public_url}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "href" not in j:
        raise SystemExit("Не удалось получить href из ответа Yandex Disk API")
    return j["href"]


def _download_file(url: str, dst_path: str) -> None:
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _extract_only_parquet(zip_path: str, extract_dir: str) -> None:
    if not zipfile.is_zipfile(zip_path):
        raise SystemExit(f"Файл {zip_path} не является zip-архивом")

    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
        parquet_files = [n for n in names if n.endswith(".parquet")]
        if not parquet_files:
            raise SystemExit("В архиве не найдено parquet-файлов")

        for pf in parquet_files:
            z.extract(pf, extract_dir)


def _find_parquet_files(extract_dir: str) -> list[str]:
    pattern = os.path.join(extract_dir, "**", "*.parquet")
    return sorted(glob.glob(pattern, recursive=True))


def load_raw_data_from_parquet(**context):
    """
    1) Проверяем dwh.raw_data:
       - если уже >0 строк, выходим (идемпотентно)
       - если 0 строк, загружаем parquet'и в dwh.raw_data
    2) Parquet берём из распакованной папки; если нет — скачиваем zip с Yandex Disk и распаковываем.
    """
    engine = _make_engine()
    raw_cnt = _get_raw_count(engine)
    if raw_cnt > 0:
        print(f"dwh.raw_data уже заполнена: {raw_cnt} строк. Пропускаем загрузку.")
        return {"loaded": False, "raw_count": raw_cnt}

    public_url = os.getenv("YANDEX_PUBLIC_URL", "https://disk.yandex.ru/d/SrJBRwi_hPOYsw")
    zip_path = os.getenv("TEMP_ZIP_PATH", "/tmp/data.zip")
    extract_dir = _resolve_extract_dir()

    # 1) parquet уже есть?
    files = _find_parquet_files(extract_dir)
    if not files:
        # 2) скачиваем zip, если его нет
        if not os.path.exists(zip_path):
            print(f"Получение прямой ссылки для: {public_url}")
            href = _yandex_public_download_href(public_url)
            print(f"Скачивание архива в: {zip_path}")
            _download_file(href, zip_path)
        else:
            print(f"Zip уже существует: {zip_path}")

        print(f"Распаковка parquet в: {extract_dir}")
        _extract_only_parquet(zip_path, extract_dir)
        files = _find_parquet_files(extract_dir)

    if not files:
        raise SystemExit("После распаковки parquet-файлы не найдены")

    print(f"Найдено parquet файлов: {len(files)} (папка: {extract_dir})")

    # На всякий: если где-то частично загрузилось, чистим raw_data перед полной перезагрузкой
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dwh.raw_data;"))

    total_rows = 0
    k=0
    for i, fp in enumerate(files, start=1):
        df = pd.read_parquet(fp)
        df.columns = [c.strip() for c in df.columns]

        # типизация проблемных полей
        if "item_replaced_id" in df.columns:
            df["item_replaced_id"] = pd.to_numeric(df["item_replaced_id"], errors="coerce").astype("Int64")

        # иногда в parquet могут быть пробелы/странные типы — лучше привести
        int_cols = ["order_id", "user_id", "driver_id", "store_id", "item_id", "item_quantity", "item_canceled_quantity"]
        for c in int_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        total_rows += len(df)
        print(f"[{i}/{len(files)}] загрузка {os.path.basename(fp)}: {len(df)} строк")

        df.to_sql(
            "raw_data",
            engine,
            schema="dwh",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        k+=1
        if k==1:
            break

    final_cnt = _get_raw_count(engine)
    print(f"Готово. dwh.raw_data строк: {final_cnt}")

    ti = context["ti"]
    ti.xcom_push(key="loaded_files", value=len(files))
    ti.xcom_push(key="loaded_rows", value=int(total_rows))
    return {"loaded": True, "loaded_files": len(files), "loaded_rows": int(total_rows), "raw_count": final_cnt}


default_args = {
    "owner": "team_9",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_data",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["team_9"],
    description="Загрузка сырья из parquet в dwh.raw_data и нормализация в DWH таблицы",
) as dag:

    start = EmptyOperator(task_id="start")

    load_raw = PythonOperator(
        task_id="load_raw_data_from_parquet",
        python_callable=load_raw_data_from_parquet,
    )

    create_users = PostgresOperator(
        task_id="create_users",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.users(user_id, user_phone)
            SELECT DISTINCT user_id, user_phone
            FROM dwh.raw_data
            WHERE user_id IS NOT NULL
            ON CONFLICT (user_id) DO NOTHING;
        """,
    )

    create_drivers = PostgresOperator(
        task_id="create_drivers",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.drivers(driver_id, driver_phone)
            SELECT DISTINCT driver_id, driver_phone
            FROM dwh.raw_data
            WHERE driver_id IS NOT NULL
            ON CONFLICT (driver_id) DO NOTHING;
        """,
    )

    create_stores = PostgresOperator(
        task_id="create_stores",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.stores(store_id, store_address)
            SELECT DISTINCT store_id, store_address
            FROM dwh.raw_data
            WHERE store_id IS NOT NULL
            ON CONFLICT (store_id) DO NOTHING;
        """,
    )

    create_items = PostgresOperator(
        task_id="create_items",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.items(item_id, item_title, item_category)
            SELECT DISTINCT item_id, item_title, item_category
            FROM dwh.raw_data
            WHERE item_id IS NOT NULL
            ON CONFLICT (item_id) DO NOTHING;
        """,
    )

    create_orders = PostgresOperator(
        task_id="create_orders",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.orders(
                order_id, user_id, store_id, driver_id, address_text,
                created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
                payment_type, delivery_cost, order_discount, order_cancellation_reason
            )
            SELECT DISTINCT ON (order_id)
                order_id, user_id, store_id, driver_id, address_text,
                created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
                payment_type, delivery_cost, order_discount, order_cancellation_reason
            FROM dwh.raw_data
            WHERE order_id IS NOT NULL
            ORDER BY order_id, created_at NULLS LAST
            ON CONFLICT (order_id) DO NOTHING;
        """,
    )

    create_order_items = PostgresOperator(
        task_id="create_order_items",
        postgres_conn_id="de_postgres",
        sql="""
            INSERT INTO dwh.order_items(
                order_id, item_id, item_quantity, item_price, item_discount,
                item_canceled_quantity, item_replaced_id
            )
            SELECT
                order_id,
                item_id,
                COALESCE(SUM(item_quantity), 0)::int AS item_quantity,
                MAX(item_price) AS item_price,
                MAX(item_discount) AS item_discount,
                COALESCE(SUM(item_canceled_quantity), 0)::int AS item_canceled_quantity,
                MAX(item_replaced_id) AS item_replaced_id
            FROM dwh.raw_data
            WHERE order_id IS NOT NULL AND item_id IS NOT NULL
            GROUP BY order_id, item_id
            ON CONFLICT (order_id, item_id) DO NOTHING;
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> load_raw
    load_raw >> [create_users, create_drivers, create_stores, create_items]
    [create_users, create_drivers, create_stores] >> create_orders
    create_items >> create_orders
    create_orders >> create_order_items >> end
