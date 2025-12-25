# dags/pyspark_scripts.py
from __future__ import annotations

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.window import Window


# -------------------------
# common
# -------------------------
PG_URL = "jdbc:postgresql://postgres:5432/de_db"
PG_PROPS = {"user": "de_user", "password": "de_password", "driver": "org.postgresql.Driver"}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    # Если запускаешь через SparkSubmitOperator с packages=..., драйвер уже подтянется.
    # Но оставляем и здесь на всякий случай.
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )


def read_pg(spark: SparkSession, table: str):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def write_pg_append(df, table: str, batchsize: int = 10000):
    (
        df.write
        .mode("append")
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .option("batchsize", batchsize)
        .save()
    )


# -------------------------
# PRODUCT MART
# запуск: pyspark_scripts.py YYYY-MM-DD
# -------------------------
def read_product_sources(spark: SparkSession):
    logger = logging.getLogger(__name__)
    logger.info("Чтение таблиц из PostgreSQL для product mart...")

    users_df = read_pg(spark, "dwh.users")
    drivers_df = read_pg(spark, "dwh.drivers")
    stores_df = read_pg(spark, "dwh.stores")
    items_df = read_pg(spark, "dwh.items")
    orders_df = read_pg(spark, "dwh.orders")
    order_items_df = read_pg(spark, "dwh.order_items")

    logger.info(f"orders: {orders_df.count()} rows")
    logger.info(f"order_items: {order_items_df.count()} rows")

    return users_df, drivers_df, stores_df, items_df, orders_df, order_items_df


def build_product_mart(users_df, drivers_df, stores_df, items_df, orders_df, order_items_df, execution_date: str):
    """
    Формирует витрину dwh.product_performance_data_mart строго по DDL (без поля id).
    execution_date приходит как YYYY-MM-DD.
    """
    logger = logging.getLogger(__name__)
    exec_date = F.to_date(F.lit(execution_date))

    logger.info(f"Обработка product mart за дату: {execution_date}")

    orders_prepared = (
        orders_df
        .withColumn("order_date", F.to_date(F.col("created_at")))
        .filter(F.col("order_date") == exec_date)
        .join(stores_df.select("store_id", "store_address"), "store_id", "left")
        .withColumn("year", F.year("created_at").cast("int"))
        .withColumn("month", F.month("created_at").cast("int"))
        .withColumn("day", F.dayofmonth("created_at").cast("int"))
        .withColumn("week", F.weekofyear("created_at").cast("int"))
        .withColumn(
            "city",
            F.when(
                F.col("store_address").isNotNull(),
                F.trim(F.split(F.col("store_address"), ",").getItem(1)),
            ).otherwise(F.lit("Unknown"))
        )
        .select(
            "order_id", "user_id", "store_id", "store_address",
            "year", "month", "day", "week", "city", "order_date"
        )
    )
    print(orders_prepared.show(5))

    logger.info(f"orders_prepared: {orders_prepared.count()} rows")

    order_items_filtered = order_items_df.join(
        orders_prepared.select("order_id", "user_id"),
        on="order_id",
        how="inner"
    )
    logger.info(f"order_items_filtered: {order_items_filtered.count()} rows")

    enriched = (
        order_items_filtered
        .join(items_df.select("item_id", "item_title", "item_category"), "item_id", "left")
        .join(
            orders_prepared.select(
                "order_id", "store_id", "store_address", "city",
                "year", "month", "day", "week", "order_date"
            ),
            "order_id",
            "left"
        )
        .withColumn("item_quantity", F.col("item_quantity").cast("int"))
        .withColumn("item_canceled_quantity", F.coalesce(F.col("item_canceled_quantity").cast("int"), F.lit(0)))
        .withColumn("item_price", F.col("item_price").cast("decimal(12,2)"))
        .withColumn("item_discount", F.coalesce(F.col("item_discount").cast("decimal(12,2)"), F.lit(0.00)))
    )

    group_cols_day = [
        "year", "month", "day", "week",
        "city", "store_id", "store_address",
        "item_category", "item_id", "item_title",
    ]

    daily = (
        enriched.groupBy(*group_cols_day).agg(
            F.sum(
                (F.col("item_quantity") - F.col("item_canceled_quantity")) *
                (F.col("item_price") - F.col("item_discount"))
            ).cast("decimal(15,2)").alias("item_revenue"),
            F.sum("item_quantity").cast("int").alias("ordered_quantity"),
            F.sum("item_canceled_quantity").cast("int").alias("canceled_quantity"),
            F.countDistinct("order_id").cast("int").alias("orders_with_item"),
            F.sum(F.when(F.col("item_canceled_quantity") > 0, 1).otherwise(0)).cast("int").alias("orders_with_cancellation"),
        )
        .withColumn("net_quantity", (F.col("ordered_quantity") - F.col("canceled_quantity")).cast("int"))
        .withColumn(
            "cancellation_rate",
            F.when(
                F.col("ordered_quantity") > 0,
                F.round(F.col("canceled_quantity") / F.col("ordered_quantity") * 100, 2)
            ).otherwise(F.lit(0.0)).cast("decimal(5,2)")
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("orders_with_item") > 0,
                F.round(F.col("item_revenue") / F.col("orders_with_item"), 2)
            ).otherwise(F.lit(0.0)).cast("decimal(10,2)")
        )
    )

    w_day_desc = Window.partitionBy("year", "month", "day", "city", "store_id").orderBy(F.col("item_revenue").desc())
    w_day_asc = Window.partitionBy("year", "month", "day", "city", "store_id").orderBy(F.col("item_revenue").asc())

    daily = (
        daily
        .withColumn("daily_rank", F.row_number().over(w_day_desc).cast("int"))
        .withColumn("daily_rank_asc", F.row_number().over(w_day_asc).cast("int"))
        .withColumn("most_popular_product_daily", (F.col("daily_rank") == 1).cast("boolean"))
        .withColumn("least_popular_product_daily", (F.col("daily_rank_asc") == 1).cast("boolean"))
        .drop("daily_rank_asc")
    )

    weekly_agg = (
        daily.groupBy("year", "week", "city", "store_id", "item_id")
        .agg(F.sum("item_revenue").cast("decimal(15,2)").alias("weekly_revenue"))
    )

    w_week_desc = Window.partitionBy("year", "week", "city", "store_id").orderBy(F.col("weekly_revenue").desc())
    w_week_asc = Window.partitionBy("year", "week", "city", "store_id").orderBy(F.col("weekly_revenue").asc())

    weekly_ranked = (
        weekly_agg
        .withColumn("weekly_rank", F.row_number().over(w_week_desc).cast("int"))
        .withColumn("weekly_rank_asc", F.row_number().over(w_week_asc).cast("int"))
        .withColumn("most_popular_product_weekly", (F.col("weekly_rank") == 1).cast("boolean"))
        .withColumn("least_popular_product_weekly", (F.col("weekly_rank_asc") == 1).cast("boolean"))
        .drop("weekly_rank_asc", "weekly_revenue")
    )

    monthly_agg = (
        daily.groupBy("year", "month", "city", "store_id", "item_id")
        .agg(F.sum("item_revenue").cast("decimal(15,2)").alias("monthly_revenue"))
    )

    w_month_desc = Window.partitionBy("year", "month", "city", "store_id").orderBy(F.col("monthly_revenue").desc())
    w_month_asc = Window.partitionBy("year", "month", "city", "store_id").orderBy(F.col("monthly_revenue").asc())

    monthly_ranked = (
        monthly_agg
        .withColumn("monthly_rank", F.row_number().over(w_month_desc).cast("int"))
        .withColumn("monthly_rank_asc", F.row_number().over(w_month_asc).cast("int"))
        .withColumn("most_popular_product_monthly", (F.col("monthly_rank") == 1).cast("boolean"))
        .withColumn("least_popular_product_monthly", (F.col("monthly_rank_asc") == 1).cast("boolean"))
        .drop("monthly_rank_asc", "monthly_revenue")
    )

    result = (
        daily
        .join(weekly_ranked, on=["year", "week", "city", "store_id", "item_id"], how="left")
        .join(monthly_ranked, on=["year", "month", "city", "store_id", "item_id"], how="left")
        .withColumn("load_date", exec_date.cast("date"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            F.col("day").cast("int"),
            F.col("city"),
            F.col("store_id").cast("int"),
            F.col("store_address"),
            F.col("item_category"),
            F.col("item_id").cast("int"),
            F.col("item_title"),
            F.col("item_revenue").cast("decimal(15,2)"),
            F.col("ordered_quantity").cast("int"),
            F.col("canceled_quantity").cast("int"),
            F.col("orders_with_item").cast("int"),
            F.col("orders_with_cancellation").cast("int"),
            F.col("net_quantity").cast("int"),
            F.col("most_popular_product_daily").cast("boolean"),
            F.col("least_popular_product_daily").cast("boolean"),
            F.col("most_popular_product_weekly").cast("boolean"),
            F.col("least_popular_product_weekly").cast("boolean"),
            F.col("most_popular_product_monthly").cast("boolean"),
            F.col("least_popular_product_monthly").cast("boolean"),
            F.col("daily_rank").cast("int"),
            F.col("weekly_rank").cast("int"),
            F.col("monthly_rank").cast("int"),
            F.col("cancellation_rate").cast("decimal(5,2)"),
            F.col("average_order_value").cast("decimal(10,2)"),
            F.col("load_date").cast("date"),
            F.col("created_at"),
            F.col("updated_at"),
        )
    )

    logger.info(f"product mart rows: {result.count()}")
    return result


def run_product_mode(process_date: str):
    logger = logging.getLogger(__name__)
    spark = create_spark_session("Product Performance Data Mart")
    try:
        users_df, drivers_df, stores_df, items_df, orders_df, order_items_df = read_product_sources(spark)
        df = build_product_mart(users_df, drivers_df, stores_df, items_df, orders_df, order_items_df, process_date)
        logger.info("Сохранение product mart в PostgreSQL...")
        write_pg_append(df, "dwh.product_performance_data_mart")
        logger.info("Product mart успешно создана и сохранена.")
    finally:
        spark.stop()


def run_order_mode(process_date: str):
    logger = logging.getLogger(__name__)
    spark = create_spark_session("Order Performance Data Mart")
    try:
        p_date = F.to_date(F.lit(process_date))

        # Читаем заказы за указанную дату
        orders = (
            read_pg(spark, "dwh.orders")
            .select(
                F.col("order_id").cast("long").alias("order_id"),
                F.col("store_id").cast("long").alias("store_id"),
                F.col("user_id").cast("long").alias("user_id"),
                F.col("driver_id").cast("long").alias("driver_id"),
                F.col("order_cancellation_reason").alias("order_cancellation_reason"),
                F.col("created_at").alias("created_at"),
                F.col("paid_at").alias("paid_at"),
                F.col("delivery_started_at").alias("delivery_started_at"),
                F.col("delivered_at").alias("delivered_at"),
                F.col("canceled_at").alias("canceled_at"),
                F.coalesce(F.col("delivery_cost"), F.lit(0)).cast("decimal(18,2)").alias("delivery_cost"),
                F.coalesce(F.col("order_discount"), F.lit(0)).cast("decimal(18,2)").alias("order_discount"),
            )
            .withColumn("order_dt", F.to_date("created_at"))
            .filter(F.col("order_dt") == p_date)
            .withColumn("is_paid", F.col("paid_at").isNotNull())
            .withColumn("is_delivered", F.col("delivered_at").isNotNull())
            .withColumn("is_canceled", F.col("canceled_at").isNotNull())
            .withColumn(
                "is_canceled_after_delivery",
                F.when(
                    (F.col("canceled_at").isNotNull()) &
                    (F.col("delivered_at").isNotNull()) &
                    (F.col("canceled_at") > F.col("delivered_at")), 1
                ).otherwise(0)
            )
            .withColumn(
                "is_service_error_cancel",
                F.when(
                    (F.col("order_cancellation_reason").isNotNull()) &
                    (
                            F.col("order_cancellation_reason").contains("Ошибка") |
                            F.col("order_cancellation_reason").contains("Проблемы с оплатой") |
                            F.col("order_cancellation_reason").contains("технические")
                    ),
                    1
                ).otherwise(0)
            )
        )

        # Читаем состав заказов
        oi = (
            read_pg(spark, "dwh.order_items")
            .select(
                F.col("order_id").cast("long").alias("order_id"),
                F.coalesce(F.col("item_quantity"), F.lit(0)).cast("long").alias("item_quantity"),
                F.coalesce(F.col("item_canceled_quantity"), F.lit(0)).cast("long").alias("item_canceled_quantity"),
                F.coalesce(F.col("item_price"), F.lit(0)).cast("decimal(18,2)").alias("item_price"),
                F.coalesce(F.col("item_discount"), F.lit(0)).cast("decimal(18,2)").alias("item_discount"),
            )
        )

        # Читаем магазины
        stores = (
            read_pg(spark, "dwh.stores")
            .select(
                F.col("store_id").cast("long").alias("store_id"),
                F.col("store_address").alias("store"),
            )
            .withColumn(
                "city",
                F.when(
                    F.col("store").isNotNull(),
                    F.regexp_extract(F.col("store"), r",\s*([^,]+?)\s*,", 1)
                ).otherwise(F.lit("Unknown"))
            )
        )

        # Агрегация по заказам (товары)
        items_by_order = (
            oi.join(orders.select("order_id"), on="order_id", how="inner")
            .groupBy("order_id")
            .agg(
                F.sum("item_quantity").cast("long").alias("qty"),
                F.sum("item_canceled_quantity").cast("long").alias("canceled_qty"),
                F.sum(
                    (F.col("item_price") * F.col("item_quantity")).cast("decimal(18,2)")
                ).cast("decimal(18,2)").alias("gross_amount"),
                F.sum(
                    (F.col("item_discount") * F.col("item_quantity")).cast("decimal(18,2)")
                ).cast("decimal(18,2)").alias("item_discount_amount"),
            )
        )

        # Агрегация по пользователям (уникальные клиенты)
        users_by_store = (
            orders.filter(F.col("is_paid"))
            .groupBy("store_id")
            .agg(F.countDistinct("user_id").cast("int").alias("unique_customers"))
        )

        # Соединяем все данные
        joined = (
            orders
            .join(items_by_order, on="order_id", how="left")
            .fillna({
                "qty": 0,
                "canceled_qty": 0,
                "gross_amount": 0,
                "item_discount_amount": 0
            })
            .withColumn("gross_amount", F.coalesce(F.col("gross_amount"), F.lit(0)).cast("decimal(18,2)"))
            .withColumn("item_discount_amount",
                        F.coalesce(F.col("item_discount_amount"), F.lit(0)).cast("decimal(18,2)"))

            # Рассчитываем дополнительные метрики на уровне заказа
            .withColumn(
                "turnover",  # Оборот (сумма заказа без скидок + доставка)
                (F.col("gross_amount") + F.col("delivery_cost")).cast("decimal(18,2)")
            )
            .withColumn(
                "revenue",  # Выручка (сумма оплаты с учетом скидок)
                (
                        F.col("gross_amount") -
                        F.col("item_discount_amount") -
                        F.col("order_discount") +
                        F.col("delivery_cost")
                ).cast("decimal(18,2)")
            )
        )

        # Группируем по дате, магазину и городу
        agg = (
            joined
            .join(stores.select("store_id", "city", "store"), on="store_id", how="left")
            .groupBy("order_dt", "store_id", "city", "store")
            .agg(
                # Основные метрики количества
                F.count(F.lit(1)).cast("int").alias("orders_total"),
                F.sum(F.when(F.col("is_paid"), 1).otherwise(0)).cast("int").alias("orders_paid"),
                F.sum(F.when(F.col("is_delivered"), 1).otherwise(0)).cast("int").alias("orders_delivered"),
                F.sum(F.when(F.col("is_canceled"), 1).otherwise(0)).cast("int").alias("orders_canceled"),

                # Дополнительные метрики отмен
                F.sum(F.col("is_canceled_after_delivery")).cast("int").alias("cancels_after_delivery"),
                F.sum(F.col("is_service_error_cancel")).cast("int").alias("cancels_service_error"),

                # Финансовые метрики
                F.sum("turnover").cast("decimal(18,2)").alias("turnover"),
                F.sum("revenue").cast("decimal(18,2)").alias("revenue"),
                F.sum("gross_amount").cast("decimal(18,2)").alias("gross_items_amount"),
                F.sum("item_discount_amount").cast("decimal(18,2)").alias("items_discount_amount"),
                F.sum("order_discount").cast("decimal(18,2)").alias("order_discount_amount"),
                F.sum("delivery_cost").cast("decimal(18,2)").alias("delivery_amount"),

                # Метрики товаров
                F.sum("qty").cast("long").alias("items_qty"),
                F.sum("canceled_qty").cast("long").alias("canceled_items_qty"),
            )
            .join(users_by_store, on="store_id", how="left")
            .fillna({
                "unique_customers": 0,
            })

            # Рассчитываем производные метрики
            .withColumn(
                "avg_order_value",
                F.when(
                    F.col("orders_paid") > 0,
                    (F.col("revenue") / F.col("orders_paid"))
                ).otherwise(F.lit(0)).cast("decimal(18,2)")
            )
            .withColumn(
                "orders_per_customer",
                F.when(
                    F.col("unique_customers") > 0,
                    (F.col("orders_paid").cast("float") / F.col("unique_customers"))
                ).otherwise(F.lit(0)).cast("decimal(10,2)")
            )
            .withColumn(
                "revenue_per_customer",
                F.when(
                    F.col("unique_customers") > 0,
                    (F.col("revenue") / F.col("unique_customers"))
                ).otherwise(F.lit(0)).cast("decimal(18,2)")
            )
            .withColumn(
                "cancellation_rate",
                F.when(
                    F.col("orders_total") > 0,
                    (F.col("orders_canceled").cast("double") / F.col("orders_total"))
                ).otherwise(F.lit(0)).cast("decimal(6,4)")
            )
            .withColumn("year", F.year("order_dt").cast("int"))
            .withColumn("month", F.month("order_dt").cast("int"))
            .withColumn("day", F.dayofmonth("order_dt").cast("int"))
            .withColumn("load_date", F.lit(process_date).cast("date"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())

            # Добавляем недостающие поля со значениями по умолчанию
            .withColumn("profit", F.lit(0).cast("decimal(18,2)"))  # Нет данных о расходах
            .withColumn("total_expenses", F.lit(0).cast("decimal(18,2)"))  # Нет данных о расходах
            .withColumn("courier_changes_count", F.lit(0).cast("int"))  # Нет данных о сменах курьеров
            .withColumn("active_couriers_count", F.lit(0).cast("int"))  # Можно рассчитать, если нужно
            .select(
                # Размерность времени
                "year", "month", "day",
                # География
                "city", "store_id", "store",
                # Основные метрики
                "orders_total", "orders_paid", "orders_delivered", "orders_canceled",
                "cancels_after_delivery", "cancels_service_error",
                # Финансовые метрики
                "turnover", "revenue", "profit",
                "gross_items_amount", "items_discount_amount", "order_discount_amount",
                "delivery_amount", "total_expenses",
                # Метрики товаров
                "items_qty", "canceled_items_qty",
                # Метрики клиентов
                "unique_customers", "avg_order_value",
                "orders_per_customer", "revenue_per_customer",
                # Метрики курьеров
                "courier_changes_count", "active_couriers_count",
                # Процентные метрики
                "cancellation_rate",
                # Технические поля
                "load_date", "created_at", "updated_at"
            )
        )

        # Добавляем расчет активных курьеров, если есть данные
        try:
            # Пытаемся рассчитать активных курьеров
            active_couriers = (
                orders.filter(F.col("driver_id").isNotNull())
                .groupBy("store_id")
                .agg(F.countDistinct("driver_id").cast("int").alias("courier_count"))
            )

            agg = agg.join(
                active_couriers.select("store_id", F.col("courier_count").alias("active_couriers_count")),
                on="store_id",
                how="left"
            ).fillna({"active_couriers_count": 0})

        except Exception as e:
            logger.warning(f"Не удалось рассчитать активных курьеров: {e}")
            # Оставляем значение по умолчанию 0

        # Сохраняем результат
        write_pg_append(agg, "dwh.order_performance_data_mart")
        logger.info(f"Order mart успешно создана и сохранена. Обработано {agg.count()} записей.")

    except Exception as e:
        logger.error(f"Ошибка при создании order mart: {e}")
        raise
    finally:
        spark.stop()



def main():
    logger = setup_logging()

    process_date =  '2025-12-10'
    mode = sys.argv[2].strip().lower() if len(sys.argv) > 2 else "product"

    logger.info(f"Дата выполнения: {process_date}, режим: {mode}")

    if mode == "order":
        run_order_mode(process_date)
    else:
        run_product_mode(process_date)


if __name__ == "__main__":
    main()
