CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.raw_data (
  order_id BIGINT,
  user_id BIGINT,
  user_phone TEXT,
  address_text TEXT,
  created_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  delivery_started_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  canceled_at TIMESTAMPTZ,
  payment_type TEXT,

  item_id BIGINT,
  item_title TEXT,
  item_category TEXT,
  item_quantity INT,
  item_price NUMERIC(12,2),
  item_canceled_quantity INT,
  item_replaced_id BIGINT,

  order_discount NUMERIC(12,2),
  item_discount NUMERIC(12,2),
  order_cancellation_reason TEXT,

  driver_id BIGINT,
  driver_phone TEXT,
  delivery_cost NUMERIC(12,2),

  store_id BIGINT,
  store_address TEXT
);

CREATE TABLE IF NOT EXISTS dwh.users (
  user_id BIGINT PRIMARY KEY,
  user_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.drivers (
  driver_id BIGINT PRIMARY KEY,
  driver_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.stores (
  store_id BIGINT PRIMARY KEY,
  store_address TEXT
);

CREATE TABLE IF NOT EXISTS dwh.items (
  item_id BIGINT PRIMARY KEY,
  item_title TEXT,
  item_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.orders (
  order_id BIGINT PRIMARY KEY,
  user_id BIGINT,
  store_id BIGINT,
  driver_id BIGINT,
  address_text TEXT,
  created_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  delivery_started_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  canceled_at TIMESTAMPTZ,
  payment_type TEXT,
  delivery_cost NUMERIC(12,2),
  order_discount NUMERIC(12,2),
  order_cancellation_reason TEXT,
  CONSTRAINT fk_orders_user   FOREIGN KEY (user_id)  REFERENCES dwh.users(user_id),
  CONSTRAINT fk_orders_store  FOREIGN KEY (store_id) REFERENCES dwh.stores(store_id),
  CONSTRAINT fk_orders_driver FOREIGN KEY (driver_id) REFERENCES dwh.drivers(driver_id)
);

CREATE TABLE IF NOT EXISTS dwh.order_items (
  order_id BIGINT,
  item_id BIGINT,
  item_quantity INT,
  item_price NUMERIC(12,2),
  item_discount NUMERIC(12,2),
  item_canceled_quantity INT,
  item_replaced_id BIGINT,
  PRIMARY KEY (order_id, item_id),
  CONSTRAINT fk_order_items_order    FOREIGN KEY (order_id) REFERENCES dwh.orders(order_id),
  CONSTRAINT fk_order_items_item     FOREIGN KEY (item_id) REFERENCES dwh.items(item_id),
  CONSTRAINT fk_order_items_replaced FOREIGN KEY (item_replaced_id) REFERENCES dwh.items(item_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON dwh.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON dwh.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_store_id   ON dwh.orders(store_id);
CREATE INDEX IF NOT EXISTS idx_orders_driver_id  ON dwh.orders(driver_id);
CREATE INDEX IF NOT EXISTS idx_order_items_item_id  ON dwh.order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON dwh.order_items(order_id);


CREATE TABLE IF NOT EXISTS dwh.product_performance_data_mart (
    -- Record identifier
    id BIGSERIAL PRIMARY KEY,

    -- Time dimensions (from your groupBy)
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),

    city VARCHAR(255),
    store_id INTEGER,
    store_address VARCHAR(500),
    item_category VARCHAR(255),
    item_id INTEGER,
    item_title VARCHAR(500) NOT NULL,

    item_revenue DECIMAL(15,2) DEFAULT 0.00,
    ordered_quantity INTEGER DEFAULT 0,
    canceled_quantity INTEGER DEFAULT 0,
    orders_with_item INTEGER DEFAULT 0,
    orders_with_cancellation INTEGER DEFAULT 0,
    net_quantity INTEGER DEFAULT 0,
    most_popular_product_daily BOOLEAN DEFAULT FALSE,
    least_popular_product_daily BOOLEAN DEFAULT FALSE,

    most_popular_product_weekly BOOLEAN DEFAULT FALSE,
    least_popular_product_weekly BOOLEAN DEFAULT FALSE,

    most_popular_product_monthly BOOLEAN DEFAULT FALSE,
    least_popular_product_monthly BOOLEAN DEFAULT FALSE,

    daily_rank INTEGER,
    weekly_rank INTEGER,
    monthly_rank INTEGER,

    -- Additional calculated metrics
    cancellation_rate DECIMAL(5,2),  -- canceled_quantity / ordered_quantity * 100
    average_order_value DECIMAL(10,2),  -- item_revenue / orders_with_item

    -- Technical fields
    load_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT positive_metrics CHECK (
        item_revenue >= 0 AND
        ordered_quantity >= 0 AND
        canceled_quantity >= 0 AND
        orders_with_item >= 0 AND
        orders_with_cancellation >= 0 AND
        net_quantity >= 0
    ),

    CONSTRAINT valid_cancellation CHECK (
        canceled_quantity <= ordered_quantity
    ),

    CONSTRAINT unique_daily_product_performance UNIQUE (
        year,
        month,
        day,
        city,
        store_id,
        item_id,
        load_date
    )
);
CREATE TABLE IF NOT EXISTS dwh.order_performance_data_mart (
  -- Размерность времени
  year                  int,
  month                 int,
  day                   int,

  city                  text,
  store_id              bigint,
  store                 text,
  orders_total          int,
  orders_paid           int,
  orders_delivered      int,
  orders_canceled       int,
  cancels_after_delivery int,
  cancels_service_error  int,
  turnover              numeric(18,2),
  revenue               numeric(18,2),
  profit                numeric(18,2),
  gross_items_amount    numeric(18,2),
  items_discount_amount numeric(18,2),
  order_discount_amount numeric(18,2),
  delivery_amount       numeric(18,2),
  total_expenses        numeric(18,2),

  items_qty             bigint,
  canceled_items_qty    bigint,
  unique_customers      int,
  avg_order_value       numeric(18,2),
  orders_per_customer   numeric(10,2),
  revenue_per_customer  numeric(18,2),

  courier_changes_count int,
  active_couriers_count int,
  cancellation_rate     numeric(6,4),
  load_date             date,
  created_at            timestamp without time zone NOT NULL DEFAULT now(),
  updated_at            timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT order_performance_data_mart_pkey PRIMARY KEY (year, month, day, store_id)
);

CREATE INDEX IF NOT EXISTS idx_order_mart_load_date
  ON dwh.order_performance_data_mart(load_date);
