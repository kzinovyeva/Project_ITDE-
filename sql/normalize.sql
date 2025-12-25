INSERT INTO dwh.users (user_id, user_phone)
SELECT DISTINCT user_id, user_phone
FROM dwh.raw_data
WHERE user_id IS NOT NULL
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO dwh.drivers (driver_id, driver_phone)
SELECT DISTINCT driver_id, driver_phone
FROM dwh.raw_data
WHERE driver_id IS NOT NULL
ON CONFLICT (driver_id) DO NOTHING;

INSERT INTO dwh.stores (store_id, store_address)
SELECT DISTINCT store_id, store_address
FROM dwh.raw_data
WHERE store_id IS NOT NULL
ON CONFLICT (store_id) DO NOTHING;

INSERT INTO dwh.items (item_id, item_title, item_category)
SELECT DISTINCT item_id, item_title, item_category
FROM dwh.raw_data
WHERE item_id IS NOT NULL
ON CONFLICT (item_id) DO NOTHING;

INSERT INTO dwh.items (item_id, item_title, item_category)
SELECT DISTINCT item_replaced_id, 'Unknown Replacement Item', 'Unknown'
FROM dwh.raw_data
WHERE item_replaced_id IS NOT NULL
ON CONFLICT (item_id) DO NOTHING;

INSERT INTO dwh.orders (
  order_id, user_id, store_id, driver_id, address_text,
  created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
  payment_type, delivery_cost, order_discount, order_cancellation_reason
)
SELECT DISTINCT
  order_id, user_id, store_id, driver_id, address_text,
  created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
  payment_type, delivery_cost, order_discount, order_cancellation_reason
FROM dwh.raw_data
WHERE order_id IS NOT NULL
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO dwh.order_items (
  order_id, item_id, item_quantity, item_price,
  item_discount, item_canceled_quantity, item_replaced_id
)
SELECT DISTINCT
  order_id, item_id, item_quantity, item_price,
  item_discount, item_canceled_quantity, item_replaced_id
FROM dwh.raw_data
WHERE order_id IS NOT NULL AND item_id IS NOT NULL
ON CONFLICT (order_id, item_id) DO NOTHING;
