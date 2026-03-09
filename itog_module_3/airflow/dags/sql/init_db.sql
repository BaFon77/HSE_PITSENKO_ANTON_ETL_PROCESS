-- 1. Таблица ORDERS (Партиционированная)
DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders
(
    order_id      TEXT,
    customer_id   TEXT,
    restaurant_id TEXT,
    driver_id     TEXT,
    order_amount  NUMERIC,
    status        TEXT,
    order_time    TIMESTAMP,
    PRIMARY KEY (order_id, order_time)
) PARTITION BY RANGE (order_time);

CREATE TABLE orders_2024 PARTITION OF orders FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE orders_2025 PARTITION OF orders FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE orders_2026 PARTITION OF orders FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE orders_default PARTITION OF orders DEFAULT;

-- 2. Таблица RESTAURANTS
DROP TABLE IF EXISTS restaurants CASCADE;
CREATE TABLE restaurants
(
    restaurant_id TEXT PRIMARY KEY,
    name          TEXT,
    city          TEXT,
    cuisine       TEXT,
    rating        NUMERIC,
    created_at    TIMESTAMP
);

-- 3. Таблица CUSTOMERS
DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers
(
    customer_id   TEXT PRIMARY KEY,
    name          TEXT,
    email         TEXT,
    address       TEXT,
    city          TEXT,
    registered_at TIMESTAMP
);

-- 4. Таблица DELIVERY_DRIVERS
DROP TABLE IF EXISTS delivery_drivers CASCADE;
CREATE TABLE delivery_drivers
(
    driver_id    TEXT PRIMARY KEY,
    name         TEXT,
    vehicle_type TEXT,
    rating       NUMERIC,
    active       BOOLEAN
);

-- 5. Таблица ORDERREVIEWS
DROP TABLE IF EXISTS orderreviews CASCADE;
CREATE TABLE orderreviews
(
    review_id TEXT PRIMARY KEY,
    order_id  TEXT,
    rating    INTEGER,
    comment   TEXT,
    created_at TIMESTAMP
);

-- 6. Таблица CUSTOMER_ACTIONS
DROP TABLE IF EXISTS customer_actions CASCADE;
CREATE TABLE customer_actions
(
    action_id        SERIAL PRIMARY KEY,
    customer_id      TEXT REFERENCES customers (customer_id),
    action_type      TEXT,
    action_timestamp TIMESTAMP
);