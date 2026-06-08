CREATE TABLE customer_orders (order_id BIGINT, customer_id BIGINT, order_date DATE, amount DECIMAL(12,2)) USING parquet;
