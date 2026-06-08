CREATE TABLE subscriptions (
  subscription_id BIGINT,
  customer_id BIGINT,
  plan_name STRING,
  is_trial BOOLEAN,
  monthly_price DECIMAL(12,2),
  started_at DATE,
  cancelled_at DATE
) USING parquet;
