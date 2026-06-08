CREATE TABLE daily_active_users (
  activity_date DATE,
  user_id BIGINT
) USING parquet;

CREATE TABLE paid_users (
  user_id BIGINT,
  plan_name STRING,
  effective_date DATE
) USING parquet;
