CREATE TABLE crm_profiles (
  profile_id BIGINT,
  email STRING,
  phone STRING,
  country STRING
) USING parquet;

CREATE TABLE identity_events (
  event_id BIGINT,
  email STRING,
  phone STRING,
  observed_at TIMESTAMP
) USING parquet;
