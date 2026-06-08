CREATE TABLE customer_accounts (customer_id BIGINT, customer_name STRING, status STRING) USING parquet;

CREATE TABLE support_cases (case_id BIGINT, customer_id BIGINT, case_status STRING) USING parquet;
