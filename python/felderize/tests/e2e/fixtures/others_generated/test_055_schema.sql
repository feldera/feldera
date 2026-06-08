CREATE TABLE system_alerts (alert_id BIGINT, severity STRING, created_at TIMESTAMP) USING parquet;

CREATE TABLE security_alerts (alert_id BIGINT, severity STRING, created_at TIMESTAMP) USING parquet;
