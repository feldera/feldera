-- rule: current_timestamp_now
-- spark: CURRENT_TIMESTAMP — current timestamp
-- feldera: NOW()
CREATE TABLE transaction_log_t3 (txn_id BIGINT, amount DECIMAL(10,2), status STRING, txn_time TIMESTAMP);
