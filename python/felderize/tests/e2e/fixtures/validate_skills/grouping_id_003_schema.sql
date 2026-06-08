-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE TABLE order_events_3 (customer_id INT, order_date DATE, category STRING, amount DECIMAL(10,2));
