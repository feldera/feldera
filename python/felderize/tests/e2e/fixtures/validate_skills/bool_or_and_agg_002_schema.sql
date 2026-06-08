-- rule: bool_or_and_agg
-- spark: bool_or(col) — true if any value is true; bool_and(col) — true if all values are true
-- feldera: bool_or(col) / bool_and(col) — both work identically in Feldera as aggregates (not window); no translation needed
CREATE TABLE device_status (
  device_id INT,
  is_online BOOLEAN,
  department STRING
);
