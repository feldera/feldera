-- rule: concat_concat_ws
-- spark: CONCAT(s1, s2, ...) — concatenate strings; CONCAT_WS(sep, s1, s2, ...) — concatenate with separator
-- feldera: CONCAT(s1, s2, ...) / CONCAT_WS(sep, s1, s2, ...) — both work identically in Feldera, no translation needed
CREATE TABLE event_logs (
  event_id INT,
  year INT,
  month INT,
  day INT,
  action STRING,
  status STRING
);
