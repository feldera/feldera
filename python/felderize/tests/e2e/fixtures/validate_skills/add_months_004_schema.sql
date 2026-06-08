-- rule: add_months
-- spark: add_months(date, n) — unquoted literal form
-- feldera: date + INTERVAL n MONTH (unquoted integer literal, equivalent to INTERVAL 'n' MONTH)
CREATE TABLE events (id INT, event_date DATE);
