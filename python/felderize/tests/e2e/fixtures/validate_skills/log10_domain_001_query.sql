-- rule: log10_domain
-- spark: LOG10(x) where x <= 0 — Spark returns NULL for LOG10(0) and LOG10(negative); Feldera returns -Infinity for LOG10(0) and panics for LOG10(negative)
-- feldera: UNSUPPORTED — same domain issue as LN. Mark unsupported if input may contain 0 or negative values.
CREATE OR REPLACE TEMP VIEW sensor_log_v1 AS SELECT id, LOG10(temperature) AS log_temp FROM sensor_readings;
