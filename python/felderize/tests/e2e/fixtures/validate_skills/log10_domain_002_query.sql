-- rule: log10_domain
-- spark: LOG10(x) where x <= 0 — Spark returns NULL for LOG10(0) and LOG10(negative); Feldera returns -Infinity for LOG10(0) and panics for LOG10(negative)
-- feldera: UNSUPPORTED — same domain issue as LN. Mark unsupported if input may contain 0 or negative values.
CREATE OR REPLACE TEMP VIEW voltage_log_v2 AS SELECT reading_id, voltage, LOG10(voltage) AS voltage_log FROM voltage_data WHERE voltage > 0;
