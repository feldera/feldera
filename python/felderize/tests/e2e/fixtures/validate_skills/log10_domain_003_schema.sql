-- rule: log10_domain
-- spark: LOG10(x) where x <= 0 — Spark returns NULL for LOG10(0) and LOG10(negative); Feldera returns -Infinity for LOG10(0) and panics for LOG10(negative)
-- feldera: UNSUPPORTED — same domain issue as LN. Mark unsupported if input may contain 0 or negative values.
CREATE TABLE intensity_measurements (measurement_id INT, signal_strength DOUBLE);
