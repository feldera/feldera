-- rule: ln_domain
-- spark: LN(x) where x <= 0 — Spark returns NULL for LN(0) and LN(negative); Feldera returns -Infinity for LN(0) and panics for LN(negative)
-- feldera: UNSUPPORTED — Feldera behavior diverges from Spark for non-positive inputs. Mark unsupported if input column may contain 0 or negative values.
CREATE OR REPLACE TEMP VIEW radiation_ln_v3 AS SELECT sample_id, element, intensity, LN(intensity) AS log_intensity FROM radiation_intensity;
