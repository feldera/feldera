-- rule: ln_domain
-- spark: LN(x) where x <= 0 — Spark returns NULL for LN(0) and LN(negative); Feldera returns -Infinity for LN(0) and panics for LN(negative)
-- feldera: UNSUPPORTED — Feldera behavior diverges from Spark for non-positive inputs. Mark unsupported if input column may contain 0 or negative values.
CREATE OR REPLACE TEMP VIEW price_ln_v1 AS SELECT id, product_name, price, LN(price) AS log_price FROM price_log;
