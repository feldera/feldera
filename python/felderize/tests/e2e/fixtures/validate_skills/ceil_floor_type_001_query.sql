-- rule: ceil_floor_type
-- spark: CEIL(x) / FLOOR(x) on DOUBLE — Spark returns BIGINT; Feldera returns DOUBLE
-- feldera: CEIL(x) / FLOOR(x) — same function, add warning about return type difference
CREATE OR REPLACE TEMP VIEW price_ceiling AS SELECT product_id, CEIL(unit_price) AS ceiling_price, FLOOR(unit_price) AS floor_price FROM price_data;
