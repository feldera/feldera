-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE OR REPLACE TEMP VIEW metrics_analysis_v2 AS SELECT product_id, price_usd, rating, CASE WHEN rating > 4.0 THEN 'high' WHEN rating > 3.0 THEN 'medium' ELSE 'low' END as quality_tier FROM product_metrics WHERE price_usd > 10.0;
