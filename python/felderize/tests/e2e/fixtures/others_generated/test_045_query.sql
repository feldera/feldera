CREATE OR REPLACE TEMP VIEW bm62_unique_buyers_by_brand AS
SELECT DISTINCT b.brand_name, o.customer_id FROM brand_orders o JOIN brands b ON o.brand_id = b.brand_id;
