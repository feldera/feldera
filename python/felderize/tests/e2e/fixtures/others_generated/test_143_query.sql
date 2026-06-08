CREATE OR REPLACE TEMP VIEW gpt143_unpivot AS
SELECT product_id, product_name, quarter, revenue
FROM quarterly_sales
UNPIVOT (revenue FOR quarter IN (q1_revenue, q2_revenue, q3_revenue, q4_revenue));
