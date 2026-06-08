CREATE OR REPLACE TEMP VIEW bm61_avg_unit_revenue AS
SELECT AVG(amount / quantity) AS avg_unit_revenue FROM line_items WHERE quantity > 0;
