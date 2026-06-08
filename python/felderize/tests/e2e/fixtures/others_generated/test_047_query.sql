CREATE OR REPLACE TEMP VIEW bm65_financial_rollup AS
SELECT dept, team, expense_type, SUM(amount) AS total_amount
FROM financial_entries GROUP BY ROLLUP(dept, team, expense_type)
ORDER BY dept, team, expense_type;
